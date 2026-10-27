"""Notifier и отправка в Telegram (раздел 16, шаг 5.12) в транзакции теста.

- NotificationService.notify: кому и в какой канал — получатели, исключения, настройки,
  привязка Telegram, повторы и детерминированные ключи дедупа;
- send_telegram: Bot API подменён respx (FakeTelegram) — успех, дубль, 429, 403 и «chat not
  found», прочие 4xx, 5xx и сеть, нет привязки, нет токена;
- конец в конец внутри транзакции теста: API → outbox → notify → команды → письмо в Mailpit и
  sendMessage (deliver_outbox), как тесты писем шага 5.3; настоящий путь relay → Redis →
  воркер (notify, send_email, send_telegram) — тест committing.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest
from celery.worker import WorkController
from httpx import AsyncClient
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.core.resources import smtp_sender
from app.db.session import create_engine, create_session_factory
from app.events.relay import CeleryPublisher, Relay, listen_dsn
from app.models import (
    NotificationSetting,
    Organization,
    OutboxEvent,
    SentNotification,
    Team,
    TelegramLink,
    User,
)
from app.notifications.telegram import TelegramApiError, TelegramClient, TelegramUnavailable
from app.services.bulk import BulkService, ReassignOwnerRequest
from app.services.notifications import NotificationService
from app.uow import UnitOfWork
from app.workers.base import is_retryable
from app.workers.celery_app import celery_app
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    DeliverOutbox,
    FakeTaskQueue,
    FakeTelegram,
    Mailpit,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    RunJob,
    member,
    read_emails,
    tokens_for,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
WAIT_SECONDS = 15


def chat_id() -> int:
    return 2_000_000 + uuid.uuid4().int % 1_000_000_000


async def link(uow: UnitOfWork, user: User) -> int:
    chat = chat_id()
    uow.session.add(TelegramLink(user_id=user.id, chat_id=chat))
    await uow.commit()
    return chat


async def setting(uow: UnitOfWork, user: User, event_type: str, channel: str, on: bool) -> None:
    uow.session.add(
        NotificationSetting(user_id=user.id, event_type=event_type, channel=channel, enabled=on)
    )
    await uow.commit()


async def add_event(uow: UnitOfWork, org_id: uuid.UUID, event_type: str, **payload: Any) -> int:
    event = uow.outbox.add(event_type, payload, org_id=org_id)
    await uow.commit()
    return event.id


async def commands(uow: UnitOfWork, after: int) -> list[tuple[str, str, str]]:
    """Команды notifier после события after: (канал, кому — email или user_id, ключ дедупа)."""
    rows = await uow.session.execute(
        select(OutboxEvent.event_type, OutboxEvent.payload)
        .where(OutboxEvent.id > after, OutboxEvent.event_type.in_(["email.send", "telegram.send"]))
        .order_by(OutboxEvent.id)
    )
    return [
        ("email", p["to"], p["dedup_key"])
        if kind == "email.send"
        else ("telegram", p["user_id"], p["dedup_key"])
        for kind, p in rows
    ]


async def payloads(uow: UnitOfWork, after: int, kind: str) -> list[dict[str, Any]]:
    rows = await uow.session.scalars(
        select(OutboxEvent.payload)
        .where(OutboxEvent.id > after, OutboxEvent.event_type == kind)
        .order_by(OutboxEvent.id)
    )
    return [dict(row) for row in rows]


def assigned(owner: User, actor: User) -> dict[str, Any]:
    return {
        "entity_type": "contact",
        "entity_id": 7,
        "title": "Іван Петренко",
        "owner_id": str(owner.id),
        "actor_id": str(actor.id),
    }


@pytest.fixture
def notifier(uow: UnitOfWork) -> NotificationService:
    return NotificationService(uow)


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


# --- notify: record.assigned


async def test_assigned_goes_to_owner_by_both_channels(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    admin, rep = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )
    await link(uow, rep)
    event_id = await add_event(uow, org.id, "record.assigned", **assigned(rep, admin))

    assert await notifier.notify(event_id) == 2

    assert await commands(uow, event_id) == [
        ("telegram", str(rep.id), f"record.assigned:{event_id}:{rep.id}:telegram"),
        ("email", rep.email, f"record.assigned:{event_id}:{rep.id}:email"),
    ]
    [telegram] = await payloads(uow, event_id, "telegram.send")
    assert telegram["text"] == (
        f"Вам призначено контакт «Іван Петренко».\nПризначив: {admin.email}."
    )
    [email] = await payloads(uow, event_id, "email.send")
    assert (email["template"], email["user_id"]) == ("record_assigned", str(rep.id))
    assert email["context"] == {"entity": "контакт", "title": "Іван Петренко", "actor": admin.email}


async def test_assigned_respects_link_settings_and_actor(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    admin, rep = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )

    # Telegram не привязан — только письмо.
    first = await add_event(uow, org.id, "record.assigned", **assigned(rep, admin))
    await notifier.notify(first)
    assert [c[0] for c in await commands(uow, first)] == ["email"]

    # Письма выключены, Telegram привязан — только Telegram.
    await setting(uow, rep, "record.assigned", "email", False)
    await link(uow, rep)
    second = await add_event(uow, org.id, "record.assigned", **assigned(rep, admin))
    await notifier.notify(second)
    assert [c[0] for c in await commands(uow, second)] == ["telegram"]

    # Себе не пишем: автор изменения — он же ответственный.
    own = await add_event(uow, org.id, "record.assigned", **assigned(rep, rep))
    assert await notifier.notify(own) == 0


async def test_inactive_and_user_without_org_get_nothing(
    uow: UnitOfWork,
    notifier: NotificationService,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
) -> None:
    admin = await member(make_user, org, "org_admin")
    gone = await member(make_user, org, "sales_rep", is_active=False)
    root = await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    stranger = await member(make_user, await make_org(), "sales_rep")  # другая организация
    await link(uow, gone)

    for user in (gone, root, stranger):
        event_id = await add_event(uow, org.id, "record.assigned", **assigned(user, admin))
        assert await notifier.notify(event_id) == 0, user.email


async def test_repeated_notify_repeats_keys_and_senders_skip(
    uow: UnitOfWork,
    make_user: MakeUser,
    org: OrgFixture,
    telegram_api: FakeTelegram,
    telegram_client: TelegramClient,
    mailpit: Mailpit,
    deliver_outbox: DeliverOutbox,
) -> None:
    admin, rep = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )
    rep_id, rep_email = rep.id, rep.email  # отправители откатывают: объекты «протухнут»
    chat = await link(uow, rep)
    event_id = await add_event(uow, org.id, "record.assigned", **assigned(rep, admin))
    notifier = NotificationService(uow)

    await notifier.notify(event_id)
    await notifier.notify(event_id)  # повторная доставка notify (relay опубликовал дважды)
    await deliver_outbox()  # и третий раз — сама доставка события

    written = await commands(uow, event_id)
    assert len(written) == 6
    keys = {key for _, _, key in written}
    assert keys == {
        f"record.assigned:{event_id}:{rep_id}:telegram",
        f"record.assigned:{event_id}:{rep_id}:email",
    }  # те же ключи дедупа — отправители повторы пропустили
    assert len(telegram_api.texts(chat)) == 1
    assert len(await read_emails(mailpit, rep_email)) == 1
    sent = await uow.session.scalar(
        select(func.count()).where(SentNotification.dedup_key.in_(keys))
    )
    assert sent == 2


async def test_missing_event_is_skipped(notifier: NotificationService) -> None:
    assert await notifier.notify(2**62) == 0


# --- notify: deal.closed


def closed(owner: User, actor: User, status: str = "won") -> dict[str, Any]:
    return {
        "deal_id": 11,
        "title": "Ліцензії",
        "status": status,
        "amount_original": "2500.50",
        "currency_original": "EUR",
        "owner_id": str(owner.id),
        "team_id": str(owner.team_id),
        "actor_id": str(actor.id),
    }


async def test_closed_goes_to_owner_and_active_leads_of_his_team(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    rep = await member(make_user, org, "sales_rep")
    lead = await member(make_user, org, "team_lead")
    retired = await member(make_user, org, "team_lead", is_active=False)
    other_lead = await member(make_user, org, "team_lead", team=1)  # другой отдел
    admin = await member(make_user, org, "org_admin")
    for user in (rep, lead, retired, other_lead):
        await link(uow, user)
    event_id = await add_event(uow, org.id, "deal.closed", **closed(rep, admin))

    await notifier.notify(event_id)

    # Email у deal.closed по умолчанию выключен — только Telegram.
    assert [(c[0], c[1]) for c in await commands(uow, event_id)] == [
        ("telegram", str(rep.id)),
        ("telegram", str(lead.id)),
    ]
    [text, _] = [p["text"] for p in await payloads(uow, event_id, "telegram.send")]
    assert text == (
        "Угоду «Ліцензії» закрито: виграно.\nСума: 2500.50 EUR.\n"
        f"Відповідальний: {rep.email}. Закрив: {admin.email}."
    )


async def test_lead_who_closed_is_excluded_and_email_can_be_enabled(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    rep, lead = await member(make_user, org, "sales_rep"), await member(make_user, org, "team_lead")
    await setting(uow, rep, "deal.closed", "email", True)
    event_id = await add_event(uow, org.id, "deal.closed", **closed(rep, lead, "lost"))

    await notifier.notify(event_id)

    assert await commands(uow, event_id) == [
        ("email", rep.email, f"deal.closed:{event_id}:{rep.id}:email")
    ]
    [email] = await payloads(uow, event_id, "email.send")
    assert email["context"]["result"] == "програно"


async def test_lead_who_owns_the_deal_gets_one_message(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    lead, admin = (
        await member(make_user, org, "team_lead"),
        await member(make_user, org, "org_admin"),
    )
    await link(uow, lead)
    event_id = await add_event(uow, org.id, "deal.closed", **closed(lead, admin))

    assert await notifier.notify(event_id) == 1  # ответственный и руководитель — один человек


# --- notify: bulk_job.completed


def completed(author: User, to_user: User | None, **fields: Any) -> dict[str, Any]:
    payload = {
        "job_id": str(uuid.uuid4()),
        "kind": "reassign_owner",
        "status": "done",
        "total": 120,
        "processed": 120,
        "failed_rows": 0,
        "failed_chunks": 0,
        "rows": {"companies": 20, "contacts": 100, "deals": 0},
        "author_id": str(author.id),
        "to_user_id": str(to_user.id) if to_user else None,
    }
    return payload | fields


async def test_job_summary_to_author_and_transfer_to_receiver(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    admin, heir = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )
    await link(uow, admin)
    await link(uow, heir)
    await setting(uow, heir, "record.assigned", "email", False)  # передача — по record.assigned
    await setting(uow, heir, "bulk_job.completed", "telegram", False)  # её не касается
    payload = completed(admin, heir)
    event_id = await add_event(uow, org.id, "bulk_job.completed", **payload)
    job = f"job:{payload['job_id']}"

    await notifier.notify(event_id)

    assert await commands(uow, event_id) == [
        ("telegram", str(admin.id), f"bulk_job.completed:{job}:{admin.id}:telegram"),
        ("email", admin.email, f"bulk_job.completed:{job}:{admin.id}:email"),
        ("telegram", str(heir.id), f"bulk_job.completed:{job}:{heir.id}:telegram"),
    ]
    summary, transfer = [p["text"] for p in await payloads(uow, event_id, "telegram.send")]
    assert summary == (
        "Масову операцію «Передача клієнтів» завершено: виконано.\n"
        "Оброблено записів: 120 з 120.\nЗмінено: компанії — 20, контакти — 100.\n"
        "Помилок у рядках: 0, невдалих частин: 0."
    )
    assert transfer == (
        f"Вам передано клієнтів: 120 (компанії — 20, контакти — 100).\nПередав: {admin.email}."
    )


async def test_author_who_is_receiver_gets_only_summary(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead")
    event_id = await add_event(uow, org.id, "bulk_job.completed", **completed(lead, lead))

    await notifier.notify(event_id)

    [email] = await payloads(uow, event_id, "email.send")
    assert email["template"] == "bulk_job_completed"


async def test_import_summary_without_rows(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    # У импорта result — {created, updated}, rows в событии null: строки «Змінено» нет.
    admin = await member(make_user, org, "org_admin")
    await link(uow, admin)
    payload = completed(admin, None, kind="import_contacts", total=3, processed=3, rows=None)
    event_id = await add_event(uow, org.id, "bulk_job.completed", **payload)

    assert await notifier.notify(event_id) == 2

    [text] = [p["text"] for p in await payloads(uow, event_id, "telegram.send")]
    assert text == (
        "Масову операцію «Імпорт контактів» завершено: виконано.\n"
        "Оброблено записів: 3 з 3.\n"
        "Помилок у рядках: 0, невдалих частин: 0."
    )
    [email] = await payloads(uow, event_id, "email.send")
    assert email["context"]["rows"] == ""


async def test_nothing_transferred_gives_no_transfer_message(
    uow: UnitOfWork, notifier: NotificationService, make_user: MakeUser, org: OrgFixture
) -> None:
    admin, heir = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )
    payload = completed(admin, heir, status="cancelled", processed=0, rows={"contacts": 0})
    event_id = await add_event(uow, org.id, "bulk_job.completed", **payload)

    await notifier.notify(event_id)

    assert [c[1] for c in await commands(uow, event_id)] == [admin.email]


async def test_email_is_not_sent_to_user_disabled_after_notify(
    uow: UnitOfWork, make_user: MakeUser, org: OrgFixture, settings: Settings, mailpit: Mailpit
) -> None:
    # Письмо ждало ретрая SMTP (или перезапуска из failed_tasks), а получателя тем временем
    # отключили: данные CRM ему не уходят, ключ дедупа не занят.
    admin, rep = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )
    rep_id, rep_email = rep.id, rep.email
    event_id = await add_event(uow, org.id, "record.assigned", **assigned(rep, admin))
    await NotificationService(uow).notify(event_id)
    [command] = await uow.session.scalars(
        select(OutboxEvent.id).where(
            OutboxEvent.id > event_id, OutboxEvent.event_type == "email.send"
        )
    )
    await uow.session.execute(update(User).where(User.id == rep_id).values(is_active=False))
    await uow.commit()

    with capture_logs() as logs:
        await NotificationService(uow, smtp_sender(settings)).send_email(command)

    assert await read_emails(mailpit, rep_email) == []
    key = f"record.assigned:{event_id}:{rep_id}:email"
    sent = await uow.session.scalar(select(func.count()).where(SentNotification.dedup_key == key))
    assert sent == 0
    assert [e["user_id"] for e in logs if e["event"] == "email_recipient_inactive"] == [str(rep_id)]


# --- send_telegram


async def telegram_command(uow: UnitOfWork, user: User, text: str = "Привіт") -> int:
    event = uow.outbox.add_telegram(user.id, text, f"tests:{uuid.uuid4().hex}", org_id=user.org_id)
    await uow.commit()
    return event.id


async def sent_keys(uow: UnitOfWork, event_id: int) -> int:
    key = await uow.session.scalar(
        select(OutboxEvent.payload["dedup_key"].astext).where(OutboxEvent.id == event_id)
    )
    rows = await uow.session.scalars(
        select(SentNotification.channel).where(SentNotification.dedup_key == key)
    )
    return len([row for row in rows if row == "telegram"])


async def linked_chat(uow: UnitOfWork, user_id: uuid.UUID) -> int | None:
    found = await uow.telegram_links.get(user_id)
    return found.chat_id if found else None


@pytest.fixture
def sender(uow: UnitOfWork, telegram_client: TelegramClient) -> NotificationService:
    return NotificationService(uow, telegram=telegram_client)


async def test_send_telegram_once(
    uow: UnitOfWork,
    sender: NotificationService,
    telegram_api: FakeTelegram,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    rep = await member(make_user, org, "sales_rep")
    chat = await link(uow, rep)
    event_id = await telegram_command(uow, rep, "Угода «X» закрита")

    await sender.send_telegram(event_id)
    await sender.send_telegram(event_id)  # повторная доставка задачи

    assert telegram_api.texts(chat) == ["Угода «X» закрита"]
    [(_, _, body)] = telegram_api.sent
    assert "parse_mode" not in body
    assert body["link_preview_options"] == {"is_disabled": True}
    assert await sent_keys(uow, event_id) == 1


async def test_429_releases_key_and_retry_sends(
    uow: UnitOfWork,
    sender: NotificationService,
    telegram_api: FakeTelegram,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    rep = await member(make_user, org, "sales_rep")
    chat = await link(uow, rep)
    event_id = await telegram_command(uow, rep)
    telegram_api.responses.append(
        httpx.Response(
            429,
            json={
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests: retry after 3",
                "parameters": {"retry_after": 3},
            },
        )
    )

    with pytest.raises(TelegramApiError) as caught:
        await sender.send_telegram(event_id)

    assert (caught.value.retry_after, is_retryable(caught.value)) == (3, True)
    assert await sent_keys(uow, event_id) == 0  # откат: ретрай отправит

    await sender.send_telegram(event_id)

    assert len(telegram_api.texts(chat)) == 1
    assert await sent_keys(uow, event_id) == 1


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(
            403,
            json={
                "ok": False,
                "error_code": 403,
                "description": "Forbidden: bot was blocked by the user",
            },
        ),
        httpx.Response(
            400, json={"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}
        ),
    ],
)
async def test_blocked_bot_or_missing_chat_unlinks_without_retry(
    response: httpx.Response,
    uow: UnitOfWork,
    sender: NotificationService,
    telegram_api: FakeTelegram,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    rep, other = (
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep"),
    )
    rep_id, other_id = rep.id, other.id
    await link(uow, rep)
    other_chat = await link(uow, other)
    event_id = await telegram_command(uow, rep)
    telegram_api.responses.append(response)

    await sender.send_telegram(event_id)  # без исключения — задача не повторяется

    assert await linked_chat(uow, rep_id) is None
    assert await linked_chat(uow, other_id) == other_chat  # чужая привязка цела
    assert await sent_keys(uow, event_id) == 0


@pytest.mark.parametrize("same_chat", [False, True])
async def test_unlink_deletes_only_the_link_that_failed(
    same_chat: bool, uow: UnitOfWork, make_user: MakeUser, org: OrgFixture
) -> None:
    # WHERE user_id AND chat_id AND linked_at: пока сообщение уходило, пользователь привязал
    # новый чат или заново тот же (upsert оставил chat_id, сменил linked_at) — удаление по
    # прочитанной привязке свежую не трогает.
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    old = await link(uow, rep)
    seen = await uow.telegram_links.get(rep_id)
    assert seen is not None
    linked_at = seen.linked_at
    fresh = old if same_chat else chat_id()
    await uow.telegram_links.upsert(rep_id, fresh, None, linked_at + timedelta(seconds=5))
    await uow.telegram_links.delete(rep_id, old, linked_at)

    assert await linked_chat(uow, rep_id) == fresh


async def test_relink_of_same_chat_after_403_survives(
    uow: UnitOfWork,
    sender: NotificationService,
    telegram_api: FakeTelegram,
    make_user: MakeUser,
    org: OrgFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Пользователь заблокировал бота — 403. Пока ответ шёл, он разблокировал бота и сделал
    # /start из того же чата: привязка закоммичена между откатом и удалением. Она остаётся.
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    chat = await link(uow, rep)
    event_id = await telegram_command(uow, rep)
    telegram_api.responses.append(
        httpx.Response(
            403,
            json={
                "ok": False,
                "error_code": 403,
                "description": "Forbidden: bot was blocked by the user",
            },
        )
    )
    rollback = uow.rollback

    async def rollback_then_relink() -> None:
        await rollback()
        monkeypatch.setattr(uow, "rollback", rollback)  # только первый откат — после 403
        await uow.telegram_links.upsert(rep_id, chat, None, datetime.now(UTC))
        await uow.commit()

    monkeypatch.setattr(uow, "rollback", rollback_then_relink)

    await sender.send_telegram(event_id)

    assert await linked_chat(uow, rep_id) == chat
    assert await sent_keys(uow, event_id) == 0


@pytest.mark.parametrize(
    ("response", "retryable"),
    [
        (
            httpx.Response(
                400,
                json={
                    "ok": False,
                    "error_code": 400,
                    "description": "Bad Request: message is too long",
                },
            ),
            False,
        ),
        (
            httpx.Response(
                401, json={"ok": False, "error_code": 401, "description": "Unauthorized"}
            ),
            False,
        ),
        (
            httpx.Response(
                500, json={"ok": False, "error_code": 500, "description": "Internal Server Error"}
            ),
            True,
        ),
        (httpx.Response(502, text="Bad Gateway"), True),
    ],
)
async def test_other_errors_raise_and_keep_link(
    response: httpx.Response,
    retryable: bool,
    uow: UnitOfWork,
    sender: NotificationService,
    telegram_api: FakeTelegram,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    chat = await link(uow, rep)
    event_id = await telegram_command(uow, rep)
    telegram_api.responses.append(response)

    with pytest.raises(TelegramApiError) as caught:
        await sender.send_telegram(event_id)

    # 5xx — обычный ретрай; прочие 4xx — сразу провал (строка failed_tasks).
    assert is_retryable(caught.value) is retryable
    assert await linked_chat(uow, rep_id) == chat
    assert await sent_keys(uow, event_id) == 0


async def test_network_error_is_retried(
    uow: UnitOfWork,
    sender: NotificationService,
    make_user: MakeUser,
    org: OrgFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rep = await member(make_user, org, "sales_rep")
    await link(uow, rep)
    event_id = await telegram_command(uow, rep)

    async def refused(*args: Any, **kwargs: Any) -> httpx.Response:
        raise httpx.ConnectError("refused")

    assert sender.telegram is not None
    monkeypatch.setattr(sender.telegram.http, "post", refused)

    with pytest.raises(TelegramUnavailable) as caught:
        await sender.send_telegram(event_id)

    assert is_retryable(caught.value)
    assert await sent_keys(uow, event_id) == 0


async def test_not_linked_or_no_token_is_skipped(
    uow: UnitOfWork,
    sender: NotificationService,
    telegram_api: FakeTelegram,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    unlinked = await telegram_command(uow, rep)  # отвязали после notify
    no_token = await telegram_command(uow, rep)

    await sender.send_telegram(unlinked)
    uow.session.add(TelegramLink(user_id=rep_id, chat_id=chat_id()))
    await uow.commit()
    await NotificationService(uow).send_telegram(no_token)  # бот не настроен
    await sender.send_telegram(2**62)  # строку удалила очистка

    assert telegram_api.sent == []
    assert await sent_keys(uow, unlinked) == await sent_keys(uow, no_token) == 0


# --- конец в конец в транзакции теста: API → outbox → notify → команды → отправка


async def test_contact_assigned_via_api_reaches_mail_and_telegram(
    client: AsyncClient,
    uow: UnitOfWork,
    make_user: MakeUser,
    org: OrgFixture,
    telegram_api: FakeTelegram,
    mailpit: Mailpit,
    deliver_outbox: DeliverOutbox,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    rep_id, rep_email = rep.id, rep.email
    chat = await link(uow, rep)

    resp = await client.post(
        "/api/v1/contacts",
        json={"first_name": "Марія", "owner_id": str(rep_id)},
        headers=await tokens_for(client, admin),
    )
    assert resp.status_code == 201
    assert telegram_api.sent == []  # API ничего не отправляет — только событие в outbox

    assert await deliver_outbox() == 3  # событие → notify → две команды
    assert await deliver_outbox() == 0

    assert telegram_api.texts(chat) == [
        f"Вам призначено контакт «Марія».\nПризначив: {admin.email}."
    ]
    [letter] = await read_emails(mailpit, rep_email)
    assert letter["Subject"] == "Вам призначено запис у SalesHub"
    assert "Марія" in letter["Text"]
    # Токен бота — в пути запроса (так устроен Bot API), но больше нигде.
    assert all(path.endswith("/sendMessage") for path in telegram_api.paths)


async def test_bulk_reassign_gives_one_message_each(
    uow: UnitOfWork,
    make_user: MakeUser,
    make_contact: MakeRecord,
    org: OrgFixture,
    telegram_api: FakeTelegram,
    mailpit: Mailpit,
    run_job: RunJob,
    deliver_outbox: DeliverOutbox,
) -> None:
    admin = await member(make_user, org, "org_admin")
    leaving, heir = (
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep"),
    )
    admin_chat, heir_chat = await link(uow, admin), await link(uow, heir)
    for _ in range(5):
        await make_contact(leaving)
    access = Access("bulk:run", "org", admin.id, org.id, None, admin.role)
    bulk = BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=2)
    job = await bulk.start_reassign_owner(
        access, ReassignOwnerRequest(leaving.id, heir.id, ("contacts",)), None
    )
    job_id = job.id

    await run_job(job_id)  # три чанка — одно событие, а не пять
    await bulk.finalize_if_complete(job_id)
    await deliver_outbox()

    assert telegram_api.texts(heir_chat) == [
        f"Вам передано клієнтів: 5 (контакти — 5).\nПередав: {admin.email}."
    ]
    [summary] = telegram_api.texts(admin_chat)
    assert summary.startswith("Масову операцію «Передача клієнтів» завершено: виконано.")
    assert len(await read_emails(mailpit, heir.email)) == 1
    assert len(await read_emails(mailpit, admin.email)) == 1
    assert telegram_api.texts(heir_chat) and len(telegram_api.sent) == 2


async def test_deal_won_reaches_owner_and_lead(
    client: AsyncClient,
    uow: UnitOfWork,
    make_user: MakeUser,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    org: OrgFixture,
    telegram_api: FakeTelegram,
    deliver_outbox: DeliverOutbox,
) -> None:
    rep, lead = await member(make_user, org, "sales_rep"), await member(make_user, org, "team_lead")
    rep_chat, lead_chat = await link(uow, rep), await link(uow, lead)
    deal = await make_deal(rep, (await make_pipeline(org.id)).stages[0], title="Сервери")
    headers = await tokens_for(client, lead)

    resp = await client.patch(
        f"/api/v1/deals/{deal.id}",
        json={
            "version": 1,
            "status": "won",
            "expected_close_date": str(date.today() + timedelta(days=1)),
        },
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    await deliver_outbox()

    # Руководитель закрыл сам — ему не пишем; ответственному — Telegram (email выключен).
    [text] = telegram_api.texts(rep_chat)
    assert text.startswith("Угоду «Сервери» закрито: виграно.")
    assert telegram_api.texts(lead_chat) == []


# --- relay → Redis → настоящий воркер: notify → send_email и send_telegram


@pytest.fixture
async def sessions(postgres_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(postgres_url)
    try:
        yield create_session_factory(engine)
    finally:
        await engine.dispose()


async def wait_for(check: Callable[[], Awaitable[bool]], what: str) -> None:
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while not await check():
        assert asyncio.get_running_loop().time() < deadline, f"не дождались: {what}"
        await asyncio.sleep(0.1)


@pytest.mark.worker
@pytest.mark.committing
async def test_relay_and_worker_deliver_domain_event(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    postgres_url: str,
    mailpit: Mailpit,
    telegram_api: FakeTelegram,
) -> None:
    org_id, admin_id, rep_id, chat = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), chat_id()
    suffix = org_id.hex[:8]
    rep_email = f"rep-{suffix}@example.com"
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Notify", slug=f"notify-{suffix}"))
        await s.flush()
        team = Team(id=uuid.uuid4(), org_id=org_id, name="Продажі")
        s.add(team)
        await s.flush()
        s.add(
            User(
                id=admin_id,
                email=f"admin-{suffix}@example.com",
                password_hash="x",
                org_id=org_id,
                role="org_admin",
            )
        )
        s.add(
            User(
                id=rep_id,
                email=rep_email,
                password_hash="x",
                org_id=org_id,
                role="sales_rep",
                team_id=team.id,
            )
        )
        await s.flush()
        s.add(TelegramLink(user_id=rep_id, chat_id=chat))
        await s.commit()
    relay = Relay(
        sessions,
        CeleryPublisher(celery_app),
        listen_dsn=listen_dsn(postgres_url),
        batch_size=100,
        poll_seconds=0.5,
    )
    task = asyncio.create_task(relay.run())
    try:
        async with sessions() as s:
            uow = UnitOfWork(s)
            payload = {
                "entity_type": "deal",
                "entity_id": 1,
                "title": "Сервери",
                "owner_id": str(rep_id),
                "actor_id": str(admin_id),
            }
            uow.outbox.add("record.assigned", payload, org_id=org_id)
            await uow.commit()

        async def delivered() -> bool:
            return bool(telegram_api.texts(chat)) and bool(await read_emails(mailpit, rep_email))

        await wait_for(delivered, "письмо и сообщение получателю")
    finally:
        relay.stop()
        await asyncio.wait_for(task, WAIT_SECONDS)
        async with sessions() as s:
            await s.execute(delete(OutboxEvent).where(OutboxEvent.org_id == org_id))
            await s.execute(delete(SentNotification).where(SentNotification.user_id == rep_id))
            await s.execute(delete(User).where(User.org_id == org_id))  # привязка — каскадом
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()

    assert telegram_api.texts(chat) == [
        f"Вам призначено угоду «Сервери».\nПризначив: admin-{suffix}@example.com."
    ]
    assert len(await read_emails(mailpit, rep_email)) == 1
