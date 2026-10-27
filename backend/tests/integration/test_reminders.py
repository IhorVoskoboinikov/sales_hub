"""Напоминания за 15 минут и утренний дайджест (раздел 16, шаг 5.14) в транзакции теста.

Время тика — freezegun: сервис берёт «сейчас» сам, как задача beat. Команды telegram.send
читаются из outbox; доставка до Bot API — deliver_outbox (relay → send_telegram → FakeTelegram),
дедуп — настоящий sent_notifications. Доставка — тоже под freezegun (deliver): у команд срок
(not_after), и отправитель сверяет его с «сейчас». Выборка идёт по всем привязанным
пользователям базы, поэтому проверки — по командам пользователей своего теста.
"""

import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest
from freezegun import freeze_time
from redis.asyncio import Redis
from sqlalchemy import DateTime, literal, select, update
from structlog.testing import capture_logs

from app.authz.roles import RoleCatalog
from app.models import Activity, NotificationSetting, OutboxEvent, SentNotification, User
from app.notifications import catalog
from app.notifications.catalog import NotificationEvent
from app.notifications.keys import digest_key, digest_key_sql, reminder_key, reminder_key_sql
from app.services import reminders
from app.services.fx import FxService, NbuClient
from app.services.reminders import REMINDER_LIMIT, ReminderService
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    NBU_URL,
    DeliverOutbox,
    FakeTelegram,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
# 27.10.2026 10:00 UTC — 12:00 в Киеве (UTC+2 после перехода на зимнее время 25.10).
NOW = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
SECOND = timedelta(seconds=1)


def at(moment: datetime) -> Any:
    # real_asyncio: «замороженное» время не трогает часы event loop — иначе встали бы таймауты.
    return freeze_time(moment, real_asyncio=True)


def utc(day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, minute, second, tzinfo=UTC)


MakeService = Callable[..., ReminderService]


@pytest.fixture
async def make_service(uow: UnitOfWork, redis_url: str) -> AsyncIterator[MakeService]:
    async with httpx.AsyncClient() as http:
        redis = Redis.from_url(redis_url)

        def make(roles: RoleCatalog = CATALOG, **fields: Any) -> ReminderService:
            fx = FxService(uow, redis, NbuClient(http, NBU_URL))
            return ReminderService(uow, roles, fx, **fields)

        yield make
        await redis.aclose()


@pytest.fixture
def service(make_service: MakeService) -> ReminderService:
    return make_service()


def role_without(role: str, action: str) -> RoleCatalog:
    """Файл ролей, где action нет только у одной роли (roles_without убирает у всех)."""
    permissions = {
        name: {a: s for a, s in CATALOG.permissions(name).items() if (name, a) != (role, action)}
        for name in CATALOG.titles
    }
    return RoleCatalog(permissions, CATALOG.titles)


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


async def link(uow: UnitOfWork, user: User) -> int:
    chat = 3_000_000 + uuid.uuid4().int % 1_000_000_000
    await uow.telegram_links.upsert(user.id, chat, None, datetime.now(UTC))
    await uow.commit()
    return chat


async def linked(
    make_user: MakeUser, uow: UnitOfWork, org: OrgFixture, role: str, **fields: Any
) -> tuple[User, int]:
    """Пользователь организации с привязанным Telegram: (пользователь, chat_id)."""
    user = await member(make_user, org, role, **fields)
    return user, await link(uow, user)


async def setting(uow: UnitOfWork, user: User, event: NotificationEvent, on: bool) -> None:
    uow.session.add(
        NotificationSetting(user_id=user.id, event_type=event, channel="telegram", enabled=on)
    )
    await uow.commit()


async def commands(uow: UnitOfWork, user: User | uuid.UUID) -> list[dict[str, Any]]:
    """Команды telegram.send пользователю — по порядку записи. Можно передать id: откат
    отправителя (дубль) «протухает» объекты сессии теста."""
    user_id = user if isinstance(user, uuid.UUID) else user.id
    rows = await uow.session.scalars(
        select(OutboxEvent.payload)
        .where(
            OutboxEvent.event_type == "telegram.send",
            OutboxEvent.payload["user_id"].astext == str(user_id),
        )
        .order_by(OutboxEvent.id)
    )
    return [dict(row) for row in rows]


async def texts(uow: UnitOfWork, user: User) -> list[str]:
    return [c["text"] for c in await commands(uow, user)]


async def tick(service: ReminderService, moment: datetime) -> int:
    with at(moment):
        return await service.send_reminders()


async def digest(service: ReminderService, moment: datetime) -> int:
    with at(moment):
        return await service.morning_digest()


async def deliver(deliver_outbox: DeliverOutbox, moment: datetime) -> int:
    with at(moment):
        return await deliver_outbox()


# --- напоминания


async def test_window_is_after_now_up_to_15_minutes_inclusive(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    await make_activity(rep, type="call", subject="зараз", due_at=NOW)
    first = await make_activity(rep, type="call", subject="за секунду", due_at=NOW + SECOND)
    meeting = await make_activity(rep, type="meeting", subject="Демо", due_at=NOW + 10 * MINUTE)
    edge = await make_activity(rep, type="call", subject="Петренко", due_at=NOW + 15 * MINUTE)
    await make_activity(rep, type="call", subject="пізніше", due_at=NOW + 15 * MINUTE + SECOND)

    await tick(service, NOW)

    assert await commands(uow, rep) == [
        {
            "user_id": str(rep.id),
            "text": "Через 1 хв (о 12:00) — дзвінок: за секунду",
            "dedup_key": reminder_key(first.id, rep.id, first.due_at),
            # Срок команды — тик + 2 хв, но не позже начала звонка.
            "not_after": (NOW + SECOND).isoformat(),
        },
        {
            "user_id": str(rep.id),
            "text": "Через 10 хв (о 12:10) — зустріч: Демо",
            "dedup_key": reminder_key(meeting.id, rep.id, meeting.due_at),
            "not_after": (NOW + 2 * MINUTE).isoformat(),
        },
        {
            "user_id": str(rep.id),
            "text": "Через 15 хв (о 12:15) — дзвінок: Петренко",
            "dedup_key": (
                f"activity.reminder:{edge.id}:{rep.id}:{int(edge.due_at.timestamp())}:telegram"
            ),
            "not_after": (NOW + 2 * MINUTE).isoformat(),
        },
    ]


async def test_time_is_shown_in_user_time_zone(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep", timezone="America/New_York")
    await make_activity(rep, type="call", subject="Нью-Йорк", due_at=NOW + 5 * MINUTE)

    await tick(service, NOW)

    assert await texts(uow, rep) == ["Через 5 хв (о 06:05) — дзвінок: Нью-Йорк"]


async def test_missed_ticks_and_late_activities_are_caught_up(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    # Beat лежал 13 минут: напоминание о звонке через 15 минут уходит первым же тиком.
    await make_activity(rep, type="call", subject="пропущений", due_at=NOW + 15 * MINUTE)
    await tick(service, NOW - 5 * MINUTE)  # срок ещё за окном
    assert await texts(uow, rep) == []

    await tick(service, NOW + 13 * MINUTE)
    # Встречу создали за 6 минут до начала — в следующий тик, а не «ровно за 15».
    await make_activity(rep, type="meeting", subject="термінова", due_at=NOW + 20 * MINUTE)
    await tick(service, NOW + 14 * MINUTE)

    assert await texts(uow, rep) == [
        "Через 2 хв (о 12:15) — дзвінок: пропущений",
        # Повтор тика до отправки пишет команду ещё раз — её погасит дедуп отправителя.
        "Через 1 хв (о 12:15) — дзвінок: пропущений",
        "Через 6 хв (о 12:20) — зустріч: термінова",
    ]


async def test_repeated_tick_does_not_send_twice(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    # Доли секунды в сроке: ключ в SQL (floor) и в Python (int) должен совпасть.
    await make_activity(
        rep, type="call", subject="Петренко", due_at=NOW + 7 * MINUTE + timedelta(seconds=0.7)
    )

    rep_id = rep.id

    await tick(service, NOW)
    await tick(service, NOW + SECOND)  # команда ещё не отправлена — пишется снова
    await deliver(deliver_outbox, NOW + SECOND)

    assert telegram_api.texts(chat) == ["Через 8 хв (о 12:07) — дзвінок: Петренко"]
    # Отправлено — следующие тики его не выбирают.
    before = len(await commands(uow, rep_id))
    await tick(service, NOW + MINUTE)
    await tick(service, NOW + 6 * MINUTE)
    assert len(await commands(uow, rep_id)) == before == 2


async def test_moved_meeting_gets_a_new_reminder(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    meeting = await make_activity(rep, type="meeting", subject="Демо", due_at=NOW + 10 * MINUTE)
    await tick(service, NOW)
    await deliver(deliver_outbox, NOW)

    meeting.due_at = NOW + 14 * MINUTE
    await uow.commit()
    await tick(service, NOW + MINUTE)
    await deliver(deliver_outbox, NOW + MINUTE)

    assert telegram_api.texts(chat) == [
        "Через 10 хв (о 12:10) — зустріч: Демо",
        "Через 13 хв (о 12:14) — зустріч: Демо",
    ]
    keys = [c["dedup_key"] for c in await commands(uow, rep)]
    assert keys == [
        reminder_key(meeting.id, rep.id, NOW + 10 * MINUTE),
        reminder_key(meeting.id, rep.id, NOW + 14 * MINUTE),
    ]


async def test_done_deleted_tasks_and_emails_are_not_reminded(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    soon = NOW + 5 * MINUTE
    await make_activity(rep, type="call", subject="виконаний", due_at=soon, done_at=NOW)
    deleted = await make_activity(rep, type="call", subject="видалений", due_at=soon)
    await uow.activities.delete(deleted)
    await uow.commit()
    await make_activity(rep, type="task", subject="завдання", due_at=soon)
    await make_activity(rep, type="email", subject="лист", due_at=soon)

    await tick(service, NOW)

    assert await commands(uow, rep) == []


async def test_only_linked_active_members_get_their_own_reminders(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    soon = NOW + 5 * MINUTE
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    lead, _ = await linked(make_user, uow, org, "team_lead")
    unlinked = await member(make_user, org, "sales_rep")
    inactive, _ = await linked(make_user, uow, org, "sales_rep", is_active=False)
    for user in (rep, lead, unlinked, inactive):
        await make_activity(user, type="call", subject=f"дзвінок {user.email}", due_at=soon)
    # Супер-админ без организации (привязку вставляем сами — бот его не привяжет): звонок,
    # записанный на него в чужой организации, напоминания не даёт.
    admin = await make_user("root@example.com", is_superadmin=True)
    await link(uow, admin)
    uow.activities.add(
        Activity(
            org_id=org.id,
            team_id=org.team_ids[0],
            owner_id=admin.id,
            type="call",
            subject="чужий",
            due_at=soon,
        )
    )
    await uow.commit()

    await tick(service, NOW)

    assert await texts(uow, rep) == [f"Через 5 хв (о 12:05) — дзвінок: дзвінок {rep.email}"]
    assert await texts(uow, lead) == [f"Через 5 хв (о 12:05) — дзвінок: дзвінок {lead.email}"]
    for user in (unlinked, inactive, admin):
        assert await commands(uow, user) == [], user.email


async def test_disabled_setting_and_default_from_catalog(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    off, _ = await linked(make_user, uow, org, "sales_rep")
    default, _ = await linked(make_user, uow, org, "sales_rep")
    on, _ = await linked(make_user, uow, org, "sales_rep")
    await setting(uow, off, NotificationEvent.ACTIVITY_REMINDER, False)
    await setting(uow, on, NotificationEvent.ACTIVITY_REMINDER, True)
    for user in (off, default, on):
        await make_activity(user, type="call", subject="дзвінок", due_at=NOW + 5 * MINUTE)

    await tick(service, NOW)
    assert [len(await commands(uow, u)) for u in (off, default, on)] == [0, 1, 1]

    # Умолчание — параметр из catalog.DEFAULTS: поменяли в коде — поменялось у всех без строки.
    pair = (NotificationEvent.ACTIVITY_REMINDER, catalog.TELEGRAM)
    monkeypatch.setitem(catalog.DEFAULTS, pair, False)
    await tick(service, NOW + MINUTE)
    assert [len(await commands(uow, u)) for u in (off, default, on)] == [0, 1, 2]


async def test_at_most_20_per_user_the_rest_next_tick(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    for i in range(REMINDER_LIMIT + 1):
        await make_activity(
            rep, type="call", subject=f"c{i:02}", due_at=NOW + 5 * MINUTE + i * SECOND
        )

    await tick(service, NOW)
    assert len(await commands(uow, rep)) == REMINDER_LIMIT
    await deliver(deliver_outbox, NOW)

    # Отправленные не занимают места в лимите: 21-е уходит следующим тиком.
    await tick(service, NOW + MINUTE)
    await deliver(deliver_outbox, NOW + MINUTE)
    assert [t.rsplit(": ", 1)[1] for t in telegram_api.texts(chat)] == [
        f"c{i:02}" for i in range(REMINDER_LIMIT + 1)
    ]


async def test_activity_of_another_org_is_not_reminded(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_org: MakeOrg,
    make_activity: MakeRecord,
) -> None:
    """У owner_id нет FK: звонок чужой организации, записанный на нашего пользователя (кривой
    импорт, перенос пользователя), напоминания не даёт — тема чужой CRM в Telegram не уходит."""
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    other = await make_org()
    await make_activity(rep, type="call", subject="свій", due_at=NOW + 5 * MINUTE)
    uow.activities.add(
        Activity(
            org_id=other.id,
            team_id=other.team_ids[0],
            owner_id=rep.id,
            type="call",
            subject="чужий",
            due_at=NOW + 5 * MINUTE,
        )
    )
    await uow.commit()

    await tick(service, NOW)

    assert await texts(uow, rep) == ["Через 5 хв (о 12:05) — дзвінок: свій"]


async def test_reassigned_call_is_reminded_to_the_new_owner(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    """Получатель — часть ключа: звонок передали после напоминания прежнему ответственному —
    новый получит своё, ключ прежнего его не гасит."""
    first, first_chat = await linked(make_user, uow, org, "sales_rep")
    second, second_chat = await linked(make_user, uow, org, "sales_rep")
    first_id, second_id = first.id, second.id
    call = await make_activity(first, type="call", subject="Петренко", due_at=NOW + 10 * MINUTE)
    call_id = call.id
    await tick(service, NOW)
    await deliver(deliver_outbox, NOW)

    await uow.session.execute(
        update(Activity).where(Activity.id == call_id).values(owner_id=second_id)
    )
    await uow.commit()
    await tick(service, NOW + MINUTE)
    await deliver(deliver_outbox, NOW + MINUTE)

    assert telegram_api.texts(first_chat) == ["Через 10 хв (о 12:10) — дзвінок: Петренко"]
    assert telegram_api.texts(second_chat) == ["Через 9 хв (о 12:10) — дзвінок: Петренко"]
    assert [c["dedup_key"] for c in await commands(uow, second_id)] == [
        reminder_key(call_id, second_id, NOW + 10 * MINUTE)
    ]
    assert len(await commands(uow, first_id)) == 1


async def test_stale_command_is_skipped_and_the_fresh_one_is_sent(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    """Telegram недоступен или очередь стоит, а тики пишут команды каждую минуту. Отправитель
    берёт их по порядку: команда старше двух минут устарела — пропуск без ключа, уходит первая
    свежая с почти верным «через N хв», а не самая старая «через 15 хв»."""
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    call = await make_activity(rep, type="call", subject="Петренко", due_at=NOW + 15 * MINUTE)
    key = reminder_key(call.id, rep.id, call.due_at)
    for minute in range(6):  # 12:00 … 12:05
        await tick(service, NOW + minute * MINUTE)

    with capture_logs() as logs:
        await deliver(deliver_outbox, NOW + 5 * MINUTE)

    # Срок — тик + 2 хв: команды 12:00…12:03 устарели, 12:04 уходит, 12:05 — дубль.
    assert telegram_api.texts(chat) == ["Через 11 хв (о 12:15) — дзвінок: Петренко"]
    ours = [e["event"] for e in logs if e.get("dedup_key") == key]
    assert ours == ["telegram_expired"] * 4 + ["telegram_duplicate"]


async def test_reminder_is_not_sent_after_the_start(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    rep_id = rep.id
    await make_activity(rep, type="meeting", subject="Демо", due_at=NOW + MINUTE)

    await tick(service, NOW)
    # Срок команды — начало встречи, а не тик + 2 хв.
    [command] = await commands(uow, rep_id)
    assert command["not_after"] == (NOW + MINUTE).isoformat()
    await deliver(deliver_outbox, NOW + MINUTE)

    assert telegram_api.texts(chat) == []
    sent = await uow.session.scalar(
        select(SentNotification.dedup_key).where(SentNotification.user_id == rep_id)
    )
    assert sent is None  # ключ не занят: устаревшая команда — не отправленное сообщение


async def test_sql_keys_match_python_keys(uow: UnitOfWork) -> None:
    """Задача вычитает отправленные по ключу из SQL, а команде ключ ставит Python."""
    due_at = datetime(2026, 10, 27, 12, 15, 0, 700_000, tzinfo=UTC)
    user_id = uuid.uuid4()
    local = datetime(2026, 10, 27, 23, 59, 59, 900_000)  # местное время, без пояса
    sql_reminder, sql_digest = (
        await uow.session.execute(
            select(
                reminder_key_sql(
                    literal(42), literal(user_id), literal(due_at, DateTime(timezone=True))
                ),
                digest_key_sql(literal(user_id), literal(local, DateTime())),
            )
        )
    ).one()

    assert sql_reminder == reminder_key(42, user_id, due_at)
    assert sql_digest == digest_key(user_id, date(2026, 10, 27))


# --- утренний дайджест


async def test_digest_window_09_to_12_in_org_time_zone(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    await make_activity(rep, type="call", subject="Петренко", due_at=NOW)

    # Без отправки ключа нет — каждый тик в окне пишет команду, вне окна — нет.
    counts = []
    for moment in (utc(27, 6, 59, 59), utc(27, 7), utc(27, 9, 59, 59), utc(27, 10)):
        before = len(await commands(uow, rep))
        await digest(service, moment)
        counts.append(len(await commands(uow, rep)) - before)

    assert counts == [0, 1, 1, 0]  # 08:59:59, 09:00, 11:59:59, 12:00 по Киеву


async def test_digest_uses_user_time_zone_over_org(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    kyiv, _ = await linked(make_user, uow, org, "sales_rep")
    new_york, _ = await linked(make_user, uow, org, "sales_rep", timezone="America/New_York")
    for user in (kyiv, new_york):
        await make_activity(user, type="call", subject="дзвінок", due_at=utc(27, 15))

    await digest(service, utc(27, 7))  # 09:00 Київ, 03:00 Нью-Йорк
    assert [len(await commands(uow, u)) for u in (kyiv, new_york)] == [1, 0]

    await digest(service, utc(27, 13))  # 15:00 Київ, 09:00 Нью-Йорк (EDT)
    assert [len(await commands(uow, u)) for u in (kyiv, new_york)] == [1, 1]
    [text] = await texts(uow, new_york)
    assert text.splitlines()[:2] == [
        "Доброго ранку! План на 27.10.2026:",
        "11:00 — дзвінок: дзвінок",
    ]


async def test_digest_window_follows_dst(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    """25.10.2026 Киев перешёл с UTC+3 на UTC+2: 09:00 — это 06:00 UTC накануне и 07:00 UTC
    в день перехода. Пояс считает Postgres, фиксированного сдвига нет."""
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    await make_activity(rep, type="call", subject="дзвінок", due_at=utc(24, 12))

    counts = []
    for moment in (utc(24, 5, 59), utc(24, 6), utc(25, 6), utc(25, 7)):
        before = len(await commands(uow, rep))
        await digest(service, moment)
        counts.append(len(await commands(uow, rep)) - before)

    assert counts == [0, 1, 0, 1]
    first, second = await commands(uow, rep)
    assert first["dedup_key"] == digest_key(rep.id, date(2026, 10, 24))
    assert second["dedup_key"] == digest_key(rep.id, date(2026, 10, 25))


async def test_digest_only_for_team_leads_and_sales_reps(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    users = {
        role: (await linked(make_user, uow, org, role))[0]
        for role in ("org_admin", "viewer", "team_lead", "sales_rep")
    }
    for user in users.values():
        # org_admin и viewer ответственными не бывают — запишем звонки на них напрямую, чтобы
        # повестка не была пустой и отсекла именно роль.
        uow.activities.add(
            Activity(
                org_id=org.id,
                team_id=org.team_ids[0],
                owner_id=user.id,
                type="call",
                subject="дзвінок",
                due_at=NOW,
            )
        )
    await uow.commit()

    await digest(service, utc(27, 7))

    assert {role: len(await commands(uow, u)) for role, u in users.items()} == {
        "org_admin": 0,
        "viewer": 0,
        "team_lead": 1,
        "sales_rep": 1,
    }


async def test_empty_agenda_is_skipped_until_there_is_something(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    # Звонок завтра — в повестку «сегодня» и просроченных он не входит.
    await make_activity(rep, type="call", subject="завтра", due_at=utc(28, 9))

    await digest(service, utc(27, 7))
    assert await commands(uow, rep) == []

    await make_activity(rep, type="task", subject="Підготувати КП", due_at=utc(27, 14))
    await digest(service, utc(27, 7, 15))
    assert await texts(uow, rep) == [
        "Доброго ранку! План на 27.10.2026:\n16:00 — завдання: Підготувати КП"
    ]


async def test_digest_excludes_unlinked_inactive_and_disabled(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unlinked = await member(make_user, org, "sales_rep")
    inactive, _ = await linked(make_user, uow, org, "sales_rep", is_active=False)
    off, _ = await linked(make_user, uow, org, "sales_rep")
    default, _ = await linked(make_user, uow, org, "sales_rep")
    on, _ = await linked(make_user, uow, org, "sales_rep")
    await setting(uow, off, NotificationEvent.DIGEST_MORNING, False)
    await setting(uow, on, NotificationEvent.DIGEST_MORNING, True)
    everyone = (unlinked, inactive, off, default, on)
    for user in everyone:
        await make_activity(user, type="call", subject="дзвінок", due_at=NOW)

    await digest(service, utc(27, 7))
    assert [len(await commands(uow, u)) for u in everyone] == [0, 0, 0, 1, 1]

    pair = (NotificationEvent.DIGEST_MORNING, catalog.TELEGRAM)
    monkeypatch.setitem(catalog.DEFAULTS, pair, False)
    await digest(service, utc(27, 7, 15))
    assert [len(await commands(uow, u)) for u in everyone] == [0, 0, 0, 1, 2]


async def test_digest_once_per_local_day_end_to_end(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    await make_activity(rep, type="call", subject="Петренко", due_at=NOW)
    await make_activity(rep, type="meeting", subject="Демо", due_at=utc(26, 8))

    await digest(service, utc(27, 7))
    await deliver(deliver_outbox, utc(27, 7))
    # Тот же местный день — уже отправлено, ключ занят.
    await digest(service, utc(27, 7, 15))
    await digest(service, utc(27, 9, 45))
    await deliver(deliver_outbox, utc(27, 9, 45))

    assert telegram_api.texts(chat) == [
        "Доброго ранку! План на 27.10.2026:\n"
        "12:00 — дзвінок: Петренко\n"
        "\n"
        "Прострочено:\n"
        "26.10 10:00 — зустріч: Демо"
    ]
    sent = await uow.session.scalar(
        select(SentNotification.dedup_key).where(SentNotification.user_id == rep.id)
    )
    assert sent == digest_key(rep.id, date(2026, 10, 27))

    # Следующий день — новый ключ и новый дайджест (звонок вчера стал просроченным).
    await digest(service, utc(28, 7))
    await deliver(deliver_outbox, utc(28, 7))
    assert len(telegram_api.texts(chat)) == 2
    assert telegram_api.texts(chat)[1].splitlines()[:2] == [
        "Доброго ранку! План на 28.10.2026:",
        "На сьогодні завдань немає.",
    ]


async def test_digest_is_not_sent_after_noon(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    deliver_outbox: DeliverOutbox,
    telegram_api: FakeTelegram,
) -> None:
    """Поставленный в 11:45 дайджест, который не успели отправить до 12:00 (воркер лежал),
    днём уже не уходит: срок команды — местный конец окна."""
    rep, chat = await linked(make_user, uow, org, "sales_rep")
    rep_id = rep.id
    await make_activity(rep, type="call", subject="Петренко", due_at=NOW)

    await digest(service, utc(27, 9, 45))  # 11:45 Київ
    [command] = await commands(uow, rep_id)
    assert command["not_after"] == utc(27, 10).isoformat()  # 12:00 Київ
    await deliver(deliver_outbox, utc(27, 10))

    assert telegram_api.texts(chat) == []


async def test_role_without_activity_read_is_skipped(
    uow: UnitOfWork,
    make_service: MakeService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    """AgendaService отвечает Forbidden — получатель пропускается, тик не падает."""
    service = make_service(role_without("sales_rep", "activity:read"))
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    lead, _ = await linked(make_user, uow, org, "team_lead")
    for user in (rep, lead):
        await make_activity(user, type="call", subject="дзвінок", due_at=NOW)

    await digest(service, utc(27, 7))

    assert [len(await commands(uow, u)) for u in (rep, lead)] == [0, 1]


async def test_digest_stops_when_budget_is_exhausted(
    uow: UnitOfWork,
    make_service: MakeService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
) -> None:
    rep, _ = await linked(make_user, uow, org, "sales_rep")
    await make_activity(rep, type="call", subject="дзвінок", due_at=NOW)

    # Бюджет 0: кончился до первого получателя — ни одной команды, остальных возьмёт
    # следующий тик.
    with capture_logs() as logs:
        assert await digest(make_service(budget_seconds=0), utc(27, 7)) == 0

    assert await commands(uow, rep) == []
    [exhausted] = [e for e in logs if e["event"] == "digest_budget_exhausted"]
    assert exhausted["commands"] == 0
    assert exhausted["left"] >= 1

    await digest(make_service(), utc(27, 7, 15))
    assert len(await commands(uow, rep)) == 1


async def test_digest_commits_in_batches(
    uow: UnitOfWork,
    service: ReminderService,
    make_user: MakeUser,
    org: OrgFixture,
    make_activity: MakeRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Падение на получателе теряет только текущую пачку: записанное раньше — закоммичено."""
    first, _ = await linked(make_user, uow, org, "sales_rep")
    second, _ = await linked(make_user, uow, org, "sales_rep")
    for user in (first, second):
        await make_activity(user, type="call", subject="дзвінок", due_at=NOW)
    # Получатели — по users.id (порядок uuid в Postgres и в Python один).
    earlier, later = sorted((first.id, second.id))
    monkeypatch.setattr(reminders, "DIGEST_BATCH", 1)
    day = service.agenda.day

    async def failing(user: User, now: datetime) -> Any:
        if user.id == later:
            raise RuntimeError("база недоступна")
        return await day(user, now)

    monkeypatch.setattr(service.agenda, "day", failing)

    with pytest.raises(RuntimeError):
        await digest(service, utc(27, 7))

    assert [len(await commands(uow, u)) for u in (earlier, later)] == [1, 0]
