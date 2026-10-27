"""Письма через outbox (разделы 4 и 15, шаг 5.3): команда email.send в транзакции сервиса →
notifications.send_email → SMTP, дедуп по sent_notifications, токен стирается после отправки.

NotificationService проверяется в транзакции теста с Mailpit контейнера. Настоящий путь
relay → Redis → воркер → Mailpit — тест committing с фикстурой celery_worker.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aiosmtplib
import pytest
from celery.worker import WorkController
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from app.core.config import Settings
from app.core.resources import smtp_sender
from app.db.session import create_engine, create_session_factory
from app.emails.sender import Email
from app.errors import ValidationFailed
from app.events.relay import CeleryPublisher, Relay, listen_dsn
from app.models import EmailToken, OutboxEvent, SentNotification
from app.services.notifications import NotificationService
from app.uow import UnitOfWork
from app.workers.celery_app import celery_app
from tests.integration.conftest import (
    DeliverOutbox,
    Mailpit,
    MakeOrg,
    MakeUser,
    member,
    read_emails,
    token_from_email,
    tokens_for,
)

WAIT_SECONDS = 15


def new_email() -> str:
    # Mailpit общий на весь запуск — уникальный адрес на тест.
    return f"note-{uuid.uuid4().hex[:8]}@example.com"


def invite_context(token: str = "tok-secret") -> dict[str, str]:
    return {
        "token": token,
        "org_name": "Acme",
        "role_title": "Спостерігач",
        "docs_url": "http://x/docs",
        "expires_at": "завтра",
    }


async def add_invite_email(uow: UnitOfWork, to: str, **fields: Any) -> int:
    event = uow.outbox.add_email(to, "invite", invite_context(), **fields)
    await uow.commit()
    return event.id


async def payload_of(session: AsyncSession, event_id: int) -> dict[str, Any]:
    # Колонкой, а не объектом: scrub_token меняет строку UPDATE-ом мимо объектов сессии.
    payload = await session.scalar(select(OutboxEvent.payload).where(OutboxEvent.id == event_id))
    assert payload is not None
    return dict(payload)


async def count_where(session: AsyncSession, condition: ColumnElement[bool]) -> int:
    return await session.scalar(select(func.count()).where(condition)) or 0


async def sent_rows(session: AsyncSession, key: str) -> list[SentNotification]:
    rows = await session.scalars(select(SentNotification).where(SentNotification.dedup_key == key))
    return list(rows)


class BrokenSender:
    """SMTP оборвал соединение посреди отправки — временный сбой, задача повторится."""

    async def send(self, email: Email) -> None:
        raise aiosmtplib.SMTPServerDisconnected("mailpit")


@pytest.fixture
def notifications(uow: UnitOfWork, settings: Settings) -> NotificationService:
    return NotificationService(uow, smtp_sender(settings))


# --- NotificationService.send_email в транзакции теста


async def test_add_email_writes_command_with_string_context(uow: UnitOfWork) -> None:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()

    event_id = await add_invite_email(
        uow, "ivan@example.com", org_id=org_id, user_id=user_id, dedup_key="email:custom"
    )

    event = await uow.outbox.get(event_id)
    assert event is not None
    assert event.event_type == "email.send"
    assert event.org_id == org_id
    assert event.payload == {
        "to": "ivan@example.com",
        "template": "invite",
        "context": invite_context(),
        "user_id": str(user_id),
        "dedup_key": "email:custom",
    }


async def test_send_twice_gives_one_email_and_one_row(
    uow: UnitOfWork, notifications: NotificationService, mailpit: Mailpit
) -> None:
    to = new_email()
    event_id = await add_invite_email(uow, to)

    await notifications.send_email(event_id)
    await notifications.send_email(event_id)  # повторная доставка той же задачи

    [letter] = await read_emails(mailpit, to)
    assert token_from_email(letter) == "tok-secret"
    [row] = await sent_rows(uow.session, f"email:{event_id}")
    assert row.channel == "email"
    assert row.user_id is None  # у приглашения пользователя ещё нет


async def test_token_is_scrubbed_after_success(
    uow: UnitOfWork, notifications: NotificationService
) -> None:
    user_id = uuid.uuid4()
    event_id = await add_invite_email(uow, new_email(), user_id=user_id)

    await notifications.send_email(event_id)

    context = (await payload_of(uow.session, event_id))["context"]
    assert "token" not in context
    assert context == {k: v for k, v in invite_context().items() if k != "token"}
    [row] = await sent_rows(uow.session, f"email:{event_id}")
    assert row.user_id == user_id


async def test_smtp_failure_keeps_token_and_retry_sends(
    uow: UnitOfWork, notifications: NotificationService, mailpit: Mailpit
) -> None:
    to = new_email()
    event_id = await add_invite_email(uow, to)

    with pytest.raises(aiosmtplib.SMTPServerDisconnected):
        await NotificationService(uow, BrokenSender()).send_email(event_id)

    # Откат: ключ дедупа не занят, токен на месте — ретраю есть что отправить.
    assert await sent_rows(uow.session, f"email:{event_id}") == []
    assert (await payload_of(uow.session, event_id))["context"]["token"] == "tok-secret"
    assert await read_emails(mailpit, to) == []

    await notifications.send_email(event_id)  # ретрай: SMTP снова доступен

    [letter] = await read_emails(mailpit, to)
    assert token_from_email(letter) == "tok-secret"
    assert len(await sent_rows(uow.session, f"email:{event_id}")) == 1


async def test_same_dedup_key_sends_once(
    uow: UnitOfWork, notifications: NotificationService, mailpit: Mailpit
) -> None:
    to, key = new_email(), f"tests:{uuid.uuid4().hex}"
    first = await add_invite_email(uow, to, dedup_key=key)
    second = await add_invite_email(uow, to, dedup_key=key)  # повтор notify — тот же ключ

    await notifications.send_email(first)
    await notifications.send_email(second)

    assert len(await read_emails(mailpit, to)) == 1
    assert len(await sent_rows(uow.session, key)) == 1


async def test_missing_event_is_skipped(
    uow: UnitOfWork, notifications: NotificationService
) -> None:
    # Строку удалила очистка — задача ничего не делает и не падает (не повторяется зря).
    await notifications.send_email(2**62)

    assert await sent_rows(uow.session, f"email:{2**62}") == []


# --- Сервисы пишут команду в своей транзакции, API почту не отправляет


async def test_invite_writes_command_and_sends_nothing_until_delivered(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    mailpit: Mailpit,
    uow: UnitOfWork,
    deliver_outbox: DeliverOutbox,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    to = new_email()

    resp = await client.post("/api/v1/invites", json={"email": to, "role": "viewer"}, headers=admin)

    assert resp.status_code == 202
    [event] = await uow.session.scalars(
        select(OutboxEvent).where(OutboxEvent.payload["to"].astext == to)
    )
    assert event.event_type == "email.send"
    assert event.org_id == org.id
    assert event.payload["template"] == "invite"
    assert event.payload["context"]["token"]  # сырой токен — только здесь, в базе — хэш
    assert await read_emails(mailpit, to) == []  # API не отправляет: письмо — дело воркера

    assert await deliver_outbox() >= 1

    [letter] = await read_emails(mailpit, to)
    assert token_from_email(letter) == event.payload["context"]["token"]


async def test_invite_rollback_leaves_no_command(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    uow: UnitOfWork,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    to = new_email()

    written: list[int] = []

    async def failing_commit(self: UnitOfWork) -> None:  # ошибка после add_email, до коммита
        # Команда уже в открытой транзакции — откатится вместе с токеном, а не «не записана».
        await self.session.flush()
        written.append(await count_where(self.session, OutboxEvent.payload["to"].astext == to))
        raise ValidationFailed("validation")

    monkeypatch.setattr(UnitOfWork, "commit", failing_commit)
    resp = await client.post("/api/v1/invites", json={"email": to, "role": "viewer"}, headers=admin)

    assert resp.status_code == 422
    assert written == [1]
    # Откатилось всё вместе: и токен, и команда на письмо — письма о том, чего нет, не будет.
    commands = await count_where(uow.session, OutboxEvent.payload["to"].astext == to)
    tokens = await count_where(uow.session, EmailToken.email == to)
    assert (commands, tokens) == (0, 0)


async def test_forgot_writes_command_for_user(
    client: AsyncClient, make_user: MakeUser, uow: UnitOfWork
) -> None:
    to = new_email()
    user = await make_user(to)
    user_id = user.id

    resp = await client.post("/api/v1/auth/password/forgot", json={"email": to})

    assert resp.status_code == 202
    [payload] = await uow.session.scalars(
        select(OutboxEvent.payload).where(OutboxEvent.payload["to"].astext == to)
    )
    assert payload["template"] == "reset_password"
    assert payload["user_id"] == str(user_id)
    assert payload["dedup_key"] is None  # ключ по умолчанию — email:{event_id}


# --- relay → Redis → настоящий воркер → Mailpit


@pytest.fixture
async def sessions(postgres_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(postgres_url)
    try:
        yield create_session_factory(engine)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE outbox_events, sent_notifications"))
        await engine.dispose()


async def wait_for_emails(mailpit: Mailpit, to: str) -> list[dict[str, Any]]:
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while not (emails := await read_emails(mailpit, to)):
        assert asyncio.get_running_loop().time() < deadline, f"письмо для {to} не пришло"
        await asyncio.sleep(0.1)
    return emails


async def committed_invite_email(sessions: async_sessionmaker[AsyncSession], to: str) -> int:
    async with sessions() as session:
        return await add_invite_email(UnitOfWork(session), to)


async def committed_tokenless_email(sessions: async_sessionmaker[AsyncSession], to: str) -> int:
    # Письмо без токена: его повтор отрисуется и уйдёт снова, если дедуп сломан. Повтор
    # приглашения упал бы раньше — токен уже стёрт, StrictUndefined, — и дубля бы не было.
    async with sessions() as session:
        uow = UnitOfWork(session)
        event = uow.outbox.add_email(to, "already_member", {"org_name": "Acme"})
        await uow.commit()
        return event.id


@pytest.mark.worker
@pytest.mark.committing
async def test_relay_and_worker_send_email_once(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    postgres_url: str,
    mailpit: Mailpit,
) -> None:
    to, again, marker = new_email(), new_email(), new_email()
    relay = Relay(
        sessions,
        CeleryPublisher(celery_app),
        listen_dsn=listen_dsn(postgres_url),
        batch_size=100,
        poll_seconds=0.5,
    )
    task = asyncio.create_task(relay.run())
    try:
        event_id = await committed_invite_email(sessions, to)
        [letter] = await wait_for_emails(mailpit, to)
        assert token_from_email(letter) == "tok-secret"
        again_id = await committed_tokenless_email(sessions, again)
        await wait_for_emails(mailpit, again)

        # Повторная публикация того же события (relay упал до отметки published_at)…
        await asyncio.to_thread(
            celery_app.send_task, "notifications.send_email", args=[again_id], retry=False
        )
        # …а за ней в ту же очередь — другое письмо: воркер solo берёт задачи по порядку, и
        # когда пришло второе, повтор уже выполнен.
        await committed_invite_email(sessions, marker)
        await wait_for_emails(mailpit, marker)
    finally:
        relay.stop()
        await asyncio.wait_for(task, WAIT_SECONDS)

    # Повтор погасил sent_notifications: письмо без токена ушло бы снова.
    assert len(await read_emails(mailpit, again)) == 1
    async with sessions() as session:
        assert "token" not in (await payload_of(session, event_id))["context"]
        assert len(await sent_rows(session, f"email:{event_id}")) == 1
        assert len(await sent_rows(session, f"email:{again_id}")) == 1
