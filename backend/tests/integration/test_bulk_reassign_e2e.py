"""Приёмка этапа 5 (шаг 5.15; раздел 10, «Приёмка этапа (5.15)»): передача клиентов конец в
конец на настоящих воркере и relay (маркеры worker и committing).

POST /bulk/reassign-owner настоящего приложения → bulk.plan в Redis → воркер режет на чанки →
группа bulk.chunk → finalize → bulk_job.completed в outbox → relay → notifications.notify →
команды email.send и telegram.send в outbox → relay → send_email (Mailpit контейнера) и
send_telegram (Bot API подменён respx — FakeTelegram; воркер в потоке этого процесса, подмена
действует и на него).

У автора и получателя привязан Telegram: в Mailpit ровно одно письмо каждому, вызовов
sendMessage ровно два — по одному в чат каждого, автору итог, получателю «вам передано N».
Повторный finalize (копия сообщения чанка — как Redis вернул бы его через visibility_timeout —
и прямой вызов) и повторная публикация того же события (строка outbox снова неопубликована,
relay отдаёт её ещё раз, плюс ещё одна доставка notify) ничего не добавляют: события второй раз
нет, notify пишет команды с теми же ключами дедупа, отправители их пропускают. Что дубли прошли
именно этими путями, а не упали, видно по логам (bulk_chunk_already_finished у копии чанка,
email_duplicate и telegram_duplicate у каждой лишней команды) и по failed_tasks: упавшая без
ретрая задача подтверждается и тоже ничего не отправляет — одних счётчиков мало. И автор — он
же получатель: одно письмо и одно сообщение с итогом.

Данные коммитятся (воркер не видит транзакцию теста), после теста фикстура удаляет организацию,
её outbox и строки sent_notifications её людей.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from celery.worker import WorkController
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import ColumnElement, String, delete, func, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.events.relay import CeleryPublisher, Relay, listen_dsn
from app.main import create_app
from app.models import (
    AuditEntry,
    BulkJob,
    Contact,
    FailedTask,
    Organization,
    OutboxEvent,
    Role,
    SentNotification,
    Team,
    TelegramLink,
    User,
)
from app.services.bulk import BulkService
from app.storage import S3FileStorage
from app.uow import UnitOfWork
from app.workers import runtime
from app.workers.celery_app import celery_app
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeTaskQueue,
    FakeTelegram,
    Mailpit,
    RustFs,
    bearer,
    login,
    password_hash,
    read_emails,
    s3_settings,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
WAIT_SECONDS = 30
CHUNK_SIZE = 2  # 5 контактов — три чанка, а событие одно
CONTACTS = 5
SUMMARY_SUBJECT = "Масову операцію завершено — SalesHub"
TRANSFER_SUBJECT = "Вам передано клієнтів — SalesHub"


@dataclass(frozen=True)
class People:
    org_id: uuid.UUID
    team_id: uuid.UUID
    admin_id: uuid.UUID
    lead_id: uuid.UUID
    leaving_id: uuid.UUID
    heir_id: uuid.UUID
    suffix: str

    def email(self, name: str) -> str:
        return f"{name}-{self.suffix}@example.com"


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def people(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[People]:
    """Организация: org_admin, руководитель отдела, уходящий менеджер и получатель — все с
    подтверждённым email и паролем PASSWORD (вход — через API). После теста всё удаляется по
    org_id: таблицы общие для всего запуска, а sent_notifications без внешнего ключа на users."""
    org_id, team_id = uuid.uuid4(), uuid.uuid4()
    ids = {name: uuid.uuid4() for name in ("admin", "lead", "leaving", "heir")}
    roles = {
        "admin": Role.ORG_ADMIN,
        "lead": Role.TEAM_LEAD,
        "leaving": Role.SALES_REP,
        "heir": Role.SALES_REP,
    }
    found = People(
        org_id,
        team_id,
        admin_id=ids["admin"],
        lead_id=ids["lead"],
        leaving_id=ids["leaving"],
        heir_id=ids["heir"],
        suffix=org_id.hex[:8],
    )
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Acceptance", slug=f"accept-{found.suffix}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Продажі"))
        await s.flush()
        for name, user_id in ids.items():
            s.add(
                User(
                    id=user_id,
                    email=found.email(name),
                    password_hash=await password_hash(),
                    email_verified_at=datetime.now(UTC),
                    org_id=org_id,
                    role=roles[name],
                    team_id=None if name == "admin" else team_id,
                )
            )
        await s.commit()
    try:
        yield found
    finally:
        async with sessions() as s:
            await s.execute(delete(AuditEntry).where(AuditEntry.org_id == org_id))
            # До outbox и job: строки находятся по ним (если тест упал на failed_tasks).
            await s.execute(delete(FailedTask).where(own_failed(org_id)))
            # События и команды организации: иначе их подобрал бы relay чужого теста.
            await s.execute(delete(OutboxEvent).where(OutboxEvent.org_id == org_id))
            await s.execute(
                delete(SentNotification).where(SentNotification.user_id.in_(ids.values()))
            )
            await s.execute(delete(BulkJob).where(BulkJob.org_id == org_id))  # чанки — каскадом
            await s.execute(delete(Contact).where(Contact.org_id == org_id))
            await s.execute(delete(User).where(User.org_id == org_id))  # привязки — каскадом
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()


@pytest.fixture
async def real_client(
    settings: Settings, rustfs: RustFs, storage: S3FileStorage
) -> AsyncIterator[AsyncClient]:
    """Приложение без подмен: настоящие сессии (коммиты) и TaskQueue в Redis."""
    app: FastAPI = create_app(settings.model_copy(update=s3_settings(rustfs)))
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
async def relay(
    sessions: async_sessionmaker[AsyncSession], postgres_url: str
) -> AsyncIterator[Relay]:
    """Настоящий relay (LISTEN/NOTIFY + опрос) в задаче этого loop, публикует в Redis воркера."""
    found = Relay(
        sessions,
        CeleryPublisher(celery_app),
        listen_dsn=listen_dsn(postgres_url),
        batch_size=100,
        poll_seconds=0.5,
    )
    task = asyncio.create_task(found.run())
    try:
        yield found
    finally:
        found.stop()
        await asyncio.wait_for(task, WAIT_SECONDS)


@pytest.fixture
def small_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    # plan выполняет воркер — размер чанка из его настроек.
    monkeypatch.setattr(
        runtime,
        "_settings",
        runtime.current_settings().model_copy(update={"bulk_chunk_size": CHUNK_SIZE}),
    )


def chat_id() -> int:
    return 3_000_000 + uuid.uuid4().int % 1_000_000_000


async def wait_for(check: Callable[[], Awaitable[bool]], what: str) -> None:
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while not await check():
        assert asyncio.get_running_loop().time() < deadline, f"не дождались: {what}"
        await asyncio.sleep(0.2)


async def link(sessions: async_sessionmaker[AsyncSession], *users: uuid.UUID) -> list[int]:
    chats = [chat_id() for _ in users]
    async with sessions() as s:
        for user_id, chat in zip(users, chats, strict=True):
            s.add(TelegramLink(user_id=user_id, chat_id=chat))
        await s.commit()
    return chats


async def add_contacts(
    sessions: async_sessionmaker[AsyncSession], people: People, owner_id: uuid.UUID
) -> None:
    async with sessions() as s:
        await s.execute(
            insert(Contact),
            [
                {
                    "org_id": people.org_id,
                    "team_id": people.team_id,
                    "owner_id": owner_id,
                    "first_name": f"Клієнт {i}",
                    "email": f"c{i}-{uuid.uuid4().hex[:8]}@example.com",
                }
                for i in range(CONTACTS)
            ],
        )
        await s.commit()


async def reassign(
    client: AsyncClient, author_email: str, people: People, to_user_id: uuid.UUID
) -> uuid.UUID:
    headers = bearer(await login(client, author_email))
    resp = await client.post(
        "/api/v1/bulk/reassign-owner",
        json={"from_user_id": str(people.leaving_id), "to_user_id": str(to_user_id)},
        headers=headers | {"Idempotency-Key": f"accept-{uuid.uuid4()}"},
    )
    assert resp.status_code == 202, resp.text
    return uuid.UUID(resp.json()["id"])


async def finished_job(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> BulkJob:
    job: BulkJob | None = None

    async def finished() -> bool:
        nonlocal job
        async with sessions() as s:
            job = await s.get(BulkJob, job_id)
        return job is not None and job.finished_at is not None

    await wait_for(finished, "финал job")
    assert job is not None
    return job


@dataclass(frozen=True)
class Outbox:
    """Что лежит в outbox организации: события bulk_job.completed этой job и команды
    notifier (канал, user_id, ключ дедупа), неопубликованных строк."""

    completed: list[int]
    commands: list[tuple[str, str, str]]
    unpublished: int


async def outbox(
    sessions: async_sessionmaker[AsyncSession], people: People, job_id: uuid.UUID
) -> Outbox:
    async with sessions() as s:
        rows = (
            await s.execute(
                select(
                    OutboxEvent.id,
                    OutboxEvent.event_type,
                    OutboxEvent.payload,
                    OutboxEvent.published_at,
                )
                .where(OutboxEvent.org_id == people.org_id)
                .order_by(OutboxEvent.id)
            )
        ).all()
    completed = [
        r.id
        for r in rows
        if r.event_type == "bulk_job.completed" and r.payload["job_id"] == str(job_id)
    ]
    commands = [
        ("email" if r.event_type == "email.send" else "telegram", r.payload["user_id"],
         r.payload["dedup_key"])
        for r in rows
        if r.event_type in ("email.send", "telegram.send")
    ]  # fmt: skip
    return Outbox(completed, commands, sum(1 for r in rows if r.published_at is None))


async def sent_count(sessions: async_sessionmaker[AsyncSession], keys: set[str]) -> int:
    async with sessions() as s:
        found = await s.scalar(select(func.count()).where(SentNotification.dedup_key.in_(keys)))
    return int(found or 0)


async def audit_count(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> int:
    async with sessions() as s:
        found = await s.scalar(
            select(func.count())
            .select_from(AuditEntry)
            .where(AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == str(job_id))
        )
    return int(found or 0)


def own_failed(org_id: uuid.UUID) -> ColumnElement[bool]:
    """Строки failed_tasks задач этой организации: первый аргумент задач bulk — id job, задач
    notifications — id строки outbox (события или команды)."""
    first = FailedTask.args[0].astext
    jobs = select(BulkJob.id.cast(String)).where(BulkJob.org_id == org_id)
    events = select(OutboxEvent.id.cast(String)).where(OutboxEvent.org_id == org_id)
    return or_(
        FailedTask.task_name.startswith("bulk.") & first.in_(jobs),
        FailedTask.task_name.startswith("notifications.") & first.in_(events),
    )


async def failed_tasks(
    sessions: async_sessionmaker[AsyncSession], people: People
) -> list[tuple[str, str]]:
    async with sessions() as s:
        rows = await s.execute(
            select(FailedTask.task_name, FailedTask.exc_type).where(own_failed(people.org_id))
        )
    return [(r.task_name, r.exc_type) for r in rows]


async def broker_idle(broker_url: str) -> bool:
    """Задач нет ни в очередях, ни у воркера: task_acks_late — выполняемая задача лежит в hash
    unacked до конца выполнения, и задачи, которые она поставит, — уже в очереди."""
    redis = Redis.from_url(broker_url)
    try:
        queued = [await redis.llen(q) for q in ("default", "bulk", "notifications")]
        return sum(queued) == 0 and int(await redis.hlen("unacked")) == 0
    finally:
        await redis.aclose()


async def settle(
    sessions: async_sessionmaker[AsyncSession],
    people: People,
    job_id: uuid.UUID,
    broker_url: str,
    commands: int,
) -> Outbox:
    """Дождаться, пока цепочка доработает: в outbox столько команд, всё опубликовано и брокер
    пуст — два раза подряд (между BRPOP воркера и записью в unacked есть короткое окно)."""
    calm = 0

    async def quiet() -> bool:
        nonlocal calm
        state = await outbox(sessions, people, job_id)
        done = len(state.commands) >= commands and state.unpublished == 0
        calm = calm + 1 if done and await broker_idle(broker_url) else 0
        return calm >= 2

    await wait_for(quiet, f"{commands} команд в outbox, всё опубликовано и выполнено")
    return await outbox(sessions, people, job_id)


@pytest.mark.worker
@pytest.mark.committing
async def test_reassign_gives_one_email_and_one_message_each(
    celery_worker: WorkController,
    small_chunks: None,
    real_client: AsyncClient,
    relay: Relay,
    sessions: async_sessionmaker[AsyncSession],
    people: People,
    settings: Settings,
    mailpit: Mailpit,
    telegram_api: FakeTelegram,
) -> None:
    admin_email, heir_email = people.email("admin"), people.email("heir")
    admin_chat, heir_chat = await link(sessions, people.admin_id, people.heir_id)
    await add_contacts(sessions, people, people.leaving_id)

    job_id = await reassign(real_client, admin_email, people, people.heir_id)

    job = await finished_job(sessions, job_id)
    assert (job.status, job.processed, job.total, job.chunks_total) == ("done", 5, 5, 3)
    first = await settle(sessions, people, job_id, settings.celery_broker_url, commands=4)

    # Одно событие на job (не на чанк и не на строку) → четыре команды: два человека × два канала.
    assert len(first.completed) == 1
    subject = f"bulk_job.completed:job:{job_id}"
    keys = {
        f"{subject}:{user_id}:{channel}"
        for user_id in (people.admin_id, people.heir_id)
        for channel in ("email", "telegram")
    }
    assert {key for _, _, key in first.commands} == keys
    assert len(first.commands) == 4
    assert await sent_count(sessions, keys) == 4
    assert await failed_tasks(sessions, people) == []

    # Telegram: ровно два sendMessage — итог в чат автора, «вам передано» в чат получателя.
    assert sorted(chat for chat, _, _ in telegram_api.sent) == sorted([admin_chat, heir_chat])
    [summary] = telegram_api.texts(admin_chat)
    assert summary == (
        "Масову операцію «Передача клієнтів» завершено: виконано.\n"
        "Оброблено записів: 5 з 5.\nЗмінено: контакти — 5.\n"
        "Помилок у рядках: 0, невдалих частин: 0."
    )
    assert telegram_api.texts(heir_chat) == [
        f"Вам передано клієнтів: 5 (контакти — 5).\nПередав: {admin_email}."
    ]

    # Почта: ровно одно письмо каждому, с тем же текстом.
    [to_admin] = await read_emails(mailpit, admin_email)
    [to_heir] = await read_emails(mailpit, heir_email)
    assert to_admin["Subject"] == SUMMARY_SUBJECT
    assert "Оброблено записів: 5 з 5." in to_admin["Text"]
    assert to_heir["Subject"] == TRANSFER_SUBJECT
    assert "Вам передано клієнтів: 5 (контакти — 5)." in to_heir["Text"]

    # Повторный finalize: копия сообщения чанка на воркере (claim → None → finalize) и прямой
    # вызов — job уже done, финальный переход второй раз не случается. Воркер в потоке этого
    # процесса — capture_logs видит и его строки (кэша логгеров нет, logging.py).
    with capture_logs() as logs:
        celery_app.send_task("bulk.chunk", args=[str(job_id), 0])
        async with sessions() as s:
            bulk = BulkService(UnitOfWork(s), CATALOG, FakeTaskQueue(), chunk_size=CHUNK_SIZE)
            assert await bulk.finalize_if_complete(job_id) is None
        again = await settle(sessions, people, job_id, settings.celery_broker_url, commands=4)
    assert again == first  # ни события, ни команд
    assert await audit_count(sessions, job_id) == 1
    # Копия дошла до claim → None и ушла в finalize, а не упала до него.
    assert [
        (e["job_id"], e["no"]) for e in logs if e["event"] == "bulk_chunk_already_finished"
    ] == [(str(job_id), 0)]
    assert not [e for e in logs if e["event"] == "bulk_chunk_done"]
    assert await failed_tasks(sessions, people) == []

    # Повторная публикация того же события: relay отдаёт строку ещё раз, и ещё одна доставка
    # notify — команды пишутся заново, но с теми же ключами, отправители их пропускают.
    [event_id] = first.completed
    async with sessions() as s:
        await s.execute(
            update(OutboxEvent).where(OutboxEvent.id == event_id).values(published_at=None)
        )
        await s.commit()
    with capture_logs() as logs:
        celery_app.send_task("notifications.notify", args=[event_id])
        repeated = await settle(sessions, people, job_id, settings.celery_broker_url, commands=12)

    assert repeated.completed == [event_id]
    assert len(repeated.commands) == 12
    assert {key for _, _, key in repeated.commands} == keys
    assert await sent_count(sessions, keys) == 4
    assert len(telegram_api.sent) == 2
    assert len(await read_emails(mailpit, admin_email)) == 1
    assert len(await read_emails(mailpit, heir_email)) == 1
    assert await failed_tasks(sessions, people) == []
    # Восемь лишних команд (две доставки notify × 4) отправители пропустили по ключу дедупа —
    # каждую своей строкой лога, — а не упали: упавшая без ретрая тоже ничего бы не отправила.
    duplicates = sorted(
        (e["event"], e["dedup_key"])
        for e in logs
        if e["event"] in ("email_duplicate", "telegram_duplicate")
    )
    channel = {"email": "email_duplicate", "telegram": "telegram_duplicate"}
    assert duplicates == sorted(
        (channel[key.rsplit(":", 1)[1]], key) for key in keys for _ in range(2)
    )


@pytest.mark.worker
@pytest.mark.committing
async def test_author_who_takes_the_clients_gets_only_summary(
    celery_worker: WorkController,
    small_chunks: None,
    real_client: AsyncClient,
    relay: Relay,
    sessions: async_sessionmaker[AsyncSession],
    people: People,
    settings: Settings,
    mailpit: Mailpit,
    telegram_api: FakeTelegram,
) -> None:
    # Руководитель забирает клиентов ушедшего менеджера себе: автор и получатель — один
    # человек, и он получает одно сообщение — итог (раздел 16, notifier схлопывает повторы).
    lead_email = people.email("lead")
    [lead_chat] = await link(sessions, people.lead_id)
    await add_contacts(sessions, people, people.leaving_id)

    job_id = await reassign(real_client, lead_email, people, people.lead_id)

    assert (await finished_job(sessions, job_id)).status == "done"
    state = await settle(sessions, people, job_id, settings.celery_broker_url, commands=2)
    assert [(channel, user) for channel, user, _ in state.commands] == [
        ("telegram", str(people.lead_id)),
        ("email", str(people.lead_id)),
    ]
    [text] = telegram_api.texts(lead_chat)
    assert text.startswith("Масову операцію «Передача клієнтів» завершено: виконано.")
    assert len(telegram_api.sent) == 1
    [letter] = await read_emails(mailpit, lead_email)
    assert letter["Subject"] == SUMMARY_SUBJECT
    assert await failed_tasks(sessions, people) == []
