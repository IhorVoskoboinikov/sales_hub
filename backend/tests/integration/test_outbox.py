"""Outbox и relay на настоящем Postgres (разделы 6, 10 и 15, «Как работает relay»).

publish_batch проверяется в транзакции теста с поддельным издателем. Цикл relay, NOTIFY,
опрос и SKIP LOCKED — в тестах committing: NOTIFY доставляется только после настоящего коммита,
а relay читает своими соединениями. После них таблица очищается (TRUNCATE). Путь
relay → Redis → воркер проверяет тест с настоящим воркером (фикстура celery_worker).
"""

import asyncio
import queue
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import pytest
import structlog
from celery.worker import WorkController
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.context import request_id_var
from app.db.session import create_engine, create_session_factory
from app.events import relay as relay_module
from app.events.relay import CeleryPublisher, Publisher, Relay, UnknownEventType, publish_batch
from app.events.routes import ROUTES
from app.models import OutboxEvent
from app.uow import UnitOfWork
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from tests.conftest import CLOSED_PORT

EVENT = "tests.event"  # тип только для тестов: маршрут ставит фикстура routes
TASK = "tests.outbox_received"
WAIT_SECONDS = 15
NOTIFY_SECONDS = 1.0  # NOTIFY будит relay сразу; опрос в этих тестах — раз в 30 с
SLOW_POLL_SECONDS = 30.0


@pytest.fixture(autouse=True)
def routes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(ROUTES, EVENT, TASK)


@dataclass
class Published:
    task_name: str
    event_id: int
    request_id: str | None


@dataclass
class FakePublisher:
    """Ничего не ставит в Celery, а запоминает, что и с каким request_id опубликовано.
    fail — бросить на публикации; received — для тестов, которые ждут событие из цикла relay."""

    fail: Exception | None = None
    published: list[Published] = field(default_factory=list)
    received: asyncio.Queue[Published] = field(default_factory=asyncio.Queue)

    async def publish(self, task_name: str, event_id: int) -> None:
        if self.fail is not None:
            raise self.fail
        item = Published(task_name, event_id, request_id_var.get())
        self.published.append(item)
        self.received.put_nowait(item)


def add_event(uow: UnitOfWork, request_id: str | None = None, **payload: object) -> OutboxEvent:
    token = request_id_var.set(request_id)
    try:
        return uow.outbox.add(EVENT, payload)
    finally:
        request_id_var.reset(token)


async def published_at(session: AsyncSession, ids: list[int]) -> dict[int, bool]:
    rows = await session.execute(
        select(OutboxEvent.id, OutboxEvent.published_at.is_not(None)).where(OutboxEvent.id.in_(ids))
    )
    return {event_id: bool(published) for event_id, published in rows}


# --- publish_batch в транзакции теста


async def test_add_keeps_request_id_and_plain_payload(uow: UnitOfWork) -> None:
    org_id = uuid.uuid4()
    token = request_id_var.set("req-1")
    try:
        event = uow.outbox.add(EVENT, {"user_id": org_id}, org_id=org_id)
    finally:
        request_id_var.reset(token)
    await uow.commit()

    saved = await uow.outbox.get(event.id)
    assert saved is not None
    assert saved.request_id == "req-1"
    assert saved.org_id == org_id
    assert saved.payload == {"user_id": str(org_id)}  # UUID — строкой: jsonb
    assert saved.created_at is not None
    assert saved.published_at is None


async def test_publish_batch_publishes_in_order_and_marks(
    uow: UnitOfWork, db_session: AsyncSession
) -> None:
    events = [add_event(uow, f"req-{i}") for i in range(3)]
    events.append(add_event(uow))  # событие без запроса — например, из задачи beat
    await uow.commit()
    ids = [event.id for event in events]
    publisher = FakePublisher()

    assert await publish_batch(db_session, publisher, batch_size=100) == 4

    assert publisher.published == [
        Published(TASK, ids[0], "req-0"),
        Published(TASK, ids[1], "req-1"),
        Published(TASK, ids[2], "req-2"),
        Published(TASK, ids[3], None),
    ]
    assert await published_at(db_session, ids) == dict.fromkeys(ids, True)
    assert request_id_var.get() is None  # request_id события не остался в контексте relay
    assert await publish_batch(db_session, publisher, batch_size=100) == 0  # второй раз — нечего


async def test_publish_batch_takes_oldest_up_to_batch_size(
    uow: UnitOfWork, db_session: AsyncSession
) -> None:
    events = [add_event(uow) for _ in range(3)]
    await uow.commit()
    ids = [event.id for event in events]
    publisher = FakePublisher()

    assert await publish_batch(db_session, publisher, batch_size=2) == 2

    assert [item.event_id for item in publisher.published] == ids[:2]
    assert await published_at(db_session, ids) == {ids[0]: True, ids[1]: True, ids[2]: False}


async def test_empty_outbox_publishes_nothing(db_session: AsyncSession) -> None:
    publisher = FakePublisher()

    assert await publish_batch(db_session, publisher, batch_size=100) == 0
    assert publisher.published == []


async def test_publisher_failure_rolls_back_batch(
    uow: UnitOfWork, db_session: AsyncSession
) -> None:
    events = [add_event(uow, "req-1") for _ in range(2)]
    await uow.commit()
    ids = [event.id for event in events]
    publisher = FakePublisher(fail=ConnectionRefusedError("redis:6379"))

    with pytest.raises(ConnectionRefusedError):
        await publish_batch(db_session, publisher, batch_size=100)

    assert await published_at(db_session, ids) == dict.fromkeys(ids, False)
    assert request_id_var.get() is None

    publisher.fail = None  # брокер поднялся — следующий батч публикует те же события
    assert await publish_batch(db_session, publisher, batch_size=100) == 2


async def test_unknown_event_type_stops_batch_before_publishing(
    uow: UnitOfWork, db_session: AsyncSession
) -> None:
    known = add_event(uow)
    unknown = uow.outbox.add("tests.no_route", {})
    await uow.commit()
    ids = [known.id, unknown.id]  # после отката объекты сессии «протухают»
    publisher = FakePublisher()

    with pytest.raises(UnknownEventType, match=r"tests\.no_route"):
        await publish_batch(db_session, publisher, batch_size=100)

    # Соседнее событие с маршрутом тоже не ушло: маршруты проверяются до первой публикации.
    assert publisher.published == []
    assert await published_at(db_session, ids) == dict.fromkeys(ids, False)


# --- Цикл relay на настоящих транзакциях


@pytest.fixture
async def sessions(postgres_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(postgres_url)
    try:
        yield create_session_factory(engine)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE outbox_events"))
        await engine.dispose()


async def insert_event(
    sessions: async_sessionmaker[AsyncSession], request_id: str | None = None
) -> int:
    async with sessions() as session:
        uow = UnitOfWork(session)
        event = add_event(uow, request_id)
        await uow.commit()
        return event.id


def make_relay(
    sessions: async_sessionmaker[AsyncSession],
    postgres_url: str,
    publisher: Publisher,
    *,
    poll_seconds: float = SLOW_POLL_SECONDS,
    listen_dsn: str | None = None,
    reconnect_seconds: float = relay_module.RECONNECT_SECONDS,
) -> Relay:
    return Relay(
        sessions,
        publisher,
        listen_dsn=listen_dsn or relay_module.listen_dsn(postgres_url),
        batch_size=100,
        poll_seconds=poll_seconds,
        reconnect_seconds=reconnect_seconds,
    )


@asynccontextmanager
async def running(relay: Relay) -> AsyncIterator[asyncio.Task[None]]:
    task = asyncio.create_task(relay.run())
    try:
        yield task
    finally:
        relay.stop()
        await asyncio.wait_for(task, WAIT_SECONDS)


async def receive(publisher: FakePublisher, within: float = WAIT_SECONDS) -> Published:
    return await asyncio.wait_for(publisher.received.get(), within)


async def wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, what
        await asyncio.sleep(0.05)


@pytest.mark.committing
async def test_notify_wakes_relay_without_polling(
    sessions: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    publisher = FakePublisher()
    relay = make_relay(sessions, postgres_url, publisher)
    async with running(relay):
        # Первое событие доказывает, что relay запущен и прошёл первый батч…
        first = await insert_event(sessions)
        assert (await receive(publisher)).event_id == first
        assert relay.listener is not None

        # …второе приходит, пока relay ждёт: опрос — через 30 с, будит только NOTIFY.
        second = await insert_event(sessions, "req-notify")
        assert await receive(publisher, NOTIFY_SECONDS) == Published(TASK, second, "req-notify")


@pytest.mark.committing
async def test_polling_works_without_listen(
    sessions: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    publisher = FakePublisher()
    relay = make_relay(
        sessions,
        postgres_url,
        publisher,
        poll_seconds=0.2,
        listen_dsn=f"postgresql://test:test@127.0.0.1:{CLOSED_PORT}/test",  # LISTEN не подключится
    )
    async with running(relay):
        event_id = await insert_event(sessions)

        assert (await receive(publisher, 2)).event_id == event_id
        assert relay.listener is None


@pytest.mark.committing
async def test_relay_listens_again_after_connection_is_killed(
    sessions: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    publisher = FakePublisher()
    relay = make_relay(sessions, postgres_url, publisher, reconnect_seconds=0.1)
    async with running(relay):
        await insert_event(sessions)
        await receive(publisher)
        old_pid = relay.listener.get_server_pid()

        async with sessions() as session:  # как рестарт Postgres для одного соединения
            await session.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": old_pid})

        await wait_until(
            lambda: relay.listener is not None and relay.listener.get_server_pid() != old_pid,
            "relay не подписался на NOTIFY заново",
        )
        event_id = await insert_event(sessions)
        assert (await receive(publisher, NOTIFY_SECONDS)).event_id == event_id


@pytest.mark.committing
async def test_relay_backs_off_and_recovers_after_publish_error(
    sessions: async_sessionmaker[AsyncSession],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(relay_module, "BACKOFF_START_SECONDS", 0.1)
    event_id = await insert_event(sessions)
    publisher = FakePublisher(fail=ConnectionRefusedError("redis:6379"))
    relay = make_relay(sessions, postgres_url, publisher)

    with structlog.testing.capture_logs() as logs:
        async with running(relay):
            await wait_until(
                lambda: any(log["event"] == "outbox_publish_failed" for log in logs),
                "ошибка публикации не записана в лог",
            )
            publisher.fail = None  # брокер поднялся: повтор после паузы, без NOTIFY
            assert (await receive(publisher)).event_id == event_id

    failure = next(log for log in logs if log["event"] == "outbox_publish_failed")
    assert failure["log_level"] == "error"
    assert "ConnectionRefusedError" in failure["error"]


@pytest.mark.committing
async def test_stop_lets_current_batch_finish(
    sessions: async_sessionmaker[AsyncSession], postgres_url: str
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    class SlowPublisher(FakePublisher):
        async def publish(self, task_name: str, event_id: int) -> None:
            started.set()
            await release.wait()
            await super().publish(task_name, event_id)

    event_id = await insert_event(sessions)
    publisher = SlowPublisher()
    relay = make_relay(sessions, postgres_url, publisher)
    task = asyncio.create_task(relay.run())
    try:
        await asyncio.wait_for(started.wait(), WAIT_SECONDS)
        relay.stop()  # SIGTERM посреди батча
        await asyncio.sleep(0.1)
        assert not task.done()  # батч не прерван…
    finally:
        release.set()
        await asyncio.wait_for(task, WAIT_SECONDS)  # …доработал, и relay вышел

    assert [item.event_id for item in publisher.published] == [event_id]
    async with sessions() as session:
        assert await published_at(session, [event_id]) == {event_id: True}


@pytest.mark.committing
async def test_stop_during_listen_skips_next_batch(
    sessions: async_sessionmaker[AsyncSession],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connecting, release = asyncio.Event(), asyncio.Event()

    async def slow_listen() -> None:  # как asyncpg.connect к медленному Postgres
        connecting.set()
        await release.wait()

    event_id = await insert_event(sessions)
    publisher = FakePublisher()
    relay = make_relay(sessions, postgres_url, publisher)
    monkeypatch.setattr(relay, "_listen", slow_listen)
    task = asyncio.create_task(relay.run())
    try:
        await asyncio.wait_for(connecting.wait(), WAIT_SECONDS)
        relay.stop()  # SIGTERM, пока relay подключается к LISTEN
    finally:
        release.set()
        await asyncio.wait_for(task, WAIT_SECONDS)

    assert publisher.published == []  # батча после stop() не было
    async with sessions() as session:
        assert await published_at(session, [event_id]) == {event_id: False}


@pytest.mark.committing
async def test_concurrent_batches_do_not_publish_same_event(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    ids = [await insert_event(sessions) for _ in range(10)]
    claimed, release = asyncio.Event(), asyncio.Event()

    class HoldingPublisher(FakePublisher):
        async def publish(self, task_name: str, event_id: int) -> None:
            claimed.set()  # строки батча уже заблокированы
            await release.wait()
            await super().publish(task_name, event_id)

    first, second = HoldingPublisher(), FakePublisher()
    async with sessions() as first_session, sessions() as second_session:
        holding = asyncio.create_task(publish_batch(first_session, first, batch_size=4))
        await asyncio.wait_for(claimed.wait(), WAIT_SECONDS)

        # Второй relay не ждёт коммита первого (SKIP LOCKED) и берёт остальные строки.
        assert await asyncio.wait_for(publish_batch(second_session, second, 100), 5) == 6
        release.set()
        assert await holding == 4

    first_ids = [item.event_id for item in first.published]
    second_ids = [item.event_id for item in second.published]
    assert first_ids == ids[:4]
    assert second_ids == ids[4:]


# --- relay → Redis → настоящий воркер

worker_received: queue.Queue[tuple[int, str | None]] = queue.Queue()


@celery_app.task(base=BaseTask, name=TASK)
def outbox_received(event_id: int) -> None:
    # Воркер в потоке этого процесса: результат — через очередь в памяти, без result backend.
    worker_received.put((event_id, request_id_var.get()))


@pytest.fixture
def worker_queue() -> Iterator[queue.Queue[tuple[int, str | None]]]:
    while not worker_received.empty():
        worker_received.get_nowait()
    yield worker_received


@pytest.mark.worker
@pytest.mark.committing
async def test_relay_delivers_event_to_worker(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    postgres_url: str,
    worker_queue: queue.Queue[tuple[int, str | None]],
) -> None:
    relay = make_relay(sessions, postgres_url, CeleryPublisher(celery_app))
    async with running(relay):
        event_id = await insert_event(sessions, "req-outbox-e2e")

        # Задача получила id события, а request_id запроса, записавшего событие, дошёл до неё
        # через колонку outbox и заголовки задачи.
        received = await asyncio.to_thread(worker_queue.get, timeout=WAIT_SECONDS)
        assert received == (event_id, "req-outbox-e2e")

    async with sessions() as session:
        assert await published_at(session, [event_id]) == {event_id: True}
