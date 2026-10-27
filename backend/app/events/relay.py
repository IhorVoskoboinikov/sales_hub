"""Outbox relay (разделы 6 и 15, «Как работает relay»): публикует outbox_events в Celery.

Отдельный долгоживущий процесс, а не задача beat. Цикл: батч → если строк ровно batch_size,
сразу следующий, иначе ждать NOTIFY outbox_new не дольше poll_seconds. NOTIFY слушает своё
соединение asyncpg вне пула SQLAlchemy (иначе соединение с подпиской вернулось бы в пул);
пока его нет — после обрыва или при старте без Postgres, — работает только опрос.

Сначала публикация, потом отметка published_at: «как минимум один раз», дубли гасят
потребители (sent_notifications, шаг 5.3). Порядок событий не гарантируется.

Запуск: python -m app.events.relay
"""

import asyncio
import contextlib
import signal
import time
from typing import Any, Protocol

import asyncpg
import structlog
from celery import Celery
from sqlalchemy import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings, get_settings
from app.core.context import request_id_var
from app.core.logging import configure_logging
from app.db.session import create_engine, create_session_factory
from app.events.routes import ROUTES
from app.uow import UnitOfWork
from app.workers.celery_app import celery_app

logger = structlog.get_logger("events.relay")

CHANNEL = "outbox_new"  # его шлёт триггер outbox_events_notify (миграция fb3ea21ceb61)
# Ошибка публикации (брокер или база недоступны) — пауза 2 → 4 → … → 30 с, успех её сбрасывает.
BACKOFF_START_SECONDS = 2.0
BACKOFF_MAX_SECONDS = 30.0
# Подписка на NOTIFY не удалась — следующая попытка не раньше чем через столько секунд.
RECONNECT_SECONDS = 5.0
CONNECT_TIMEOUT_SECONDS = 10.0


class UnknownEventType(LookupError):
    """У типа события нет маршрута в ROUTES: событие записала версия кода новее, чем у relay,
    или тип добавили без маршрута (это ловит тест). Батч не публикуется, пока relay не
    обновят, — алерт OutboxLag (этап 8)."""


class Publisher(Protocol):
    """Куда relay отдаёт событие. В проде — CeleryPublisher, в тестах — свой (раздел 10)."""

    async def publish(self, task_name: str, event_id: int) -> None: ...


class CeleryPublisher:
    def __init__(self, app: Celery) -> None:
        self.app = app

    async def publish(self, task_name: str, event_id: int) -> None:
        # send_task блокирующий — в поток, чтобы не стоял loop (и с ним LISTEN). to_thread
        # копирует контекст: before_task_publish увидит request_id события. retry=False —
        # брокер недоступен, и ошибка приходит после попыток подключения kombu (≈ 6 с), а не
        # повторами внутри send_task: батч откатывается, паузу держит цикл relay. Брокер
        # завис (соединение есть, ответа нет) — ошибку через 5 с даёт socket_timeout
        # из broker_transport_options (celery_app.py).
        await asyncio.to_thread(self.app.send_task, task_name, args=[event_id], retry=False)


def route(event_type: str) -> str:
    try:
        return ROUTES[event_type]
    except KeyError:
        raise UnknownEventType(event_type) from None


async def publish_batch(session: AsyncSession, publisher: Publisher, batch_size: int) -> int:
    """Опубликовать до batch_size самых старых событий одной транзакцией; вернуть их число.

    Любая ошибка (брокер недоступен, тип без маршрута) — откат: строки остаются
    неопубликованными, их возьмёт следующий батч. Уже отданные в брокер события того же батча
    уйдут ещё раз — «как минимум один раз»."""
    uow = UnitOfWork(session)
    try:
        events = await uow.outbox.claim_batch(batch_size)
        # Маршруты — до первой публикации: событие без маршрута останавливает батч раньше,
        # чем его соседи уйдут в брокер (и потом второй раз).
        task_names = [route(event.event_type) for event in events]
        for event, task_name in zip(events, task_names, strict=True):
            # request_id запроса, который записал событие: его положат в заголовки задачи.
            token = request_id_var.set(event.request_id)
            try:
                await publisher.publish(task_name, event.id)
            finally:
                request_id_var.reset(token)
        if events:
            await uow.outbox.mark_published([event.id for event in events])
        await uow.commit()
    except BaseException:
        await uow.rollback()
        raise
    return len(events)


def listen_dsn(database_url: str) -> str:
    # asyncpg не понимает схему postgresql+asyncpg из настроек SQLAlchemy.
    url = make_url(database_url).set(drivername="postgresql")
    return url.render_as_string(hide_password=False)


class Relay:
    """Цикл публикации. stop() — мягкая остановка: текущий батч доработает, следующего не будет."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        publisher: Publisher,
        *,
        listen_dsn: str,
        batch_size: int,
        poll_seconds: float,
        reconnect_seconds: float = RECONNECT_SECONDS,
    ) -> None:
        self.sessions = sessions
        self.publisher = publisher
        self.listen_dsn = listen_dsn
        self.batch_size = batch_size
        self.poll_seconds = poll_seconds
        self.reconnect_seconds = reconnect_seconds
        self._wake = asyncio.Event()  # пришёл NOTIFY (или stop): не ждать конца опроса
        self._stopping = asyncio.Event()
        self._listener: Any = None  # asyncpg.Connection с LISTEN; None — работает только опрос
        self._reconnect_at = 0.0

    @property
    def listener(self) -> Any:
        """Соединение с подпиской на NOTIFY или None (для тестов и логов)."""
        return self._listener

    def stop(self) -> None:
        self._stopping.set()
        self._wake.set()

    async def run(self) -> None:
        delay = BACKOFF_START_SECONDS
        try:
            while not self._stopping.is_set():
                await self._listen()
                if self._stopping.is_set():  # stop() пришёл, пока подключались: батча нет
                    break
                # До батча: NOTIFY, пришедший во время батча, снова поставит флаг и не потеряется.
                self._wake.clear()
                try:
                    async with self.sessions() as session:
                        published = await publish_batch(session, self.publisher, self.batch_size)
                except Exception as exc:
                    logger.error("outbox_publish_failed", error=repr(exc), retry_in=delay)
                    await self._pause(delay)
                    delay = min(delay * 2, BACKOFF_MAX_SECONDS)
                    continue
                delay = BACKOFF_START_SECONDS
                if published:
                    logger.debug("outbox_published", count=published)
                if published < self.batch_size:  # полный батч — за ним, скорее всего, ещё
                    await self._wait_for_notify()
        finally:
            await self._unlisten()

    async def _wait_for_notify(self) -> None:
        if self._stopping.is_set():
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), self.poll_seconds)

    async def _pause(self, seconds: float) -> None:
        # Ждём только остановку, не NOTIFY: иначе каждое новое событие обрывало бы паузу.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)

    async def _listen(self) -> None:
        """Подписаться на NOTIFY, если подписки нет и пора пробовать снова."""
        if self._listener is not None or time.monotonic() < self._reconnect_at:
            return
        connection = None
        try:
            connection = await asyncpg.connect(self.listen_dsn, timeout=CONNECT_TIMEOUT_SECONDS)
            await connection.add_listener(CHANNEL, self._on_notify)
        except Exception as exc:
            if connection is not None:
                connection.terminate()
            self._reconnect_at = time.monotonic() + self.reconnect_seconds
            logger.warning("outbox_listen_failed", error=repr(exc), retry_in=self.reconnect_seconds)
            return
        connection.add_termination_listener(self._on_terminated)
        self._listener = connection
        logger.info("outbox_listening", channel=CHANNEL)

    async def _unlisten(self) -> None:
        connection, self._listener = self._listener, None
        if connection is None:
            return
        connection.remove_termination_listener(self._on_terminated)  # это не обрыв
        with contextlib.suppress(Exception):
            await connection.close(timeout=CONNECT_TIMEOUT_SECONDS)

    def _on_notify(self, connection: Any, pid: int, channel: str, payload: str) -> None:
        self._wake.set()

    def _on_terminated(self, connection: Any) -> None:
        # Postgres перезапустился или соединение убили: дальше опрос, пока не подпишемся снова.
        if connection is not self._listener:
            return
        self._listener = None
        logger.warning("outbox_listen_lost")
        self._wake.set()  # следующий круг цикла — сразу: переподключиться и проверить outbox


async def main(settings: Settings) -> None:
    configure_logging(settings.log_level, json=settings.log_json)
    # Свой engine, а не Resources: relay не нужны Redis, HTTP, OPA и файл ролей, а Resources
    # прочитал бы data.yaml и собрал SMTP-отправителя. Одно соединение пула (батч идёт по
    # одному) плюс LISTEN — те самые «relay 2» в бюджете соединений раздела 15.
    engine = create_engine(settings.database_url, pool_size=1, max_overflow=0)
    relay = Relay(
        create_session_factory(engine),
        CeleryPublisher(celery_app),
        listen_dsn=listen_dsn(settings.database_url),
        batch_size=settings.outbox_batch_size,
        poll_seconds=settings.outbox_poll_seconds,
    )
    loop = asyncio.get_running_loop()
    # SIGTERM — docker stop и Kubernetes, SIGINT — watchfiles и Ctrl+C.
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, relay.stop)
    logger.info(
        "outbox_relay_started",
        batch_size=settings.outbox_batch_size,
        poll_seconds=settings.outbox_poll_seconds,
    )
    try:
        await relay.run()
    finally:
        await engine.dispose()
    logger.info("outbox_relay_stopped")


if __name__ == "__main__":
    asyncio.run(main(get_settings()))
