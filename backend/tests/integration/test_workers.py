"""Задачи на настоящем воркере и Redis (раздел 10, «Celery в тестах»).

Воркер — в потоке этого же процесса, поэтому тестовые задачи ниже отдают результат через
queue.Queue в памяти: result backend не нужен. Задачи регистрируются при импорте модуля —
до старта воркера (фикстура celery_worker сессионная и стартует после сбора тестов).
"""

import asyncio
import queue
import time
from collections.abc import Iterator
from typing import Any

import pytest
import structlog
from celery.worker import WorkController
from redis import Redis
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.core.context import request_id_var
from app.workers import base
from app.workers.base import BaseTask, is_retryable
from app.workers.celery_app import celery_app
from app.workers.runtime import run_async
from app.workers.tasks.maintenance import HEARTBEAT_KEY, HEARTBEAT_TTL_SECONDS

WAIT_SECONDS = 15

seen_context: queue.Queue[dict[str, Any]] = queue.Queue()
attempts: queue.Queue[int] = queue.Queue()


async def _read_context() -> dict[str, Any]:
    # Внутри корутины: run_async запускает её в loop процесса с контекстом задачи.
    return {"request_id_var": request_id_var.get(), **structlog.contextvars.get_contextvars()}


@celery_app.task(base=BaseTask, name="tests.capture_context")
def capture_context() -> None:
    seen_context.put(run_async(_read_context()))


@celery_app.task(base=BaseTask, name="tests.flaky", bind=True)
def flaky(self: BaseTask, fail_times: int) -> None:
    attempts.put(self.request.retries)
    if self.request.retries < fail_times:
        raise ConnectionRefusedError("postgres:5432")  # как упавшая база


@pytest.fixture
def redis_db0(redis_url: str) -> Iterator[Redis]:
    client = Redis.from_url(redis_url)
    yield client
    client.close()


def wait_for[T](what: queue.Queue[T]) -> T:
    return what.get(timeout=WAIT_SECONDS)


@pytest.mark.worker
def test_heartbeat_sets_key(celery_worker: WorkController, redis_db0: Redis) -> None:
    redis_db0.delete(HEARTBEAT_KEY)
    started = int(time.time())

    celery_app.send_task("workers.heartbeat")

    deadline = time.monotonic() + WAIT_SECONDS
    while (value := redis_db0.get(HEARTBEAT_KEY)) is None:
        assert time.monotonic() < deadline, "воркер не выполнил workers.heartbeat"
        time.sleep(0.1)
    assert started <= int(value) <= int(time.time())
    assert 0 < redis_db0.ttl(HEARTBEAT_KEY) <= HEARTBEAT_TTL_SECONDS


@pytest.mark.worker
def test_request_and_user_reach_the_task(celery_worker: WorkController) -> None:
    token = request_id_var.set("req-from-api")
    try:
        with structlog.contextvars.bound_contextvars(user_id="user-1"):
            result = celery_app.send_task("tests.capture_context")
    finally:
        request_id_var.reset(token)

    seen = wait_for(seen_context)
    assert seen["request_id_var"] == "req-from-api"
    assert seen["request_id"] == "req-from-api"
    assert seen["user_id"] == "user-1"
    assert seen["task_id"] == result.id
    assert seen["task_name"] == "tests.capture_context"


@pytest.mark.worker
def test_task_without_request_gets_task_id(celery_worker: WorkController) -> None:
    result = celery_app.send_task("tests.capture_context")  # как задача beat

    seen = wait_for(seen_context)
    assert seen["request_id"] == result.id
    assert seen["request_id_var"] == result.id
    assert seen["user_id"] is None


@pytest.mark.worker
def test_temporary_failure_is_retried(
    celery_worker: WorkController, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(base, "backoff", lambda retries: 0.1)  # не ждать 2 + 4 секунды

    celery_app.send_task("tests.flaky", args=[2])

    # Две неудачи и успех на третьей попытке: номера повторов 0, 1, 2.
    assert [wait_for(attempts) for _ in range(3)] == [0, 1, 2]
    with pytest.raises(queue.Empty):
        attempts.get(timeout=1)  # после успеха повторов больше нет


@pytest.mark.worker
def test_retries_stop_after_max_retries(
    celery_worker: WorkController, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(base, "backoff", lambda retries: 0.1)

    celery_app.send_task("tests.flaky", args=[100])

    # Первая попытка и max_retries повторов — потом задача провалена.
    assert [wait_for(attempts) for _ in range(BaseTask.max_retries + 1)] == list(range(6))
    with pytest.raises(queue.Empty):
        attempts.get(timeout=1)


async def test_real_deadlock_is_retryable(postgres_url: str) -> None:
    """Две транзакции ждут друг друга: Postgres убивает одну с SQLSTATE 40P01, и is_retryable
    распознаёт настоящую ошибку asyncpg, а не только подделку из unit-тестов."""
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    lock = "SELECT pg_advisory_xact_lock(:key)"
    keys = (941_001, 941_002)  # не пересекаются с ключами приложения
    try:
        async with engine.connect() as first, engine.connect() as second:
            await first.execute(text(lock), {"key": keys[0]})
            await second.execute(text(lock), {"key": keys[1]})
            waiting = asyncio.create_task(first.execute(text(lock), {"key": keys[1]}))
            await asyncio.sleep(0.2)  # первая уже ждёт вторую
            results = await asyncio.gather(
                waiting, second.execute(text(lock), {"key": keys[0]}), return_exceptions=True
            )
    finally:
        await engine.dispose()

    errors = [r for r in results if isinstance(r, BaseException)]
    assert len(errors) == 1
    assert isinstance(errors[0], DBAPIError)
    assert getattr(errors[0].orig, "sqlstate", None) == "40P01"
    assert is_retryable(errors[0])
