"""Event loop и Resources процесса воркера (раздел 15, «Async внутри Celery»).

Celery вызывает обычные функции, а сервисы асинхронные. Один loop на процесс: loop и ресурсы
(пул Postgres, Redis, HTTP) создаются лениво, при первой задаче, и живут до конца процесса —
пул соединений переживает задачу. Поэтому только пул prefork (или solo в тестах), не gevent.
"""

import asyncio
import os
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

from celery.signals import worker_process_shutdown

from app.core.config import Settings, get_settings
from app.core.resources import Resources

# Пул на процесс: 10 процессов воркеров × 2 — в бюджете соединений раздела 15.
POOL_SIZE = 2
MAX_OVERFLOW = 0  # задача одна на процесс — больше двух соединений ей не нужно


@dataclass(frozen=True)
class Runtime:
    pid: int  # чей это loop: после fork ресурсы родителя не годятся
    loop: asyncio.AbstractEventLoop
    resources: Resources


_runtime: Runtime | None = None  # loop + Resources этого процесса
_settings: Settings | None = None  # configure() — для тестов; иначе get_settings()


def configure(settings: Settings) -> None:
    """Подменить настройки (тесты: адреса контейнеров). Вызывать до первой задачи."""
    global _settings
    _settings = settings


def current_settings() -> Settings:
    return _settings or get_settings()


def get_runtime() -> Runtime:
    # Лениво и по pid: worker_process_init срабатывает только в дочерних процессах prefork,
    # а не в тестовом воркере (pool=solo) и не в родительском процессе, где обрабатываются
    # сигналы. После fork ресурсы родителя не годятся — создаём свои.
    global _runtime
    if _runtime is None or _runtime.pid != os.getpid():
        # Сначала ресурсы, потом loop: create бросает (нет файла ролей) — ничего не создано и
        # не утекло, каждая следующая задача падает с той же понятной ошибкой.
        resources = Resources.create(
            current_settings(), pool_size=POOL_SIZE, max_overflow=MAX_OVERFLOW
        )
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _runtime = Runtime(os.getpid(), loop, resources)
    return _runtime


def resources() -> Resources:
    """Ресурсы этого процесса — для задач."""
    return get_runtime().resources


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    """Выполнить корутину в loop процесса и вернуть её результат."""
    loop = get_runtime().loop
    task = loop.create_task(coro)
    try:
        return loop.run_until_complete(task)
    except BaseException:  # SoftTimeLimitExceeded прерывает run_until_complete:
        task.cancel()  # без отмены корутина проснулась бы во время следующей задачи
        loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
        raise


def shutdown() -> None:
    """Закрыть ресурсы и loop процесса. Повторный вызов ничего не делает."""
    global _runtime
    runtime, _runtime = _runtime, None
    if runtime is None or runtime.pid != os.getpid():
        return
    try:
        runtime.loop.run_until_complete(runtime.resources.aclose())
    finally:
        runtime.loop.close()


@worker_process_shutdown.connect
def _shutdown_process(**_: Any) -> None:
    # Процесс пула завершается: вернуть соединения Postgres и Redis, а не бросить их.
    shutdown()
