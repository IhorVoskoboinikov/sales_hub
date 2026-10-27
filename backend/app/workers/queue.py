"""Постановка задач Celery по имени (send_task) — порты для API и сервисов.

TaskQueue — постановка задачи из API после коммита (раздел 6, «Как API ставит задачи»).
Строка bulk_jobs — сама себе outbox: API после коммита ставит bulk.plan best-effort, без
ретраев публикации. Брокер недоступен — ошибка только в лог (task_enqueue_failed), клиент всё
равно получает 202: job в pending подберёт bulk.reap_stuck. Задачи ставятся по имени
(send_task): API модули задач не импортирует. В тестах API порт подменяется фейком.

TaskSender — перезапуск упавшей задачи супер-админом (раздел 15, «Задачи, упавшие после всех
ретраев»): здесь ошибка брокера — не лог, а 503, и нужен id новой задачи.

broker_queue_length — длина очереди в брокере (LLEN в db 1): reaper не переотправляет чанки,
пока в очереди bulk есть сообщения.
"""

import asyncio
from functools import cache
from typing import Any, Protocol

import structlog
from celery import Celery
from redis.asyncio import Redis

from app.errors import ServiceUnavailable

logger = structlog.get_logger("workers")

# Как у брокера Celery (broker_transport_options): зависший Redis не держит задачу вечно.
BROKER_SOCKET_TIMEOUT = 5


class TaskQueue(Protocol):
    async def enqueue(self, name: str, *args: Any) -> None: ...


class CeleryTaskQueue:
    def __init__(self, celery_app: Celery) -> None:
        self.celery_app = celery_app

    async def enqueue(self, name: str, *args: Any) -> None:
        """Поставить задачу name(*args). Никогда не бросает: потерю подберёт reaper.

        send_task синхронный, поэтому — в потоке (контекст с request_id копируется туда, и
        before_task_publish кладёт его в заголовки задачи). retry=False: при недоступном Redis
        ошибка приходит после попыток подключения kombu, а не после ретраев публикации."""
        try:
            await asyncio.to_thread(self.celery_app.send_task, name, args=list(args), retry=False)
        except Exception as exc:
            logger.error("task_enqueue_failed", task=name, args=list(args), error=repr(exc))


class TaskSender(Protocol):
    def replayable(self, name: str) -> bool | None:
        """None — такой задачи нет среди зарегистрированных; False — её нельзя перезапускать
        руками (replayable=False у BaseTask: bulk и импорт)."""
        ...

    async def send(self, name: str, args: list[Any], kwargs: dict[str, Any]) -> str:
        """Поставить задачу и вернуть её id. Брокер недоступен — ServiceUnavailable."""
        ...


@cache
def _load_task_modules(celery_app: Celery) -> None:
    # API модули задач сам не импортирует (ставит по имени), а без импорта задачи не
    # зарегистрированы в celery_app.tasks. Модули — из include в celery_app.py, один раз.
    celery_app.loader.import_default_modules()


class CeleryTaskSender:
    def __init__(self, celery_app: Celery) -> None:
        self.celery_app = celery_app

    def replayable(self, name: str) -> bool | None:
        _load_task_modules(self.celery_app)
        task = self.celery_app.tasks.get(name)
        if task is None:
            return None
        # Встроенные задачи Celery (celery.*) — не BaseTask, атрибута нет: не перезапускаем.
        return bool(getattr(task, "replayable", False))

    async def send(self, name: str, args: list[Any], kwargs: dict[str, Any]) -> str:
        """send_task в потоке (контекст с request_id копируется, before_task_publish кладёт его
        в заголовки), retry=False: при недоступном Redis ошибка придёт после попыток
        подключения kombu, а не после ретраев публикации."""
        try:
            result = await asyncio.to_thread(
                self.celery_app.send_task, name, args=args, kwargs=kwargs, retry=False
            )
        except Exception as exc:
            logger.error("task_send_failed", task=name, error=repr(exc))
            raise ServiceUnavailable() from exc
        return str(result.id)


async def broker_queue_length(broker_url: str, queue: str) -> int:
    """Сколько сообщений ждёт в очереди: у транспорта Redis очередь — список с её именем
    (сообщения с приоритетом 0, у нас других нет). Взятые воркером, но не подтверждённые
    сообщения лежат в другом ключе (unacked) и сюда не входят."""
    redis = Redis.from_url(
        broker_url,
        socket_timeout=BROKER_SOCKET_TIMEOUT,
        socket_connect_timeout=BROKER_SOCKET_TIMEOUT,
    )
    try:
        return int(await redis.llen(queue))
    finally:
        await redis.aclose()
