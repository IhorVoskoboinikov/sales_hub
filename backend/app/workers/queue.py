"""TaskQueue — постановка задачи из API после коммита (раздел 6, «Как API ставит задачи»).

Строка bulk_jobs — сама себе outbox: API после коммита ставит bulk.plan best-effort, без
ретраев публикации. Брокер недоступен — ошибка только в лог (task_enqueue_failed), клиент всё
равно получает 202: job в pending подберёт bulk.reap_stuck (шаг 5.5). Задачи ставятся по имени
(send_task): API модули задач не импортирует. В тестах API порт подменяется фейком.
"""

import asyncio
from typing import Any, Protocol

import structlog
from celery import Celery

logger = structlog.get_logger("workers")


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
