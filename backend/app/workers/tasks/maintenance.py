"""Обслуживание: задачи beat (раздел 15, «Расписание beat»)."""

import time

from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from app.workers.runtime import resources, run_async

HEARTBEAT_KEY = "workers:heartbeat"
HEARTBEAT_TTL_SECONDS = 600


@celery_app.task(base=BaseTask, name="workers.heartbeat")
def heartbeat() -> None:
    """Отметка «beat и очередь default живы»: время последнего выполнения в Redis (db 0).
    Старше 3 минут — алерт CeleryBeatDown (этап 8)."""
    run_async(_heartbeat())


async def _heartbeat() -> None:
    await resources().redis.set(HEARTBEAT_KEY, int(time.time()), ex=HEARTBEAT_TTL_SECONDS)
