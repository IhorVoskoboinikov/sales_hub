"""Обслуживание: задачи beat (раздел 15, «Расписание beat»). Каждая — тонкая обёртка: своя
сессия из Resources процесса и тот же сервис или функция, что вызывает CLI. Все идемпотентны,
все в очереди default (в TASK_ROUTES их нет)."""

import time

from app.db.partitions import ensure_partitions
from app.services.maintenance import CleanupService
from app.services.reports import refresh_reports
from app.services.trash import TrashService
from app.uow import UnitOfWork
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from app.workers.runtime import current_settings, resources, run_async

HEARTBEAT_KEY = "workers:heartbeat"
HEARTBEAT_TTL_SECONDS = 600


@celery_app.task(base=BaseTask, name="workers.heartbeat")
def heartbeat() -> None:
    """Отметка «beat и очередь default живы»: время последнего выполнения в Redis (db 0).
    Старше 3 минут — алерт CeleryBeatDown (этап 8)."""
    run_async(_heartbeat())


async def _heartbeat() -> None:
    await resources().redis.set(HEARTBEAT_KEY, int(time.time()), ex=HEARTBEAT_TTL_SECONDS)


@celery_app.task(base=BaseTask, name="reports.refresh")
def refresh() -> None:
    """Каждые 5 минут: REFRESH deal_stats. Уже идёт (прошлый запуск или make refresh-reports) —
    пропуск."""
    run_async(_refresh())


async def _refresh() -> None:
    async with resources().session_factory() as session:
        await refresh_reports(UnitOfWork(session))


@celery_app.task(base=BaseTask, name="maintenance.create_partitions")
def create_partitions() -> None:
    """Ежедневно в 01:00: партиции activities и audit_log на 2 месяца вперёд — та же функция,
    что у migrate (python -m app.cli ensure-partitions)."""
    run_async(_create_partitions())


async def _create_partitions() -> None:
    async with resources().session_factory() as session:
        await ensure_partitions(session)


@celery_app.task(base=BaseTask, name="trash.purge")
def purge_trash() -> None:
    """Ежедневно в 03:00: корзина старше TRASH_RETENTION_DAYS — насовсем (раздел 3). Не дольше
    200 секунд — меньше soft_time_limit по умолчанию (240)."""
    run_async(_purge_trash())


async def _purge_trash() -> None:
    async with resources().session_factory() as session:
        service = TrashService(
            UnitOfWork(session), retention_days=current_settings().trash_retention_days
        )
        await service.purge()


@celery_app.task(base=BaseTask, name="maintenance.cleanup")
def cleanup() -> None:
    """Ежедневно в 04:00: токены, outbox, ключи дедупа, файлы импорта, старые job. Упавший шаг
    только логируется — остальные выполняются."""
    run_async(_cleanup())


async def _cleanup() -> None:
    async with resources().session_factory() as session:
        await CleanupService(UnitOfWork(session), resources().storage).run()
