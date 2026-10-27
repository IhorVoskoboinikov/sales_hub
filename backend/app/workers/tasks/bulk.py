"""Массовые операции (разделы 6 и 15): bulk.plan, bulk.chunk и bulk.reap_stuck — тонкие обёртки
над BulkService.

Без chord: plan после своего коммита ставит обычную группу чанков, а завершение job решает
finalize_if_complete по строкам bulk_job_chunks. Очереди — по имени в TASK_ROUTES: bulk.chunk →
bulk, bulk.plan и bulk.reap_stuck → default (за тысячей чужих чанков короткие задачи ждали бы
зря).

replayable=False: из failed_tasks такие задачи руками не перезапускают — повторяют новой job,
а её условия идемпотентны. bulk.reap_stuck перезапускать можно: он и так идёт раз в минуту.
"""

import uuid
from typing import Any

import structlog
from celery import group
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.bulk import BulkService, ReapPolicy, failure_code
from app.uow import UnitOfWork
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from app.workers.queue import CeleryTaskQueue, broker_queue_length
from app.workers.runtime import current_settings, resources, run_async

logger = structlog.get_logger("workers")


def bulk_service(session: AsyncSession) -> BulkService:
    """Сервис на сессию задачи: роли — из Resources процесса, размер чанка — из настроек."""
    return BulkService(
        UnitOfWork(session),
        resources().roles,
        CeleryTaskQueue(celery_app),
        chunk_size=current_settings().bulk_chunk_size,
    )


@celery_app.task(base=BaseTask, name="bulk.plan", replayable=False)
def plan(job_id: str) -> None:
    """Нарезать job на чанки и поставить их группой — после коммита plan, иначе чанк мог бы
    начаться раньше, чем его строка видна в базе."""
    numbers = run_async(_plan(uuid.UUID(job_id)))
    if numbers:
        group(chunk.s(job_id, no) for no in numbers).apply_async()


async def _plan(job_id: uuid.UUID) -> list[int]:
    async with resources().session_factory() as session:
        return await bulk_service(session).plan(job_id)


class ChunkTask(BaseTask):
    """bulk.chunk: после последнего ретрая (или сразу — для неповторяемой ошибки) чанк
    отмечается failed, и job завершается без него — partial_failed, а не повисшая job.

    Жёсткий time_limit убивает процесс пула без on_failure: такой чанк остаётся pending, его
    переотправит bulk.reap_stuck."""

    def on_failure(self, exc: Exception, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        job_id, no = args
        try:
            run_async(_fail_chunk(uuid.UUID(job_id), int(no), failure_code(exc)))
        except Exception as error:  # не удалось отметить: чанк останется pending для reaper
            logger.error("bulk_chunk_fail_not_recorded", job_id=job_id, no=no, error=repr(error))


@celery_app.task(
    base=ChunkTask,
    name="bulk.chunk",
    replayable=False,
    soft_time_limit=60,
    time_limit=90,
)
def chunk(job_id: str, no: int) -> None:
    run_async(_run_chunk(uuid.UUID(job_id), no))


async def _run_chunk(job_id: uuid.UUID, no: int) -> None:
    async with resources().session_factory() as session:  # своя сессия и UoW на задачу
        await bulk_service(session).run_chunk(job_id, no)


async def _fail_chunk(job_id: uuid.UUID, no: int, error: str) -> None:
    async with resources().session_factory() as session:
        await bulk_service(session).fail_chunk(job_id, no, error)


@celery_app.task(base=BaseTask, name="bulk.reap_stuck")
def reap_stuck() -> None:
    """Раз в минуту из beat: подобрать job с потерянными задачами (раздел 15). Два прохода
    одновременно безопасны — каждая job берётся FOR UPDATE SKIP LOCKED."""
    run_async(_reap_stuck())


async def _reap_stuck() -> None:
    settings = current_settings()
    policy = ReapPolicy(
        pending_seconds=settings.bulk_reap_pending_seconds,
        stall_seconds=settings.bulk_reap_stall_seconds,
        import_stall_seconds=settings.import_reap_stall_seconds,
        max_reaps=settings.bulk_max_reaps,
    )

    async def queue_length(queue: str) -> int:
        return await broker_queue_length(settings.celery_broker_url, queue)

    async with resources().session_factory() as session:
        await bulk_service(session).reap_stuck(policy, queue_length)
