"""Импорт контактов из CSV (разделы 6 и 15): imports.import_contacts — тонкая обёртка над
ImportService.run.

Очередь — imports (TASK_ROUTES), лимит запуска 10 / 12 минут: длинный импорт разбит на шаги,
повтор продолжает с последнего завершённого. Жёсткий time_limit убивает процесс пула — job
остаётся running, и через IMPORT_REAP_STALL_SECONDS без прогресса её снова отправит
bulk.reap_stuck. Одна копия на job — сессионный advisory lock на втором соединении пула процесса
(пул воркера — 2 соединения: сессия задачи и блокировка).

replayable=False: из failed_tasks руками не перезапускают — job подбирает reaper, а новый
импорт — новая job.
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.locks import AdvisoryLock
from app.services.imports import ImportService
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from app.workers.runtime import current_settings, resources, run_async
from app.workers.tasks.bulk import bulk_service


def import_service(session: AsyncSession) -> ImportService:
    """Сервис на сессию задачи: хранилище — из Resources процесса, лимит строк — из настроек."""
    return ImportService(
        bulk_service(session), resources().storage, max_rows=current_settings().import_max_rows
    )


@celery_app.task(
    base=BaseTask,
    name="imports.import_contacts",
    replayable=False,
    soft_time_limit=600,
    time_limit=720,
)
def import_contacts(job_id: str) -> None:
    run_async(_import_contacts(uuid.UUID(job_id)))


async def _import_contacts(job_id: uuid.UUID) -> None:
    async with resources().session_factory() as session:  # своя сессия и UoW на задачу
        await import_service(session).run(job_id, AdvisoryLock(resources().engine))
