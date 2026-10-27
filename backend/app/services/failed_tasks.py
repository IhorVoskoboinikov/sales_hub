"""Задачи Celery, упавшие после всех ретраев (раздел 15): список и перезапуск у супер-админа.

Строки пишет обработчик сигнала task_failure (app/workers/base.py). Перезапуск — та же задача по
имени с теми же аргументами; задачи bulk и импорта (replayable=False) так не повторяют — их
повторяют новой job, а её условия идемпотентны.
"""

import uuid

import structlog

from app.errors import Conflict, ServiceUnavailable
from app.models import FailedTask
from app.repositories.failed_tasks import FailedTaskFilters
from app.repositories.pagination import Page, PageResult
from app.uow import UnitOfWork
from app.workers.queue import TaskSender

logger = structlog.get_logger("workers")


class FailedTaskService:
    def __init__(self, uow: UnitOfWork, sender: TaskSender) -> None:
        self.uow = uow
        self.sender = sender

    async def list(self, filters: FailedTaskFilters, page: Page) -> PageResult[FailedTask]:
        return await self.uow.failed_tasks.list(filters, page)

    async def get(self, failed_task_id: int) -> FailedTask:
        return await self.uow.failed_tasks.get_or_404(failed_task_id)

    async def replay(self, failed_task_id: int, admin_id: uuid.UUID) -> str:
        """Поставить задачу снова и вернуть id новой задачи.

        Строка FOR UPDATE: два одновременных replay идут по очереди, второй получит 409
        failed_task.already_replayed. Публикация — до коммита: упадёт коммит, и задача
        выполнится лишний раз, это безопасно (задачи идемпотентны); наоборот — отметка без
        задачи — было бы хуже. Брокер недоступен — 503 и откат."""
        try:
            task = await self.uow.failed_tasks.get_or_404(failed_task_id, for_update=True)
            if task.replayed_at is not None:
                raise Conflict("failed_task.already_replayed", replay_task_id=task.replay_task_id)
            replayable = self.sender.replayable(task.task_name)
            if replayable is None:  # задачу переименовали или удалили
                raise Conflict("failed_task.unknown_task", task_name=task.task_name)
            if not replayable:
                raise Conflict("failed_task.not_replayable", task_name=task.task_name)
            task_name = task.task_name
            replay_task_id = await self.sender.send(task_name, list(task.args), dict(task.kwargs))
            await self.uow.failed_tasks.mark_replayed(failed_task_id, admin_id, replay_task_id)
            self.uow.audit.record(
                actor_id=admin_id,
                org_id=None,  # системное действие супер-админа, вне организаций
                action="failed_task.replayed",
                entity_type="failed_task",
                entity_id=failed_task_id,
                changes={"task_name": task_name, "replay_task_id": replay_task_id},
            )
            await self.uow.commit()
        except (Conflict, ServiceUnavailable):
            await self.uow.rollback()  # отпустить строку сразу, не дожидаясь конца запроса
            raise
        logger.info(
            "failed_task_replayed",
            failed_task_id=failed_task_id,
            task_name=task_name,
            replay_task_id=replay_task_id,
        )
        return replay_task_id
