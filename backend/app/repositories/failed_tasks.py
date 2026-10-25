"""Задачи Celery, упавшие после всех ретраев (раздел 15): запись из сигнала task_failure,
список и перезапуск у супер-админа."""

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import NotFound
from app.models import FailedTask
from app.repositories.pagination import Key, Page, PageResult, fetch_page, paginate

# Новые сначала: индекс (failed_at, id) читается в обратном порядке.
FAILED_TASKS_ORDER = (Key(FailedTask.failed_at, desc=True), Key(FailedTask.id, desc=True))


@dataclass(frozen=True)
class FailedTaskFilters:
    task_name: str | None = None
    replayed: bool | None = None  # True — только перезапущенные, False — ещё нет


class FailedTaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def record(self, values: dict[str, Any]) -> bool:
        """Записать упавшую задачу. False — строка с этим task_id уже есть: повторно
        доставленная задача упала снова, дубля нет. Коммит — за вызывающим."""
        recorded = await self.session.scalar(
            insert(FailedTask)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[FailedTask.task_id])
            .returning(FailedTask.id)
        )
        return recorded is not None

    async def list(self, filters: FailedTaskFilters, page: Page) -> PageResult[FailedTask]:
        query = select(FailedTask)
        if filters.task_name is not None:
            query = query.where(FailedTask.task_name == filters.task_name)
        if filters.replayed is not None:
            replayed = FailedTask.replayed_at.is_not(None)
            query = query.where(replayed if filters.replayed else ~replayed)
        query = paginate(query, FAILED_TASKS_ORDER, page)
        return await fetch_page(self.session, query, FAILED_TASKS_ORDER, page)

    async def get_or_404(self, failed_task_id: int, *, for_update: bool = False) -> FailedTask:
        """FOR UPDATE — для перезапуска: два одновременных replay одной строки идут по очереди,
        и второй увидит, что задачу уже перезапустили."""
        query = select(FailedTask).where(FailedTask.id == failed_task_id)
        if for_update:
            query = query.with_for_update()
        task = await self.session.scalar(query.execution_options(populate_existing=True))
        if task is None:
            raise NotFound(entity="failed_task", id=failed_task_id)
        return task

    async def mark_replayed(
        self, failed_task_id: int, replayed_by: uuid.UUID, replay_task_id: str
    ) -> None:
        await self.session.execute(
            update(FailedTask)
            .where(FailedTask.id == failed_task_id)
            .values(replayed_at=func.now(), replayed_by=replayed_by, replay_task_id=replay_task_id)
        )
