"""Окончательное удаление корзины — задача beat trash.purge, ежедневно в 03:00 (раздел 3,
«Удаление: в корзину, а не насовсем»).

Компании, контакты и сделки, удалённые раньше TRASH_RETENTION_DAYS (30) дней назад, удаляются
физически: по каждой организации, от детей к родителям — сделки → контакты → компании (так у
FK SET NULL меньше работы: сделки удалённого контакта уже ушли), чанками по 1 000 строк, каждый
в своей транзакции. Строки чанка берутся FOR UPDATE SKIP LOCKED: ту, что сейчас восстанавливают,
purge пропускает. Вместе с записью уходят её заметки, у контакта — ещё его активности без
сделки; активности сделки удаляет FK (CASCADE). В журнал purge не пишет: мягкое удаление там
уже есть.

Запуск не дольше бюджета (200 секунд, меньше soft_time_limit задачи): остаток удалит
следующий. Повтор безопасен — удалённого второй раз не найти.
"""

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import structlog

from app.models import Company, Contact, Deal
from app.repositories.trash import TrashModel
from app.uow import UnitOfWork

logger = structlog.get_logger("trash")

PURGE_CHUNK = 1000
PURGE_BUDGET_SECONDS = 200.0
# От детей к родителям: (модель, entity_type заметок, ключ в итоге).
ORDER: tuple[tuple[TrashModel, str, str], ...] = (
    (Deal, "deal", "deals"),
    (Contact, "contact", "contacts"),
    (Company, "company", "companies"),
)


@dataclass
class PurgeResult:
    deleted: dict[str, int] = field(default_factory=lambda: {name: 0 for *_, name in ORDER})
    complete: bool = True  # False — бюджет времени кончился, остаток — следующему запуску


class TrashService:
    def __init__(
        self,
        uow: UnitOfWork,
        *,
        retention_days: int,
        chunk_size: int = PURGE_CHUNK,
        budget_seconds: float = PURGE_BUDGET_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.uow = uow
        self.retention = timedelta(days=retention_days)
        self.chunk_size = chunk_size
        self.budget_seconds = budget_seconds
        self.clock = clock

    async def purge(self, now: datetime | None = None) -> PurgeResult:
        before = (now or datetime.now(UTC)) - self.retention
        deadline = self.clock() + self.budget_seconds
        result = PurgeResult()
        org_ids = await self.uow.trash.org_ids()
        await self.uow.rollback()  # список прочитан — транзакцию не держим
        for org_id in org_ids:
            for model, entity_type, name in ORDER:
                while True:
                    if self.clock() >= deadline:
                        result.complete = False
                        logger.warning("trash_purge_budget_exhausted", **result.deleted)
                        return result
                    deleted = await self._chunk(model, entity_type, org_id, before)
                    result.deleted[name] += deleted
                    if deleted < self.chunk_size:  # корзина организации кончилась (или занята)
                        break
        logger.info("trash_purged", **result.deleted)
        return result

    async def _chunk(
        self, model: TrashModel, entity_type: str, org_id: uuid.UUID, before: datetime
    ) -> int:
        """Один чанк — одна транзакция: взять строки SKIP LOCKED, удалить с детьми, коммит."""
        ids = await self.uow.trash.lock_doomed(model, org_id, before, self.chunk_size)
        if not ids:
            await self.uow.rollback()
            return 0
        await self.uow.trash.delete_with_children(model, entity_type, ids)
        await self.uow.commit()
        return len(ids)
