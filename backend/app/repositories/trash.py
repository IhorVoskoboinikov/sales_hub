"""Запросы trash.purge: окончательное удаление корзины (раздел 3, «Удаление: в корзину»).

Строки корзины организации старше срока находит частичный индекс (org_id, deleted_at, id)
WHERE deleted_at IS NOT NULL (ix_*_trash). Предикат индекса в запросе — литерал
deleted_at IS NOT NULL, а не параметр: иначе в общем плане asyncpg частичный индекс не подошёл
бы. Запросы — статические методы *_query: те же строит make explain (scripts/explain_lists.py).
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Delete, Select, any_, delete, literal, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Activity, Note, Organization

# CRM-модель корзины (Deal, Contact, Company): у всех id, org_id, deleted_at.
TrashModel = Any


def id_array(ids: list[int]) -> Any:
    """= ANY(:ids) одним параметром-массивом, а не IN с тысячей параметров: у asyncpg один
    подготовленный запрос на любой размер чанка."""
    return any_(literal(ids, ARRAY(BigInteger)))


class TrashRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def org_ids(self) -> list[uuid.UUID]:
        """Все организации: таблица маленькая, а по каждой корзину ищет индекс."""
        return list(await self.session.scalars(select(Organization.id).order_by(Organization.id)))

    @staticmethod
    def doomed_query(
        model: TrashModel, org_id: uuid.UUID, before: datetime, limit: int
    ) -> Select[Any]:
        """Чанк корзины организации, удалённый раньше before, под блокировкой до коммита.
        SKIP LOCKED: строку, которую сейчас восстанавливают (restore берёт её FOR UPDATE),
        purge пропускает — она достанется следующему запуску или уже не будет в корзине."""
        return (
            select(model.id)
            .where(
                model.org_id == org_id,
                model.deleted_at.is_not(None),  # предикат частичного индекса — литералом
                model.deleted_at < before,
            )
            .order_by(model.deleted_at, model.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    async def lock_doomed(
        self, model: TrashModel, org_id: uuid.UUID, before: datetime, limit: int
    ) -> list[int]:
        return list(await self.session.scalars(self.doomed_query(model, org_id, before, limit)))

    @staticmethod
    def notes_query(entity_type: str, ids: list[int]) -> Delete:
        """Заметки удаляемых записей: у заметки родитель полиморфный, FK на него нет."""
        return (
            delete(Note)
            .where(Note.entity_type == entity_type, Note.entity_id == id_array(ids))
            .execution_options(synchronize_session=False)
        )

    @staticmethod
    def contact_activities_query(ids: list[int]) -> Delete:
        """Активности контакта без сделки. С живой сделкой — остаются: их contact_id обнулит FK
        (SET NULL), история сделки не теряется. Активности сделок удаляет FK (CASCADE)."""
        return (
            delete(Activity)
            .where(Activity.contact_id == id_array(ids), Activity.deal_id.is_(None))
            .execution_options(synchronize_session=False)
        )

    async def delete_with_children(
        self, model: TrashModel, entity_type: str, ids: list[int]
    ) -> None:
        """Заметки, активности контакта без сделки и сами строки. Ссылки на них (company_id,
        contact_id) обнуляет FK ON DELETE SET NULL — без version + 1 и без журнала."""
        await self.session.execute(self.notes_query(entity_type, ids))
        if entity_type == "contact":
            await self.session.execute(self.contact_activities_query(ids))
        await self.session.execute(
            delete(model)
            .where(model.id == id_array(ids))
            .execution_options(synchronize_session=False)
        )
