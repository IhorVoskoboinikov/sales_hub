"""Общая основа репозиториев CRM-сущностей с корзиной (компании, контакты, сделки).

Два правила, которые здесь нельзя забыть (раздел 3 и раздел 5):
1. Удалённые в корзину строки не видны: каждый запрос начинается с deleted_at IS NULL.
2. Без Access данные не отдаются: и список, и один объект сами добавляют access.sql_filter().

Запросы строятся методами *_query() и выполняются отдельно — так скрипт EXPLAIN (scripts/
explain_lists.py) проверяет ровно тот SQL, который выполняет API.
"""

import uuid
from typing import Any, cast

from sqlalchemy import ColumnElement, Select, and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.access import Access
from app.errors import NotFound
from app.models import Activity
from app.repositories.bulk import move_team_filter


class SoftDeleteRepository[M]:
    model: Any  # модель с колонками org_id, team_id, owner_id, deleted_at
    entity: str  # имя для ошибки 404: contact, deal, company

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def alive(self) -> Select[tuple[M]]:
        """Живые строки — основа всех запросов (общий фильтр корзины)."""
        return select(self.model).where(self.model.deleted_at.is_(None))

    def visible_query(self, access: Access) -> Select[tuple[M]]:
        return self.alive().where(access.sql_filter(self.model))

    async def get_visible_or_404(
        self, entity_id: int, access: Access, *, for_update: bool = False
    ) -> M:
        """Объект вне уровня — 404, как несуществующий: не раскрываем, что он есть."""
        query = self.visible_query(access).where(self.model.id == entity_id)
        return await self._one_or_404(query, entity_id, for_update)

    async def get_deleted_or_404(
        self, entity_id: int, access: Access, *, for_update: bool = False
    ) -> M:
        """Объект из корзины в уровне доступа — для восстановления."""
        query = select(self.model).where(
            self.model.id == entity_id,
            self.model.deleted_at.is_not(None),
            access.sql_filter(self.model),
        )
        return await self._one_or_404(query, entity_id, for_update)

    async def _one_or_404(self, query: Select[Any], entity_id: int, for_update: bool) -> M:
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        obj: M | None = await self.session.scalar(query)
        if obj is None:
            raise NotFound(entity=self.entity, id=entity_id)
        return obj

    def add(self, obj: M) -> None:
        self.session.add(obj)


def live(model: Any) -> ColumnElement[bool]:
    """Живая запись: не в корзине. У активностей корзины нет — живые там открытые задачи
    (done_at IS NULL); выполненные — история, она остаётся с прежним отделом."""
    if model is Activity:
        return Activity.done_at.is_(None)
    return cast(ColumnElement[bool], model.deleted_at.is_(None))


class LiveRecords:
    """Есть ли у отдела или человека живые CRM-записи: компании, контакты, сделки (и закрытые —
    они в отчётах отдела) и открытые активности. Отдел с такими записями нельзя удалить, а
    человека — сделать org_admin или viewer: отдела у него больше не будет, а team_id записей —
    это отдел ответственного. Запросы идут по частичным индексам *_team_id и *_owner_id
    (WHERE deleted_at IS NULL, у активностей — WHERE done_at IS NULL)."""

    def __init__(self, session: AsyncSession, models: tuple[Any, ...]) -> None:
        self.session = session
        self.models = models

    async def exist(self, *, team_id: Any = None, owner_id: Any = None) -> bool:
        for model in self.models:
            column = model.team_id if team_id is not None else model.owner_id
            value = team_id if team_id is not None else owner_id
            if await self._any(model, and_(column == value, live(model))):
                return True
        return False

    async def exist_outside_team(self, *, owner_id: uuid.UUID, team_id: uuid.UUID) -> bool:
        """Есть ли у человека записи, которые должна догнать job move_team: живые компании и
        контакты, открытые живые сделки и открытые активности с team_id не его отдела team_id.
        Условие — то же, по которому job режет и обновляет чанки."""
        for model in self.models:
            if await self._any(model, move_team_filter(model, owner_id, team_id)):
                return True
        return False

    async def _any(self, model: Any, where: ColumnElement[bool]) -> bool:
        return await self.session.scalar(select(model.id).where(where).limit(1)) is not None
