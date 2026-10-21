"""Активности, заметки и журнал — таблицы, у которых нет корзины.

activities и audit_log партиционированы по месяцам. К keyset-условию (created_at, id) < (…)
добавляется простое created_at <= :c: сравнение строк Postgres для отсечения партиций не
использует, а простое условие — использует, и следующая страница не читает более новые месяцы.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Select, delete, literal_column, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.access import Access
from app.models import Activity, AuditEntry, Note
from app.models.activities import OPEN
from app.repositories.pagination import Key, Page, PageResult, decode_cursor, fetch_page, paginate

HISTORY_ORDER = (Key(Activity.created_at, desc=True), Key(Activity.id, desc=True))
TASKS_ORDER = (Key(Activity.due_at), Key(Activity.id))
AUDIT_ORDER = (Key(AuditEntry.occurred_at, desc=True), Key(AuditEntry.id, desc=True))
NOTES_ORDER = (Key(Note.created_at, desc=True), Key(Note.id, desc=True))


def prune(query: Select[Any], column: Any, keys: tuple[Key, ...], page: Page) -> Select[Any]:
    """created_at <= :c рядом с keyset-условием — чтобы Postgres отсёк более новые партиции."""
    if page.cursor:
        newest = decode_cursor(page.cursor, keys)[0]
        query = query.where(column <= newest)
    return query


@dataclass(frozen=True)
class TaskWindow:
    """Срок открытых задач: [due_from, due_to). None — без границы."""

    due_from: datetime | None = None
    due_to: datetime | None = None


class ActivityRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def history_query(
        self, *, deal_id: int | None, contact_id: int | None, page: Page
    ) -> Select[Any]:
        """История карточки: все активности родителя. Видимость родителя уже проверил сервис —
        кто видит сделку, видит всю её историю (раздел 5)."""
        query = select(Activity)
        if deal_id is not None:
            query = query.where(Activity.deal_id == deal_id)
        if contact_id is not None:
            query = query.where(Activity.contact_id == contact_id)
        query = prune(query, Activity.created_at, HISTORY_ORDER, page)
        return paginate(query, HISTORY_ORDER, page)

    async def history(
        self, *, deal_id: int | None, contact_id: int | None, page: Page
    ) -> PageResult[Activity]:
        query = self.history_query(deal_id=deal_id, contact_id=contact_id, page=page)
        return await fetch_page(self.session, query, HISTORY_ORDER, page)

    def tasks_query(self, access: Access, window: TaskWindow, page: Page) -> Select[Any]:
        """Открытые задачи в уровне activity:read по сроку. Предикат done_at IS NULL — литералом,
        как в частичных индексах ix_activities_*_due_at."""
        query = select(Activity).where(literal_column(OPEN), access.sql_filter(Activity))
        if window.due_from is not None:
            query = query.where(Activity.due_at >= window.due_from)
        if window.due_to is not None:
            query = query.where(Activity.due_at < window.due_to)
        return paginate(query, TASKS_ORDER, page)

    async def tasks(self, access: Access, window: TaskWindow, page: Page) -> PageResult[Activity]:
        query = self.tasks_query(access, window, page)
        return await fetch_page(self.session, query, TASKS_ORDER, page)

    async def get_visible(self, activity_id: int, access: Access) -> Activity | None:
        """По id без created_at Postgres смотрит индексы всех партиций — это видно в EXPLAIN."""
        return await self.session.scalar(
            select(Activity).where(Activity.id == activity_id, access.sql_filter(Activity))
        )

    def add(self, activity: Activity) -> None:
        self.session.add(activity)

    async def delete(self, activity: Activity) -> None:
        # Удаление жёсткое (раздел 3); по первичному ключу целиком — одна партиция.
        await self.session.execute(
            delete(Activity).where(
                Activity.id == activity.id, Activity.created_at == activity.created_at
            )
        )


class NoteRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list(self, entity_type: str, entity_id: int, page: Page) -> PageResult[Note]:
        query = select(Note).where(Note.entity_type == entity_type, Note.entity_id == entity_id)
        return await fetch_page(self.session, paginate(query, NOTES_ORDER, page), NOTES_ORDER, page)

    def add(self, note: Note) -> None:
        self.session.add(note)


@dataclass(frozen=True)
class AuditFilters:
    entity_type: str | None = None
    entity_id: str | None = None
    actor_id: uuid.UUID | None = None
    action: str | None = None
    occurred_from: datetime | None = None
    occurred_to: datetime | None = None


class AuditQueries:
    """Чтение журнала организации. audit:read есть только на уровне org (data.yaml), а у
    audit_log нет team_id и owner_id, поэтому фильтр — просто org_id."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def list_query(self, org_id: uuid.UUID, filters: AuditFilters, page: Page) -> Select[Any]:
        query = select(AuditEntry).where(AuditEntry.org_id == org_id)
        for column, value in (
            (AuditEntry.entity_type, filters.entity_type),
            (AuditEntry.entity_id, filters.entity_id),
            (AuditEntry.actor_id, filters.actor_id),
            (AuditEntry.action, filters.action),
        ):
            if value is not None:
                query = query.where(column == value)
        if filters.occurred_from is not None:
            query = query.where(AuditEntry.occurred_at >= filters.occurred_from)
        if filters.occurred_to is not None:
            query = query.where(AuditEntry.occurred_at < filters.occurred_to)
        query = prune(query, AuditEntry.occurred_at, AUDIT_ORDER, page)
        return paginate(query, AUDIT_ORDER, page)

    async def list(
        self, org_id: uuid.UUID, filters: AuditFilters, page: Page
    ) -> PageResult[AuditEntry]:
        query = self.list_query(org_id, filters, page)
        return await fetch_page(self.session, query, AUDIT_ORDER, page)
