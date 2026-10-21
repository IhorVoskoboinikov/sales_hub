"""Активности, заметки и журнал (разделы 3 и 5).

- Активности и заметки видны по родителю: кто видит сделку или контакт, видит всю их историю.
  Уровень activity:read применяется к спискам вне карточки — «мои задачи на сегодня».
- Активность без срока — запись о том, что уже было (done_at = сейчас); со сроком — открытая
  задача, пока её не отметят выполненной.
- Заметку добавляет тот, кто может менять контакт.
"""

from datetime import UTC, datetime, time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Forbidden, NotFound, ValidationFailed
from app.models import Activity, AuditEntry, Note
from app.repositories.activities import AuditFilters, TaskWindow
from app.repositories.crm import SoftDeleteRepository
from app.repositories.pagination import Page, PageResult
from app.services.crm import ensure_reference, get_for_change, resolve_owner
from app.uow import UnitOfWork

Due = Literal["today", "overdue"]


class ActivityService:
    def __init__(self, uow: UnitOfWork, roles: RoleCatalog) -> None:
        self.uow = uow
        self.roles = roles

    async def history(
        self, access: Access, deal_id: int | None, contact_id: int | None, page: Page
    ) -> PageResult[Activity]:
        """История карточки: родитель по уровню его чтения (не видно — 404), дальше без Access."""
        if deal_id is not None:
            await self._parent(self.uow.deals, deal_id, access, "deal:read")
        if contact_id is not None:
            await self._parent(self.uow.contacts, contact_id, access, "contact:read")
        return await self.uow.activities.history(deal_id=deal_id, contact_id=contact_id, page=page)

    async def tasks(self, access: Access, due: Due | None, page: Page) -> PageResult[Activity]:
        """Открытые задачи в моём уровне по сроку. «Сегодня» — по часовому поясу пользователя,
        а если он не задан — организации."""
        now = datetime.now(UTC)
        window = TaskWindow()
        if due == "overdue":
            window = TaskWindow(due_to=now)
        elif due == "today":
            zone = await self._zone(access)
            start = datetime.combine(now.astimezone(zone).date(), time(), zone)
            window = TaskWindow(due_from=start, due_to=start + timedelta(days=1))
        return await self.uow.activities.tasks(access, window, page)

    async def create(self, access: Access, fields: dict[str, Any]) -> Activity:
        await ensure_reference(
            self.uow.deals, fields.get("deal_id"), access, self.roles, "deal:read", "deal_id"
        )
        await ensure_reference(
            self.uow.contacts,
            fields.get("contact_id"),
            access,
            self.roles,
            "contact:read",
            "contact_id",
        )
        owner = await resolve_owner(self.uow, access, fields.pop("owner_id", None))
        access.ensure_allowed(owner)
        activity = Activity(org_id=owner.org_id, team_id=owner.team_id, owner_id=owner.owner_id)
        for field, value in fields.items():
            setattr(activity, field, value)
        if activity.due_at is None:
            activity.done_at = datetime.now(UTC)  # без срока — уже состоялась
        self.uow.activities.add(activity)
        await self.uow.commit()
        return activity

    async def update(self, access: Access, activity_id: int, fields: dict[str, Any]) -> Activity:
        activity = await self._for_change(access, activity_id)
        owner_id = fields.pop("owner_id", None)
        if owner_id is not None and owner_id != activity.owner_id:
            owner = await resolve_owner(self.uow, access, owner_id)
            access.ensure_allowed(owner)
            activity.owner_id, activity.team_id = owner.owner_id, owner.team_id
        if "done" in fields:
            done = fields.pop("done")
            if done and activity.done_at is None:
                activity.done_at = datetime.now(UTC)
            elif not done and activity.done_at is not None:
                if activity.due_at is None and "due_at" not in fields:
                    # Открытая задача без срока не попала бы ни в один список.
                    raise ValidationFailed(field="due_at")
                activity.done_at = None
        for field, value in fields.items():
            setattr(activity, field, value)
        await self.uow.commit()
        return activity

    async def delete(self, access: Access, activity_id: int) -> None:
        activity = await self._for_change(access, activity_id)
        await self.uow.activities.delete(activity)
        await self.uow.commit()

    async def _for_change(self, access: Access, activity_id: int) -> Activity:
        """Видна на чтение (иначе 404) и в уровне записи (иначе 403) — как у сделок."""
        read = access.for_action("activity:read", self.roles) or access
        activity = await self.uow.activities.get_visible(activity_id, read)
        if activity is None:
            raise NotFound(entity="activity", id=activity_id)
        access.ensure_allowed(activity)
        return activity

    async def _parent(
        self, repo: SoftDeleteRepository[Any], entity_id: int, access: Access, action: str
    ) -> None:
        read = access.for_action(action, self.roles)
        if read is None:
            raise Forbidden(action=action)
        await repo.get_visible_or_404(entity_id, read)

    async def _zone(self, access: Access) -> ZoneInfo:
        user = await self.uow.users.get(access.user_id)
        if user is not None and user.timezone:
            return ZoneInfo(user.timezone)
        org = await self.uow.organizations.get(access.org_id)
        return ZoneInfo(org.timezone if org else "UTC")


class NoteService:
    """Заметки контакта: читает тот, кто видит контакт, добавляет тот, кто может его менять."""

    def __init__(self, uow: UnitOfWork, roles: RoleCatalog) -> None:
        self.uow = uow
        self.roles = roles

    async def list(self, access: Access, contact_id: int, page: Page) -> PageResult[Note]:
        await self.uow.contacts.get_visible_or_404(contact_id, access)
        return await self.uow.notes.list("contact", contact_id, page)

    async def create(self, access: Access, contact_id: int, body: str) -> Note:
        await get_for_change(self.uow.contacts, contact_id, access, self.roles, "contact:read")
        note = Note(
            org_id=access.org_id,
            entity_type="contact",
            entity_id=contact_id,
            author_id=access.user_id,
            body=body,
        )
        self.uow.notes.add(note)
        await self.uow.commit()
        return note


class AuditService:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def list(
        self, access: Access, filters: AuditFilters, page: Page
    ) -> PageResult[AuditEntry]:
        if access.scope != "org":
            # У audit_log нет отдела и ответственного — журнал читается только целиком.
            raise Forbidden(reason="out_of_scope")
        return await self.uow.audit_log.list(access.org_id, filters, page)
