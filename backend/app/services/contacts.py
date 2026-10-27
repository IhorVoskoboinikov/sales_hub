"""Компании и контакты (раздел 3, правила доступа — раздел 5).

Порядок любого изменения:
1. старая версия видна на чтение (иначе 404) и в уровне действия (иначе 403) — get_for_change;
2. версия из тела совпадает с текущей (иначе 409 version.conflict);
3. все проверки и чтения (ответственный, ссылки, дубль email) — до присваивания полей:
   иначе autoflush отправил бы UPDATE раньше времени;
4. новая версия в моём уровне (ensure_allowed, иначе 403 out_of_scope);
5. журнал, доменные события (хук _events: outbox в той же транзакции) и commit.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Conflict
from app.events.types import EventType, RecordAssigned
from app.models import Company, Contact, Deal
from app.repositories.audit import diff
from app.repositories.contacts import ContactFilters
from app.repositories.crm import SoftDeleteRepository
from app.repositories.pagination import Page, PageResult
from app.services.crm import (
    Owner,
    ensure_reference,
    get_for_change,
    owner_for_restore,
    resolve_owner,
)
from app.uow import UnitOfWork

Record = Company | Contact | Deal


def check_version(obj: Record, version: int) -> None:
    """Клиент правил версию, которую видел. Если её успели изменить — 409, а не тихая
    перезапись чужой правки (оптимистическая блокировка)."""
    if obj.version != version:
        raise Conflict("version.conflict", current_version=obj.version)


class CrmService[R: Record]:
    """Общее для компаний и контактов: создать, изменить, удалить в корзину, восстановить."""

    entity: str  # company / contact — для журнала и имён действий (contact:read)
    # record.assigned новому ответственному (раздел 16). У компаний — нет: это общий справочник.
    assigned_event = True

    def __init__(self, uow: UnitOfWork, roles: RoleCatalog) -> None:
        self.uow = uow
        self.roles = roles

    @property
    def repo(self) -> SoftDeleteRepository[R]:
        raise NotImplementedError

    async def get(self, access: Access, entity_id: int) -> R:
        return await self.repo.get_visible_or_404(entity_id, access)

    def _audit(self, access: Access, action: str, obj: Record, changes: Any) -> None:
        self.uow.audit.record(
            actor_id=access.user_id,
            org_id=access.org_id,
            action=f"{self.entity}.{action}",
            entity_type=self.entity,
            entity_id=obj.id,
            changes=changes,
        )

    async def _new_owner(self, access: Access, owner_id: uuid.UUID | None) -> Owner:
        owner = await resolve_owner(self.uow, access, owner_id)
        access.ensure_allowed(owner)  # sales_rep — только себя, team_lead — свой отдел
        return owner

    async def _create(self, access: Access, obj: R, fields: dict[str, Any]) -> R:
        owner = await self._new_owner(access, fields.pop("owner_id", None))
        obj.org_id, obj.team_id, obj.owner_id = owner.org_id, owner.team_id, owner.owner_id
        for field, value in fields.items():
            setattr(obj, field, value)
        self.repo.add(obj)
        await self.uow.flush()  # id из базы — для журнала
        self._audit(access, "created", obj, {k: v for k, v in fields.items() if v is not None})
        self._events(access, obj, None)
        await self.uow.commit()
        return obj

    async def _for_change(self, access: Access, entity_id: int) -> R:
        return await get_for_change(self.repo, entity_id, access, self.roles, f"{self.entity}:read")

    async def _update(self, access: Access, obj: R, version: int, fields: dict[str, Any]) -> R:
        check_version(obj, version)
        owner_id = fields.pop("owner_id", None)
        owner = None
        if owner_id is not None and owner_id != obj.owner_id:
            owner = await self._new_owner(access, owner_id)
        # Дальше — только присваивания: никаких запросов до commit.
        before = {field: getattr(obj, field) for field in [*fields, "owner_id", "team_id"]}
        for field, value in fields.items():
            setattr(obj, field, value)
        if owner is not None:
            obj.owner_id, obj.team_id = owner.owner_id, owner.team_id
        if changed := diff(before, {f: getattr(obj, f) for f in before}):
            self._audit(access, "updated", obj, changed)
            self._events(access, obj, before)
            await self.uow.commit()  # UPDATE … AND version = :v — гонка даст 409
        return obj

    def _events(self, access: Access, obj: R, before: dict[str, Any] | None) -> None:
        """Доменные события изменения — в outbox той же транзакции, перед коммитом (раздел 16):
        откатилось изменение — откатилось и событие. before — значения полей до изменения
        (None — запись создана). Только присваивания: id у записи уже есть (flush в _create).

        record.assigned — новому ответственному, если его назначил не он сам: создание с чужим
        ответственным или смена ответственного. Массовые операции событий по строкам не пишут —
        у передачи клиентов одно сообщение через bulk_job.completed."""
        if not self.assigned_event or obj.owner_id == access.user_id:
            return
        if before is not None and before["owner_id"] == obj.owner_id:
            return
        payload: RecordAssigned = {
            "entity_type": self.entity,
            "entity_id": obj.id,
            "title": self._title(obj),
            "owner_id": str(obj.owner_id),
            "actor_id": str(access.user_id),
        }
        self.uow.outbox.add(EventType.RECORD_ASSIGNED, dict(payload), org_id=obj.org_id)

    @staticmethod
    def _title(obj: R) -> str:
        """Как запись называется в уведомлении: имя контакта, название сделки."""
        if isinstance(obj, Contact):
            return f"{obj.first_name} {obj.last_name}".strip()
        if isinstance(obj, Deal):
            return obj.title
        return obj.name

    async def delete(self, access: Access, entity_id: int) -> None:
        obj = await self._for_change(access, entity_id)
        obj.deleted_at = datetime.now(UTC)
        self._audit(access, "deleted", obj, {})
        await self.uow.commit()

    async def restore(self, access: Access, entity_id: int) -> R:
        # FOR UPDATE до коммита: trash.purge (SKIP LOCKED) такую строку пропустит, а restore,
        # который ждал чанк purge, после его коммита строки уже не найдёт — 404.
        obj = await self.repo.get_deleted_or_404(entity_id, access, for_update=True)
        await self._before_restore(access, obj)
        owner = await owner_for_restore(self.uow, obj)
        access.ensure_allowed(owner)  # ответственный мог перейти в чужой для меня отдел
        changes = diff({"team_id": obj.team_id}, {"team_id": owner.team_id})
        obj.deleted_at = None
        obj.team_id = owner.team_id
        self._audit(access, "restored", obj, changes)
        await self.uow.commit()
        return obj

    async def _before_restore(self, access: Access, obj: R) -> None:
        """Дополнительные проверки восстановления (у контакта — дубль email)."""


class CompanyService(CrmService[Company]):
    entity = "company"
    assigned_event = False  # компании — общий справочник: о смене ответственного не пишем

    @property
    def repo(self) -> SoftDeleteRepository[Company]:
        return self.uow.companies

    async def list(self, access: Access, q: str | None, page: Page) -> PageResult[Company]:
        return await self.uow.companies.list(access, q, page)

    async def create(self, access: Access, fields: dict[str, Any]) -> Company:
        return await self._create(access, Company(), fields)

    async def update(
        self, access: Access, company_id: int, version: int, fields: dict[str, Any]
    ) -> Company:
        company = await self._for_change(access, company_id)
        return await self._update(access, company, version, fields)


class ContactService(CrmService[Contact]):
    entity = "contact"

    @property
    def repo(self) -> SoftDeleteRepository[Contact]:
        return self.uow.contacts

    async def list(
        self, access: Access, filters: ContactFilters, page: Page
    ) -> PageResult[Contact]:
        return await self.uow.contacts.list(access, filters, page)

    async def create(self, access: Access, fields: dict[str, Any]) -> Contact:
        await self._check_company(access, fields.get("company_id"))
        await self._check_email(access, fields.get("email"))
        return await self._create(access, Contact(), fields)

    async def update(
        self, access: Access, contact_id: int, version: int, fields: dict[str, Any]
    ) -> Contact:
        contact = await self._for_change(access, contact_id)
        check_version(contact, version)
        if "company_id" in fields and fields["company_id"] != contact.company_id:
            await self._check_company(access, fields["company_id"])
        if "email" in fields and fields["email"] != contact.email:
            await self._check_email(access, fields["email"], exclude=contact.id)
        return await self._update(access, contact, version, fields)

    async def trash(self, access: Access, page: Page) -> PageResult[Contact]:
        """Удалённые контакты в уровне contact:delete — их можно восстановить 30 дней."""
        return await self.uow.contacts.trash(access, page)

    async def _before_restore(self, access: Access, obj: Contact) -> None:
        await self._check_email(access, obj.email, exclude=obj.id)

    async def _check_company(self, access: Access, company_id: int | None) -> None:
        await ensure_reference(
            self.uow.companies, company_id, access, self.roles, "company:read", "company_id"
        )

    async def _check_email(
        self, access: Access, email: str | None, exclude: int | None = None
    ) -> None:
        """Email уникален среди живых контактов организации (раздел 5, «Дубль, которого не
        видно»). Дубль вне моего уровня — показываем только, кто его ведёт."""
        if not email:
            return
        found = await self.uow.contacts.find_by_email(access.org_id, email)
        if found is None or found[0].id == exclude:
            return
        duplicate, owner_email = found
        read = access.for_action("contact:read", self.roles)
        if read is not None and read.matches(duplicate):
            raise Conflict("contact.duplicate_email", contact_id=duplicate.id, owner=owner_email)
        raise Conflict("contact.duplicate_email", owner=owner_email)
