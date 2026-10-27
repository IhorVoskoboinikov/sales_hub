"""Общие правила CRM-сервисов (раздел 5): кто ответственный, какой отдел, видны ли ссылки.

- owner_id: по умолчанию — тот, кто создаёт; руководитель и админ могут указать другого.
  Ответственным может быть только активный team_lead или sales_rep той же организации.
- team_id: всегда отдел ответственного — его ставит сервис, из тела запроса он не берётся.
  Пересчитывается только при создании, смене ответственного, восстановлении из корзины и (с
  этапа 5) когда закрытая сделка или выполненная задача снова открыта: move_team такие записи
  не переносит, и снова открытая запись догоняет текущий отдел ответственного.
- Ссылка на другую запись (company_id, contact_id…) должна быть видна на чтение, иначе 422.
"""

import uuid
from dataclasses import dataclass
from typing import Any

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Conflict, NotFound, ValidationFailed
from app.models import ROLES_WITH_TEAM, User
from app.repositories.crm import SoftDeleteRepository
from app.uow import UnitOfWork


@dataclass(frozen=True)
class Owner:
    """Новые org_id, team_id и owner_id записи — то, что проверяет access.ensure_allowed()."""

    org_id: uuid.UUID
    team_id: uuid.UUID
    owner_id: uuid.UUID


def can_own(user: User | None, org_id: uuid.UUID) -> bool:
    return (
        user is not None
        and user.org_id == org_id
        and user.is_active
        and user.role in ROLES_WITH_TEAM
        and user.team_id is not None
    )


async def resolve_owner(
    uow: UnitOfWork, access: Access, owner_id: uuid.UUID | None, *, field: str = "owner_id"
) -> Owner:
    """Ответственный и его отдел. Можно ли мне назначить этого человека, проверяет потом
    access.ensure_allowed(): sales_rep может назначить только себя, team_lead — свой отдел.
    field — поле запроса для ошибки 422 (у передачи клиентов — to_user_id)."""
    user = await uow.users.get_for_share(owner_id or access.user_id)
    if not can_own(user, access.org_id):
        # org_admin и viewer сами записей не ведут — нужно указать, кому запись.
        code = "owner.invalid" if owner_id is not None else "owner.required"
        raise ValidationFailed(code, field=field)
    assert user is not None and user.team_id is not None
    return Owner(org_id=access.org_id, team_id=user.team_id, owner_id=user.id)


async def owner_for_restore(uow: UnitOfWork, record: Any) -> Owner:
    """Запись из корзины возвращается в текущий отдел своего ответственного. Если он отключён
    или больше не ведёт записи — 409: сначала запись нужно передать другому."""
    user = await uow.users.get_for_share(record.owner_id)
    if not can_own(user, record.org_id):
        raise Conflict("owner.inactive")
    assert user is not None and user.team_id is not None
    return Owner(org_id=record.org_id, team_id=user.team_id, owner_id=user.id)


async def ensure_reference(
    repo: SoftDeleteRepository[Any],
    entity_id: int | None,
    access: Access,
    roles: RoleCatalog,
    action: str,
    field: str,
) -> None:
    """Ссылка из тела запроса видна мне на чтение (action — например, contact:read).
    Невидимая или несуществующая — 422 с полем: привязать сделку к чужому контакту нельзя."""
    if entity_id is None:
        return
    read = access.for_action(action, roles)
    if read is None:
        raise ValidationFailed(field=field)
    try:
        await repo.get_visible_or_404(entity_id, read)
    except NotFound:
        raise ValidationFailed(field=field) from None


async def get_for_change[M](
    repo: SoftDeleteRepository[M],
    entity_id: int,
    access: Access,
    roles: RoleCatalog,
    read_action: str,
) -> M:
    """Старая версия для изменения или удаления (раздел 5): ищем по уровню чтения — не видна,
    404; видна, но вне уровня действия (чужая компания у sales_rep) — 403."""
    read = access.for_action(read_action, roles) or access
    obj = await repo.get_visible_or_404(entity_id, read)
    access.ensure_allowed(obj)  # type: ignore[arg-type]
    return obj
