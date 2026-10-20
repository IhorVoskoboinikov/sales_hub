"""Люди организации: список, смена роли и отдела, отключение (раздел 5, «Кто управляет
пользователями и ролями»).

OPA решает только «может ли роль вызвать PATCH /users/{id}» (member:manage — только
org_admin). Всё остальное — бизнес-правила, их проверяет этот сервис.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from app.authz.access import Access
from app.errors import Conflict, Forbidden, NotFound, ValidationFailed
from app.models import ROLES_WITH_TEAM, Role, User
from app.repositories.audit import diff
from app.repositories.user_cache import UserCache
from app.uow import UnitOfWork


@dataclass(frozen=True)
class MemberChanges:
    """None — поле не меняется. Для отдела null значим («снять отдел»), поэтому отдельный
    флаг: передан ли team_id вообще."""

    role: Role | None = None
    team_given: bool = False
    team_id: uuid.UUID | None = None
    is_active: bool | None = None


class MemberService:
    def __init__(self, uow: UnitOfWork, cache: UserCache) -> None:
        self.uow = uow
        self.cache = cache

    async def list(self, access: Access) -> list[User]:
        return await self.uow.users.list_visible(access)

    async def change(self, access: Access, user_id: uuid.UUID, changes: MemberChanges) -> User:
        if user_id == access.user_id:
            # Иначе админ мог бы случайно разжаловать себя и потерять доступ к настройкам.
            raise Forbidden("member.cannot_change_self")
        # Блокировка организации: два админа, снимающие друг друга одновременно, выполнятся
        # по очереди, и второй увидит, что админ остался один (иначе — ни одного).
        await self.uow.organizations.get(access.org_id, for_update=True)
        # Порядок блокировок: организация → пользователь (→ его токены при отключении).
        user = await self.uow.users.get_visible(user_id, access, for_update=True)
        if user is None:
            raise NotFound(entity="user", id=str(user_id))
        assert user.role is not None  # у пользователя организации роль есть всегда (CHECK)

        role = changes.role or Role(user.role)
        team_id = await self._new_team(access, user, role, changes)
        is_active = user.is_active if changes.is_active is None else changes.is_active

        loses_admin = (
            user.role == Role.ORG_ADMIN
            and user.is_active
            and (role != Role.ORG_ADMIN or not is_active)
        )
        if loses_admin and await self.uow.users.count_active_admins(access.org_id) <= 1:
            raise Conflict("member.last_admin")

        if user.is_active and not is_active:
            # Отключение действует сразу: все сессии закрыты, живые access-токены мертвы.
            user.token_version += 1
            await self.uow.refresh_tokens.revoke_all_for_user(user.id, datetime.now(UTC))
        before = {"role": user.role, "team_id": user.team_id, "is_active": user.is_active}
        user.role, user.team_id, user.is_active = role, team_id, is_active
        after = {"role": user.role, "team_id": user.team_id, "is_active": user.is_active}
        if changed := diff(before, after):
            self.uow.audit.record(
                actor_id=access.user_id,
                org_id=access.org_id,
                action="member.updated",
                entity_type="user",
                entity_id=user.id,
                changes=changed,
            )
        await self.uow.commit()
        # После коммита: иначе другой запрос успел бы положить в кэш старую роль.
        await self.cache.invalidate(user.id)
        return user

    async def _new_team(
        self, access: Access, user: User, role: Role, changes: MemberChanges
    ) -> uuid.UUID | None:
        if role not in ROLES_WITH_TEAM:
            if changes.team_id is not None:
                raise ValidationFailed("member.team_not_allowed", field="team_id")
            return None  # стал org_admin или viewer — отдел снимается сам
        team_id = changes.team_id if changes.team_given else user.team_id
        if team_id is None:
            raise ValidationFailed("member.team_required", field="team_id")
        if await self.uow.teams.get(team_id, access.org_id, lock="key_share") is None:
            raise ValidationFailed("member.team_not_found", field="team_id")
        return team_id
