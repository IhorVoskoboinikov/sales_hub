"""Организации (только супер-админ) и отделы (админ организации) — раздел 5."""

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from app.authz.access import Access
from app.errors import Conflict, NotFound
from app.models import Organization, Team
from app.repositories.audit import diff
from app.uow import UnitOfWork


class OrganizationService:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def list_all(self) -> list[Organization]:
        return await self.uow.organizations.list()

    async def create(self, fields: dict[str, Any], actor_id: uuid.UUID) -> Organization:
        if await self.uow.organizations.get_by_slug(fields["slug"]) is not None:
            raise Conflict("org.slug_taken")
        org = Organization(id=uuid.uuid4(), created_at=datetime.now(UTC), **fields)
        self.uow.organizations.add(org)
        self.uow.audit.record(
            actor_id=actor_id,
            org_id=org.id,
            action="org.created",
            entity_type="organization",
            entity_id=org.id,
            changes=fields,
        )
        await self.uow.commit()
        return org

    async def get_mine(self, access: Access) -> Organization:
        org = await self.uow.organizations.get(access.org_id)
        if org is None:
            raise NotFound(entity="organization")
        return org

    async def update_mine(self, access: Access, changes: dict[str, Any]) -> Organization:
        org = await self.get_mine(access)
        changed = diff({field: getattr(org, field) for field in changes}, changes)
        for field, value in changes.items():
            setattr(org, field, value)
        if changed:
            self._audit(access, "org.updated", "organization", org.id, changed)
        await self.uow.commit()
        return org

    def _audit(
        self, access: Access, action: str, entity_type: str, entity_id: object, changes: Any
    ) -> None:
        self.uow.audit.record(
            actor_id=access.user_id,
            org_id=access.org_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            changes=changes,
        )


class TeamService:
    """Отделы — ресурс уровня организации: Access здесь всегда org, фильтр — по org_id."""

    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def list(self, access: Access) -> list[Team]:
        return await self.uow.teams.list(access.org_id)

    async def create(self, access: Access, name: str) -> Team:
        await self._ensure_name_free(access.org_id, name)
        team = Team(id=uuid.uuid4(), org_id=access.org_id, name=name, created_at=datetime.now(UTC))
        self.uow.teams.add(team)
        self._audit(access, "team.created", team.id, {"name": name})
        await self.uow.commit()
        return team

    async def rename(self, access: Access, team_id: uuid.UUID, name: str) -> Team:
        team = await self._get_or_404(access, team_id)
        if team.name != name:
            await self._ensure_name_free(access.org_id, name)
            self._audit(access, "team.renamed", team.id, {"name": [team.name, name]})
            team.name = name
            await self.uow.commit()
        return team

    async def delete(self, access: Access, team_id: uuid.UUID) -> None:
        team = await self._get_or_404(access, team_id, lock="update")
        if await self.uow.teams.count_members(team.id):
            raise Conflict("team.has_members")
        if await self.uow.live_records.exist(team_id=team.id):
            raise Conflict("team.has_records")
        await self.uow.teams.delete(team)
        self._audit(access, "team.deleted", team.id, {"name": team.name})
        await self.uow.commit()

    async def _get_or_404(
        self, access: Access, team_id: uuid.UUID, lock: Literal["update"] | None = None
    ) -> Team:
        team = await self.uow.teams.get(team_id, access.org_id, lock=lock)
        if team is None:  # отдел чужой организации — как несуществующий
            raise NotFound(entity="team", id=str(team_id))
        return team

    def _audit(self, access: Access, action: str, team_id: uuid.UUID, changes: Any) -> None:
        self.uow.audit.record(
            actor_id=access.user_id,
            org_id=access.org_id,
            action=action,
            entity_type="team",
            entity_id=team_id,
            changes=changes,
        )

    async def _ensure_name_free(self, org_id: uuid.UUID, name: str) -> None:
        if await self.uow.teams.get_by_name(org_id, name) is not None:
            raise Conflict("team.name_taken")
