"""Отделы продаж организации."""

import uuid

from fastapi import Depends, status

from app.api.deps import get_team_service
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED
from app.authz.access import Access
from app.authz.deps import authorize
from app.schemas.organizations import TeamIn, TeamOut
from app.services.organizations import TeamService


@protected_router.get("/teams", tags=["teams"], responses=PROTECTED)
async def list_teams(
    access: Access = Depends(authorize), teams: TeamService = Depends(get_team_service)
) -> list[TeamOut]:
    """Отделы моей организации."""
    return [TeamOut.model_validate(team) for team in await teams.list(access)]


@protected_router.post(
    "/teams",
    tags=["teams"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **CONFLICT},
)
async def create_team(
    body: TeamIn,
    access: Access = Depends(authorize),
    teams: TeamService = Depends(get_team_service),
) -> TeamOut:
    """Создать отдел. Название уникально в организации."""
    return TeamOut.model_validate(await teams.create(access, body.name))


@protected_router.patch(
    "/teams/{team_id}", tags=["teams"], responses={**PROTECTED, **NOT_FOUND, **CONFLICT}
)
async def rename_team(
    team_id: uuid.UUID,
    body: TeamIn,
    access: Access = Depends(authorize),
    teams: TeamService = Depends(get_team_service),
) -> TeamOut:
    """Переименовать отдел."""
    return TeamOut.model_validate(await teams.rename(access, team_id, body.name))


@protected_router.delete(
    "/teams/{team_id}",
    tags=["teams"],
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT},
)
async def delete_team(
    team_id: uuid.UUID,
    access: Access = Depends(authorize),
    teams: TeamService = Depends(get_team_service),
) -> None:
    """Удалить отдел. Если в нём есть люди — 409 team.has_members: сначала переведите их."""
    await teams.delete(access, team_id)
