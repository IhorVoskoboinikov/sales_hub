"""Организации: создают супер-админы (/api/v1/admin/orgs), свою смотрит и настраивает её
админ (/api/v1/org). org_id в путях организации нет — она одна и берётся из пользователя."""

import uuid

from fastapi import Depends, status

from app.api.deps import current_user, get_invite_service, get_org_service
from app.api.v1 import admin_router, protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, RATE_LIMITED
from app.authz.access import Access
from app.authz.deps import authorize
from app.schemas.auth import InviteIn, InviteOut
from app.schemas.organizations import OrgCreate, OrgOut, OrgUpdate
from app.services.auth import CurrentUser
from app.services.invites import InviteService
from app.services.organizations import OrganizationService


@admin_router.get("/orgs", responses=PROTECTED)
async def list_orgs(orgs: OrganizationService = Depends(get_org_service)) -> list[OrgOut]:
    """Все организации."""
    return [OrgOut.model_validate(org) for org in await orgs.list_all()]


@admin_router.post(
    "/orgs", status_code=status.HTTP_201_CREATED, responses={**PROTECTED, **CONFLICT}
)
async def create_org(
    body: OrgCreate,
    admin: CurrentUser = Depends(current_user),
    orgs: OrganizationService = Depends(get_org_service),
) -> OrgOut:
    """Создать организацию. Дальше — пригласить её первого админа:
    POST /api/v1/admin/orgs/{org_id}/invites."""
    return OrgOut.model_validate(await orgs.create(body.model_dump(), admin.id))


@admin_router.post(
    "/orgs/{org_id}/invites",
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **NOT_FOUND, **RATE_LIMITED},
)
async def invite_first_admin(
    org_id: uuid.UUID,
    body: InviteIn,
    admin: CurrentUser = Depends(current_user),
    invites: InviteService = Depends(get_invite_service),
) -> InviteOut:
    """Пригласить администратора организации (роль org_admin). Письмо с токеном уйдёт после
    ответа — смотрите в Mailpit (localhost:8025). Если email уже есть в базе — тот же 202,
    а человеку уходит письмо «у вас уже есть учётная запись»."""
    invite = await invites.invite_first_admin(org_id, str(body.email), admin.id)
    return InviteOut.model_validate(invite)


@protected_router.get("/org", tags=["org"], responses=PROTECTED)
async def get_org(
    access: Access = Depends(authorize), orgs: OrganizationService = Depends(get_org_service)
) -> OrgOut:
    """Моя организация."""
    return OrgOut.model_validate(await orgs.get_mine(access))


@protected_router.patch("/org", tags=["org"], responses=PROTECTED)
async def update_org(
    body: OrgUpdate,
    access: Access = Depends(authorize),
    orgs: OrganizationService = Depends(get_org_service),
) -> OrgOut:
    """Настройки организации: название, валюта отображения, часовой пояс."""
    return OrgOut.model_validate(
        await orgs.update_mine(access, body.model_dump(exclude_unset=True))
    )
