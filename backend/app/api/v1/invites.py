"""Приглашения в свою организацию (админ организации)."""

from fastapi import Depends, status

from app.api.deps import get_invite_service
from app.api.v1 import protected_router
from app.api.v1.errors import PROTECTED, RATE_LIMITED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.schemas.auth import InviteOut, MemberInviteIn
from app.services.invites import InviteService


@protected_router.post(
    "/invites",
    tags=["users"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **VALIDATION, **RATE_LIMITED},
)
async def invite_member(
    body: MemberInviteIn,
    access: Access = Depends(authorize),
    invites: InviteService = Depends(get_invite_service),
) -> InviteOut:
    """Пригласить человека в свою организацию с ролью и отделом. Отдел обязателен для
    team_lead и sales_rep, у org_admin и viewer его нет. Если email уже есть в базе — тот же
    202 (чтобы нельзя было проверить чужие email), а человеку уходит письмо об этом."""
    invite = await invites.invite_member(access, str(body.email), body.role, body.team_id)
    return InviteOut.model_validate(invite)
