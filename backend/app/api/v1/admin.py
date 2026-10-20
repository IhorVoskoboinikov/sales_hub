"""Эндпоинты супер-админа: /api/v1/admin. Весь роутер закрыт зависимостью require_superadmin."""

from typing import Any

from fastapi import APIRouter, Depends, status

from app.api.deps import get_invite_service, require_superadmin
from app.schemas.auth import InviteIn, InviteOut
from app.schemas.errors import ErrorOut
from app.services.invites import InviteService

router = APIRouter(
    prefix="/api/v1/admin", tags=["admin"], dependencies=[Depends(require_superadmin)]
)

ADMIN_ERRORS: dict[int | str, dict[str, Any]] = {
    401: {"model": ErrorOut},
    403: {"model": ErrorOut, "description": "Не супер-админ"},
}


@router.post(
    "/invites",
    status_code=status.HTTP_202_ACCEPTED,
    responses={**ADMIN_ERRORS, 409: {"model": ErrorOut, "description": "Email уже в базе"}},
)
async def invite(body: InviteIn, invites: InviteService = Depends(get_invite_service)) -> InviteOut:
    """Пригласить по email (только этап 2: без организации и роли). Письмо с токеном уйдёт
    после ответа — смотрите в Mailpit (localhost:8025). Повторное приглашение того же email
    отменяет предыдущее.

    На этапе 3 заменяется на POST /api/v1/admin/orgs/{org_id}/invites и POST /api/v1/invites."""
    return InviteOut.model_validate(await invites.invite(str(body.email)))
