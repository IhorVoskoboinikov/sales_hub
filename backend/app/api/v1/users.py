"""Люди организации."""

import uuid

from fastapi import Depends

from app.api.deps import get_member_service
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.schemas.organizations import MemberOut, MemberUpdate, MemberUpdateOut
from app.services.members import MemberChanges, MemberService


@protected_router.get("/users", tags=["users"], responses=PROTECTED)
async def list_users(
    access: Access = Depends(authorize), members: MemberService = Depends(get_member_service)
) -> list[MemberOut]:
    """Люди организации. Руководитель отдела и менеджер видят только свой отдел."""
    return [MemberOut.model_validate(user) for user in await members.list(access)]


@protected_router.patch(
    "/users/{user_id}",
    tags=["users"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT, **VALIDATION},
)
async def update_user(
    user_id: uuid.UUID,
    body: MemberUpdate,
    access: Access = Depends(authorize),
    members: MemberService = Depends(get_member_service),
) -> MemberUpdateOut:
    """Сменить роль, отдел или отключить пользователя. Правила:

    - себе менять нельзя — 403 `member.cannot_change_self`;
    - в организации остаётся хотя бы один активный админ — 409 `member.last_admin`;
    - отдел обязателен для team_lead и sales_rep и не положен остальным — 422;
    - перевод в другой отдел человека с записями: его живые компании и контакты, открытые
      сделки и открытые активности догоняет фоновая job `move_team` — её id в `bulk_job_id`
      (null, если переносить нечего); пока она идёт, часть записей ещё в старом отделе;
    - сделать org_admin или viewer человека с живыми записями нельзя — 409
      `member.has_records`: сначала передайте их (`POST /bulk/reassign-owner`);
    - отключение (`is_active: false`) сразу закрывает все сессии пользователя.

    Новая роль действует со следующего запроса: роли нет в токене."""
    changes = MemberChanges(
        role=body.role,
        team_given="team_id" in body.model_fields_set,
        team_id=body.team_id,
        is_active=body.is_active,
    )
    changed = await members.change(access, user_id, changes)
    out = MemberUpdateOut.model_validate(changed.user)
    out.bulk_job_id = changed.bulk_job_id
    return out
