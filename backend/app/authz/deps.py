"""Зависимости прав: opa_decision (шаг 1, OPA) и authorize (шаг 2, Access)."""

import structlog
from fastapi import Depends, Request

from app.api.deps import current_user, get_opa, get_roles
from app.authz.access import Access
from app.authz.opa import Decision, OpaClient
from app.authz.roles import RoleCatalog
from app.errors import Forbidden
from app.services.auth import CurrentUser

logger = structlog.get_logger("authz")


async def opa_decision(
    request: Request,
    user: CurrentUser = Depends(current_user),
    opa: OpaClient = Depends(get_opa),
) -> Decision:
    """Шаг 1: может ли роль вызвать этот эндпоинт. Спрашиваем OPA один раз на запрос."""
    # Шаблон пути (/api/v1/users/{user_id}), а не реальный URL: в routes.rego — шаблоны.
    route = request.scope["route"].path
    decision = await opa.decide(
        request.method, route, user.id, role=user.role, is_superadmin=user.is_superadmin
    )
    if not decision.allow:
        logger.info("authz_denied", action=decision.action, route=route, role=user.role)
        raise Forbidden(action=decision.action)
    return decision


async def authorize(
    user: CurrentUser = Depends(current_user),
    decision: Decision = Depends(opa_decision),
    roles: RoleCatalog = Depends(get_roles),
) -> Access:
    """Шаг 2: уровень из data.yaml → Access для сервиса и репозитория.

    Стоит и на роутере, и в параметрах эндпоинта — FastAPI кэширует зависимость в пределах
    запроса, поэтому OPA спрашивается один раз."""
    if user.org_id is None or decision.action is None:
        raise Forbidden()  # без организации данных нет; OPA такого не пропустит, это страховка
    return Access(
        action=decision.action,
        scope=roles.scope_for(user.role, decision.action),
        user_id=user.id,
        org_id=user.org_id,
        team_id=user.team_id,
    )
