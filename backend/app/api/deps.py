"""Зависимости FastAPI: отдают то, что создано при старте приложения (lifespan в main.py),
и собирают сервисы для эндпоинтов."""

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import cast

import structlog
from fastapi import Depends, Query, Request
from fastapi.security import OAuth2PasswordBearer
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.authz.opa import OpaClient
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.core.jwt import KeySet
from app.errors import Unauthorized
from app.repositories.pagination import DEFAULT_LIMIT, MAX_LIMIT, Page
from app.repositories.rate_limiter import RateLimiter
from app.repositories.user_cache import UserCache
from app.services.activities import ActivityService, AuditService, NoteService
from app.services.auth import AuthService, ClientInfo, CurrentUser
from app.services.contacts import CompanyService, ContactService
from app.services.deals import DealService, PipelineService
from app.services.fx import FxService, NbuClient
from app.services.invites import InviteService
from app.services.members import MemberService
from app.services.organizations import OrganizationService, TeamService
from app.services.password_reset import PasswordResetService
from app.services.reports import ReportService
from app.uow import UnitOfWork

# Кнопка Authorize в Swagger: форма логина отправляется на tokenUrl.
# auto_error=False — нет токена → наш Unauthorized в общем формате, а не HTTPException FastAPI.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)


def get_app_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_engine(request: Request) -> AsyncEngine:
    return cast(AsyncEngine, request.app.state.engine)


def get_redis(request: Request) -> Redis:
    return cast(Redis, request.app.state.redis)


def get_key_set(request: Request) -> KeySet:
    return cast(KeySet, request.app.state.keys)


def get_opa(request: Request) -> OpaClient:
    return cast(OpaClient, request.app.state.opa)


def get_roles(request: Request) -> RoleCatalog:
    return cast(RoleCatalog, request.app.state.roles)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    factory = cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)
    async with factory() as session:  # одна сессия на запрос
        yield session  # закроется после ответа


def get_uow(session: AsyncSession = Depends(get_session)) -> UnitOfWork:
    return UnitOfWork(session)


def get_user_cache(redis: Redis = Depends(get_redis)) -> UserCache:
    return UserCache(redis)


def get_rate_limiter(redis: Redis = Depends(get_redis)) -> RateLimiter:
    return RateLimiter(redis)


def get_auth_service(
    uow: UnitOfWork = Depends(get_uow),
    keys: KeySet = Depends(get_key_set),
    cache: UserCache = Depends(get_user_cache),
    limiter: RateLimiter = Depends(get_rate_limiter),
    settings: Settings = Depends(get_app_settings),
) -> AuthService:
    return AuthService(uow, keys, cache, limiter, timedelta(days=settings.refresh_ttl_days))


def get_invite_service(
    uow: UnitOfWork = Depends(get_uow),
    limiter: RateLimiter = Depends(get_rate_limiter),
    settings: Settings = Depends(get_app_settings),
) -> InviteService:
    # Письма — командой в outbox (InviteService), отправляет их воркер: SMTP у API нет.
    return InviteService(
        uow,
        limiter,
        timedelta(days=settings.invite_ttl_days),
        f"{settings.public_url}/docs",
    )


def get_password_reset_service(
    uow: UnitOfWork = Depends(get_uow),
    cache: UserCache = Depends(get_user_cache),
    limiter: RateLimiter = Depends(get_rate_limiter),
    settings: Settings = Depends(get_app_settings),
) -> PasswordResetService:
    return PasswordResetService(
        uow,
        cache,
        limiter,
        timedelta(minutes=settings.reset_password_ttl_minutes),
        f"{settings.public_url}/docs",
    )


def get_org_service(uow: UnitOfWork = Depends(get_uow)) -> OrganizationService:
    return OrganizationService(uow)


def get_team_service(uow: UnitOfWork = Depends(get_uow)) -> TeamService:
    return TeamService(uow)


def get_member_service(
    uow: UnitOfWork = Depends(get_uow), cache: UserCache = Depends(get_user_cache)
) -> MemberService:
    return MemberService(uow, cache)


def get_company_service(
    uow: UnitOfWork = Depends(get_uow), roles: RoleCatalog = Depends(get_roles)
) -> CompanyService:
    return CompanyService(uow, roles)


def get_contact_service(
    uow: UnitOfWork = Depends(get_uow), roles: RoleCatalog = Depends(get_roles)
) -> ContactService:
    return ContactService(uow, roles)


def get_pipeline_service(uow: UnitOfWork = Depends(get_uow)) -> PipelineService:
    return PipelineService(uow)


def get_fx_service(
    request: Request, uow: UnitOfWork = Depends(get_uow), redis: Redis = Depends(get_redis)
) -> FxService:
    return FxService(uow, redis, cast(NbuClient, request.app.state.nbu))


def get_deal_service(
    uow: UnitOfWork = Depends(get_uow),
    roles: RoleCatalog = Depends(get_roles),
    fx: FxService = Depends(get_fx_service),
) -> DealService:
    return DealService(uow, roles, fx)


def get_activity_service(
    uow: UnitOfWork = Depends(get_uow), roles: RoleCatalog = Depends(get_roles)
) -> ActivityService:
    return ActivityService(uow, roles)


def get_note_service(
    uow: UnitOfWork = Depends(get_uow), roles: RoleCatalog = Depends(get_roles)
) -> NoteService:
    return NoteService(uow, roles)


def get_audit_service(uow: UnitOfWork = Depends(get_uow)) -> AuditService:
    return AuditService(uow)


def get_report_service(
    uow: UnitOfWork = Depends(get_uow), fx: FxService = Depends(get_fx_service)
) -> ReportService:
    return ReportService(uow, fx)


def get_page(
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Строк на странице"),
    cursor: str | None = Query(None, description="next_cursor из предыдущей страницы"),
) -> Page:
    return Page(limit=limit, cursor=cursor)


def get_client_info(request: Request) -> ClientInfo:
    user_agent = request.headers.get("user-agent")
    return ClientInfo(
        ip=request.client.host if request.client else None,
        user_agent=user_agent[:255] if user_agent else None,
    )


async def current_user(
    request: Request,
    token: str | None = Depends(oauth2_scheme),
    auth: AuthService = Depends(get_auth_service),
) -> CurrentUser:
    """Кто делает запрос (раздел 4): Bearer-токен → подпись и сроки → пользователь."""
    if token is None:
        raise Unauthorized()
    user = await auth.authenticate(token)
    request.state.user_id = user.id  # для access-лога
    structlog.contextvars.bind_contextvars(user_id=str(user.id))  # для остальных строк лога
    return user
