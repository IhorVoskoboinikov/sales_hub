"""Зависимости FastAPI: отдают то, что создано при старте приложения (lifespan в main.py),
и собирают сервисы для эндпоинтов."""

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
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
from app.errors import Forbidden, Unauthorized
from app.repositories.pagination import DEFAULT_LIMIT, MAX_LIMIT, Page
from app.repositories.rate_limiter import RateLimiter
from app.repositories.user_cache import UserCache
from app.services.activities import ActivityService, AuditService, NoteService
from app.services.auth import AuthService, ClientInfo, CurrentUser
from app.services.bulk import BulkService
from app.services.contacts import CompanyService, ContactService
from app.services.deals import DealService, PipelineService
from app.services.exports import ExportService, ExportSlots
from app.services.failed_tasks import FailedTaskService
from app.services.fx import FxService, NbuClient
from app.services.imports import ImportService
from app.services.invites import InviteService
from app.services.members import MemberService
from app.services.notification_settings import NotificationSettingsService
from app.services.organizations import OrganizationService, TeamService
from app.services.password_reset import PasswordResetService
from app.services.reports import ReportService
from app.services.telegram import TelegramService
from app.storage import FileStorage
from app.uow import UnitOfWork
from app.workers.celery_app import celery_app
from app.workers.queue import CeleryTaskQueue, CeleryTaskSender, TaskQueue, TaskSender

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


def get_storage(request: Request) -> FileStorage:
    """Хранилище файлов (S3) процесса. В тестах API — фейк в памяти."""
    return cast(FileStorage, request.app.state.storage)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    factory = cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)
    async with factory() as session:  # одна сессия на запрос
        yield session  # закроется после ответа


# Короткая сессия вне сессии запроса: `async with scope() as session`. Нужна потоку SSE — он
# опрашивает job раз в секунду и не держит соединение между опросами (раздел 15).
SessionScope = Callable[[], AbstractAsyncContextManager[AsyncSession]]


def get_session_scope(request: Request) -> SessionScope:
    return cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)


def get_uow(session: AsyncSession = Depends(get_session)) -> UnitOfWork:
    return UnitOfWork(session)


def get_task_queue() -> TaskQueue:
    """Постановка задач Celery после коммита (best-effort). В тестах — фейк."""
    return CeleryTaskQueue(celery_app)


def get_task_sender() -> TaskSender:
    """Перезапуск упавшей задачи супер-админом: ошибка брокера — 503. В тестах — фейк."""
    return CeleryTaskSender(celery_app)


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
    uow: UnitOfWork = Depends(get_uow),
    cache: UserCache = Depends(get_user_cache),
    queue: TaskQueue = Depends(get_task_queue),
) -> MemberService:
    # queue — bulk.plan для job move_team после перевода человека с записями.
    return MemberService(uow, cache, queue)


def get_company_service(
    uow: UnitOfWork = Depends(get_uow), roles: RoleCatalog = Depends(get_roles)
) -> CompanyService:
    return CompanyService(uow, roles)


def get_contact_service(
    uow: UnitOfWork = Depends(get_uow), roles: RoleCatalog = Depends(get_roles)
) -> ContactService:
    return ContactService(uow, roles)


def get_export_service(
    uow: UnitOfWork = Depends(get_uow),
    limiter: RateLimiter = Depends(get_rate_limiter),
    settings: Settings = Depends(get_app_settings),
) -> ExportService:
    return ExportService(
        uow,
        limiter,
        max_rows=settings.export_max_rows,
        rate_per_hour=settings.export_rate_per_hour,
    )


async def export_slot(
    request: Request, settings: Settings = Depends(get_app_settings)
) -> AsyncIterator[None]:
    """Слот выгрузки CSV на всё время запроса — пока тело не отдано (как и сессия запроса):
    больше EXPORT_MAX_CONCURRENT выгрузок на процесс — 429 export.busy."""
    slots = cast(ExportSlots, request.app.state.export_slots)
    with slots.hold(settings.export_max_concurrent):
        yield


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


def get_bulk_service(
    uow: UnitOfWork = Depends(get_uow),
    roles: RoleCatalog = Depends(get_roles),
    queue: TaskQueue = Depends(get_task_queue),
    settings: Settings = Depends(get_app_settings),
) -> BulkService:
    return BulkService(uow, roles, queue, chunk_size=settings.bulk_chunk_size)


def get_import_service(
    bulk: BulkService = Depends(get_bulk_service), storage: FileStorage = Depends(get_storage)
) -> ImportService:
    return ImportService(bulk, storage)


def get_failed_task_service(
    uow: UnitOfWork = Depends(get_uow), sender: TaskSender = Depends(get_task_sender)
) -> FailedTaskService:
    return FailedTaskService(uow, sender)


def get_telegram_service(
    uow: UnitOfWork = Depends(get_uow),
    cache: UserCache = Depends(get_user_cache),
    settings: Settings = Depends(get_app_settings),
) -> TelegramService:
    # Имя бота — только если бот настроен целиком: без токена ссылка вела бы в никуда.
    return TelegramService(
        uow,
        cache,
        link_ttl=timedelta(minutes=settings.telegram_link_ttl_minutes),
        bot_username=settings.telegram_bot_username if settings.telegram_enabled else None,
    )


def get_notification_settings_service(
    uow: UnitOfWork = Depends(get_uow),
) -> NotificationSettingsService:
    return NotificationSettingsService(uow)


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


async def current_member(user: CurrentUser = Depends(current_user)) -> CurrentUser:
    """Пользователь организации — для личных эндпоинтов /api/v1/me/* (раздел 5): они без OPA и
    работают только с записями самого пользователя. Без организации (супер-админ) — 403."""
    if user.org_id is None:
        raise Forbidden()
    return user
