"""Сборка приложения. Запуск: uvicorn app.main:create_app --factory"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import health, well_known
from app.api.v1 import (  # noqa: F401 — модули с эндпоинтами регистрируют маршруты
    activities,
    admin_router,
    audit,
    auth,
    bulk,
    bulk_jobs,
    companies,
    contacts,
    deals,
    failed_tasks,
    invites,
    me,
    organizations,
    pipelines,
    protected_router,
    reports,
    teams,
    users,
)
from app.authz.openapi import fetch_route_actions, install_openapi
from app.authz.roles import RoleCatalog
from app.core.config import Settings, get_settings
from app.core.jwt import KeySet
from app.core.logging import configure_logging
from app.core.resources import Resources
from app.errors.handlers import register_error_handlers
from app.middleware.access_log import AccessLogMiddleware
from app.middleware.request_id import RequestIdMiddleware
from app.middleware.unhandled_errors import UnhandledErrorMiddleware
from app.services.exports import ExportSlots


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, json=settings.log_json)

    # Тот же data.yaml, что читает OPA. Нет файла — приложение не создаётся.
    roles = RoleCatalog.load(settings.policies_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Создаётся один раз при старте и закрывается при остановке процесса.
        # Соединения открываются лениво — при первом запросе, а не здесь.
        app.state.keys = KeySet.from_settings(settings)  # нет ключа — сервер не стартует
        resources = Resources.create(settings, roles=roles)
        # Те же атрибуты app.state, что читает api/deps.py.
        app.state.engine = resources.engine
        app.state.session_factory = resources.session_factory
        app.state.redis = resources.redis
        app.state.opa = resources.opa
        app.state.nbu = resources.nbu
        app.state.storage = resources.storage
        try:
            if settings.show_docs:
                app.state.route_actions = await fetch_route_actions(
                    resources.opa, settings.opa_startup_attempts
                )
            yield
        finally:
            await resources.aclose()

    app = FastAPI(
        title="SalesHub API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.show_docs else None,
        redoc_url="/redoc" if settings.show_docs else None,
        openapi_url="/openapi.json" if settings.show_docs else None,
    )
    app.state.settings = settings
    app.state.roles = roles
    app.state.export_slots = ExportSlots()  # идущие выгрузки CSV процесса (раздел 6)

    register_error_handlers(app)
    install_openapi(app, app.state.roles)

    # add_middleware добавляет слой снаружи предыдущих, поэтому порядок здесь — изнутри наружу.
    # Запрос проходит: RequestId → AccessLog → UnhandledError → маршрут.
    app.add_middleware(UnhandledErrorMiddleware)
    app.add_middleware(AccessLogMiddleware)
    app.add_middleware(RequestIdMiddleware)

    app.include_router(health.router)
    app.include_router(well_known.router)
    app.include_router(auth.router)
    app.include_router(me.router)
    # Модули с эндпоинтами импортированы выше и объявили маршруты на этих роутерах. FastAPI 0.151
    # подключает роутер обёрткой (_IncludedRouter) и берёт его маршруты при каждом поиске.
    app.include_router(protected_router)
    app.include_router(admin_router)
    return app
