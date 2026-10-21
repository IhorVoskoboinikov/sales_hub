"""Сборка приложения. Запуск: uvicorn app.main:create_app --factory"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from redis.asyncio import Redis

from app.api import health, well_known
from app.api.v1 import (  # noqa: F401 — модули с эндпоинтами регистрируют маршруты
    activities,
    admin_router,
    audit,
    auth,
    companies,
    contacts,
    deals,
    invites,
    organizations,
    pipelines,
    protected_router,
    teams,
    users,
)
from app.authz.opa import OpaClient
from app.authz.openapi import fetch_route_actions, install_openapi
from app.authz.roles import RoleCatalog
from app.core.config import Settings, get_settings
from app.core.jwt import KeySet
from app.core.logging import configure_logging
from app.db.session import create_engine, create_session_factory
from app.emails.sender import SmtpEmailSender
from app.errors.handlers import register_error_handlers
from app.middleware.access_log import AccessLogMiddleware
from app.middleware.request_id import RequestIdMiddleware
from app.middleware.unhandled_errors import UnhandledErrorMiddleware
from app.services.fx import NbuClient


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, json=settings.log_json)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Создаётся один раз при старте и закрывается при остановке процесса.
        # Соединения открываются лениво — при первом запросе, а не здесь.
        app.state.keys = KeySet.from_settings(settings)  # нет ключа — сервер не стартует
        engine = create_engine(settings.database_url)
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        app.state.redis = Redis.from_url(
            settings.redis_url, socket_connect_timeout=2, socket_timeout=2
        )
        http = httpx.AsyncClient()  # один пул соединений к OPA на процесс
        app.state.opa = OpaClient(http, settings.opa_url)
        app.state.nbu = NbuClient(http, settings.nbu_url)  # тот же пул: OPA и НБУ — разные хосты
        if settings.show_docs:
            app.state.route_actions = await fetch_route_actions(
                app.state.opa, settings.opa_startup_attempts
            )
        try:
            yield
        finally:
            await http.aclose()
            await app.state.redis.aclose()
            await engine.dispose()

    app = FastAPI(
        title="SalesHub API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.show_docs else None,
        redoc_url="/redoc" if settings.show_docs else None,
        openapi_url="/openapi.json" if settings.show_docs else None,
    )
    app.state.settings = settings
    # Тот же data.yaml, что читает OPA. Нет файла — приложение не создаётся.
    app.state.roles = RoleCatalog.load(settings.policies_dir)
    app.state.email_sender = SmtpEmailSender(
        host=settings.smtp_host,
        port=settings.smtp_port,
        sender=settings.email_from,
        username=settings.smtp_username,
        password=settings.smtp_password.get_secret_value() if settings.smtp_password else None,
        start_tls=settings.smtp_start_tls,
    )

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
    # Модули с эндпоинтами импортированы выше и объявили маршруты на этих роутерах. FastAPI 0.151
    # подключает роутер обёрткой (_IncludedRouter) и берёт его маршруты при каждом поиске.
    app.include_router(protected_router)
    app.include_router(admin_router)
    return app
