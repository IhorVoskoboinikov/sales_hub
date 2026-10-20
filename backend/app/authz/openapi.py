"""«Требуется: …» в Swagger (раздел 5): у каждой защищённой операции — действие из routes.rego
и уровни ролей из data.yaml. Так права видны прямо в документации, а не только в коде."""

import asyncio
from typing import Any

import structlog
from fastapi import FastAPI

from app.authz.opa import OpaClient
from app.authz.roles import RoleCatalog
from app.errors import ServiceUnavailable

logger = structlog.get_logger("authz")

HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


async def fetch_route_actions(opa: OpaClient, attempts: int) -> dict[str, str]:
    """Маршруты из OPA при старте. OPA может подняться чуть позже API — пробуем несколько раз;
    не вышло — документация будет без прав, а приложение всё равно стартует."""
    for attempt in range(1, attempts + 1):
        try:
            return await opa.route_actions()
        except ServiceUnavailable:
            if attempt < attempts:
                await asyncio.sleep(1)
    logger.warning("openapi_without_permissions", reason="opa_unavailable")
    return {}


def describe_permissions(
    schema: dict[str, Any], actions: dict[str, str], roles: RoleCatalog
) -> dict[str, Any]:
    for path, operations in schema.get("paths", {}).items():
        for method, operation in operations.items():
            if method not in HTTP_METHODS:
                continue
            action = actions.get(f"{method.upper()} {path}")
            if action is None:
                continue
            if action.startswith("admin:"):
                who = "только супер-админ"
            else:
                levels = roles.levels(action)
                who = ", ".join(f"{role} — {level}" for role, level in levels.items())
            note = f"**Требуется:** `{action}` ({who})."
            description = operation.get("description", "")
            operation["description"] = f"{note}\n\n{description}" if description else note
            operation["x-required-action"] = action  # то же — для генераторов клиентов
    return schema


def install_openapi(app: FastAPI, roles: RoleCatalog) -> None:
    """Схема строится при первом запросе /openapi.json — к этому моменту lifespan уже прочитал
    маршруты из OPA в app.state.route_actions."""
    build = app.openapi  # штатная сборка FastAPI; она же кэширует схему в app.openapi_schema

    def openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = build()
        actions: dict[str, str] = getattr(app.state, "route_actions", {})
        if not actions:
            # Без прав не кэшируем. Маршруты читаются только при старте, поэтому права
            # появятся в Swagger после перезапуска API, когда OPA уже отвечает.
            app.openapi_schema = None
            return schema
        app.openapi_schema = describe_permissions(schema, actions, roles)
        return app.openapi_schema

    app.openapi = openapi  # type: ignore[method-assign]
