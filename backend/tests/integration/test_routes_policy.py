"""Маршруты приложения и routes.rego не должны расходиться (раздел 5).

Эндпоинт без строки в routes.rego получил бы 403 у всех — CI ловит это раньше.
Маршруты берём у живого OPA, а не из файла: проверяем то, что OPA действительно загрузил.
"""

from typing import Any

import httpx
import pytest
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute
from httpx import AsyncClient

from app.api.v1 import admin_router, protected_router
from app.authz.deps import opa_decision
from app.authz.opa import OpaClient

pytestmark = pytest.mark.policy  # совпадает с policies/: make test-policy


def needs_opa(dependant: Dependant) -> bool:
    return any(d.call is opa_decision or needs_opa(d) for d in dependant.dependencies)


def opa_routes() -> set[str]:
    """Все «METHOD /шаблон» роутеров protected и admin. С FastAPI 0.151 app.routes хранит не
    маршруты, а обёртки подключённых роутеров, поэтому берём маршруты у самих роутеров."""
    routes = [
        route
        for router in (protected_router, admin_router)
        for route in router.routes
        if isinstance(route, APIRoute)
    ]
    assert all(needs_opa(route.dependant) for route in routes), "маршрут без проверки OPA"
    return {f"{method} {route.path}" for route in routes for method in route.methods}


async def route_actions(opa_url: str) -> dict[str, Any]:
    async with httpx.AsyncClient() as http:
        return await OpaClient(http, opa_url).route_actions()


async def test_routes_match_policy(opa_url: str) -> None:
    in_app, in_policy = opa_routes(), set(await route_actions(opa_url))

    assert in_app - in_policy == set(), "эндпоинты без строки в policies/saleshub/routes.rego"
    assert in_policy - in_app == set(), "строки routes.rego без эндпоинта (устарели?)"


async def test_swagger_shows_required_action(client: AsyncClient) -> None:
    schema = (await client.get("/openapi.json")).json()

    operation = schema["paths"]["/api/v1/users/{user_id}"]["patch"]
    assert operation["x-required-action"] == "member:manage"
    assert operation["description"].startswith("**Требуется:** `member:manage` (org_admin — org)")
