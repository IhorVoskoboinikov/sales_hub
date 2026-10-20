"""Маршруты приложения и routes.rego не должны расходиться (раздел 5).

Эндпоинт без строки в routes.rego получил бы 403 у всех — CI ловит это раньше.
Маршруты берём у живого OPA, а не из файла: проверяем то, что OPA действительно загрузил.
"""

from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute

from app.authz.deps import opa_decision
from app.authz.opa import OpaClient


def needs_opa(dependant: Dependant) -> bool:
    return any(d.call is opa_decision or needs_opa(d) for d in dependant.dependencies)


def opa_routes(app: FastAPI) -> set[str]:
    """Все «METHOD /шаблон», которые спрашивают OPA (роутеры protected и admin)."""
    return {
        f"{method} {route.path}"
        for route in app.routes
        if isinstance(route, APIRoute) and needs_opa(route.dependant)
        for method in route.methods
    }


async def route_actions(opa_url: str) -> dict[str, Any]:
    async with httpx.AsyncClient() as http:
        return await OpaClient(http, opa_url).route_actions()


async def test_every_protected_route_is_in_routes_rego(app: FastAPI, opa_url: str) -> None:
    missing = opa_routes(app) - set(await route_actions(opa_url))

    assert missing == set(), f"добавьте строки в policies/saleshub/routes.rego: {missing}"
