"""Матрица прав «роль × эндпоинт × свой / чужой объект» (разделы 5 и 10).

Ожидаемый ответ не пишется руками, а считается из тех же источников, что и права:
- маршрут → действие — у живого OPA (routes.rego);
- роль → действие и уровень — из policies/saleshub/data.yaml.

Правило: действия нет у роли → 403; объект в уровне роли → 2xx; объект вне уровня (чужая
организация) → 404 — не раскрываем, что он существует. Супер-админ проходит только admin:*.
Новый маршрут в routes.rego без описания здесь роняет test_every_route_has_a_case.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from httpx import AsyncClient

from app.authz.opa import ROUTES_PATH
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.core.jwt import KeySet
from app.models import Team, User
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import MakeOrg, MakeUser, OrgFixture, member

ROLES = ["org_admin", "team_lead", "sales_rep", "viewer", "super_admin"]
CATALOG = RoleCatalog.load(POLICIES_DIR)


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class Case:
    """Как вызвать эндпоинт. target — чей объект в пути: None — пути без id."""

    body: Callable[[], dict[str, Any]] | None = None
    targets: tuple[str | None, ...] = (None,)


# Описание каждого маршрута routes.rego: тело запроса и какие объекты подставлять в путь.
# same_org — объект моей организации, other_org — чужой.
CASES: dict[str, Case] = {
    "GET /api/v1/admin/orgs": Case(),
    "POST /api/v1/admin/orgs": Case(lambda: {"name": "Acme", "slug": unique("acme")}),
    "POST /api/v1/admin/orgs/{org_id}/invites": Case(
        lambda: {"email": f"{unique('boss')}@example.com"}, ("same_org", "other_org")
    ),
    "GET /api/v1/org": Case(),
    "PATCH /api/v1/org": Case(lambda: {}),
    "GET /api/v1/teams": Case(),
    "POST /api/v1/teams": Case(lambda: {"name": unique("Відділ")}),
    "PATCH /api/v1/teams/{team_id}": Case(
        lambda: {"name": unique("Відділ")}, ("same_org", "other_org")
    ),
    "DELETE /api/v1/teams/{team_id}": Case(None, ("same_org", "other_org")),
    "GET /api/v1/users": Case(),
    "PATCH /api/v1/users/{user_id}": Case(lambda: {"is_active": True}, ("same_org", "other_org")),
    "POST /api/v1/invites": Case(
        lambda: {"email": f"{unique('rep')}@example.com", "role": "viewer"}
    ),
}


@pytest.fixture(scope="module")
def route_actions(opa_url: str) -> dict[str, str]:
    # Синхронно: один запрос на весь модуль, event loop для этого не нужен.
    resp = httpx.post(f"{opa_url}/v1/data/{ROUTES_PATH}", json={})
    return dict(resp.json()["result"])


def all_cases() -> list[tuple[str, str, str | None]]:
    return [
        (role, route, target)
        for route, case in CASES.items()
        for role in ROLES
        for target in case.targets
    ]


async def test_every_route_has_a_case(route_actions: dict[str, str]) -> None:
    assert set(route_actions) == set(CASES), "опишите новый маршрут в CASES"


def expected_status(role: str, action: str, target: str | None, success: int) -> int:
    if role == "super_admin":
        return success if action.startswith("admin:") else 403
    if action not in CATALOG.permissions(role):
        return 403
    if target == "other_org":
        return 404  # чужая организация вне любого уровня
    return success


@dataclass
class World:
    mine: OrgFixture
    other: OrgFixture
    users: dict[str, User]  # роль → пользователь моей организации
    empty_team: dict[str, uuid.UUID]  # same_org / other_org → отдел без людей
    any_user: dict[str, uuid.UUID]  # same_org / other_org → пользователь (не сам вызывающий)


async def build_world(make_org: MakeOrg, make_user: MakeUser, uow: UnitOfWork) -> World:
    mine, other = await make_org(), await make_org()
    users = {role: await member(make_user, mine, role) for role in ROLES[:4]}
    users["super_admin"] = await make_user(f"{unique('root')}@example.com", is_superadmin=True)
    empty: dict[str, uuid.UUID] = {}
    for key, org in (("same_org", mine), ("other_org", other)):
        team = Team(id=uuid.uuid4(), org_id=org.id, name=unique("Порожній"))
        uow.teams.add(team)
        empty[key] = team.id
    await uow.commit()
    targets = {
        "same_org": (await member(make_user, mine, "viewer")).id,
        "other_org": (await member(make_user, other, "viewer")).id,
    }
    return World(mine, other, users, empty, targets)


def url_for(route: str, target: str | None, world: World) -> str:
    path = route.split(" ", 1)[1]
    if target is None:
        return path
    org = world.mine if target == "same_org" else world.other
    return (
        path.replace("{team_id}", str(world.empty_team[target]))
        .replace("{user_id}", str(world.any_user[target]))
        .replace("{org_id}", str(org.id))
    )


SUCCESS = {"GET": 200, "PATCH": 200, "POST": 201, "DELETE": 204}
ACCEPTED = {"POST /api/v1/invites", "POST /api/v1/admin/orgs/{org_id}/invites"}


@pytest.mark.parametrize(("role", "route", "target"), all_cases())
async def test_access(
    role: str,
    route: str,
    target: str | None,
    route_actions: dict[str, str],
    client: AsyncClient,
    make_org: MakeOrg,
    make_user: MakeUser,
    uow: UnitOfWork,
    settings: Settings,
) -> None:
    world = await build_world(make_org, make_user, uow)
    user = world.users[role]
    # Access-токен выпускаем напрямую: логин с argon2 на сотню кейсов был бы медленным.
    token = KeySet.from_settings(settings).issue_access_token(user.id, uuid.uuid4(), 0)
    method = route.split(" ", 1)[0]
    case = CASES[route]
    success = 202 if route in ACCEPTED else SUCCESS[method]

    resp = await client.request(
        method,
        url_for(route, target, world),
        json=case.body() if case.body else None,
        headers={"Authorization": f"Bearer {token}"},
    )

    expected = expected_status(role, route_actions[route], target, success)
    assert resp.status_code == expected, resp.text
