"""opa_decision и authorize без настоящего OPA: что уходит в OPA и что получает эндпоинт."""

import uuid
from typing import Any

import pytest
from fastapi import APIRouter, Depends, FastAPI
from httpx import AsyncClient

from app.api.deps import current_user, get_opa
from app.authz.access import Access
from app.authz.deps import authorize
from app.authz.opa import Decision
from app.errors import ServiceUnavailable
from app.services.auth import CurrentUser

ORG, TEAM = uuid.uuid4(), uuid.uuid4()
USER = CurrentUser(
    id=uuid.uuid4(),
    email="ivan@example.com",
    is_superadmin=False,
    session_id=uuid.uuid4(),
    token_version=0,
    org_id=ORG,
    team_id=TEAM,
    role="sales_rep",
)

# Роутер по правилу проекта: полный префикс, authorize на роутере.
router = APIRouter(prefix="/api/v1/test", dependencies=[Depends(authorize)])


@router.get("/items/{item_id}")
async def get_item(item_id: int, access: Access = Depends(authorize)) -> dict[str, Any]:
    return {"scope": access.scope, "action": access.action, "team": str(access.team_id)}


class FakeOpa:
    def __init__(self, decision: Decision | Exception) -> None:
        self.decision = decision
        self.calls: list[dict[str, Any]] = []

    async def decide(self, method: str, route: str, user_id: uuid.UUID, **user: Any) -> Decision:
        self.calls.append({"method": method, "route": route, **user})
        if isinstance(self.decision, Exception):
            raise self.decision
        return self.decision


def use(app: FastAPI, opa: FakeOpa) -> None:
    app.dependency_overrides[get_opa] = lambda: opa
    app.dependency_overrides[current_user] = lambda: USER


@pytest.fixture
def app(app: FastAPI) -> FastAPI:
    app.include_router(router)
    return app


async def test_opa_gets_route_template_and_endpoint_gets_access(
    app: FastAPI, client: AsyncClient
) -> None:
    opa = FakeOpa(Decision(allow=True, action="member:read"))
    use(app, opa)

    resp = await client.get("/api/v1/test/items/42")

    assert resp.status_code == 200
    # Уровень — из data.yaml: sales_rep видит людей своего отдела.
    assert resp.json() == {"scope": "team", "action": "member:read", "team": str(TEAM)}
    # Шаблон пути, а не /items/42; OPA спрошен один раз, хотя authorize стоит дважды.
    assert opa.calls == [
        {
            "method": "GET",
            "route": "/api/v1/test/items/{item_id}",
            "role": "sales_rep",
            "is_superadmin": False,
        }
    ]


async def test_denied_is_403_with_action(app: FastAPI, client: AsyncClient) -> None:
    use(app, FakeOpa(Decision(allow=False, action="member:manage")))

    resp = await client.get("/api/v1/test/items/1")

    assert resp.status_code == 403
    assert resp.json()["details"] == {"action": "member:manage"}


async def test_opa_down_is_503(app: FastAPI, client: AsyncClient) -> None:
    use(app, FakeOpa(ServiceUnavailable(failed=["opa"])))

    resp = await client.get("/api/v1/test/items/1")

    assert resp.status_code == 503


async def test_action_unknown_to_backend_is_503(app: FastAPI, client: AsyncClient) -> None:
    # OPA разрешил действие, которого у роли нет в data.yaml бэкенда, — файлы разошлись.
    use(app, FakeOpa(Decision(allow=True, action="deal:delete")))

    resp = await client.get("/api/v1/test/items/1")

    assert resp.status_code == 503
    assert resp.json()["code"] == "authz.policy_out_of_sync"
