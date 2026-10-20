"""OpaClient против настоящего OPA с политиками проекта."""

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from app.authz.opa import OpaClient
from app.errors import ServiceUnavailable

USER = uuid.uuid4()


@pytest.fixture
async def opa(opa_url: str) -> AsyncIterator[OpaClient]:
    async with httpx.AsyncClient() as http:
        yield OpaClient(http, opa_url)


async def test_allowed_decision_has_action(opa: OpaClient) -> None:
    decision = await opa.decide("GET", "/api/v1/users", USER, role="sales_rep", is_superadmin=False)

    assert decision.allow
    assert decision.action == "member:read"


async def test_denied_and_unknown_routes(opa: OpaClient) -> None:
    denied = await opa.decide(
        "POST", "/api/v1/invites", USER, role="sales_rep", is_superadmin=False
    )
    unknown = await opa.decide("GET", "/api/v1/secret", USER, role="org_admin", is_superadmin=False)

    assert (denied.allow, denied.action) == (False, "member:manage")
    assert (unknown.allow, unknown.action) == (False, None)


async def test_route_actions_lists_routes_rego(opa: OpaClient) -> None:
    actions = await opa.route_actions()

    assert actions["PATCH /api/v1/users/{user_id}"] == "member:manage"


async def test_unreachable_opa_is_503() -> None:
    async with httpx.AsyncClient() as http:
        opa = OpaClient(http, "http://127.0.0.1:9")
        with pytest.raises(ServiceUnavailable) as exc:
            await opa.decide("GET", "/api/v1/org", USER, role="viewer", is_superadmin=False)
    assert exc.value.details == {"failed": ["opa"]}
