"""«Требуется: …» в описании операций Swagger."""

from typing import Any

import pytest

from app.authz.openapi import describe_permissions
from app.authz.roles import RoleCatalog
from tests.conftest import POLICIES_DIR

pytestmark = pytest.mark.policy  # совпадает с policies/: make test-policy

ROLES = RoleCatalog.load(POLICIES_DIR)


def schema() -> dict[str, Any]:
    return {
        "paths": {
            "/api/v1/users": {"get": {"description": "Люди организации."}},
            "/api/v1/admin/orgs": {"post": {}},
            "/api/v1/auth/me": {"get": {"description": "Профиль."}},
        }
    }


def test_operations_get_action_and_role_levels() -> None:
    actions = {"GET /api/v1/users": "member:read", "POST /api/v1/admin/orgs": "admin:orgs"}

    paths = describe_permissions(schema(), actions, ROLES)["paths"]

    users = paths["/api/v1/users"]["get"]
    assert users["description"] == (
        "**Требуется:** `member:read` (org_admin — org, team_lead — team, sales_rep — team, "
        "viewer — org).\n\nЛюди организации."
    )
    assert users["x-required-action"] == "member:read"
    assert "только супер-админ" in paths["/api/v1/admin/orgs"]["post"]["description"]
    # Эндпоинты без OPA (вход, профиль) не трогаем.
    assert paths["/api/v1/auth/me"]["get"] == {"description": "Профиль."}
