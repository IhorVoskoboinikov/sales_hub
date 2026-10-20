"""Каталог ролей из policies/saleshub/data.yaml — того же файла, что читает OPA."""

import uuid
from pathlib import Path

import pytest

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import ServiceUnavailable
from tests.conftest import POLICIES_DIR


@pytest.fixture(scope="module")
def roles() -> RoleCatalog:
    return RoleCatalog.load(POLICIES_DIR)


def test_levels_come_from_data_yaml(roles: RoleCatalog) -> None:
    assert roles.scope_for("sales_rep", "member:read") == "team"
    assert roles.scope_for("org_admin", "member:manage") == "org"
    assert roles.levels("member:manage") == {"org_admin": "org"}
    assert roles.titles["viewer"].startswith("Наблюдатель")


def test_action_missing_in_data_yaml_means_files_diverged(roles: RoleCatalog) -> None:
    with pytest.raises(ServiceUnavailable) as exc:
        roles.scope_for("viewer", "member:manage")

    assert exc.value.code == "authz.policy_out_of_sync"


def test_user_without_role_has_no_permissions(roles: RoleCatalog) -> None:
    assert roles.permissions(None) == {}


def test_bad_level_fails_at_startup(tmp_path: Path) -> None:
    (tmp_path / "saleshub").mkdir()
    (tmp_path / "saleshub" / "data.yaml").write_text(
        "roles:\n  viewer:\n    permissions:\n      org:read: everyone\n"
    )

    with pytest.raises(RuntimeError, match="неизвестные уровни"):
        RoleCatalog.load(tmp_path)


def test_access_for_another_action_keeps_the_person(roles: RoleCatalog) -> None:
    access = Access("deal:write", "own", uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "sales_rep")

    read = access.for_action("company:read", roles)

    assert read is not None
    assert (read.action, read.scope) == ("company:read", "org")  # компании видны всем
    assert (read.user_id, read.org_id, read.team_id) == (
        access.user_id,
        access.org_id,
        access.team_id,
    )
    assert access.for_action("member:manage", roles) is None  # у роли такого действия нет
