"""CHECK-ограничения users (раздел 3): база сама не даёт записать невозможную комбинацию,
даже если сервис ошибся."""

import uuid
from typing import Any

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Organization, Team, User
from app.uow import UnitOfWork


async def org_with_team(uow: UnitOfWork, slug: str) -> tuple[Organization, Team]:
    org = Organization(id=uuid.uuid4(), name=slug, slug=f"{slug}-{uuid.uuid4().hex[:6]}")
    uow.session.add(org)
    await uow.flush()
    team = Team(id=uuid.uuid4(), org_id=org.id, name="Продажі")
    uow.session.add(team)
    await uow.flush()
    return org, team


def user(**fields: Any) -> User:
    return User(email=f"{uuid.uuid4().hex[:8]}@example.com", password_hash="x", **fields)


async def test_valid_combinations(uow: UnitOfWork) -> None:
    org, team = await org_with_team(uow, "acme")

    uow.session.add_all(
        [
            user(is_superadmin=True),
            user(org_id=org.id, role="org_admin"),
            user(org_id=org.id, role="viewer"),
            user(org_id=org.id, role="team_lead", team_id=team.id),
            user(org_id=org.id, role="sales_rep", team_id=team.id),
        ]
    )
    await uow.flush()


@pytest.mark.parametrize(
    ("case", "constraint"),
    [
        ("unknown_role", "ck_users_role_valid"),
        ("role_without_org", "ck_users_role_with_org"),
        ("org_without_role", "ck_users_role_with_org"),
        ("superadmin_in_org", "ck_users_superadmin_without_org"),
        ("sales_rep_without_team", "ck_users_team_for_role"),
        ("org_admin_with_team", "ck_users_team_for_role"),
        ("team_of_other_org", "fk_users_team_id_teams"),
    ],
)
async def test_invalid_combinations(uow: UnitOfWork, case: str, constraint: str) -> None:
    org, team = await org_with_team(uow, "acme")
    _, other_team = await org_with_team(uow, "globex")
    bad = {
        "unknown_role": user(org_id=org.id, role="owner"),
        "role_without_org": user(role="viewer"),
        "org_without_role": user(org_id=org.id),
        "superadmin_in_org": user(is_superadmin=True, org_id=org.id, role="org_admin"),
        "sales_rep_without_team": user(org_id=org.id, role="sales_rep"),
        "org_admin_with_team": user(org_id=org.id, role="org_admin", team_id=team.id),
        "team_of_other_org": user(org_id=org.id, role="sales_rep", team_id=other_team.id),
    }[case]
    uow.session.add(bad)

    with pytest.raises(IntegrityError, match=constraint):
        await uow.flush()
    await uow.rollback()
