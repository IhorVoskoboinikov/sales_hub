"""Люди организации: кто кого видит и правила смены роли, отдела и статуса (раздел 5)."""

import uuid
from typing import Any

from fastapi import FastAPI
from httpx import AsyncClient, Response

from tests.integration.conftest import (
    MakeOrg,
    MakeUser,
    OrgFixture,
    bearer,
    login,
    member,
    tokens_for,
)


async def patch(client: AsyncClient, admin: dict[str, str], user_id: Any, **body: Any) -> Response:
    return await client.patch(f"/api/v1/users/{user_id}", json=body, headers=admin)


async def emails(client: AsyncClient, tokens: dict[str, str]) -> set[str]:
    return {u["email"] for u in (await client.get("/api/v1/users", headers=tokens)).json()}


async def staff(make_user: MakeUser, org: OrgFixture) -> dict[str, Any]:
    return {
        "admin": await member(make_user, org, "org_admin"),
        "viewer": await member(make_user, org, "viewer"),
        "lead1": await member(make_user, org, "team_lead", team=0),
        "rep1": await member(make_user, org, "sales_rep", team=0),
        "rep2": await member(make_user, org, "sales_rep", team=1),
    }


async def test_who_sees_whom(client: AsyncClient, make_user: MakeUser, make_org: MakeOrg) -> None:
    org, other = await make_org(), await make_org()
    people = await staff(make_user, org)
    stranger = await member(make_user, other, "org_admin")
    everyone = {u.email for u in people.values()}
    team1 = {people["lead1"].email, people["rep1"].email}

    for role in ("admin", "viewer"):
        assert await emails(client, await tokens_for(client, people[role])) == everyone
    for role in ("lead1", "rep1"):
        assert await emails(client, await tokens_for(client, people[role])) == team1
    assert stranger.email not in everyone


async def test_admin_changes_role_and_team(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    team2 = org.team_ids[1]
    people = await staff(make_user, org)
    admin = await tokens_for(client, people["admin"])

    moved = await patch(client, admin, people["rep1"].id, team_id=str(team2))
    promoted = await patch(client, admin, people["rep2"].id, role="org_admin")

    assert (moved.json()["role"], moved.json()["team_id"]) == ("sales_rep", str(team2))
    # Стал org_admin — отдел снялся сам.
    assert (promoted.json()["role"], promoted.json()["team_id"]) == ("org_admin", None)


async def test_new_role_works_on_next_request(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    people = await staff(make_user, org)
    admin = await tokens_for(client, people["admin"])
    viewer = await tokens_for(client, people["viewer"])
    assert (await client.patch("/api/v1/org", json={}, headers=viewer)).status_code == 403

    await patch(client, admin, people["viewer"].id, role="org_admin")

    # Тот же токен: роли в нём нет, а кэш пользователя сброшен после смены.
    assert (await client.patch("/api/v1/org", json={}, headers=viewer)).status_code == 200


async def test_rules(client: AsyncClient, make_user: MakeUser, make_org: MakeOrg) -> None:
    org, other = await make_org(), await make_org()
    people = await staff(make_user, org)
    admin = await tokens_for(client, people["admin"])
    ids = {name: user.id for name, user in people.items()}

    cases = {
        "member.cannot_change_self": (403, await patch(client, admin, ids["admin"], role="viewer")),
        "member.team_required": (
            422,
            await patch(client, admin, ids["viewer"], role="sales_rep"),
        ),
        "member.team_not_allowed": (
            422,
            await patch(client, admin, ids["rep1"], role="viewer", team_id=str(org.team_ids[0])),
        ),
        "member.team_not_found": (
            422,
            await patch(client, admin, ids["rep1"], team_id=str(other.team_ids[0])),
        ),
    }

    for code, (status, resp) in cases.items():
        assert (resp.status_code, resp.json()["code"]) == (status, code)


async def test_demoted_admin_loses_member_management(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    first = await member(make_user, org, "org_admin")
    second = await member(make_user, org, "org_admin")
    first_id = first.id
    first_tokens = await tokens_for(client, first)
    second_tokens = await tokens_for(client, second)

    demoted = await patch(client, first_tokens, second.id, role="viewer")
    assert demoted.status_code == 200

    # second теперь viewer: управлять людьми OPA ему уже не даёт — со следующего же запроса.
    # (Правило «последний админ» через API недостижимо — его проверяет тест на гонку.)
    resp = await patch(client, second_tokens, first_id, role="viewer")
    assert resp.status_code == 403
    assert resp.json()["details"] == {"action": "member:manage"}


async def test_other_org_user_is_not_found(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org, other = await make_org(), await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    stranger = await member(make_user, other, "viewer")

    resp = await patch(client, admin, stranger.id, role="org_admin")

    assert resp.status_code == 404


async def test_deactivation_logs_user_out_immediately(
    app: FastAPI, client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep")
    rep_email, rep_id = rep.email, rep.id
    rep_tokens = await login(client, rep_email)

    resp = await patch(client, admin, rep_id, is_active=False)

    assert resp.json()["is_active"] is False
    assert (await client.get("/api/v1/auth/me", headers=bearer(rep_tokens))).status_code == 401
    refresh = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": rep_tokens["refresh_token"]}
    )
    assert refresh.status_code == 401


async def test_only_org_admin_manages(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    people = await staff(make_user, org)
    lead = await tokens_for(client, people["lead1"])

    resp = await patch(client, lead, people["rep1"].id, team_id=str(uuid.uuid4()))

    assert resp.status_code == 403
