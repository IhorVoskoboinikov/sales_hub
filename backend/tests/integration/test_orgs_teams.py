"""Организации (супер-админ) и отделы (админ организации) — шаг 3.5."""

import uuid

from httpx import AsyncClient

from tests.integration.conftest import MakeOrg, MakeUser, member, tokens_for


async def superadmin(client: AsyncClient, make_user: MakeUser) -> dict[str, str]:
    user = await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    return await tokens_for(client, user)


async def test_superadmin_creates_and_lists_orgs(client: AsyncClient, make_user: MakeUser) -> None:
    root = await superadmin(client, make_user)
    slug = f"acme-{uuid.uuid4().hex[:6]}"

    created = await client.post(
        "/api/v1/admin/orgs", json={"name": "Acme", "slug": slug}, headers=root
    )

    assert created.status_code == 201
    assert created.json()["display_currency"] == "UAH"
    assert created.json()["timezone"] == "Europe/Kyiv"
    listed = await client.get("/api/v1/admin/orgs", headers=root)
    assert slug in [o["slug"] for o in listed.json()]
    again = await client.post(
        "/api/v1/admin/orgs", json={"name": "Acme 2", "slug": slug}, headers=root
    )
    assert again.status_code == 409
    assert again.json()["code"] == "org.slug_taken"


async def test_org_validation(client: AsyncClient, make_user: MakeUser) -> None:
    root = await superadmin(client, make_user)

    resp = await client.post(
        "/api/v1/admin/orgs",
        json={"name": "X", "slug": "Bad Slug", "display_currency": "uah", "timezone": "Mars/Base"},
        headers=root,
    )

    assert resp.status_code == 422
    fields = {e["loc"][-1] for e in resp.json()["details"]["errors"]}
    assert fields == {"slug", "display_currency", "timezone"}


async def test_org_admin_cannot_use_admin_endpoints(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))

    resp = await client.get("/api/v1/admin/orgs", headers=admin)

    assert resp.status_code == 403
    assert resp.json()["details"] == {"action": "admin:orgs"}


async def test_superadmin_does_not_see_org_data(client: AsyncClient, make_user: MakeUser) -> None:
    root = await superadmin(client, make_user)

    assert (await client.get("/api/v1/org", headers=root)).status_code == 403
    assert (await client.get("/api/v1/teams", headers=root)).status_code == 403


async def test_everyone_reads_own_org_only_admin_changes_it(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))

    seen = await client.get("/api/v1/org", headers=rep)
    assert seen.json()["id"] == str(org.id)

    forbidden = await client.patch("/api/v1/org", json={"name": "Hacked"}, headers=rep)
    assert forbidden.status_code == 403

    changed = await client.patch("/api/v1/org", json={"display_currency": "USD"}, headers=admin)
    assert changed.status_code == 200
    assert changed.json()["display_currency"] == "USD"
    assert changed.json()["name"] == org.name  # не переданное поле не изменилось


async def test_teams_crud(client: AsyncClient, make_user: MakeUser, make_org: MakeOrg) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))

    created = await client.post("/api/v1/teams", json={"name": "Корпоративні"}, headers=admin)
    assert created.status_code == 201
    team_id = created.json()["id"]

    dup = await client.post("/api/v1/teams", json={"name": "Продажі 1"}, headers=admin)
    assert dup.json()["code"] == "team.name_taken"

    renamed = await client.patch(
        f"/api/v1/teams/{team_id}", json={"name": "Корпоративні продажі"}, headers=admin
    )
    assert renamed.json()["name"] == "Корпоративні продажі"

    names = [t["name"] for t in (await client.get("/api/v1/teams", headers=admin)).json()]
    assert names == ["Корпоративні продажі", "Продажі 1", "Продажі 2"]

    assert (await client.delete(f"/api/v1/teams/{team_id}", headers=admin)).status_code == 204


async def test_team_with_people_cannot_be_deleted(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    await member(make_user, org, "sales_rep", team=0)

    resp = await client.delete(f"/api/v1/teams/{org.team_ids[0]}", headers=admin)

    assert resp.status_code == 409
    assert resp.json()["code"] == "team.has_members"


async def test_other_org_team_is_not_found(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    mine, other = await make_org(), await make_org()
    mine_ids = {str(team_id) for team_id in mine.team_ids}
    other_team = other.team_ids[0]
    admin = await tokens_for(client, await member(make_user, mine, "org_admin"))

    resp = await client.patch(f"/api/v1/teams/{other_team}", json={"name": "Моє"}, headers=admin)

    assert resp.status_code == 404
    listed = {t["id"] for t in (await client.get("/api/v1/teams", headers=admin)).json()}
    assert listed == mine_ids
