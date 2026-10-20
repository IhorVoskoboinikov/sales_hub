"""Журнал изменений (шаг 3.8): каждое изменение организации, отделов и людей — запись
в audit_log в той же транзакции."""

import uuid
from typing import Any

from httpx import AsyncClient
from sqlalchemy import select

from app.models import AuditEntry
from app.uow import UnitOfWork
from tests.integration.conftest import Mailpit, MakeOrg, MakeUser, member, tokens_for


async def entries(uow: UnitOfWork, **filters: Any) -> list[AuditEntry]:
    query = select(AuditEntry).filter_by(**filters).order_by(AuditEntry.id)
    return list(await uow.session.scalars(query))


async def test_member_change_is_logged_with_diff(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, uow: UnitOfWork
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    admin_id = admin.id
    rep = await member(make_user, org, "sales_rep", team=0)
    rep_id = rep.id
    headers = await tokens_for(client, admin)

    resp = await client.patch(
        f"/api/v1/users/{rep_id}",
        json={"team_id": str(org.team_ids[1])},
        headers={**headers, "X-Request-ID": "req-audit-1"},
    )

    assert resp.status_code == 200
    [entry] = await entries(uow, action="member.updated", entity_id=str(rep_id))
    assert entry.actor_id == admin_id
    assert entry.org_id == org.id
    assert entry.changes == {"team_id": [str(org.team_ids[0]), str(org.team_ids[1])]}
    assert entry.request_id == "req-audit-1"  # по нему в логах находится весь запрос


async def test_failed_change_leaves_no_entry(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, uow: UnitOfWork
) -> None:
    org = await make_org()
    headers = await tokens_for(client, await member(make_user, org, "org_admin"))
    viewer = await member(make_user, org, "viewer")
    viewer_id = viewer.id

    resp = await client.patch(
        f"/api/v1/users/{viewer_id}", json={"role": "sales_rep"}, headers=headers
    )  # нет отдела — 422

    assert resp.status_code == 422
    assert await entries(uow, entity_id=str(viewer_id)) == []


async def test_superadmin_and_team_actions_are_logged(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    mailpit: Mailpit,
    uow: UnitOfWork,
) -> None:
    root = await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    root_id = root.id
    root_headers = await tokens_for(client, root)
    slug = f"audit-{uuid.uuid4().hex[:6]}"
    created = await client.post(
        "/api/v1/admin/orgs", json={"name": "Audit", "slug": slug}, headers=root_headers
    )
    org_id = uuid.UUID(created.json()["id"])
    email = f"boss-{uuid.uuid4().hex[:8]}@example.com"
    await client.post(
        f"/api/v1/admin/orgs/{org_id}/invites", json={"email": email}, headers=root_headers
    )

    actions = [(e.action, e.actor_id) for e in await entries(uow, org_id=org_id)]

    assert actions == [("org.created", root_id), ("invite.created", root_id)]

    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    team = await client.post("/api/v1/teams", json={"name": "Нові"}, headers=admin)
    await client.patch(f"/api/v1/teams/{team.json()['id']}", json={"name": "Старі"}, headers=admin)
    await client.delete(f"/api/v1/teams/{team.json()['id']}", headers=admin)

    team_log = await entries(uow, entity_id=team.json()["id"])
    assert [(e.action, e.changes) for e in team_log] == [
        ("team.created", {"name": "Нові"}),
        ("team.renamed", {"name": ["Нові", "Старі"]}),
        ("team.deleted", {"name": "Старі"}),
    ]
