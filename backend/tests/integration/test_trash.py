"""Корзина — шаг 4.6: удаление, список удалённых, восстановление (раздел 3)."""

from httpx import AsyncClient

from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, member, tokens_for


async def test_delete_moves_to_trash_and_restore_brings_back(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep", team=0)
    contact = await make_contact(rep)
    contact_id = contact.id
    lead = await tokens_for(client, await member(make_user, org, "team_lead", team=0))
    as_rep = await tokens_for(client, rep)

    by_rep = await client.delete(f"/api/v1/contacts/{contact_id}", headers=as_rep)
    deleted = await client.delete(f"/api/v1/contacts/{contact_id}", headers=lead)
    hidden = await client.get(f"/api/v1/contacts/{contact_id}", headers=as_rep)
    trash = await client.get("/api/v1/contacts/trash", headers=lead)
    restored = await client.post(f"/api/v1/contacts/{contact_id}/restore", headers=lead)
    back = await client.get(f"/api/v1/contacts/{contact_id}", headers=as_rep)

    assert by_rep.status_code == 403  # менеджер не удаляет даже свои записи
    assert deleted.status_code == 204
    assert hidden.status_code == 404
    assert [c["id"] for c in trash.json()["items"]] == [contact_id]
    assert trash.json()["items"][0]["deleted_at"] is not None
    assert restored.status_code == 200
    assert back.status_code == 200


async def test_restore_checks_duplicate_email(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    old = await make_contact(rep, email="same@example.com")
    old_id = old.id
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    await client.delete(f"/api/v1/contacts/{old_id}", headers=admin)
    # Email удалённого свободен (частичный UNIQUE) — заводим новый контакт с ним.
    await make_contact(rep, email="same@example.com")

    resp = await client.post(f"/api/v1/contacts/{old_id}/restore", headers=admin)

    assert (resp.status_code, resp.json()["code"]) == (409, "contact.duplicate_email")


async def test_restore_needs_active_owner_and_follows_his_team(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_company: MakeRecord
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=0)
    gone = await make_company(rep)
    company_id, rep_id = gone.id, rep.id
    await client.delete(f"/api/v1/companies/{company_id}", headers=admin)
    # Живых записей у менеджера нет — его можно перевести в другой отдел.
    moved = await client.patch(
        f"/api/v1/users/{rep_id}", json={"team_id": str(org.team_ids[1])}, headers=admin
    )
    old_lead = await tokens_for(client, await member(make_user, org, "team_lead", team=0))

    by_old_lead = await client.post(f"/api/v1/companies/{company_id}/restore", headers=old_lead)
    by_admin = await client.post(f"/api/v1/companies/{company_id}/restore", headers=admin)

    assert moved.status_code == 200
    assert by_old_lead.status_code == 403  # запись вернулась бы в чужой для него отдел
    assert by_admin.json()["team_id"] == str(org.team_ids[1])  # текущий отдел ответственного

    await client.delete(f"/api/v1/companies/{company_id}", headers=admin)
    await client.patch(f"/api/v1/users/{rep_id}", json={"is_active": False}, headers=admin)
    inactive = await client.post(f"/api/v1/companies/{company_id}/restore", headers=admin)
    assert (inactive.status_code, inactive.json()["code"]) == (409, "owner.inactive")


async def test_deleted_deal_leaves_lists_and_kanban(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_company: MakeRecord,
) -> None:
    org = await make_org()
    pipeline = await make_pipeline(org.id)
    rep = await member(make_user, org, "sales_rep")
    deal = await make_deal(rep, pipeline.stages[0])
    company = await make_company(rep)
    deal_id, company_id, pipeline_id = deal.id, company.id, pipeline.id
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    as_rep = await tokens_for(client, rep)

    await client.delete(f"/api/v1/deals/{deal_id}", headers=admin)
    await client.delete(f"/api/v1/companies/{company_id}", headers=admin)
    listed = await client.get("/api/v1/deals", headers=as_rep)
    kanban = await client.get(f"/api/v1/deals/kanban?pipeline_id={pipeline_id}", headers=as_rep)
    # К компании из корзины привязать новый контакт нельзя — её не видно.
    linked = await client.post(
        "/api/v1/contacts", json={"first_name": "А", "company_id": company_id}, headers=as_rep
    )
    restored = await client.post(f"/api/v1/deals/{deal_id}/restore", headers=admin)

    assert listed.json()["items"] == []
    assert kanban.json()["stages"][0]["deals_count"] == 0
    assert (linked.status_code, linked.json()["details"]) == (422, {"field": "company_id"})
    assert restored.json()["id"] == deal_id
