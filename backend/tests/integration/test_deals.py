"""Воронки, сделки и канбан — шаг 4.3."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, member, tokens_for


async def test_pipeline_stages_are_replaced_as_a_list(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    created = await client.post(
        "/api/v1/pipelines",
        json={
            "name": "B2B",
            "stages": [{"name": "Новий"}, {"name": "Пропозиція", "probability": 40}],
        },
        headers=admin,
    )
    pipeline = created.json()
    first, second = pipeline["stages"]

    # Переставить, переименовать, добавить третий.
    updated = await client.patch(
        f"/api/v1/pipelines/{pipeline['id']}",
        json={
            "stages": [
                {"id": second["id"], "name": "Пропозиція", "probability": 40},
                {"id": first["id"], "name": "Перший контакт"},
                {"name": "Переговори", "probability": 70},
            ]
        },
        headers=admin,
    )
    duplicate = await client.post(
        "/api/v1/pipelines", json={"name": "B2B", "stages": [{"name": "X"}]}, headers=admin
    )
    unknown = await client.patch(
        f"/api/v1/pipelines/{pipeline['id']}",
        json={"stages": [{"id": 10**12, "name": "X"}]},
        headers=admin,
    )

    assert created.status_code == 201
    stages = updated.json()["stages"]
    assert [(s["name"], s["position"]) for s in stages] == [
        ("Пропозиція", 0),
        ("Перший контакт", 1),
        ("Переговори", 2),
    ]
    assert stages[0]["id"] == second["id"]
    assert (duplicate.status_code, duplicate.json()["code"]) == (409, "pipeline.name_taken")
    assert (unknown.status_code, unknown.json()["code"]) == (422, "pipeline.stage_not_found")
    twice = await client.patch(
        f"/api/v1/pipelines/{pipeline['id']}",
        json={"stages": [{"id": first["id"], "name": "A"}, {"id": first["id"], "name": "B"}]},
        headers=admin,
    )
    assert (twice.status_code, twice.json()["code"]) == (422, "pipeline.stage_duplicate")


async def test_stage_with_deals_cannot_be_removed_even_from_trash(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    pipeline = await make_pipeline(org.id)
    deal = await make_deal(await member(make_user, org, "sales_rep"), pipeline.stages[1])
    deal.deleted_at = datetime.now(UTC)  # сделка в корзине всё равно держит этап (FK)
    await db_session.commit()
    keep = pipeline.stages[0]

    resp = await client.patch(
        f"/api/v1/pipelines/{pipeline.id}",
        json={"stages": [{"id": keep.id, "name": keep.name}]},
        headers=admin,
    )

    assert (resp.status_code, resp.json()["code"]) == (409, "pipeline.stage_has_deals")


async def test_deal_takes_pipeline_from_stage_and_status_sets_closed_at(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_pipeline: MakeRecord
) -> None:
    org = await make_org()
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))
    sales, partners = await make_pipeline(org.id), await make_pipeline(org.id)
    stage_id, other_stage_id, partners_id = sales.stages[0].id, partners.stages[1].id, partners.id

    created = await client.post(
        "/api/v1/deals",
        json={"title": "Ліцензії", "amount": "2500.50", "currency": "USD", "stage_id": stage_id},
        headers=rep,
    )
    deal = created.json()
    url = f"/api/v1/deals/{deal['id']}"
    moved = await client.patch(url, json={"version": 1, "stage_id": other_stage_id}, headers=rep)
    won = await client.patch(url, json={"version": 2, "status": "won"}, headers=rep)
    reopened = await client.patch(url, json={"version": 3, "status": "open"}, headers=rep)

    assert created.status_code == 201, created.text
    assert (deal["pipeline_id"], deal["status"], deal["closed_at"]) == (sales.id, "open", None)
    assert Decimal(deal["amount_usd"]) == Decimal("2500.50")
    assert (moved.json()["pipeline_id"], moved.json()["stage_id"]) == (partners_id, other_stage_id)
    assert won.json()["closed_at"] is not None
    assert reopened.json()["closed_at"] is None


async def test_invisible_references_are_422(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_contact: MakeRecord,
) -> None:
    org, other = await make_org(), await make_org()
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))
    mine, foreign = await make_pipeline(org.id), await make_pipeline(other.id)
    colleague_contact = await make_contact(await member(make_user, org, "sales_rep"))
    stage_id, foreign_stage, contact_id = (
        mine.stages[0].id,
        foreign.stages[0].id,
        colleague_contact.id,
    )

    async def create(**body: Any) -> Any:
        body = {"title": "X", "amount": "1", "stage_id": stage_id, **body}
        resp = await client.post("/api/v1/deals", json=body, headers=rep)
        return resp.status_code, resp.json().get("details")

    assert await create(stage_id=foreign_stage) == (422, {"field": "stage_id"})
    # Контакт коллеги менеджеру не виден — привязать к нему сделку нельзя.
    assert await create(contact_id=contact_id) == (422, {"field": "contact_id"})


async def test_kanban_counts_only_my_level(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
) -> None:
    org, other = await make_org(), await make_org()
    pipeline = await make_pipeline(org.id)
    new, offer = pipeline.stages
    rep = await member(make_user, org, "sales_rep", team=0)
    lead = await member(make_user, org, "team_lead", team=0)
    rep2 = await member(make_user, org, "sales_rep", team=1)
    for i in range(3):
        await make_deal(rep, new, amount_usd=Decimal(100 + i))
    await make_deal(rep2, new, amount_usd=Decimal(1000))
    await make_deal(rep2, offer, amount_usd=Decimal(5))
    await make_deal(rep, offer, status="won", closed_at=datetime.now(UTC))  # закрытые не в канбане
    url = f"/api/v1/deals/kanban?pipeline_id={pipeline.id}&per_stage=2"

    as_rep = (await client.get(url, headers=await tokens_for(client, rep))).json()["stages"]
    as_lead = (await client.get(url, headers=await tokens_for(client, lead))).json()["stages"]
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    as_admin = (await client.get(url, headers=admin)).json()["stages"]
    foreign = await client.get(
        f"/api/v1/deals/kanban?pipeline_id={(await make_pipeline(other.id)).id}", headers=admin
    )

    assert [(s["deals_count"], Decimal(s["amount_usd"])) for s in as_rep] == [
        (3, Decimal(303)),
        (0, Decimal(0)),
    ]
    assert [len(s["deals"]) for s in as_rep] == [2, 0]  # per_stage
    titles = [d["id"] for d in as_rep[0]["deals"]]
    assert titles == sorted(titles, reverse=True)  # новые сначала
    assert [s["deals_count"] for s in as_lead] == [3, 0]
    assert [(s["deals_count"], Decimal(s["amount_usd"])) for s in as_admin] == [
        (4, Decimal(1303)),
        (1, Decimal(5)),
    ]
    assert foreign.status_code == 404


async def test_overdue_list_and_member_with_deals(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
) -> None:
    org = await make_org()
    stage = (await make_pipeline(org.id)).stages[0]
    rep = await member(make_user, org, "sales_rep", team=0)
    today = date.today()
    for days in (5, 1, -3):
        await make_deal(rep, stage, title=f"due{days}", expected_close_date=today - timedelta(days))
    await make_deal(rep, stage, title="none")
    auth = await tokens_for(client, rep)
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))

    overdue = await client.get("/api/v1/deals", params={"overdue": True}, headers=auth)
    moved = await client.patch(
        f"/api/v1/users/{rep.id}", json={"team_id": str(org.team_ids[1])}, headers=admin
    )

    assert [d["title"] for d in overdue.json()["items"]] == ["due5", "due1"]  # самые старые сначала
    assert (moved.status_code, moved.json()["code"]) == (409, "member.has_records")
