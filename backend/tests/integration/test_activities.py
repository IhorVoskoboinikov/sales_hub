"""Активности, заметки и журнал на партициях — шаг 4.5."""

from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, member, tokens_for


def iso(moment: datetime) -> str:
    return moment.isoformat()


async def test_logged_activity_is_done_planned_one_is_open(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    rep = await tokens_for(client, await member(make_user, await make_org(), "sales_rep"))
    due = datetime.now(UTC) + timedelta(hours=2)

    call = await client.post(
        "/api/v1/activities", json={"type": "call", "subject": "Дзвінок"}, headers=rep
    )
    task = await client.post(
        "/api/v1/activities",
        json={"type": "task", "subject": "Надіслати КП", "due_at": iso(due)},
        headers=rep,
    )
    open_tasks = await client.get("/api/v1/activities", headers=rep)

    assert call.json()["done_at"] is not None  # без срока — уже состоялась
    assert task.json()["done_at"] is None
    assert [a["id"] for a in open_tasks.json()["items"]] == [task.json()["id"]]


async def test_tasks_today_overdue_and_scope(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_activity: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep", team=0)
    peer = await member(make_user, org, "sales_rep", team=0)
    rep2 = await member(make_user, org, "sales_rep", team=1)
    now = datetime.now(UTC)
    late = await make_activity(rep, subject="late", due_at=now - timedelta(hours=1))
    later = await make_activity(rep, subject="next week", due_at=now + timedelta(days=7))
    await make_activity(peer, subject="peer")
    await make_activity(rep2, subject="other team")
    late_id, later_id = late.id, later.id
    as_rep = await tokens_for(client, rep)
    as_lead = await tokens_for(client, await member(make_user, org, "team_lead", team=0))

    mine = await client.get("/api/v1/activities", headers=as_rep)
    overdue = await client.get("/api/v1/activities", params={"due": "overdue"}, headers=as_rep)
    team = await client.get("/api/v1/activities", headers=as_lead)

    assert [a["id"] for a in mine.json()["items"]] == [late_id, later_id]  # по сроку
    assert [a["id"] for a in overdue.json()["items"]] == [late_id]
    assert {a["subject"] for a in team.json()["items"]} == {"late", "next week", "peer"}


async def test_today_uses_organization_timezone(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_activity: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    kyiv_midnight = datetime.now(UTC).astimezone(ZoneInfo("Europe/Kyiv"))
    start = kyiv_midnight.replace(hour=0, minute=0, second=0, microsecond=0)
    inside = await make_activity(rep, subject="today", due_at=start + timedelta(minutes=1))
    await make_activity(rep, subject="yesterday", due_at=start - timedelta(minutes=1))
    await make_activity(rep, subject="tomorrow", due_at=start + timedelta(days=1, minutes=1))
    inside_id = inside.id

    today = await client.get(
        "/api/v1/activities", params={"due": "today"}, headers=await tokens_for(client, rep)
    )

    assert [a["id"] for a in today.json()["items"]] == [inside_id]


async def test_card_history_follows_parent_visibility(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
) -> None:
    org = await make_org()
    lead = await member(make_user, org, "team_lead", team=0)
    rep = await member(make_user, org, "sales_rep", team=0)
    deal = await make_deal(rep, (await make_pipeline(org.id)).stages[0])
    # Руководитель записал звонок по сделке менеджера — менеджер видит его в истории.
    await make_activity(lead, deal_id=deal.id, subject="Дзвінок керівника")
    await make_activity(rep, deal_id=deal.id, subject="Мій дзвінок")
    deal_id = deal.id
    stranger = await member(make_user, org, "sales_rep", team=1)

    history = await client.get(
        "/api/v1/activities", params={"deal_id": deal_id}, headers=await tokens_for(client, rep)
    )
    hidden = await client.get(
        "/api/v1/activities",
        params={"deal_id": deal_id},
        headers=await tokens_for(client, stranger),
    )

    assert {a["subject"] for a in history.json()["items"]} == {"Дзвінок керівника", "Мій дзвінок"}
    assert hidden.status_code == 404


async def test_done_reopen_and_delete(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_activity: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep", team=0)
    peer = await member(make_user, org, "sales_rep", team=0)
    task = await make_activity(rep)
    theirs = await make_activity(peer)
    task_id, theirs_id = task.id, theirs.id
    auth = await tokens_for(client, rep)
    url = f"/api/v1/activities/{task_id}"

    done = await client.patch(url, json={"done": True}, headers=auth)
    reopened = await client.patch(url, json={"done": False}, headers=auth)
    foreign = await client.delete(f"/api/v1/activities/{theirs_id}", headers=auth)
    deleted = await client.delete(url, headers=auth)
    gone = await client.patch(url, json={"done": True}, headers=auth)

    assert done.json()["done_at"] is not None
    assert reopened.json()["done_at"] is None
    assert foreign.status_code == 404  # чужая задача менеджеру не видна
    assert deleted.status_code == 204
    assert gone.status_code == 404


async def test_notes_follow_contact(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep", team=0)
    contact = await make_contact(rep)
    url = f"/api/v1/contacts/{contact.id}/notes"
    auth = await tokens_for(client, rep)
    viewer = await tokens_for(client, await member(make_user, org, "viewer"))
    colleague = await tokens_for(client, await member(make_user, org, "sales_rep", team=1))

    created = await client.post(url, json={"body": "Цікавиться тарифом Pro"}, headers=auth)
    read_by_viewer = await client.get(url, headers=viewer)
    by_colleague = await client.post(url, json={"body": "x"}, headers=colleague)

    assert created.status_code == 201
    assert [n["body"] for n in read_by_viewer.json()["items"]] == ["Цікавиться тарифом Pro"]
    assert by_colleague.status_code == 404


async def test_audit_log_lists_org_changes_newest_first(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep")
    rep_auth = await tokens_for(client, rep)
    ids = []
    for name in ("Перша", "Друга", "Третя"):
        resp = await client.post("/api/v1/companies", json={"name": name}, headers=rep_auth)
        ids.append(str(resp.json()["id"]))

    first = await client.get("/api/v1/audit", params={"limit": 2}, headers=admin)
    second = await client.get(
        "/api/v1/audit", params={"limit": 2, "cursor": first.json()["next_cursor"]}, headers=admin
    )
    one: Any = await client.get(
        "/api/v1/audit",
        params={"entity_type": "company", "entity_id": ids[1]},
        headers=admin,
    )
    lead = await tokens_for(client, await member(make_user, org, "team_lead"))

    pages = first.json()["items"] + second.json()["items"]
    assert [e["entity_id"] for e in pages] == list(reversed(ids))
    assert [e["action"] for e in one.json()["items"]] == ["company.created"]
    assert (await client.get("/api/v1/audit", headers=lead)).status_code == 403


async def test_rows_land_in_monthly_partitions(
    make_user: MakeUser,
    make_org: MakeOrg,
    make_activity: MakeRecord,
    db_session: AsyncSession,
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    activity = await make_activity(rep)

    partition = await db_session.scalar(
        text("SELECT tableoid::regclass::text FROM activities WHERE id = :id"),
        {"id": activity.id},
    )

    assert partition == f"activities_p{datetime.now(UTC):%Y_%m}"


async def test_time_without_timezone_is_422(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    """«10:00» без часового пояса означало бы время часов сервера — такое не принимаем."""
    org = await make_org()
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))

    naive = await client.post(
        "/api/v1/activities",
        json={"type": "task", "subject": "X", "due_at": "2026-10-22T10:00:00"},
        headers=rep,
    )
    audit = await client.get("/api/v1/audit", params={"from": "2026-10-01T00:00:00"}, headers=admin)

    assert (naive.status_code, audit.status_code) == (422, 422)
