"""Доменные события в outbox (раздел 16, шаг 5.12): record.assigned, deal.closed и
bulk_job.completed пишутся в транзакции изменения — и не пишутся там, где их быть не должно.

CRM — через API (сессия запроса — транзакция теста), bulk — сервисом и фикстурой run_job.
"""

import uuid
from datetime import date, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import select

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.models import OutboxEvent, User
from app.services.bulk import BulkService, ReassignOwnerRequest
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeJob,
    MakeOrg,
    MakeRecord,
    MakeUser,
    RunJob,
    member,
    tokens_for,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)


async def events(uow: UnitOfWork, org_id: uuid.UUID, event_type: str) -> list[dict[str, Any]]:
    rows = await uow.session.scalars(
        select(OutboxEvent.payload)
        .where(OutboxEvent.org_id == org_id, OutboxEvent.event_type == event_type)
        .order_by(OutboxEvent.id)
    )
    return [dict(row) for row in rows]


def bulk_access(user: User) -> Access:
    assert user.org_id is not None
    scope = CATALOG.scope_for(user.role, "bulk:run")
    return Access("bulk:run", scope, user.id, user.org_id, user.team_id, user.role)


# --- record.assigned (CrmService)


async def test_contact_created_for_someone_else_is_assigned(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, uow: UnitOfWork
) -> None:
    org = await make_org()
    admin_user = await member(make_user, org, "org_admin")
    rep_user = await member(make_user, org, "sales_rep")
    admin_id, rep_id = admin_user.id, rep_user.id
    admin, rep = await tokens_for(client, admin_user), await tokens_for(client, rep_user)

    created = await client.post(
        "/api/v1/contacts",
        json={"first_name": "Іван", "last_name": "Петренко", "owner_id": str(rep_id)},
        headers=admin,
    )
    own = await client.post("/api/v1/contacts", json={"first_name": "Свій"}, headers=rep)

    assert (created.status_code, own.status_code) == (201, 201)
    assert await events(uow, org.id, "record.assigned") == [
        {
            "entity_type": "contact",
            "entity_id": created.json()["id"],
            "title": "Іван Петренко",
            "owner_id": str(rep_id),
            "actor_id": str(admin_id),
        }
    ]  # свой контакт — себе не назначают


async def test_owner_change_is_assigned_only_when_owner_changes(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    uow: UnitOfWork,
) -> None:
    org = await make_org()
    lead_user = await member(make_user, org, "team_lead")
    rep, other = (
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep"),
    )
    lead_id, other_id = lead_user.id, other.id
    contact = await make_contact(rep, first_name="Олена")
    url = f"/api/v1/contacts/{contact.id}"
    lead = await tokens_for(client, lead_user)

    renamed = await client.patch(url, json={"version": 1, "first_name": "Оля"}, headers=lead)
    moved = await client.patch(url, json={"version": 2, "owner_id": str(other_id)}, headers=lead)
    same = await client.patch(url, json={"version": 3, "owner_id": str(other_id)}, headers=lead)
    to_me = await client.patch(url, json={"version": 3, "owner_id": str(lead_id)}, headers=lead)

    assert [r.status_code for r in (renamed, moved, same, to_me)] == [200, 200, 200, 200]
    assert [
        (e["owner_id"], e["actor_id"]) for e in await events(uow, org.id, "record.assigned")
    ] == [(str(other_id), str(lead_id))]  # имя, тот же ответственный и «себе» — без событий
    assert (await events(uow, org.id, "record.assigned"))[0]["title"] == "Оля"


async def test_companies_give_no_assigned_event(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_company: MakeRecord,
    uow: UnitOfWork,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep, other = (
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep"),
    )
    other_id = other.id
    company = await make_company(rep)

    created = await client.post(
        "/api/v1/companies", json={"name": "Acme", "owner_id": str(other_id)}, headers=admin
    )
    moved = await client.patch(
        f"/api/v1/companies/{company.id}",
        json={"version": 1, "owner_id": str(other_id)},
        headers=admin,
    )

    assert (created.status_code, moved.status_code) == (201, 200)
    assert await events(uow, org.id, "record.assigned") == []  # компании — общий справочник


# --- deal.closed (DealService)


async def test_deal_assigned_and_closed_by_patch(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    uow: UnitOfWork,
) -> None:
    org = await make_org()
    admin_user = await member(make_user, org, "org_admin")
    rep_user = await member(make_user, org, "sales_rep")
    admin_id, rep_id, team_id = admin_user.id, rep_user.id, rep_user.team_id
    admin, rep = await tokens_for(client, admin_user), await tokens_for(client, rep_user)
    stage_id = (await make_pipeline(org.id)).stages[0].id

    created = await client.post(
        "/api/v1/deals",
        json={
            "title": "Ліцензії",
            "amount": "100",
            "currency": "EUR",
            "stage_id": stage_id,
            "owner_id": str(rep_id),
        },
        headers=admin,
    )
    deal_id = created.json()["id"]
    url = f"/api/v1/deals/{deal_id}"
    won = await client.patch(url, json={"version": 1, "status": "won"}, headers=rep)
    again = await client.patch(url, json={"version": 2, "status": "won"}, headers=rep)
    reopened = await client.patch(url, json={"version": 2, "status": "open"}, headers=rep)
    lost = await client.patch(url, json={"version": 3, "status": "lost"}, headers=admin)

    assert [r.status_code for r in (created, won, again, reopened, lost)] == [201] + [200] * 4
    [assigned] = await events(uow, org.id, "record.assigned")
    assert (assigned["entity_type"], assigned["title"]) == ("deal", "Ліцензії")
    closed = await events(uow, org.id, "deal.closed")
    assert closed == [
        {
            "deal_id": deal_id,
            "title": "Ліцензії",
            "status": "won",
            "amount_original": "100.00",
            "currency_original": "EUR",
            "owner_id": str(rep_id),
            "team_id": str(team_id),
            "actor_id": str(rep_id),
        },
        {
            "deal_id": deal_id,
            "title": "Ліцензії",
            "status": "lost",
            "amount_original": "100.00",
            "currency_original": "EUR",
            "owner_id": str(rep_id),
            "team_id": str(team_id),
            "actor_id": str(admin_id),
        },
    ]  # повтор won и переоткрытие событий не дают


async def test_deal_created_closed_gives_no_deal_closed(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    uow: UnitOfWork,
) -> None:
    org = await make_org()
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))
    stage_id = (await make_pipeline(org.id)).stages[0].id

    resp = await client.post(
        "/api/v1/deals",
        json={
            "title": "X",
            "amount": "1",
            "currency": "USD",
            "stage_id": stage_id,
            "status": "won",
        },
        headers=rep,
    )

    assert resp.status_code == 201, resp.text
    assert await events(uow, org.id, "deal.closed") == []


# --- bulk_job.completed (BulkService._record_final): одно событие на job, по строкам — нет


async def test_reassign_writes_one_event_for_job_and_none_per_row(
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    make_company: MakeRecord,
    uow: UnitOfWork,
    run_job: RunJob,
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    leaving, heir = (
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep"),
    )
    for _ in range(3):
        await make_contact(leaving)
    await make_company(leaving)
    bulk = BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=2)
    request = ReassignOwnerRequest(leaving.id, heir.id, ("companies", "contacts"))
    job = await bulk.start_reassign_owner(bulk_access(admin), request, None)
    job_id, admin_id, heir_id = job.id, admin.id, heir.id

    await run_job(job_id)
    assert await bulk.finalize_if_complete(job_id) is None  # повторный finalize — ничего

    assert await events(uow, org.id, "bulk_job.completed") == [
        {
            "job_id": str(job_id),
            "kind": "reassign_owner",
            "status": "done",
            "total": 4,
            "processed": 4,
            "failed_rows": 0,
            "failed_chunks": 0,
            "rows": {"companies": 1, "contacts": 3},
            "author_id": str(admin_id),
            "to_user_id": str(heir_id),
        }
    ]
    assert await events(uow, org.id, "record.assigned") == []


async def test_close_overdue_writes_job_event_but_no_deal_closed(
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_job: MakeJob,
    uow: UnitOfWork,
    run_job: RunJob,
) -> None:
    org = await make_org()
    admin, rep = (
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
    )
    stage = (await make_pipeline(org.id)).stages[0]
    for _ in range(2):
        await make_deal(rep, stage, expected_close_date=date.today() - timedelta(days=3))
    params = {
        "before": (date.today() + timedelta(days=1)).isoformat(),
        "pipeline_id": None,
        "access": bulk_access(admin).to_params(),
    }
    job = await make_job(admin, kind="close_overdue_deals", status="pending", params=params)

    await run_job(job.id)

    [completed] = await events(uow, org.id, "bulk_job.completed")
    assert (completed["kind"], completed["status"], completed["rows"]) == (
        "close_overdue_deals",
        "done",
        {"deals": 2},
    )
    assert completed["to_user_id"] is None
    assert await events(uow, org.id, "deal.closed") == []  # массовое закрытие — не по сделкам


async def test_cancelled_pending_import_has_event_without_rows(
    make_user: MakeUser, make_org: MakeOrg, make_job: MakeJob, uow: UnitOfWork
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    job = await make_job(
        admin,
        kind="import_contacts",
        status="pending",
        params={"access": bulk_access(admin).to_params()},
    )
    bulk = BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=2)
    access = Access(
        "bulk_job:cancel", "org", admin.id, org.id, None, admin.role
    )  # org_admin отменяет любую job организации

    await bulk.cancel(access, job.id)

    [completed] = await events(uow, org.id, "bulk_job.completed")
    assert (completed["kind"], completed["status"], completed["rows"]) == (
        "import_contacts",
        "cancelled",
        None,
    )  # итог импорта — {created, updated}, строк по сущностям у него нет


async def test_cancelled_running_job_has_event_with_rows_done_before_cancel(
    make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord, uow: UnitOfWork
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    leaving, heir = (
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep"),
    )
    for _ in range(6):
        await make_contact(leaving)
    heir_id = heir.id
    bulk = BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=2)
    cancel = Access("bulk_job:cancel", "org", admin.id, org.id, None, admin.role)
    request = ReassignOwnerRequest(leaving.id, heir_id, ("contacts",))
    job_id = (await bulk.start_reassign_owner(bulk_access(admin), request, None)).id
    assert await bulk.plan(job_id) == [0, 1, 2]
    await bulk.run_chunk(job_id, 0)  # первый чанк успел
    await bulk.cancel(cancel, job_id)  # running → cancelling: события ещё нет
    assert await events(uow, org.id, "bulk_job.completed") == []

    # Финал cancelled ставит finalize после последнего (пропущенного) чанка.
    await bulk.run_chunk(job_id, 1)
    await bulk.run_chunk(job_id, 2)

    [completed] = await events(uow, org.id, "bulk_job.completed")
    assert (completed["status"], completed["processed"], completed["rows"]) == (
        "cancelled",
        2,
        {"contacts": 2},
    )
    assert completed["to_user_id"] == str(heir_id)


async def test_move_team_event_goes_to_admin_without_receiver(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    uow: UnitOfWork,
    run_job: RunJob,
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep", team=0)
    admin_id, rep_id = admin.id, rep.id
    await make_contact(rep)

    resp = await client.patch(
        f"/api/v1/users/{rep_id}",
        json={"team_id": str(org.team_ids[1])},
        headers=await tokens_for(client, admin),
    )
    assert resp.status_code == 200, resp.text
    await run_job(resp.json()["bulk_job_id"])

    [completed] = await events(uow, org.id, "bulk_job.completed")
    assert (completed["kind"], completed["status"], completed["author_id"]) == (
        "move_team",
        "done",
        str(admin_id),
    )
    # Итог — админу, который перевёл; «вам передано» некому: ответственный не менялся.
    assert completed["to_user_id"] is None
    assert completed["rows"]["contacts"] == 1


async def test_add_tags_event_rows_include_failed(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    uow: UnitOfWork,
    run_job: RunJob,
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    await make_contact(rep, tags=[f"t{i}" for i in range(20)])  # 20 тегов — пропуск
    await make_contact(rep)

    resp = await client.post(
        "/api/v1/bulk/add-tags",
        json={"tag": "vip", "filter": {}},
        headers=await tokens_for(client, admin),
    )
    assert resp.status_code == 202, resp.text
    await run_job(resp.json()["id"])

    [completed] = await events(uow, org.id, "bulk_job.completed")
    assert (completed["kind"], completed["status"], completed["failed_rows"]) == (
        "add_tags",
        "partial_failed",
        1,
    )
    assert completed["rows"] == {"contacts": 1, "failed": 1}
