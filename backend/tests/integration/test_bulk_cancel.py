"""Отмена bulk job (раздел 15, «Жизненный цикл bulk job»): переходы статуса и коды ответа
POST /bulk-jobs/{job_id}/cancel, отмена посреди идущей job. Права ролей — в
test_access_matrix.py.

Чанки вызываются напрямую в сессии теста (как в test_bulk.py), а отменяет — API: так видно, что
отмена из запроса и чанки воркера сходятся к одному итогу.
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Forbidden
from app.models import AuditEntry, BulkJob, BulkJobChunk, Contact, User
from app.services.bulk import BulkService, ReassignOwnerRequest
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeJob,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
    tokens_for,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


@pytest.fixture
def bulk(uow: UnitOfWork) -> BulkService:
    return BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=2)


def cancel_url(job_id: Any) -> str:
    return f"/api/v1/bulk-jobs/{job_id}/cancel"


def access(user: User, action: str) -> Access:
    assert user.org_id is not None
    scope = CATALOG.scope_for(user.role, action)
    return Access(action, scope, user.id, user.org_id, user.team_id, user.role)


async def job_of(uow: UnitOfWork, job_id: uuid.UUID | str) -> BulkJob:
    job = await uow.bulk_jobs.get(uuid.UUID(str(job_id)))
    assert job is not None
    return job


async def audit_of(uow: UnitOfWork, job_id: uuid.UUID | str) -> list[AuditEntry]:
    return list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == str(job_id)
            )
        )
    )


async def test_pending_job_is_cancelled_at_once(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    bulk: BulkService,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    leaving = await member(make_user, org, "sales_rep")
    heir = await member(make_user, org, "sales_rep", team=1)
    admin_id = admin.id
    request = ReassignOwnerRequest(leaving.id, heir.id, ("contacts",))
    job_id = (await bulk.start_reassign_owner(access(admin, "bulk:run"), request, None)).id

    resp = await client.post(cancel_url(job_id), headers=await tokens_for(client, admin))

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert (data["id"], data["status"]) == (str(job_id), "cancelled")
    assert data["finished_at"] is not None
    assert data["result"] == {"rows": {"contacts": 0}}
    # Отменил тот, кто сделал финальный переход, — он и пишет одну строку журнала.
    [entry] = await audit_of(uow, job_id)
    assert (entry.action, entry.actor_id) == ("bulk.reassign_owner", admin_id)
    assert entry.changes["status"] == "cancelled"
    assert "access" not in entry.changes
    # plan, который всё-таки пришёл (или его переотправил reaper), ничего не делает.
    assert await bulk.plan(job_id) == []
    assert (await job_of(uow, job_id)).status == "cancelled"


async def test_running_job_becomes_cancelling_and_repeat_is_unchanged(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob, uow: UnitOfWork
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    headers = await tokens_for(client, lead)
    job_id = (await make_job(lead, status="running", chunks_total=2)).id

    first = await client.post(cancel_url(job_id), headers=headers)
    second = await client.post(cancel_url(job_id), headers=headers)

    assert (first.status_code, first.json()["status"]) == (202, "cancelling")
    assert (second.status_code, second.json()["status"]) == (202, "cancelling")
    job = await job_of(uow, job_id)
    assert (job.status, job.finished_at) == ("cancelling", None)
    assert await audit_of(uow, job_id) == []  # итог напишет finalize


async def test_cancelled_job_is_202_unchanged(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    admin = await member(make_user, org, "org_admin")
    job_id = (await make_job(admin, status="cancelled")).id

    resp = await client.post(cancel_url(job_id), headers=await tokens_for(client, admin))

    assert (resp.status_code, resp.json()["status"]) == (202, "cancelled")


@pytest.mark.parametrize("status", ["done", "partial_failed", "failed"])
async def test_finished_job_is_not_cancellable(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob, status: str
) -> None:
    admin = await member(make_user, org, "org_admin")
    job_id = (await make_job(admin, status=status)).id

    resp = await client.post(cancel_url(job_id), headers=await tokens_for(client, admin))

    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "bulk.not_cancellable"
    assert resp.json()["details"] == {"status": status}


@pytest.mark.parametrize("status", ["pending", "running"])
async def test_move_team_is_not_cancellable(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_job: MakeJob,
    uow: UnitOfWork,
    status: str,
) -> None:
    admin = await member(make_user, org, "org_admin")
    job_id = (await make_job(admin, kind="move_team", status=status)).id

    resp = await client.post(cancel_url(job_id), headers=await tokens_for(client, admin))

    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "bulk.not_cancellable"
    assert (await job_of(uow, job_id)).status == status


async def test_invisible_job_is_404(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob, uow: UnitOfWork
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    other_lead = await member(make_user, org, "team_lead", team=1)
    job_id = (await make_job(other_lead, status="running")).id

    resp = await client.post(cancel_url(job_id), headers=await tokens_for(client, lead))

    assert resp.status_code == 404
    assert resp.json()["details"] == {"entity": "bulk_job", "id": str(job_id)}
    assert (await job_of(uow, job_id)).status == "running"


async def test_visible_job_outside_cancel_scope_is_403(
    make_user: MakeUser, org: OrgFixture, make_job: MakeJob, bulk: BulkService, uow: UnitOfWork
) -> None:
    # В data.yaml bulk_job:cancel у всех ролей равен bulk_job:read (тест политик не даёт ему
    # быть шире) — поэтому уровень уже, чем чтение, задан здесь руками.
    admin = await member(make_user, org, "org_admin")
    lead = await member(make_user, org, "team_lead", team=0)
    job_id = (await make_job(lead, status="running")).id
    narrow = Access("bulk_job:cancel", "own", admin.id, org.id, None, admin.role)

    with pytest.raises(Forbidden):
        await bulk.cancel(narrow, job_id)

    assert (await job_of(uow, job_id)).status == "running"


async def test_author_cancels_own_job_after_moving_to_other_team(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    lead = await member(make_user, org, "team_lead", team=0)
    lead_headers = await tokens_for(client, lead)
    job_id = (await make_job(lead, status="running")).id
    moved = await client.patch(
        f"/api/v1/users/{lead.id}", json={"team_id": str(org.team_ids[1])}, headers=admin
    )
    assert moved.status_code == 200, moved.text

    # team_id job — прежний отдел автора: свою job он и видит, и отменяет.
    resp = await client.post(cancel_url(job_id), headers=lead_headers)

    assert (resp.status_code, resp.json()["status"]) == (202, "cancelling")


async def test_cancel_during_running_job_skips_rest_and_ends_cancelled(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
    bulk: BulkService,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    leaving = await member(make_user, org, "sales_rep")
    heir = await member(make_user, org, "sales_rep", team=1)
    leaving_id, heir_id = leaving.id, heir.id
    for _ in range(6):
        await make_contact(leaving)
    headers = await tokens_for(client, admin)
    request = ReassignOwnerRequest(leaving_id, heir_id, ("contacts",))
    job_id = (await bulk.start_reassign_owner(access(admin, "bulk:run"), request, None)).id
    assert await bulk.plan(job_id) == [0, 1, 2]
    await bulk.run_chunk(job_id, 0)  # первый чанк успел

    resp = await client.post(cancel_url(job_id), headers=headers)
    assert (resp.status_code, resp.json()["status"]) == (202, "cancelling")
    await bulk.run_chunk(job_id, 1)
    await bulk.run_chunk(job_id, 2)

    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.total) == ("cancelled", 2, 6)
    assert job.finished_at is not None
    assert job.result == {"rows": {"contacts": 2}}
    chunks = list(
        await uow.session.scalars(
            select(BulkJobChunk.status).where(BulkJobChunk.job_id == job_id).order_by("no")
        )
    )
    assert chunks == ["done", "skipped", "skipped"]
    # Уже изменённые строки остаются изменёнными — ровно столько, сколько в processed.
    owners = list(
        await uow.session.scalars(
            select(Contact.owner_id).where(Contact.owner_id.in_([leaving_id, heir_id]))
        )
    )
    assert (owners.count(heir_id), owners.count(leaving_id)) == (2, 4)
    [entry] = await audit_of(uow, job_id)
    assert entry.changes["status"] == "cancelled"
    assert entry.changes["rows"] == {"contacts": 2}
    resp = await client.get(f"/api/v1/bulk-jobs/{job_id}", headers=headers)
    assert (resp.json()["status"], resp.json()["processed"]) == ("cancelled", 2)
