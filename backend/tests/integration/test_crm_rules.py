"""Правила этапа 4.1, которые живут не в CRM-эндпоинтах, а рядом (раздел 5):

- отдел с живыми записями не удалить — 409 team.has_records;
- человека с живыми записями не сделать org_admin / viewer — 409 member.has_records; перевести
  в другой отдел можно (с этапа 5): записи догоняет bulk job move_team; отключить можно;
- версия записи растёт при каждом UPDATE, устаревшая версия — 409 version.conflict.
"""

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError

from app.models import AuditEntry, BulkJob, Contact
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeOrg,
    MakeRecord,
    MakeUser,
    RunJob,
    member,
    tokens_for,
)


async def test_member_with_records_moves_but_cannot_lose_team(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    task_queue: FakeTaskQueue,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin_user = await member(make_user, org, "org_admin")
    admin, admin_id = await tokens_for(client, admin_user), admin_user.id
    rep = await member(make_user, org, "sales_rep", team=1)
    rep_id = rep.id  # ответ 409 откатывает сессию теста, и объекты «протухают»
    contact_id = (await make_contact(rep)).id
    url = f"/api/v1/users/{rep_id}"
    new_team = org.team_ids[0]

    moved = await client.patch(url, json={"team_id": str(new_team)}, headers=admin)
    demoted = await client.patch(url, json={"role": "viewer"}, headers=admin)
    promoted = await client.patch(url, json={"role": "team_lead"}, headers=admin)  # тот же отдел
    disabled = await client.patch(url, json={"is_active": False}, headers=admin)

    assert moved.status_code == 200
    assert moved.json()["team_id"] == str(new_team)
    job_id = uuid.UUID(moved.json()["bulk_job_id"])
    assert task_queue.enqueued == [("bulk.plan", (str(job_id),))]  # после коммита
    job = await db_session.get(BulkJob, job_id)
    assert job is not None
    assert (job.kind, job.status, job.owner_id) == ("move_team", "pending", admin_id)
    assert job.params["user_id"] == str(rep_id)
    assert (demoted.status_code, demoted.json()["code"]) == (409, "member.has_records")
    assert (promoted.status_code, promoted.json()["bulk_job_id"]) == (200, None)
    # Отключить можно: записи остаются, их передадут другому (bulk).
    assert disabled.status_code == 200

    await run_job(job_id)

    team = await db_session.scalar(select(Contact.team_id).where(Contact.id == contact_id))
    assert team == new_team  # запись догнала отдел ответственного
    entry = await db_session.scalar(select(AuditEntry).where(AuditEntry.entity_id == str(job_id)))
    assert entry is not None
    assert entry.action == "bulk.move_team"
    assert entry.changes == {
        "user_id": str(rep_id),
        "status": "done",
        "rows": {"companies": 0, "contacts": 1, "deals": 0, "activities": 0},
    }


async def test_records_in_trash_do_not_block(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=1)
    contact = await make_contact(rep)
    contact.deleted_at = datetime.now(UTC)
    await db_session.commit()

    moved = await client.patch(f"/api/v1/users/{rep.id}", json={"role": "viewer"}, headers=admin)

    assert moved.status_code == 200


async def test_empty_team_with_records_cannot_be_deleted(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=1)
    await make_contact(rep)
    # Человек ушёл из отдела в обход правил (например, данными до этапа 4) — записи остались.
    await db_session.execute(
        update(type(rep)).where(type(rep).id == rep.id).values(role="viewer", team_id=None)
    )
    await db_session.commit()

    resp = await client.delete(f"/api/v1/teams/{org.team_ids[1]}", headers=admin)

    assert (resp.status_code, resp.json()["code"]) == (409, "team.has_records")


async def test_team_with_open_activity_cannot_be_deleted(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_activity: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=1)
    await make_activity(rep)  # только открытая задача — тоже живая запись отдела
    await db_session.execute(
        update(type(rep)).where(type(rep).id == rep.id).values(role="viewer", team_id=None)
    )
    await db_session.commit()

    resp = await client.delete(f"/api/v1/teams/{org.team_ids[1]}", headers=admin)

    assert (resp.status_code, resp.json()["code"]) == (409, "team.has_records")


async def test_version_grows_on_every_update(
    make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord, db_session: AsyncSession
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    contact: Contact = await make_contact(rep)
    assert contact.version == 1

    contact.first_name = "Петро"
    await db_session.commit()

    assert contact.version == 2


async def test_stale_update_is_rejected_by_database(
    make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord, db_session: AsyncSession
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    contact: Contact = await make_contact(rep)
    # Кто-то другой успел изменить строку: версия в базе уже 2, а в нашем объекте — 1.
    await db_session.execute(
        update(Contact)
        .where(Contact.id == contact.id)
        .values(version=Contact.version + 1)
        .execution_options(synchronize_session=False)  # наш объект об этом не знает
    )

    contact.first_name = "Петро"
    # UPDATE … WHERE id = :id AND version = 1 не найдёт строку. В API это 409 version.conflict.
    with pytest.raises(StaleDataError):
        await db_session.flush()
