"""Перевод человека с записями в другой отдел — bulk job move_team (раздел 6, «Смена отдела
человека»).

PATCH /users/{id} меняет отдел сразу и в той же транзакции создаёт job; plan и чанки вызываются
напрямую в транзакции теста (фикстура run_job или BulkService) — как их вызывают задачи
воркера. Записи, которые переходят: живые компании и контакты, открытые сделки и открытые
активности; закрытые сделки, выполненные активности и корзина остаются в прежнем отделе.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.roles import RoleCatalog
from app.models import Activity, BulkJob, Company, Contact, Deal, DealStatus, Team
from app.services.bulk import BulkService
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeOrg,
    MakeRecord,
    MakeUser,
    RunJob,
    member,
    tokens_for,
)


async def transfer(
    client: AsyncClient, admin: dict[str, str], user_id: uuid.UUID, team_id: uuid.UUID
) -> Response:
    return await client.patch(
        f"/api/v1/users/{user_id}", json={"team_id": str(team_id)}, headers=admin
    )


def job_id_of(resp: Response) -> uuid.UUID:
    assert resp.status_code == 200, resp.text
    return uuid.UUID(resp.json()["bulk_job_id"])


async def team_of(session: AsyncSession, model: Any, record_id: int) -> uuid.UUID | None:
    return await session.scalar(select(model.team_id).where(model.id == record_id))


async def job_of(session: AsyncSession, job_id: uuid.UUID) -> BulkJob:
    job = await session.get(BulkJob, job_id, populate_existing=True)
    assert job is not None
    return job


async def test_only_alive_and_open_records_follow_member(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_company: MakeRecord,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    old_team, new_team = org.team_ids
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=0)
    stage = (await make_pipeline(org.id)).stages[0]
    gone = datetime.now(UTC)
    follow = {
        Company: (await make_company(rep)).id,
        Contact: (await make_contact(rep)).id,
        Deal: (await make_deal(rep, stage)).id,
        Activity: (await make_activity(rep)).id,
    }
    # История и корзина: выиграл сделку тот отдел, где человек был тогда.
    stay = [
        (Contact, (await make_contact(rep, deleted_at=gone)).id),
        (Deal, (await make_deal(rep, stage, status=DealStatus.WON, closed_at=gone)).id),
        (Activity, (await make_activity(rep, due_at=None, done_at=gone)).id),
    ]

    job_id = job_id_of(await transfer(client, admin, rep.id, new_team))
    await run_job(job_id)

    for model, record_id in follow.items():
        assert await team_of(db_session, model, record_id) == new_team, model
    for model, record_id in stay:
        assert await team_of(db_session, model, record_id) == old_team, model
    job = await job_of(db_session, job_id)
    assert (job.kind, job.status, job.total, job.processed) == ("move_team", "done", 4, 4)
    assert job.result == {"rows": {"companies": 1, "contacts": 1, "deals": 1, "activities": 1}}
    version = await db_session.scalar(select(Contact.version).where(Contact.id == follow[Contact]))
    assert version == 2  # открытая карточка получит 409 на PATCH
    # Автор job — админ: он видит её прогресс, как любую свою job.
    progress = await client.get(f"/api/v1/bulk-jobs/{job_id}", headers=admin)
    assert (progress.status_code, progress.json()["status"]) == (200, "done")


async def test_open_activity_alone_needs_job_and_history_does_not(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
    task_queue: FakeTaskQueue,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    new_team = org.team_ids[1]
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    with_task = await member(make_user, org, "sales_rep", team=0)
    with_history = await member(make_user, org, "sales_rep", team=0)
    task_id = (await make_activity(with_task)).id
    gone = datetime.now(UTC)
    stage = (await make_pipeline(org.id)).stages[0]
    await make_activity(with_history, due_at=None, done_at=gone)
    await make_deal(with_history, stage, status=DealStatus.LOST, closed_at=gone)
    await make_contact(with_history, deleted_at=gone)
    with_task_id = with_task.id

    task_moved = await transfer(client, admin, with_task.id, new_team)
    history_moved = await transfer(client, admin, with_history.id, new_team)
    # У viewer отдела нет, а открытая задача — живая запись: сначала её передают.
    demoted = await client.patch(
        f"/api/v1/users/{with_task_id}", json={"role": "viewer"}, headers=admin
    )

    job_id = job_id_of(task_moved)
    assert (history_moved.status_code, history_moved.json()["bulk_job_id"]) == (200, None)
    assert history_moved.json()["team_id"] == str(new_team)  # перевод — сразу, без job
    assert task_queue.enqueued == [("bulk.plan", (str(job_id),))]
    assert (demoted.status_code, demoted.json()["code"]) == (409, "member.has_records")
    await run_job(job_id)
    assert await team_of(db_session, Activity, task_id) == new_team


async def test_chain_of_transfers_converges_to_latest_team(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    uow: UnitOfWork,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    third = Team(id=uuid.uuid4(), org_id=org.id, name="Продажі 3")
    uow.teams.add(third)
    await uow.commit()
    third_id = third.id  # откат в сервисе «протухает» ORM-объекты сессии
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=0)
    contacts = [(await make_contact(rep)).id for _ in range(3)]
    bulk = BulkService(uow, RoleCatalog.load(POLICIES_DIR), FakeTaskQueue(), chunk_size=2)

    first = job_id_of(await transfer(client, admin, rep.id, org.team_ids[1]))
    numbers = await bulk.plan(first)  # снимок: три контакта не в отделе 2
    # Создан между переводами: сразу в отделе 2 (отдел ответственного), в план first не попал.
    contacts.append((await make_contact(rep)).id)
    second = job_id_of(await transfer(client, admin, rep.id, third_id))
    for no in numbers:  # чанки first идут уже после второго перевода
        await bulk.run_chunk(first, no)
    await run_job(second)

    assert second != first  # две move_team одного человека не мешают друг другу
    for contact_id in contacts:
        assert await team_of(db_session, Contact, contact_id) == third_id
    # Чанк берёт отдел из текущей строки человека — first сразу догнала последний отдел, а
    # second досталась только запись, созданная между переводами.
    done = [await job_of(db_session, job_id) for job_id in (first, second)]
    assert [(job.status, job.processed) for job in done] == [("done", 3), ("done", 1)]


async def test_transfer_back_before_job_runs(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    old_team, new_team = org.team_ids
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=0)
    contact_id = (await make_contact(rep)).id

    first = job_id_of(await transfer(client, admin, rep.id, new_team))
    back = await transfer(client, admin, rep.id, old_team)
    await run_job(first)

    # Записи уже в отделе человека — вторая job не нужна, а первая ничего не меняет.
    assert (back.status_code, back.json()["bulk_job_id"]) == (200, None)
    job = await job_of(db_session, first)
    assert (job.status, job.total, job.processed) == ("done", 0, 0)
    assert await team_of(db_session, Contact, contact_id) == old_team


async def test_move_team_of_two_people_runs_in_parallel(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    new_team = org.team_ids[1]
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    reps = [await member(make_user, org, "sales_rep", team=0) for _ in range(2)]
    contacts = [(await make_contact(rep)).id for rep in reps for _ in range(2)]

    # Не 409 bulk.already_running: move_team нет в индексе «одна job вида на организацию».
    jobs = [job_id_of(await transfer(client, admin, rep.id, new_team)) for rep in reps]
    statuses = [(await job_of(db_session, job_id)).status for job_id in jobs]
    for job_id in reversed(jobs):
        await run_job(job_id)

    assert statuses == ["pending", "pending"]  # обе активны одновременно
    for job_id in jobs:
        assert (await job_of(db_session, job_id)).processed == 2
    for contact_id in contacts:
        assert await team_of(db_session, Contact, contact_id) == new_team


async def test_repeated_transfer_to_same_team_heals_failed_job(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    uow: UnitOfWork,
    run_job: RunJob,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    new_team = org.team_ids[1]
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=0)
    rep_id = rep.id
    contact_id = (await make_contact(rep)).id
    bulk = BulkService(uow, RoleCatalog.load(POLICIES_DIR), FakeTaskQueue(), chunk_size=2)
    failed = job_id_of(await transfer(client, admin, rep_id, new_team))
    for no in await bulk.plan(failed):
        await bulk.fail_chunk(failed, no, "OperationalError")  # чанк упал после всех ретраев

    again = job_id_of(await transfer(client, admin, rep_id, new_team))  # тот же отдел
    await run_job(again)
    nothing_left = await transfer(client, admin, rep_id, new_team)

    assert (await job_of(db_session, failed)).status == "failed"
    assert again != failed
    assert await team_of(db_session, Contact, contact_id) == new_team
    assert (nothing_left.status_code, nothing_left.json()["bulk_job_id"]) == (200, None)


async def test_reopened_history_follows_current_team(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    new_team = org.team_ids[1]
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=0)
    old_lead = await tokens_for(client, await member(make_user, org, "team_lead", team=0))
    rep_headers = await tokens_for(client, rep)
    gone = datetime.now(UTC)
    stage = (await make_pipeline(org.id)).stages[0]
    deal = await make_deal(rep, stage, status=DealStatus.WON, closed_at=gone)
    deal_id, version = deal.id, deal.version
    task_id = (await make_activity(rep, due_at=None, done_at=gone)).id
    moved = await transfer(client, admin, rep.id, new_team)
    assert (moved.status_code, moved.json()["bulk_job_id"]) == (200, None)  # одна история
    reopen_deal = {"version": version, "status": "open"}
    reopen_task = {"done": False, "due_at": "2030-01-01T09:00:00Z"}

    # Прежний руководитель видит историю своего отдела, но снова открытая запись ушла бы в
    # чужой для него отдел.
    by_old_lead = await client.patch(f"/api/v1/deals/{deal_id}", json=reopen_deal, headers=old_lead)
    deal_resp = await client.patch(
        f"/api/v1/deals/{deal_id}", json=reopen_deal, headers=rep_headers
    )
    task_resp = await client.patch(
        f"/api/v1/activities/{task_id}", json=reopen_task, headers=rep_headers
    )

    assert (by_old_lead.status_code, by_old_lead.json()["details"]) == (
        403,
        {"reason": "out_of_scope"},
    )
    assert deal_resp.status_code == 200, deal_resp.text
    assert task_resp.status_code == 200, task_resp.text
    # Закрытые записи move_team не переносит — снова открытые догоняют отдел ответственного.
    assert await team_of(db_session, Deal, deal_id) == new_team
    assert await team_of(db_session, Activity, task_id) == new_team
