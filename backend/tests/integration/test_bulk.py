"""Массовая передача клиентов на уровне сервиса (раздел 6): plan, чанки, finalize.

Всё в транзакции теста: задачи Celery здесь не участвуют, plan и чанки вызываются напрямую — так
же, как их вызывают обёртки задач. Сервис после ошибки откатывает свою часть, а откат «протухает»
ORM-объекты сессии, поэтому id людей и записей тест держит в переменных, а записи перечитывает.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select, update

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.models import (
    Activity,
    AuditEntry,
    BulkJob,
    BulkJobChunk,
    Company,
    Contact,
    Deal,
    DealStatus,
    User,
)
from app.services.bulk import BulkService, ReassignOwnerRequest
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
CHUNK_SIZE = 3


def bulk_access(user: User) -> Access:
    """Access автора, как его собирает authorize для POST /bulk/reassign-owner."""
    assert user.org_id is not None
    scope = CATALOG.scope_for(user.role, "bulk:run")
    return Access("bulk:run", scope, user.id, user.org_id, user.team_id, user.role)


@pytest.fixture
def bulk(uow: UnitOfWork) -> BulkService:
    return BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=CHUNK_SIZE)


async def start(
    bulk: BulkService, author: User, from_id: uuid.UUID, to_id: uuid.UUID, *include: str
) -> uuid.UUID:
    request = ReassignOwnerRequest(from_id, to_id, *([tuple(include)] if include else []))
    job = await bulk.start_reassign_owner(bulk_access(author), request, None)
    return job.id


async def run_all(bulk: BulkService, job_id: uuid.UUID) -> None:
    """plan и все чанки по очереди — как воркер, но в одной сессии."""
    for no in await bulk.plan(job_id):
        await bulk.run_chunk(job_id, no)


async def job_of(uow: UnitOfWork, job_id: uuid.UUID) -> BulkJob:
    job = await uow.bulk_jobs.get(job_id)
    assert job is not None
    return job


async def chunks_of(uow: UnitOfWork, job_id: uuid.UUID) -> list[BulkJobChunk]:
    return list(
        await uow.session.scalars(
            select(BulkJobChunk)
            .where(BulkJobChunk.job_id == job_id)
            .order_by(BulkJobChunk.no)
            .execution_options(populate_existing=True)
        )
    )


async def owner_of(uow: UnitOfWork, model: Any, record_id: int) -> tuple[uuid.UUID, uuid.UUID]:
    row = (
        await uow.session.execute(
            select(model.owner_id, model.team_id).where(model.id == record_id)
        )
    ).one()
    return row.owner_id, row.team_id


async def audit_rows(uow: UnitOfWork, job_id: uuid.UUID) -> list[AuditEntry]:
    return list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == str(job_id)
            )
        )
    )


class People:
    """Организация: админ, руководитель отдела 1, уходящий менеджер и получатели в отделах."""

    def __init__(self, org: OrgFixture, users: dict[str, User]) -> None:
        self.org = org
        self.users = users
        self.ids = {name: user.id for name, user in users.items()}


@pytest.fixture
async def people(make_org: MakeOrg, make_user: MakeUser) -> People:
    org = await make_org()
    users = {
        "admin": await member(make_user, org, "org_admin"),
        "lead": await member(make_user, org, "team_lead", team=0),
        "leaving": await member(make_user, org, "sales_rep", team=0),
        "heir": await member(make_user, org, "sales_rep", team=0),
        "other_team": await member(make_user, org, "sales_rep", team=1),
    }
    return People(org, users)


async def test_plan_cuts_chunks_by_rows_not_id_ranges(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving, heir = people.users["leaving"], people.users["heir"]
    mine = []
    for _ in range(7):  # между контактами уходящего — чужие: равные диапазоны id были бы пусты
        mine.append((await make_contact(leaving)).id)
        await make_contact(heir)
        await make_contact(heir)
    job_id = await start(bulk, people.users["admin"], leaving.id, heir.id, "contacts")

    numbers = await bulk.plan(job_id)

    chunks = await chunks_of(uow, job_id)
    assert numbers == [0, 1, 2]
    assert [(c.lo, c.hi) for c in chunks] == [
        (mine[0], mine[3] - 1),
        (mine[3], mine[6] - 1),
        (mine[6], mine[6]),
    ]
    assert {c.entity for c in chunks} == {"contacts"}
    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.chunks_total) == ("running", 7, 3)
    assert job.started_at is not None
    # Повтор plan (задачу доставили дважды) ничего не делает: job уже не pending.
    assert await bulk.plan(job_id) == []
    assert len(await chunks_of(uow, job_id)) == 3


async def test_chunk_of_one_row(uow: UnitOfWork, people: People, make_contact: MakeRecord) -> None:
    leaving, heir = people.users["leaving"], people.users["heir"]
    mine = [(await make_contact(leaving)).id for _ in range(3)]
    bulk = BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=1)  # BULK_CHUNK_SIZE=1
    job_id = await start(bulk, people.users["admin"], leaving.id, heir.id, "contacts")

    await run_all(bulk, job_id)

    assert [(c.lo, c.hi) for c in await chunks_of(uow, job_id)] == [
        (mine[0], mine[1] - 1),
        (mine[1], mine[2] - 1),
        (mine[2], mine[2]),
    ]
    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed) == ("done", 3, 3)


async def test_record_entering_selection_inside_chunk_range_grows_total(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving, heir = people.users["leaving"], people.users["heir"]
    mine = [(await make_contact(leaving)).id]
    joined = (await make_contact(heir)).id  # между контактами уходящего — в диапазоне чанка 0
    mine += [(await make_contact(leaving)).id for _ in range(3)]
    job_id = await start(bulk, people.users["admin"], leaving.id, heir.id, "contacts")
    numbers = await bulk.plan(job_id)
    assert (await job_of(uow, job_id)).total == 4
    # После plan контакт передали уходящему: снимок — диапазоны id, чанк возьмёт и его.
    await uow.session.execute(
        update(Contact).where(Contact.id == joined).values(owner_id=leaving.id)
    )
    await uow.commit()

    for no in numbers:
        await bulk.run_chunk(job_id, no)

    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed) == ("done", 5, 5)  # прогресс не больше 100 %
    assert await owner_of(uow, Contact, joined) == (heir.id, heir.team_id)


async def test_reassign_moves_only_alive_and_open_records(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_company: MakeRecord,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
) -> None:
    leaving, heir_id = people.users["leaving"], people.ids["other_team"]
    team_2 = people.org.team_ids[1]
    gone = datetime.now(UTC)
    stage = (await make_pipeline(people.org.id)).stages[0]
    moved = {
        Company: (await make_company(leaving)).id,
        Contact: (await make_contact(leaving)).id,
        Deal: (await make_deal(leaving, stage)).id,
        Activity: (await make_activity(leaving)).id,
    }
    stays = {
        Company: [(await make_company(leaving, deleted_at=gone)).id],
        Contact: [(await make_contact(leaving, deleted_at=gone)).id],
        Deal: [
            (await make_deal(leaving, stage, status=DealStatus.WON, closed_at=gone)).id,
            (await make_deal(leaving, stage, deleted_at=gone)).id,
        ],
        Activity: [(await make_activity(leaving, due_at=None, done_at=gone)).id],
    }
    job_id = await start(bulk, people.users["admin"], leaving.id, heir_id)

    await run_all(bulk, job_id)

    for model, record_id in moved.items():
        # Новый ответственный и его отдел (админ передаёт в любой отдел).
        assert await owner_of(uow, model, record_id) == (heir_id, team_2), model
    for model, ids in stays.items():
        for record_id in ids:
            owner_id, _ = await owner_of(uow, model, record_id)
            assert owner_id == people.ids["leaving"], model
    versions = await uow.session.scalars(
        select(Contact.version).where(Contact.id == moved[Contact])
    )
    assert list(versions) == [2]  # открытая карточка получит 409 на PATCH
    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.finished_at is not None) == (
        "done",
        4,
        4,
        True,
    )
    rows = {"companies": 1, "contacts": 1, "deals": 1, "activities": 1}
    assert job.result == {"rows": rows}
    [entry] = await audit_rows(uow, job_id)  # одна строка журнала на job
    assert entry.action == "bulk.reassign_owner"
    assert entry.actor_id == people.ids["admin"]
    assert entry.changes["rows"] == rows
    assert entry.changes["status"] == "done"
    assert entry.changes["from_user_id"] == str(people.ids["leaving"])
    assert "access" not in entry.changes


async def test_team_lead_moves_only_records_of_own_team(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    in_team = (await make_contact(leaving)).id
    # team_id отстаёт (move_team ещё идёт): запись вне уровня руководителя — не его.
    lagging_contact = await make_contact(leaving)
    lagging_contact.team_id = people.org.team_ids[1]
    lagging = lagging_contact.id
    await uow.commit()
    job_id = await start(bulk, people.users["lead"], leaving.id, people.ids["heir"], "contacts")

    await run_all(bulk, job_id)

    assert (await owner_of(uow, Contact, in_team))[0] == people.ids["heir"]
    assert (await owner_of(uow, Contact, lagging))[0] == people.ids["leaving"]
    assert (await job_of(uow, job_id)).processed == 1


async def test_chunk_is_idempotent(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    for _ in range(4):
        await make_contact(leaving)
    job_id = await start(bulk, people.users["admin"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)

    await bulk.run_chunk(job_id, 0)
    await bulk.run_chunk(job_id, 0)  # повторная доставка той же задачи

    job = await job_of(uow, job_id)
    assert (job.processed, job.status) == (3, "running")  # не 6: прогресс не посчитан дважды
    chunks = await chunks_of(uow, job_id)
    assert [(c.status, c.rows) for c in chunks] == [("done", 3), ("pending", None)]

    await bulk.run_chunk(job_id, 1)
    await bulk.run_chunk(job_id, 1)

    job = await job_of(uow, job_id)
    assert (job.processed, job.total, job.status) == (4, 4, "done")
    assert len(await audit_rows(uow, job_id)) == 1  # повторный finalize итог не дублирует


async def test_deactivated_author_fails_job(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    contacts = [(await make_contact(leaving)).id for _ in range(4)]
    job_id = await start(bulk, people.users["lead"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)
    people.users["lead"].is_active = False  # руководителя отключили, пока job ждала воркер
    await uow.commit()

    await bulk.run_chunk(job_id, 0)
    await bulk.run_chunk(job_id, 1)

    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.processed) == ("failed", "bulk.author_changed", 0)
    chunks = await chunks_of(uow, job_id)
    assert [c.status for c in chunks] == ["skipped", "skipped"]
    for contact_id in contacts:
        assert (await owner_of(uow, Contact, contact_id))[0] == people.ids["leaving"]
    [entry] = await audit_rows(uow, job_id)
    assert entry.changes["status"] == "failed"


async def test_author_moved_to_other_team_fails_job(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    await make_contact(leaving)
    job_id = await start(bulk, people.users["lead"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)
    people.users["lead"].team_id = people.org.team_ids[1]
    await uow.commit()

    await bulk.run_chunk(job_id, 0)

    job = await job_of(uow, job_id)
    assert (job.status, job.error) == ("failed", "bulk.author_changed")


async def test_deactivated_target_fails_job(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    contact_id = (await make_contact(leaving)).id
    job_id = await start(bulk, people.users["lead"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)
    people.users["heir"].is_active = False
    await uow.commit()

    await bulk.run_chunk(job_id, 0)

    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.processed) == ("failed", "bulk.target_changed", 0)
    assert (await owner_of(uow, Contact, contact_id))[0] == people.ids["leaving"]


async def test_target_moved_out_of_author_team_fails_job(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    await make_contact(leaving)
    job_id = await start(bulk, people.users["lead"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)
    people.users["heir"].team_id = people.org.team_ids[1]  # вышел из уровня руководителя
    await uow.commit()

    await bulk.run_chunk(job_id, 0)

    assert (await job_of(uow, job_id)).error == "bulk.target_changed"


async def test_records_follow_target_team_at_chunk_time(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    contact_id = (await make_contact(leaving)).id
    job_id = await start(bulk, people.users["admin"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)
    people.users["heir"].team_id = people.org.team_ids[1]  # перевели до чанка
    await uow.commit()

    await bulk.run_chunk(job_id, 0)

    assert await owner_of(uow, Contact, contact_id) == (people.ids["heir"], people.org.team_ids[1])


async def test_failed_chunk_gives_partial_failed(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    for _ in range(4):
        await make_contact(leaving)
    job_id = await start(bulk, people.users["admin"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)

    await bulk.run_chunk(job_id, 0)
    await bulk.fail_chunk(job_id, 1, "SoftTimeLimitExceeded")  # on_failure после ретраев
    await bulk.fail_chunk(job_id, 1, "SoftTimeLimitExceeded")  # повтор ничего не добавляет

    job = await job_of(uow, job_id)
    assert (job.status, job.failed_chunks, job.processed) == ("partial_failed", 1, 3)
    chunks = await chunks_of(uow, job_id)
    assert [(c.status, c.error) for c in chunks] == [
        ("done", None),
        ("failed", "SoftTimeLimitExceeded"),
    ]
    assert len(await audit_rows(uow, job_id)) == 1


async def test_all_chunks_failed_gives_failed(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    await make_contact(leaving)
    job_id = await start(bulk, people.users["admin"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)

    await bulk.fail_chunk(job_id, 0, "OperationalError")

    assert (await job_of(uow, job_id)).status == "failed"


async def test_empty_selection_is_done_at_once(
    bulk: BulkService, uow: UnitOfWork, people: People
) -> None:
    job_id = await start(bulk, people.users["admin"], people.ids["leaving"], people.ids["heir"])

    assert await bulk.plan(job_id) == []

    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.chunks_total) == ("done", 0, 0)
    zero = {"companies": 0, "contacts": 0, "deals": 0, "activities": 0}
    assert job.result == {"rows": zero}
    [entry] = await audit_rows(uow, job_id)
    assert entry.changes["rows"] == zero


async def test_finalize_waits_for_pending_chunks(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    leaving = people.users["leaving"]
    for _ in range(4):
        await make_contact(leaving)
    job_id = await start(bulk, people.users["admin"], leaving.id, people.ids["heir"], "contacts")
    await bulk.plan(job_id)

    assert await bulk.finalize_if_complete(job_id) is None  # оба чанка ещё pending
    await bulk.run_chunk(job_id, 1)  # чанки идут в любом порядке
    assert (await job_of(uow, job_id)).status == "running"
    await bulk.run_chunk(job_id, 0)

    assert (await job_of(uow, job_id)).status == "done"
    assert await bulk.finalize_if_complete(job_id) is None  # уже завершена
    count = await uow.session.scalar(
        select(func.count()).select_from(AuditEntry).where(AuditEntry.entity_id == str(job_id))
    )
    assert count == 1
