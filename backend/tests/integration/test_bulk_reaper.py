"""bulk.reap_stuck на уровне сервиса (раздел 15, «Жизненный цикл bulk job»): правила (1)–(3),
правило импорта, граница BULK_MAX_REAPS и проверка очереди bulk.

Всё в транзакции теста: «время без прогресса» тест задаёт, сдвигая updated_at назад, — now()
внутри транзакции не меняется. TaskQueue — фейк: видно, что reaper поставил и с какими
аргументами. Длину очереди bulk (LLEN в брокере) подменяет FakeQueueLength. Одновременные
проходы reaper на настоящих транзакциях — в test_bulk_worker.py.
"""

import uuid
from datetime import timedelta
from typing import Any

import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select, update

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.models import AuditEntry, BulkJob, BulkJobChunk, User
from app.services.bulk import BulkService, ReapPolicy, ReapReport, ReassignOwnerRequest
from app.uow import UnitOfWork
from app.workers.queue import broker_queue_length
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeJob,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
POLICY = ReapPolicy()  # как в Settings по умолчанию: 120 / 300 / 900 секунд, 3 попытки
MINUTE = 60


class FakeQueueLength:
    """Длина очереди брокера: length — сколько сообщений «ждёт», calls — какие очереди спросили."""

    def __init__(self, length: int = 0) -> None:
        self.length = length
        self.calls: list[str] = []

    async def __call__(self, queue: str) -> int:
        self.calls.append(queue)
        return self.length


@pytest.fixture
def task_queue() -> FakeTaskQueue:
    return FakeTaskQueue()


@pytest.fixture
def bulk(uow: UnitOfWork, task_queue: FakeTaskQueue) -> BulkService:
    return BulkService(uow, CATALOG, task_queue, chunk_size=2)


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


class People:
    def __init__(self, admin: User, leaving: User, heir: User) -> None:
        assert admin.org_id is not None
        self.admin_id, self.org_id = admin.id, admin.org_id
        self.leaving_id, self.heir_id = leaving.id, heir.id
        self.admin, self.leaving = admin, leaving

    def access(self) -> Access:
        return Access("bulk:run", "org", self.admin_id, self.org_id, None, "org_admin")


@pytest.fixture
async def people(make_user: MakeUser, org: OrgFixture) -> People:
    return People(
        await member(make_user, org, "org_admin"),
        await member(make_user, org, "sales_rep"),
        await member(make_user, org, "sales_rep", team=1),
    )


async def planned_job(
    bulk: BulkService, people: People, make_contact: MakeRecord, contacts: int
) -> uuid.UUID:
    """Идущая job reassign_owner: plan нарезал контакты на чанки по 2 строки."""
    for _ in range(contacts):
        await make_contact(people.leaving)
    request = ReassignOwnerRequest(people.leaving_id, people.heir_id, ("contacts",))
    job_id = (await bulk.start_reassign_owner(people.access(), request, None)).id
    await bulk.plan(job_id)
    return job_id


async def age(uow: UnitOfWork, job_id: uuid.UUID, seconds: int, **values: Any) -> None:
    """Последний прогресс seconds назад (и другие поля job)."""
    await uow.session.execute(
        update(BulkJob)
        .where(BulkJob.id == job_id)
        .values(updated_at=func.now() - timedelta(seconds=seconds), **values)
    )
    await uow.commit()


async def job_of(uow: UnitOfWork, job_id: uuid.UUID) -> BulkJob:
    job = await uow.bulk_jobs.get(job_id)
    assert job is not None
    return job


async def chunk_statuses(uow: UnitOfWork, job_id: uuid.UUID) -> list[tuple[str, str | None]]:
    rows = await uow.session.execute(
        select(BulkJobChunk.status, BulkJobChunk.error)
        .where(BulkJobChunk.job_id == job_id)
        .order_by(BulkJobChunk.no)
        .execution_options(populate_existing=True)
    )
    return [(row.status, row.error) for row in rows]


async def audit_of(uow: UnitOfWork, job_id: uuid.UUID) -> list[AuditEntry]:
    return list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == str(job_id)
            )
        )
    )


def sent_for(queue: FakeTaskQueue, job_id: uuid.UUID) -> list[tuple[str, tuple[Any, ...]]]:
    """Что reaper поставил для этой job (в базе могут быть и чужие job других тестов)."""
    return [(name, args) for name, args in queue.enqueued if args[0] == str(job_id)]


async def reap(bulk: BulkService, length: int = 0) -> ReapReport:
    return await bulk.reap_stuck(POLICY, FakeQueueLength(length))


# --- (1) pending дольше 2 минут


@pytest.mark.parametrize("kind", ["reassign_owner", "move_team"])
async def test_stale_pending_job_gets_plan_again(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
    kind: str,
) -> None:
    job_id = (await make_job(people.admin, kind=kind, status="pending")).id
    await age(uow, job_id, 3 * MINUTE)

    report = await reap(bulk)

    assert job_id in report.replanned
    assert sent_for(task_queue, job_id) == [("bulk.plan", (str(job_id),))]
    job = await job_of(uow, job_id)
    assert (job.status, job.reaps) == ("pending", 1)
    # Повторная отправка начинает отсчёт заново: следующий проход сразу же ничего не шлёт.
    await reap(bulk)
    assert len(sent_for(task_queue, job_id)) == 1


async def test_fresh_pending_job_is_left(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
) -> None:
    job_id = (await make_job(people.admin, status="pending")).id
    await age(uow, job_id, MINUTE)

    await reap(bulk)

    assert sent_for(task_queue, job_id) == []
    assert (await job_of(uow, job_id)).reaps == 0


async def test_pending_job_gives_up_after_max_reaps(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
) -> None:
    job_id = (await make_job(people.admin, status="pending")).id
    await age(uow, job_id, 3 * MINUTE, reaps=POLICY.max_reaps)

    report = await reap(bulk)

    assert job_id in report.stuck
    assert sent_for(task_queue, job_id) == []
    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.reaps) == ("failed", "bulk.stuck", POLICY.max_reaps)
    assert job.finished_at is not None
    [entry] = await audit_of(uow, job_id)
    assert (entry.action, entry.changes["status"]) == ("bulk.reassign_owner", "failed")


# --- (2) потерянный finalize


async def test_lost_finalize_is_finished(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    job_id = await planned_job(bulk, people, make_contact, 2)
    # Чанк выполнен, а finalize не случился (воркер упал между коммитом чанка и finalize).
    await uow.session.execute(
        update(BulkJobChunk).where(BulkJobChunk.job_id == job_id).values(status="done", rows=2)
    )
    await uow.commit()

    report = await reap(bulk)

    assert job_id in report.finalized
    job = await job_of(uow, job_id)
    assert (job.status, job.result) == ("done", {"rows": {"contacts": 2}})
    assert len(await audit_of(uow, job_id)) == 1
    # Следующий проход job уже не трогает.
    assert job_id not in (await reap(bulk)).finalized


# --- (3) чанки без прогресса 5 минут


async def test_stalled_chunks_are_sent_again(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
    task_queue: FakeTaskQueue,
) -> None:
    job_id = await planned_job(bulk, people, make_contact, 6)
    await bulk.run_chunk(job_id, 0)
    await age(uow, job_id, 6 * MINUTE)
    task_queue.enqueued.clear()
    length = FakeQueueLength(0)

    report = await bulk.reap_stuck(POLICY, length)

    assert report.resent[job_id] == [1, 2]
    assert sent_for(task_queue, job_id) == [
        ("bulk.chunk", (str(job_id), 1)),
        ("bulk.chunk", (str(job_id), 2)),
    ]
    assert "bulk" in length.calls  # LLEN очереди чанков
    assert (await job_of(uow, job_id)).reaps == 1
    await reap(bulk)  # updated_at свежий — второй раз не шлёт
    assert len(sent_for(task_queue, job_id)) == 2


async def test_busy_bulk_queue_postpones_resend(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
    task_queue: FakeTaskQueue,
) -> None:
    job_id = await planned_job(bulk, people, make_contact, 4)
    await age(uow, job_id, 6 * MINUTE)
    task_queue.enqueued.clear()

    # В очереди bulk сообщения: чанки, скорее всего, ждут за чужой job — не слать и не считать.
    report = await reap(bulk, length=5)

    assert report.queue_busy
    assert sent_for(task_queue, job_id) == []
    assert (await job_of(uow, job_id)).reaps == 0


async def test_recent_progress_is_left(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
    task_queue: FakeTaskQueue,
) -> None:
    job_id = await planned_job(bulk, people, make_contact, 4)
    await age(uow, job_id, 4 * MINUTE)
    task_queue.enqueued.clear()

    report = await reap(bulk)

    assert job_id not in report.resent
    assert sent_for(task_queue, job_id) == []


async def test_stalled_job_gives_up_after_max_reaps(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
    task_queue: FakeTaskQueue,
) -> None:
    job_id = await planned_job(bulk, people, make_contact, 6)
    await bulk.run_chunk(job_id, 0)
    await age(uow, job_id, 6 * MINUTE, reaps=POLICY.max_reaps)
    task_queue.enqueued.clear()

    report = await reap(bulk)

    assert job_id in report.stuck
    assert sent_for(task_queue, job_id) == []
    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.processed) == ("failed", "bulk.stuck", 2)
    assert await chunk_statuses(uow, job_id) == [
        ("done", None),
        ("skipped", "bulk.stuck"),
        ("skipped", "bulk.stuck"),
    ]
    [entry] = await audit_of(uow, job_id)
    assert entry.changes["status"] == "failed"
    assert entry.changes["rows"] == {"contacts": 2}


async def test_stalled_cancelling_job_gives_up_as_cancelled(
    bulk: BulkService, uow: UnitOfWork, people: People, make_contact: MakeRecord
) -> None:
    job_id = await planned_job(bulk, people, make_contact, 4)
    await age(uow, job_id, 6 * MINUTE, reaps=POLICY.max_reaps, status="cancelling")

    await reap(bulk)

    # Отмену уже попросили — финал cancelled (finalize проверяет cancelling первым).
    job = await job_of(uow, job_id)
    assert (job.status, job.error) == ("cancelled", "bulk.stuck")


# --- импорт: своё правило, одна задача на job


async def test_stale_pending_import_is_sent_again(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
) -> None:
    admin = people.admin
    job_id = (await make_job(admin, kind="import_contacts", status="pending", params={})).id
    await age(uow, job_id, 3 * MINUTE)

    report = await reap(bulk)

    assert job_id in report.reimported
    assert sent_for(task_queue, job_id) == [("imports.import_contacts", (str(job_id),))]
    assert (await job_of(uow, job_id)).reaps == 1


@pytest.mark.parametrize("status", ["running", "cancelling"])
async def test_running_import_is_sent_again_only_after_15_minutes(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
    status: str,
) -> None:
    admin = people.admin
    job_id = (await make_job(admin, kind="import_contacts", status=status, params={})).id
    await age(uow, job_id, 10 * MINUTE)  # дольше чанка, но меньше лимита запуска импорта

    await reap(bulk)
    assert sent_for(task_queue, job_id) == []

    await age(uow, job_id, 16 * MINUTE)
    report = await reap(bulk)

    assert job_id in report.reimported
    assert sent_for(task_queue, job_id) == [("imports.import_contacts", (str(job_id),))]


async def test_import_gives_up_after_max_reaps(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
) -> None:
    admin = people.admin
    job_id = (
        await make_job(
            admin,
            kind="import_contacts",
            status="running",
            params={"file_name": "clients.csv"},
            result={"created": 10, "updated": 2},
        )
    ).id
    await age(uow, job_id, 16 * MINUTE, reaps=POLICY.max_reaps)

    report = await reap(bulk)

    assert job_id in report.stuck
    assert sent_for(task_queue, job_id) == []
    job = await job_of(uow, job_id)
    assert (job.status, job.error) == ("failed", "bulk.stuck")
    assert job.result == {"created": 10, "updated": 2}  # итоги импорта reaper не трогает
    [entry] = await audit_of(uow, job_id)
    assert (entry.action, entry.changes) == (
        "bulk.import_contacts",
        {"file_name": "clients.csv", "status": "failed", "rows": {}},
    )


async def test_import_is_never_finalized_by_chunk_rules(
    bulk: BulkService,
    uow: UnitOfWork,
    people: People,
    make_job: MakeJob,
) -> None:
    # У импорта нет чанков: правило (2) «нет pending-чанков» его бы сразу завершило.
    admin = people.admin
    job_id = (await make_job(admin, kind="import_contacts", status="running", params={})).id

    report = await reap(bulk)

    assert job_id not in report.finalized
    assert (await job_of(uow, job_id)).status == "running"


async def test_broker_queue_length_reads_redis_list(celery_broker_url: str) -> None:
    # У транспорта Redis очередь Celery — список с её именем в базе брокера (db 1).
    name = f"tests-queue-{uuid.uuid4().hex[:8]}"
    redis = Redis.from_url(celery_broker_url)
    try:
        assert await broker_queue_length(celery_broker_url, name) == 0
        await redis.rpush(name, "m1", "m2", "m3")
        assert await broker_queue_length(celery_broker_url, name) == 3
    finally:
        await redis.delete(name)
        await redis.aclose()
