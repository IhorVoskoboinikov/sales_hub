"""Массовые операции на настоящих транзакциях (маркер committing) и на настоящем воркере.

- два чанка заканчивают одновременно — job завершается ровно один раз, одна строка журнала;
- два прохода bulk.reap_stuck одновременно — одна повторная отправка; job, которую держит
  другая транзакция, reaper пропускает (SKIP LOCKED); сдавшись, он не ждёт строку идущего чанка;
- конец в конец: POST-сценарий через сервис → bulk.plan в Redis → воркер режет на чанки →
  группа bulk.chunk → finalize. Данные коммитятся, после теста фикстура удаляет организацию.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

import pytest
from celery.worker import WorkController
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.models import (
    AuditEntry,
    BulkJob,
    BulkJobChunk,
    Company,
    Contact,
    Organization,
    Role,
    Team,
    User,
)
from app.services.bulk import BulkService, ReapPolicy, ReassignOwnerRequest
from app.uow import UnitOfWork
from app.workers import runtime
from app.workers.celery_app import celery_app
from app.workers.queue import CeleryTaskQueue
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import FakeTaskQueue

CATALOG = RoleCatalog.load(POLICIES_DIR)
WAIT_SECONDS = 30


@dataclass(frozen=True)
class CommittedOrg:
    org_id: uuid.UUID
    team_id: uuid.UUID
    admin_id: uuid.UUID
    leaving_id: uuid.UUID
    heir_id: uuid.UUID

    def admin_access(self) -> Access:
        return Access("bulk:run", "org", self.admin_id, self.org_id, None, Role.ORG_ADMIN)


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def committed(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[CommittedOrg]:
    """Организация с админом, уходящим менеджером и получателем — закоммиченные. После теста
    всё удаляется по org_id: таблицы общие для всего запуска."""
    org_id, team_id = uuid.uuid4(), uuid.uuid4()
    ids = {name: uuid.uuid4() for name in ("admin", "leaving", "heir")}
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Bulk", slug=f"bulk-{org_id.hex[:8]}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Продажі"))
        await s.flush()
        for name, user_id in ids.items():
            role = Role.ORG_ADMIN if name == "admin" else Role.SALES_REP
            s.add(
                User(
                    id=user_id,
                    email=f"{name}-{org_id.hex[:8]}@example.com",
                    password_hash="x",
                    org_id=org_id,
                    role=role,
                    team_id=None if name == "admin" else team_id,
                )
            )
        await s.commit()
    try:
        yield CommittedOrg(org_id, team_id, ids["admin"], ids["leaving"], ids["heir"])
    finally:
        async with sessions() as s:
            await s.execute(delete(AuditEntry).where(AuditEntry.org_id == org_id))
            await s.execute(delete(BulkJob).where(BulkJob.org_id == org_id))  # чанки — каскадом
            await s.execute(delete(Contact).where(Contact.org_id == org_id))
            await s.execute(delete(Company).where(Company.org_id == org_id))
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()


async def add_contacts(
    sessions: async_sessionmaker[AsyncSession], org: CommittedOrg, owner_id: uuid.UUID, n: int
) -> None:
    async with sessions() as s:
        await s.execute(
            insert(Contact),
            [
                {
                    "org_id": org.org_id,
                    "team_id": org.team_id,
                    "owner_id": owner_id,
                    "first_name": f"Клієнт {i}",
                    "email": f"c{i}-{uuid.uuid4().hex[:8]}@example.com",
                }
                for i in range(n)
            ],
        )
        await s.commit()


def service(
    session: AsyncSession, chunk_size: int = 2, queue: FakeTaskQueue | None = None
) -> BulkService:
    return BulkService(
        UnitOfWork(session), CATALOG, queue or FakeTaskQueue(), chunk_size=chunk_size
    )


async def empty_queue(queue: str) -> int:
    return 0  # LLEN очереди bulk: сообщений нет


async def started_job(
    sessions: async_sessionmaker[AsyncSession], org: CommittedOrg, contacts: int
) -> uuid.UUID:
    """Закоммиченная job reassign_owner: до plan (contacts = 0 — только pending) или с чанками."""
    if contacts:
        await add_contacts(sessions, org, org.leaving_id, contacts)
    async with sessions() as s:
        bulk = service(s)
        request = ReassignOwnerRequest(org.leaving_id, org.heir_id, ("contacts",))
        job_id = (await bulk.start_reassign_owner(org.admin_access(), request, None)).id
        if contacts:
            await bulk.plan(job_id)
    return job_id


async def age_job(
    sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID, minutes: int, **values: object
) -> None:
    async with sessions() as s:
        await s.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id)
            .values(updated_at=func.now() - timedelta(minutes=minutes), **values)
        )
        await s.commit()


async def read_job(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> BulkJob:
    async with sessions() as s:
        job = await s.get(BulkJob, job_id)
        assert job is not None
        return job


def plans_of(queues: list[FakeTaskQueue], job_id: uuid.UUID) -> int:
    return sum(
        1
        for queue in queues
        for name, args in queue.enqueued
        if name == "bulk.plan" and args == (str(job_id),)
    )


async def audit_count(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> int:
    async with sessions() as s:
        return (
            await s.scalar(
                select(func.count())
                .select_from(AuditEntry)
                .where(AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == str(job_id))
            )
            or 0
        )


@pytest.mark.committing
async def test_two_chunks_finishing_together_finalize_once(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedOrg
) -> None:
    await add_contacts(sessions, committed, committed.leaving_id, 4)
    async with sessions() as s:
        bulk = service(s)
        request = ReassignOwnerRequest(committed.leaving_id, committed.heir_id, ("contacts",))
        job_id = (await bulk.start_reassign_owner(committed.admin_access(), request, None)).id
        assert await bulk.plan(job_id) == [0, 1]

    async with sessions() as holder, sessions() as first, sessions() as second:
        # Строку job держит третья транзакция: оба чанка выполнят UPDATE своих строк и встанут
        # на последнем шаге (processed += n) — и закончат почти одновременно.
        await holder.execute(select(BulkJob).where(BulkJob.id == job_id).with_for_update())
        chunks = [
            asyncio.create_task(service(first).run_chunk(job_id, 0)),
            asyncio.create_task(service(second).run_chunk(job_id, 1)),
        ]
        await asyncio.sleep(0.5)
        assert not any(task.done() for task in chunks)
        await holder.rollback()
        await asyncio.gather(*chunks)

    async with sessions() as s:
        job = await s.get(BulkJob, job_id)
        assert job is not None
        assert (job.status, job.processed, job.total) == ("done", 4, 4)
    assert await audit_count(sessions, job_id) == 1


@pytest.mark.committing
async def test_concurrent_finalize_has_one_winner(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedOrg
) -> None:
    await add_contacts(sessions, committed, committed.leaving_id, 2)
    async with sessions() as s:
        bulk = service(s)
        request = ReassignOwnerRequest(committed.leaving_id, committed.heir_id, ("contacts",))
        job_id = (await bulk.start_reassign_owner(committed.admin_access(), request, None)).id
        await bulk.plan(job_id)
        # Чанк выполнен, а finalize ещё не вызван (упал между коммитом чанка и finalize).
        await s.execute(
            BulkJobChunk.__table__.update()
            .where(BulkJobChunk.job_id == job_id)
            .values(status="done", rows=0)
        )
        await s.commit()

    async with sessions() as holder, sessions() as first, sessions() as second:
        await holder.execute(select(BulkJob).where(BulkJob.id == job_id).with_for_update())
        racers = [
            asyncio.create_task(service(first).finalize_if_complete(job_id)),
            asyncio.create_task(service(second).finalize_if_complete(job_id)),
        ]
        await asyncio.sleep(0.5)
        assert not any(task.done() for task in racers)  # оба ждут строку job
        await holder.rollback()
        results = await asyncio.gather(*racers)

    assert sorted(results, key=lambda status: status is None) == ["done", None]  # у одного
    assert await audit_count(sessions, job_id) == 1


@pytest.mark.committing
async def test_concurrent_reapers_resend_once(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedOrg
) -> None:
    job_id = await started_job(sessions, committed, 0)
    await age_job(sessions, job_id, 3)  # pending 3 минуты — потерянный bulk.plan
    queues = [FakeTaskQueue(), FakeTaskQueue()]

    # Beat поставил следующий проход, пока медленный ещё идёт: оба видят одну job.
    async with sessions() as first, sessions() as second:
        await asyncio.gather(
            service(first, queue=queues[0]).reap_stuck(ReapPolicy(), empty_queue),
            service(second, queue=queues[1]).reap_stuck(ReapPolicy(), empty_queue),
        )

    assert plans_of(queues, job_id) == 1
    assert (await read_job(sessions, job_id)).reaps == 1


@pytest.mark.committing
async def test_reaper_skips_job_locked_by_another(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedOrg
) -> None:
    job_id = await started_job(sessions, committed, 0)
    await age_job(sessions, job_id, 3)
    queue = FakeTaskQueue()

    async with sessions() as holder:
        # Строку держит другая копия reaper (или отмена): SKIP LOCKED — не ждать, не слать.
        await holder.execute(select(BulkJob).where(BulkJob.id == job_id).with_for_update())
        async with sessions() as s:
            await asyncio.wait_for(
                service(s, queue=queue).reap_stuck(ReapPolicy(), empty_queue), timeout=5
            )
        assert plans_of([queue], job_id) == 0
        await holder.rollback()

    async with sessions() as s:
        await service(s, queue=queue).reap_stuck(ReapPolicy(), empty_queue)
    assert plans_of([queue], job_id) == 1


@pytest.mark.committing
async def test_giving_up_does_not_wait_for_running_chunk(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedOrg
) -> None:
    job_id = await started_job(sessions, committed, 4)  # чанки 0 и 1
    await age_job(sessions, job_id, 6, reaps=3)

    async with sessions() as chunk:
        # Чанк 0 «выполняется»: его строка взята FOR UPDATE, как в run_chunk. Reaper сдаётся и
        # не ждёт её (иначе чанк, дойдя до строки job, ждал бы reaper — deadlock).
        await chunk.execute(
            select(BulkJobChunk)
            .where(BulkJobChunk.job_id == job_id, BulkJobChunk.no == 0)
            .with_for_update()
        )
        async with sessions() as s:
            await asyncio.wait_for(service(s).reap_stuck(ReapPolicy(), empty_queue), timeout=5)
        job = await read_job(sessions, job_id)
        assert (job.status, job.error) == ("running", "bulk.stuck")  # чанк 0 ещё pending
        await chunk.rollback()  # процесс чанка умер, строка осталась pending

    # Живой чанк доработал бы сам и вызвал finalize (error уже есть — failed). Умерший — снова
    # «без прогресса» (set_error обновил updated_at), и проход через 5 минут дожимает: оставшийся
    # чанк skipped, job failed.
    await age_job(sessions, job_id, 6)
    async with sessions() as s:
        await service(s).reap_stuck(ReapPolicy(), empty_queue)
    job = await read_job(sessions, job_id)
    assert (job.status, job.error) == ("failed", "bulk.stuck")
    async with sessions() as s:
        statuses = list(
            await s.scalars(
                select(BulkJobChunk.status)
                .where(BulkJobChunk.job_id == job_id)
                .order_by(BulkJobChunk.no)
            )
        )
    assert statuses == ["skipped", "skipped"]
    assert await audit_count(sessions, job_id) == 1


@pytest.mark.worker
@pytest.mark.committing
async def test_reassign_end_to_end_on_worker(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    committed: CommittedOrg,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Маленькие чанки: несколько сотен контактов — десяток задач bulk.chunk в группе.
    monkeypatch.setattr(
        runtime, "_settings", runtime.current_settings().model_copy(update={"bulk_chunk_size": 50})
    )
    await add_contacts(sessions, committed, committed.leaving_id, 320)
    await add_contacts(sessions, committed, committed.heir_id, 30)  # чужие строки между ними
    await add_contacts(sessions, committed, committed.leaving_id, 30)

    async with sessions() as s:
        bulk = BulkService(UnitOfWork(s), CATALOG, CeleryTaskQueue(celery_app), chunk_size=50)
        request = ReassignOwnerRequest(committed.leaving_id, committed.heir_id)
        job_id = (await bulk.start_reassign_owner(committed.admin_access(), request, "e2e")).id

    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while True:
        async with sessions() as s:
            job = await s.get(BulkJob, job_id)
            assert job is not None
            if job.finished_at is not None:
                break
        assert asyncio.get_running_loop().time() < deadline, f"job не завершилась: {job.status}"
        await asyncio.sleep(0.2)

    assert (job.status, job.total, job.processed, job.chunks_total) == ("done", 350, 350, 7)
    assert job.result == {"rows": {"companies": 0, "contacts": 350, "deals": 0, "activities": 0}}
    async with sessions() as s:
        left = await s.scalar(
            select(func.count())
            .select_from(Contact)
            .where(Contact.owner_id == committed.leaving_id)
        )
        statuses = await s.scalars(
            select(BulkJobChunk.status).where(BulkJobChunk.job_id == job_id).distinct()
        )
        assert left == 0
        assert list(statuses) == ["done"]
    assert await audit_count(sessions, job_id) == 1


@pytest.mark.worker
@pytest.mark.committing
async def test_reaper_on_worker_recovers_lost_plan(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    committed: CommittedOrg,
) -> None:
    # API закоммитил job, а bulk.plan в брокер не попал (фейковая очередь его «потеряла»).
    job_id = await started_job(sessions, committed, 0)
    await add_contacts(sessions, committed, committed.leaving_id, 5)
    await age_job(sessions, job_id, 3)

    celery_app.send_task("bulk.reap_stuck")  # как beat: reaper → bulk.plan → чанки → finalize

    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while (job := await read_job(sessions, job_id)).finished_at is None:
        assert asyncio.get_running_loop().time() < deadline, f"job не завершилась: {job.status}"
        await asyncio.sleep(0.2)
    assert (job.status, job.processed, job.reaps) == ("done", 5, 1)
