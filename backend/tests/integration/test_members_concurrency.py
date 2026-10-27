"""Гонки в управлении людьми (настоящие транзакции, мимо общей транзакции теста).

Два админа одновременно снимают или отключают друг друга: без блокировки организации оба
увидели бы «админов двое» — и не осталось бы ни одного. Перевод менеджера в другой отдел, пока
ему создают клиента: клиент не остаётся в старом отделе — его догоняет job move_team.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy import delete as delete_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.errors import Conflict
from app.models import AuditEntry, BulkJob, Contact, Organization, Role, Team, User
from app.repositories.user_cache import UserCache
from app.services.bulk import BulkService
from app.services.crm import resolve_owner
from app.services.members import MemberChanges, MemberService
from app.services.organizations import TeamService
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import FakeTaskQueue


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


def admin_access(user: User) -> Access:
    assert user.org_id is not None
    return Access("member:manage", "org", user.id, user.org_id, None)


@pytest.mark.parametrize(
    "change",
    [MemberChanges(role=Role.VIEWER), MemberChanges(is_active=False)],
    ids=["demote", "deactivate"],
)
async def test_two_admins_cannot_remove_each_other(
    settings: Settings, sessions: async_sessionmaker[AsyncSession], change: MemberChanges
) -> None:
    org_id = uuid.uuid4()
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Race", slug=f"race-{org_id.hex[:8]}"))
        await s.flush()
        a, b = (
            User(
                id=uuid.uuid4(),
                email=f"{name}-{org_id.hex[:8]}@example.com",
                password_hash="x",
                org_id=org_id,
                role=Role.ORG_ADMIN,
            )
            for name in ("a", "b")
        )
        s.add_all([a, b])
        await s.commit()
    redis = Redis.from_url(settings.redis_url)
    try:
        async with sessions() as first, sessions() as second:
            svc_a = MemberService(UnitOfWork(first), UserCache(redis), FakeTaskQueue())
            svc_b = MemberService(UnitOfWork(second), UserCache(redis), FakeTaskQueue())
            # A снимает B и держит блокировку организации, пока не закоммитит…
            await first.get(Organization, org_id, with_for_update=True)
            b_demotes_a = asyncio.create_task(svc_b.change(admin_access(b), a.id, change))
            await asyncio.sleep(0.5)
            assert not b_demotes_a.done()  # …B ждёт, а не считает админов по старым данным

            await svc_a.change(admin_access(a), b.id, change)

            with pytest.raises(Conflict) as exc:
                await b_demotes_a
            assert exc.value.code == "member.last_admin"
    finally:
        await redis.aclose()
        async with sessions() as s:
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()


async def test_team_delete_waits_for_member_moving_in(
    settings: Settings, sessions: async_sessionmaker[AsyncSession]
) -> None:
    """Перевод человека в отдел и удаление этого отдела одновременно: без блокировок удаление
    видело пустой отдел, а коммит перевода падал на внешнем ключе — 500."""
    org_id, team_id, user_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Race", slug=f"race-{org_id.hex[:8]}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Нові"))
        s.add(
            User(
                id=user_id,
                email=f"v-{org_id.hex[:8]}@example.com",
                password_hash="x",
                org_id=org_id,
                role=Role.VIEWER,
            )
        )
        await s.commit()
    access = Access("org:manage", "org", uuid.uuid4(), org_id, None)
    try:
        async with sessions() as moving, sessions() as deleting:
            # Перевод проверил отдел (FOR KEY SHARE) и ещё не закоммитил…
            assert await UnitOfWork(moving).teams.get(team_id, org_id, lock="key_share")
            delete = asyncio.create_task(TeamService(UnitOfWork(deleting)).delete(access, team_id))
            await asyncio.sleep(0.5)
            assert not delete.done()  # …удаление ждёт

            user = await moving.get(User, user_id)
            assert user is not None
            user.role, user.team_id = Role.SALES_REP, team_id
            await moving.commit()

            with pytest.raises(Conflict) as exc:
                await delete
            assert exc.value.code == "team.has_members"
    finally:
        async with sessions() as s:
            await s.execute(delete_(User).where(User.org_id == org_id))
            await s.execute(delete_(Team).where(Team.org_id == org_id))
            await s.execute(delete_(Organization).where(Organization.id == org_id))
            await s.commit()


async def test_record_created_during_member_move_follows_to_new_team(
    settings: Settings, sessions: async_sessionmaker[AsyncSession]
) -> None:
    """Менеджеру создают клиента, а админ одновременно переводит его в другой отдел.

    Создание держит FOR SHARE на ответственном, перевод ждёт его и потом видит новую запись в
    старом отделе — создаёт job move_team, и после неё клиент в новом отделе, а не остался в
    старом. И без deadlock: проверка FK контакта на организацию (FOR KEY SHARE) не конфликтует
    с FOR NO KEY UPDATE организации, которую держит перевод."""
    org_id, rep_id = uuid.uuid4(), uuid.uuid4()
    teams = [uuid.uuid4(), uuid.uuid4()]
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Race", slug=f"race-{org_id.hex[:8]}"))
        await s.flush()
        s.add_all(Team(id=t, org_id=org_id, name=f"Відділ {i}") for i, t in enumerate(teams))
        await s.flush()
        s.add(
            User(
                id=rep_id,
                email=f"rep-{org_id.hex[:8]}@example.com",
                password_hash="x",
                org_id=org_id,
                role=Role.SALES_REP,
                team_id=teams[0],
            )
        )
        await s.commit()
    rep_access = Access("contact:write", "own", rep_id, org_id, teams[0], "sales_rep")
    admin = admin_access_for(org_id)
    queue = FakeTaskQueue()
    redis = Redis.from_url(settings.redis_url)
    try:
        async with sessions() as s:  # автор job — настоящий пользователь (FK bulk_jobs)
            s.add(
                User(
                    id=admin.user_id,
                    email=f"admin-{org_id.hex[:8]}@example.com",
                    password_hash="x",
                    org_id=org_id,
                    role=Role.ORG_ADMIN,
                )
            )
            await s.commit()
        async with sessions() as creating, sessions() as moving:
            uow = UnitOfWork(creating)
            owner = await resolve_owner(uow, rep_access, None)  # FOR SHARE на менеджере
            move = asyncio.create_task(
                MemberService(UnitOfWork(moving), UserCache(redis), queue).change(
                    admin, rep_id, MemberChanges(team_given=True, team_id=teams[1])
                )
            )
            await asyncio.sleep(0.5)
            assert not move.done()  # перевод ждёт, пока запись создастся

            contact = Contact(
                org_id=org_id, team_id=owner.team_id, owner_id=rep_id, first_name="Іван"
            )
            uow.contacts.add(contact)
            await uow.commit()
            contact_id = contact.id

            changed = await move
        assert changed.bulk_job_id is not None  # перевод увидел запись в старом отделе
        assert queue.enqueued == [("bulk.plan", (str(changed.bulk_job_id),))]

        async with sessions() as working:  # как воркер: plan и чанки в своих транзакциях
            bulk = BulkService(
                UnitOfWork(working), RoleCatalog.load(POLICIES_DIR), queue, chunk_size=10
            )
            for no in await bulk.plan(changed.bulk_job_id):
                await bulk.run_chunk(changed.bulk_job_id, no)
        async with sessions() as s:
            moved = await s.get(Contact, contact_id)
            job = await s.get(BulkJob, changed.bulk_job_id)
        assert moved is not None and moved.team_id == teams[1]
        assert job is not None and (job.status, job.processed) == ("done", 1)
    finally:
        await redis.aclose()
        async with sessions() as s:
            await s.execute(delete_(BulkJob).where(BulkJob.org_id == org_id))
            await s.execute(delete_(AuditEntry).where(AuditEntry.org_id == org_id))
            await s.execute(delete_(Contact).where(Contact.org_id == org_id))
            await s.execute(delete_(User).where(User.org_id == org_id))
            await s.execute(delete_(Team).where(Team.org_id == org_id))
            await s.execute(delete_(Organization).where(Organization.id == org_id))
            await s.commit()


def admin_access_for(org_id: uuid.UUID) -> Access:
    return Access("member:manage", "org", uuid.uuid4(), org_id, None)
