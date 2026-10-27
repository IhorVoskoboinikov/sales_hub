"""Обслуживание на настоящих транзакциях (маркер committing), шаг 5.8: trash.purge пропускает
строку, которую держит восстановление (SKIP LOCKED — в одной транзакции теста её не проверить),
восстановление ждёт чанк purge и получает 404, задача beat trash.purge на настоящем воркере
(celery_worker).

Данные коммитятся; после теста фикстура удаляет организацию со всем, что в ней создано.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from celery.worker import WorkController
from sqlalchemy import delete, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.errors import NotFound
from app.models import Contact, Note, Organization, Role, Team, User
from app.services.contacts import ContactService
from app.services.trash import TrashService
from app.uow import UnitOfWork
from app.workers.celery_app import celery_app
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import password_hash

WAIT_SECONDS = 15
OLD = datetime.now(UTC) - timedelta(days=31)


@dataclass(frozen=True)
class CommittedRep:
    org_id: uuid.UUID
    team_id: uuid.UUID
    rep_id: uuid.UUID


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def committed(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[CommittedRep]:
    """Организация с менеджером — закоммиченные; после теста всё удаляется."""
    org_id, team_id, rep_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Purge", slug=f"purge-{org_id.hex[:8]}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Продажі"))
        await s.flush()
        s.add(
            User(
                id=rep_id,
                email=f"rep-{org_id.hex[:8]}@example.com",
                password_hash=await password_hash(),
                org_id=org_id,
                role=Role.SALES_REP,
                team_id=team_id,
            )
        )
        await s.commit()
    try:
        yield CommittedRep(org_id, team_id, rep_id)
    finally:
        async with sessions() as s:
            await s.execute(delete(Note).where(Note.org_id == org_id))
            await s.execute(delete(Contact).where(Contact.org_id == org_id))
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()


async def old_trash_contact(
    sessions: async_sessionmaker[AsyncSession], rep: CommittedRep, *, note: bool = False
) -> int:
    async with sessions() as s:
        contact = Contact(
            org_id=rep.org_id,
            team_id=rep.team_id,
            owner_id=rep.rep_id,
            first_name="Видалений",
            email=f"gone-{uuid.uuid4().hex[:8]}@example.com",
            deleted_at=OLD,
        )
        s.add(contact)
        await s.flush()
        if note:
            s.add(
                Note(
                    org_id=rep.org_id,
                    entity_type="contact",
                    entity_id=contact.id,
                    author_id=rep.rep_id,
                    body="Нотатка",
                )
            )
        await s.commit()
        return contact.id


def rep_access(rep: CommittedRep) -> Access:
    return Access("contact:write", "own", rep.rep_id, rep.org_id, rep.team_id, Role.SALES_REP)


async def alive(sessions: async_sessionmaker[AsyncSession], ids: list[int]) -> set[int]:
    async with sessions() as s:
        return set(await s.scalars(select(Contact.id).where(Contact.id.in_(ids))))


@pytest.mark.committing
async def test_purge_skips_row_locked_by_restore(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedRep
) -> None:
    restoring = await old_trash_contact(sessions, committed)
    forgotten = await old_trash_contact(sessions, committed)

    async with sessions() as restore:
        # Как CrmService.restore: строка корзины FOR UPDATE до коммита, транзакция ещё идёт.
        await UnitOfWork(restore).contacts.get_deleted_or_404(
            restoring, rep_access(committed), for_update=True
        )
        async with sessions() as s:
            # SKIP LOCKED: purge не ждёт восстановления, а пропускает его строку.
            result = await asyncio.wait_for(
                TrashService(UnitOfWork(s), retention_days=30).purge(), timeout=WAIT_SECONDS
            )
        await restore.execute(
            update(Contact).where(Contact.id == restoring).values(deleted_at=None)
        )
        await restore.commit()

    assert result.deleted["contacts"] >= 1
    assert await alive(sessions, [restoring, forgotten]) == {restoring}
    async with sessions() as s:
        assert await s.scalar(select(Contact.deleted_at).where(Contact.id == restoring)) is None


@pytest.mark.committing
async def test_restore_waits_for_purge_chunk_and_gets_404(
    sessions: async_sessionmaker[AsyncSession], committed: CommittedRep
) -> None:
    contact_id = await old_trash_contact(sessions, committed)

    async with sessions() as purge:
        # Как чанк purge: строка под FOR UPDATE, удаление ещё не закоммичено.
        await purge.execute(select(Contact.id).where(Contact.id == contact_id).with_for_update())
        async with sessions() as s:
            service = ContactService(UnitOfWork(s), RoleCatalog.load(POLICIES_DIR))
            restore = asyncio.create_task(service.restore(rep_access(committed), contact_id))
            await asyncio.sleep(0.3)
            assert not restore.done()  # restore ждёт блокировку уже на чтении
            await purge.execute(delete(Contact).where(Contact.id == contact_id))
            await purge.commit()
            # Без FOR UPDATE restore прочитал бы строку и получил 409 на UPDATE … AND version.
            with pytest.raises(NotFound):
                await asyncio.wait_for(restore, timeout=WAIT_SECONDS)


@pytest.mark.worker
@pytest.mark.committing
async def test_purge_task_on_worker(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    committed: CommittedRep,
) -> None:
    contact_id = await old_trash_contact(sessions, committed, note=True)

    celery_app.send_task("trash.purge")  # как beat в 03:00

    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while await alive(sessions, [contact_id]):
        assert asyncio.get_running_loop().time() < deadline, "воркер не выполнил trash.purge"
        await asyncio.sleep(0.2)
    async with sessions() as s:
        notes = await s.scalar(
            text("SELECT count(*) FROM notes WHERE entity_type = 'contact' AND entity_id = :id"),
            {"id": contact_id},
        )
    assert notes == 0
