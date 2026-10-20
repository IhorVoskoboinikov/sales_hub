"""Два админа одновременно снимают друг друга (настоящие транзакции, мимо общей транзакции
теста). Без блокировки организации оба увидели бы «админов двое» — и не осталось бы ни одного.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.authz.access import Access
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.errors import Conflict
from app.models import Organization, Role, User
from app.repositories.user_cache import UserCache
from app.services.members import MemberChanges, MemberService
from app.uow import UnitOfWork


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


def admin_access(user: User) -> Access:
    assert user.org_id is not None
    return Access("member:manage", "org", user.id, user.org_id, None)


async def test_two_admins_cannot_demote_each_other(
    settings: Settings, sessions: async_sessionmaker[AsyncSession]
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
    demote = MemberChanges(role=Role.VIEWER)
    try:
        async with sessions() as first, sessions() as second:
            svc_a = MemberService(UnitOfWork(first), UserCache(redis))
            svc_b = MemberService(UnitOfWork(second), UserCache(redis))
            # A снимает B и держит блокировку организации, пока не закоммитит…
            await first.get(Organization, org_id, with_for_update=True)
            b_demotes_a = asyncio.create_task(svc_b.change(admin_access(b), a.id, demote))
            await asyncio.sleep(0.5)
            assert not b_demotes_a.done()  # …B ждёт, а не считает админов по старым данным

            await svc_a.change(admin_access(a), b.id, demote)

            with pytest.raises(Conflict) as exc:
                await b_demotes_a
            assert exc.value.code == "member.last_admin"
    finally:
        await redis.aclose()
        async with sessions() as s:
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()
