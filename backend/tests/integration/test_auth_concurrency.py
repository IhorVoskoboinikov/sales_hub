"""Гонка refresh и logout-all на настоящих транзакциях (мимо общей транзакции теста).

Без блокировки строки users refresh, начатый одновременно с logout-all, выдавал новый
refresh-токен, которого UPDATE в logout-all уже не видел, — и сессия переживала «выйти везде».
Теперь обе операции сначала берут блокировку на пользователя и выполняются по очереди.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from redis.asyncio import Redis
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.jwt import KeySet
from app.db.session import create_engine, create_session_factory
from app.errors import Unauthorized
from app.models import RefreshToken, User
from app.repositories.rate_limiter import RateLimiter
from app.repositories.user_cache import UserCache
from app.services.auth import AuthService, ClientInfo, CurrentUser
from app.services.users import create_superadmin
from app.uow import UnitOfWork
from tests.integration.conftest import PASSWORD

CLIENT = ClientInfo(ip="127.0.0.1", user_agent="test")


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def redis(settings: Settings) -> AsyncIterator[Redis]:
    client = Redis.from_url(settings.redis_url)
    yield client
    await client.aclose()


def service(session: AsyncSession, settings: Settings, redis: Redis) -> AuthService:
    return AuthService(
        UnitOfWork(session),
        KeySet.from_settings(settings),
        UserCache(redis),
        RateLimiter(redis),
        timedelta(days=30),
    )


async def test_refresh_waits_for_logout_all(
    settings: Settings, sessions: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    email = f"race-{uuid.uuid4().hex[:8]}@example.com"
    async with sessions() as s:
        user, _ = await create_superadmin(UnitOfWork(s), email, PASSWORD)
        pair = await service(s, settings, redis).login(email, PASSWORD, CLIENT)
    try:
        async with sessions() as logout_session, sessions() as refresh_session:
            # logout-all взял блокировку пользователя и ещё не закоммитил…
            logout = service(logout_session, settings, redis)
            locked = await logout_session.get(User, user.id, with_for_update=True)
            assert locked is not None

            # …refresh стартует в это же время и должен ждать, а не выдавать токен.
            refresh = asyncio.create_task(
                service(refresh_session, settings, redis).refresh(pair.refresh_token, CLIENT)
            )
            await asyncio.sleep(0.5)
            assert not refresh.done()

            current = CurrentUser(
                id=user.id,
                email=email,
                is_superadmin=True,
                session_id=uuid.uuid4(),
                token_version=user.token_version,
            )
            await logout.logout_all(current)  # отзыв всех токенов и коммит — refresh проснётся

            with pytest.raises(Unauthorized):
                await refresh

        async with sessions() as s:
            alive = await s.scalars(
                select(RefreshToken).where(
                    RefreshToken.user_id == user.id, RefreshToken.revoked_at.is_(None)
                )
            )
            assert alive.all() == []
    finally:
        async with sessions() as s:
            await s.execute(delete(User).where(User.id == user.id))
            await s.commit()
