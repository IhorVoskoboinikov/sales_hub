"""Кэш пользователя: устаревшая версия не возвращается в кэш после invalidate."""

import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from app.core.config import Settings
from app.repositories.user_cache import CachedUser, UserCache


@pytest.fixture
async def cache(settings: Settings) -> AsyncIterator[UserCache]:
    redis = Redis.from_url(settings.redis_url)
    yield UserCache(redis)
    await redis.aclose()


def snapshot(user_id: uuid.UUID, version: int) -> CachedUser:
    return CachedUser(
        id=user_id,
        email="ivan@example.com",
        is_active=True,
        is_superadmin=False,
        email_verified=True,
        token_version=version,
    )


async def test_roundtrip(cache: UserCache) -> None:
    user_id = uuid.uuid4()

    await cache.set(snapshot(user_id, 1))

    assert await cache.get(user_id) == snapshot(user_id, 1)


async def test_stale_write_after_invalidate_is_ignored(cache: UserCache) -> None:
    # Запрос прочитал версию 1 из базы до коммита logout-all, а положить в кэш успел после.
    user_id = uuid.uuid4()
    await cache.set(snapshot(user_id, 1))

    await cache.invalidate(user_id)  # logout-all закоммитил версию 2
    await cache.set(snapshot(user_id, 1))  # опоздавшая запись старой версии

    assert await cache.get(user_id) is None  # промах → следующий запрос прочитает базу
