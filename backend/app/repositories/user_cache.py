"""Кэш пользователя в Redis на 60 секунд: access-токен проверяется на каждом запросе,
и без кэша каждый запрос ходил бы в Postgres за одной и той же строкой users."""

import uuid

import structlog
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = structlog.get_logger("user_cache")

TTL_SECONDS = 60


class CachedUser(BaseModel):
    """То, что нужно для проверки access-токена. Без пароля и прочего лишнего."""

    id: uuid.UUID
    email: str
    is_active: bool
    is_superadmin: bool
    email_verified: bool
    token_version: int


class UserCache:
    """Если Redis недоступен — не падаем: пользователь просто читается из Postgres."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    @staticmethod
    def _key(user_id: uuid.UUID) -> str:
        return f"user:{user_id}"

    async def get(self, user_id: uuid.UUID) -> CachedUser | None:
        try:
            raw = await self.redis.get(self._key(user_id))
        except RedisError as exc:
            logger.warning("user_cache_unavailable", error=repr(exc))
            return None
        return CachedUser.model_validate_json(raw) if raw else None

    async def set(self, user: CachedUser) -> None:
        try:
            await self.redis.set(self._key(user.id), user.model_dump_json(), ex=TTL_SECONDS)
        except RedisError as exc:
            logger.warning("user_cache_unavailable", error=repr(exc))

    async def invalidate(self, user_id: uuid.UUID) -> None:
        """Вызывать после коммита любого изменения, которое влияет на проверку токена
        (token_version, is_active), — иначе старое значение проживёт ещё до 60 секунд."""
        try:
            await self.redis.delete(self._key(user_id))
        except RedisError as exc:
            logger.warning("user_cache_unavailable", error=repr(exc))
