"""Кэш пользователя в Redis на 60 секунд: access-токен проверяется на каждом запросе,
и без кэша каждый запрос ходил бы в Postgres за одной и той же строкой users."""

import uuid

import structlog
from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = structlog.get_logger("user_cache")

TTL_SECONDS = 60
# Сколько живёт «надгробие» после invalidate (см. ниже).
TOMBSTONE_SECONDS = 5


class CachedUser(BaseModel):
    """То, что нужно для проверки access-токена. Без пароля и прочего лишнего."""

    id: uuid.UUID
    email: str
    is_active: bool
    is_superadmin: bool
    email_verified: bool
    token_version: int
    # Права: роль уходит в OPA, организация и отдел — в Access (раздел 5).
    org_id: uuid.UUID | None
    team_id: uuid.UUID | None
    role: str | None


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
        if not raw:
            return None
        try:
            return CachedUser.model_validate_json(raw)
        except ValidationError:
            # Запись старого формата (после выкатки с новыми полями) — как промах.
            return None

    async def set(self, user: CachedUser) -> None:
        # NX — только если ключа нет. Ключ есть, если запись уже свежая или стоит «надгробие».
        try:
            await self.redis.set(
                self._key(user.id), user.model_dump_json(), ex=TTL_SECONDS, nx=True
            )
        except RedisError as exc:
            logger.warning("user_cache_unavailable", error=repr(exc))

    async def invalidate(self, user_id: uuid.UUID) -> None:
        """Вызывать после коммита любого изменения, которое влияет на проверку токена
        (token_version, is_active), — иначе старое значение проживёт ещё до 60 секунд.

        Ключ не удаляется, а заменяется пустым «надгробием» на 5 секунд. Зачем: запрос,
        который прочитал пользователя из базы до коммита, может положить в кэш старую версию
        уже после удаления ключа — и она прожила бы 60 секунд. С «надгробием» его SET NX
        не сработает, а get() считает пустое значение промахом и читает базу."""
        try:
            await self.redis.set(self._key(user_id), "", ex=TOMBSTONE_SECONDS)
        except RedisError as exc:
            logger.warning("user_cache_unavailable", error=repr(exc))
