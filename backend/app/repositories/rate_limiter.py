"""Ограничение частоты запросов на Redis (раздел 4): фиксированное окно.

Ключ — «что и кто» (например, логин этого email с этого IP), значение — счётчик попыток.
Первая попытка создаёт счётчик со сроком жизни окна; когда срок выходит, счётчик исчезает
и попытки снова разрешены.
"""

from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.errors import RateLimited, ServiceUnavailable


@dataclass(frozen=True)
class Limit:
    name: str
    max_attempts: int
    window_seconds: int


# Значения — из раздела 4.
LOGIN = Limit("login", 5, 15 * 60)  # неудачных попыток на email + IP; успешный вход обнуляет
PASSWORD_CHANGE = Limit("password_change", 5, 15 * 60)  # неверных старых паролей на пользователя
PASSWORD_FORGOT = Limit("password_forgot", 3, 60 * 60)  # писем на email
INVITE = Limit(
    "invite", 50, 60 * 60
)  # приглашений; этап 2 — на супер-админа, с этапа 3 — на организацию


class RateLimiter:
    """Если Redis недоступен — 503: без счётчика пароли можно было бы перебирать бесконечно."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def hit(self, limit: Limit, subject: str) -> None:
        """Засчитать попытку; если лимит уже исчерпан — 429.

        Попытка засчитывается до проверки (пароля, email), а не после: INCR атомарен, поэтому
        сто одновременных запросов получат номера 1…100, и пройдут только первые. Проверка
        «сначала GET, потом INCR» пропустила бы их все — каждый увидел бы старый счётчик."""
        count = await self._increment(limit, subject)
        if count > limit.max_attempts:
            raise await self._limited(limit, subject)

    async def reset(self, limit: Limit, subject: str) -> None:
        """Обнулить счётчик — после успеха, если считать нужно только неудачи (вход)."""
        try:
            await self.redis.delete(self._key(limit, subject))
        except RedisError as exc:
            raise ServiceUnavailable(failed=["redis"]) from exc

    @staticmethod
    def _key(limit: Limit, subject: str) -> str:
        return f"rate:{limit.name}:{subject}"

    async def _increment(self, limit: Limit, subject: str) -> int:
        key = self._key(limit, subject)
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.incr(key)
                pipe.expire(key, limit.window_seconds, nx=True)  # срок ставит только первая
                count, _ = await pipe.execute()
        except RedisError as exc:
            raise ServiceUnavailable(failed=["redis"]) from exc
        return int(count)

    async def _limited(self, limit: Limit, subject: str) -> RateLimited:
        try:
            ttl = await self.redis.ttl(self._key(limit, subject))
        except RedisError:
            ttl = limit.window_seconds
        return RateLimited(retry_after=max(int(ttl), 1))
