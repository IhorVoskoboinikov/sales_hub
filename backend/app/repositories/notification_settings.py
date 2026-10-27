import uuid
from collections.abc import Collection, Iterable

from sqlalchemy import delete, insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import NotificationSetting

# (событие, канал) → enabled — строки одного пользователя.
UserOverrides = dict[tuple[str, str], bool]

# Ключ — 64-битный хэш строки, как у остальных advisory lock приложения (app/db/locks.py).
REPLACE_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))")


class NotificationSettingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def for_user(self, user_id: uuid.UUID) -> UserOverrides:
        return (await self.for_users([user_id])).get(user_id, {})

    async def for_users(self, user_ids: Collection[uuid.UUID]) -> dict[uuid.UUID, UserOverrides]:
        """Отличия от умолчаний у нескольких получателей одним запросом (notifier)."""
        found: dict[uuid.UUID, UserOverrides] = {}
        if not user_ids:
            return found
        rows = await self.session.execute(
            select(
                NotificationSetting.user_id,
                NotificationSetting.event_type,
                NotificationSetting.channel,
                NotificationSetting.enabled,
            ).where(NotificationSetting.user_id.in_(list(user_ids)))
        )
        for user_id, event_type, channel, enabled in rows:
            found.setdefault(user_id, {})[(event_type, channel)] = enabled
        return found

    async def replace(self, user_id: uuid.UUID, rows: Iterable[tuple[str, str, bool]]) -> None:
        """Полная замена: удалить строки пользователя и вставить новые (только отличия от
        умолчаний — их отбирает сервис). Замены одного пользователя идут по очереди — под
        pg_advisory_xact_lock до конца транзакции (коммит — за сервисом), и побеждает последний
        PUT целиком. Без блокировки DELETE второго PUT не видел бы строк, вставленных первым до
        его коммита: остались бы пары, которых последний PUT не передавал, а вставки пар в
        разном порядке давали бы deadlock. Строку users не блокируем: её держат вход и выдача
        токенов (раздел 4), а настройкам нужна только очередь своих PUT."""
        await self.session.execute(REPLACE_LOCK, {"key": f"notification_settings:{user_id}"})
        await self.session.execute(
            delete(NotificationSetting).where(NotificationSetting.user_id == user_id)
        )
        values = [
            {"user_id": user_id, "event_type": event_type, "channel": channel, "enabled": enabled}
            for event_type, channel, enabled in rows
        ]
        if values:
            await self.session.execute(insert(NotificationSetting).values(values))
