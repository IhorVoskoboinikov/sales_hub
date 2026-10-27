"""Личные настройки уведомлений: GET / PUT /api/v1/me/notifications (раздел 16, «Настройки»).

В notification_settings — только отличия от умолчаний каталога (app/notifications/catalog.py):
итог пары = строка пользователя, иначе умолчание. Личные настройки в журнал не пишутся.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from app.errors import ValidationFailed
from app.models import NotificationChannel
from app.notifications import catalog
from app.notifications.catalog import NotificationEvent
from app.services.auth import CurrentUser
from app.uow import UnitOfWork


@dataclass(frozen=True)
class ChannelSetting:
    event_type: NotificationEvent
    channel: NotificationChannel
    enabled: bool
    is_default: bool = True


@dataclass(frozen=True)
class NotificationSettings:
    telegram_linked: bool
    channels: list[ChannelSetting]


class NotificationSettingsService:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def get(self, user: CurrentUser) -> NotificationSettings:
        """Все восемь разрешённых пар с итоговым значением и признаком «как по умолчанию»."""
        overrides = await self.uow.notification_settings.for_user(user.id)
        channels = []
        for event, channel in catalog.ALLOWED:
            enabled = catalog.enabled(overrides, event, channel)
            is_default = enabled == catalog.DEFAULTS[(event, channel)]
            channels.append(ChannelSetting(event, channel, enabled, is_default))
        linked = await self.uow.telegram_links.get(user.id) is not None
        return NotificationSettings(linked, channels)

    async def replace(
        self, user: CurrentUser, channels: Sequence[ChannelSetting]
    ) -> NotificationSettings:
        """Полная замена: строки пользователя удаляются, вставляются только отличия от умолчаний
        (пара со значением по умолчанию строки не даёт). Неразрешённая пара (activity.reminder ×
        email) или повтор пары — 422 validation с номером элемента."""
        seen: set[catalog.Pair] = set()
        for index, item in enumerate(channels):
            pair = (item.event_type, item.channel)
            if pair not in catalog.DEFAULTS:
                raise ValidationFailed(field="channels", index=index, reason="not_allowed")
            if pair in seen:
                raise ValidationFailed(field="channels", index=index, reason="duplicate")
            seen.add(pair)
        rows = [
            (item.event_type.value, item.channel.value, item.enabled)
            for item in channels
            if item.enabled != catalog.DEFAULTS[(item.event_type, item.channel)]
        ]
        await self.uow.notification_settings.replace(user.id, rows)
        await self.uow.commit()
        return await self.get(user)
