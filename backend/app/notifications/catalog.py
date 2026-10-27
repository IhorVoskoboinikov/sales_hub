"""Каталог уведомлений (раздел 16, «События и каналы»): какие пары «событие × канал» бывают и
какие включены по умолчанию.

Это единственный источник правды: notification_settings хранит только отличия от DEFAULTS, а
GET /api/v1/me/notifications показывает все пары ALLOWED. Итог = строка пользователя, иначе
умолчание (enabled). activity.reminder и digest.morning здесь уже есть, хотя пишут их только
шаги 5.13–5.14: настройку можно выключить заранее.
"""

from collections.abc import Mapping
from enum import StrEnum

from app.models import NotificationChannel


class NotificationEvent(StrEnum):
    """События, о которых можно получать уведомления. Не то же, что EventType: напоминание и
    дайджест — задачи beat, в outbox доменными событиями они не попадают."""

    RECORD_ASSIGNED = "record.assigned"
    DEAL_CLOSED = "deal.closed"
    ACTIVITY_REMINDER = "activity.reminder"
    DIGEST_MORNING = "digest.morning"
    BULK_JOB_COMPLETED = "bulk_job.completed"


TELEGRAM, EMAIL = NotificationChannel.TELEGRAM, NotificationChannel.EMAIL

Pair = tuple[NotificationEvent, NotificationChannel]

# Пара → включена ли по умолчанию. Порядок — порядок ответа GET /me/notifications.
DEFAULTS: dict[Pair, bool] = {
    (NotificationEvent.RECORD_ASSIGNED, TELEGRAM): True,
    (NotificationEvent.RECORD_ASSIGNED, EMAIL): True,
    (NotificationEvent.DEAL_CLOSED, TELEGRAM): True,
    (NotificationEvent.DEAL_CLOSED, EMAIL): False,
    (NotificationEvent.ACTIVITY_REMINDER, TELEGRAM): True,
    (NotificationEvent.DIGEST_MORNING, TELEGRAM): True,
    (NotificationEvent.BULK_JOB_COMPLETED, TELEGRAM): True,
    (NotificationEvent.BULK_JOB_COMPLETED, EMAIL): True,
}
# Разрешённые пары — ровно ключи DEFAULTS: у разрешённой пары всегда есть умолчание.
ALLOWED: tuple[Pair, ...] = tuple(DEFAULTS)

# Строки notification_settings пользователя: (событие, канал) → enabled.
Overrides = Mapping[tuple[str, str], bool]


def channels(event: NotificationEvent) -> list[NotificationChannel]:
    """Разрешённые каналы события в порядке каталога (сначала Telegram)."""
    return [channel for ev, channel in ALLOWED if ev == event]


def enabled(overrides: Overrides, event: NotificationEvent, channel: NotificationChannel) -> bool:
    """Итог для пары: строка пользователя, иначе умолчание. Неразрешённая пара — выключена."""
    pair = (event, channel)
    if pair not in DEFAULTS:
        return False
    return overrides.get((event.value, channel.value), DEFAULTS[pair])
