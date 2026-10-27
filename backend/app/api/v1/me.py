"""Личные настройки (раздел 16): привязка Telegram и каналы уведомлений.

Свой роутер /api/v1/me только с current_member, без OPA (раздел 5): эндпоинты работают с
записями самого пользователя, а действия роли им не нужны. Поэтому маршрутов нет в routes.rego,
а unit-тест проверяет, что на них нет opa_decision.
"""

from fastapi import APIRouter, Depends, status

from app.api.deps import (
    current_member,
    get_notification_settings_service,
    get_telegram_service,
)
from app.api.v1.errors import Responses
from app.schemas.errors import ErrorOut
from app.schemas.me import (
    NotificationChannelOut,
    NotificationSettingsIn,
    NotificationSettingsOut,
    TelegramLinkOut,
    TelegramStatusOut,
)
from app.services.auth import CurrentUser
from app.services.notification_settings import (
    ChannelSetting,
    NotificationSettings,
    NotificationSettingsService,
)
from app.services.telegram import TelegramService

router = APIRouter(prefix="/api/v1/me", tags=["me"])

MEMBER: Responses = {
    401: {"model": ErrorOut, "description": "Нет или истёк access-токен"},
    403: {"model": ErrorOut, "description": "У пользователя нет организации (супер-админ)"},
}
INVALID_CHANNELS: Responses = {
    422: {
        "model": ErrorOut,
        "description": "`validation`: неизвестная или неразрешённая пара, повтор пары "
        "(`details.index` — номер элемента, `details.reason` — `not_allowed` / `duplicate`)",
    }
}
TELEGRAM_DISABLED: Responses = {
    503: {"model": ErrorOut, "description": "Бот не настроен: `telegram.disabled`"}
}


@router.post(
    "/telegram/link",
    status_code=status.HTTP_201_CREATED,
    responses={**MEMBER, **TELEGRAM_DISABLED},
)
async def create_telegram_link(
    user: CurrentUser = Depends(current_member),
    telegram: TelegramService = Depends(get_telegram_service),
) -> TelegramLinkOut:
    """Ссылка на бота для привязки Telegram. Живёт 10 минут (TELEGRAM_LINK_TTL_MINUTES) и
    работает один раз; новый запрос
    гасит предыдущую ссылку. Бот привяжет чат, из которого её открыли: прежний чат этого
    пользователя отвязывается, а чат, привязанный к другому аккаунту, переходит к этому."""
    offer = await telegram.issue_link(user)
    return TelegramLinkOut(url=offer.url, expires_at=offer.expires_at)


@router.get("/telegram", responses=MEMBER)
async def get_telegram(
    user: CurrentUser = Depends(current_member),
    telegram: TelegramService = Depends(get_telegram_service),
) -> TelegramStatusOut:
    """Привязан ли Telegram, к какому аккаунту и когда."""
    found = await telegram.status(user)
    return TelegramStatusOut(
        linked=found.linked, username=found.username, linked_at=found.linked_at
    )


@router.delete("/telegram", status_code=status.HTTP_204_NO_CONTENT, responses=MEMBER)
async def delete_telegram(
    user: CurrentUser = Depends(current_member),
    telegram: TelegramService = Depends(get_telegram_service),
) -> None:
    """Отвязать Telegram: сообщения в него больше не приходят. 204 — и когда привязки не было."""
    await telegram.unlink(user)


def settings_out(found: NotificationSettings) -> NotificationSettingsOut:
    return NotificationSettingsOut(
        telegram_linked=found.telegram_linked,
        channels=[
            NotificationChannelOut(
                event_type=item.event_type,
                channel=item.channel,
                enabled=item.enabled,
                is_default=item.is_default,
            )
            for item in found.channels
        ],
    )


@router.get("/notifications", responses=MEMBER)
async def get_notifications(
    user: CurrentUser = Depends(current_member),
    service: NotificationSettingsService = Depends(get_notification_settings_service),
) -> NotificationSettingsOut:
    """Каналы уведомлений по событиям: все разрешённые пары «событие × канал» с итоговым
    значением. Telegram приходит, только если он привязан (`telegram_linked`)."""
    return settings_out(await service.get(user))


@router.put("/notifications", responses={**MEMBER, **INVALID_CHANNELS})
async def put_notifications(
    body: NotificationSettingsIn,
    user: CurrentUser = Depends(current_member),
    service: NotificationSettingsService = Depends(get_notification_settings_service),
) -> NotificationSettingsOut:
    """Заменить настройки целиком: пары, которых нет в списке, возвращаются к умолчанию. Ответ —
    как у GET."""
    channels = [
        ChannelSetting(item.event_type, item.channel, item.enabled) for item in body.channels
    ]
    return settings_out(await service.replace(user, channels))
