"""Личные настройки пользователя: /api/v1/me/* (раздел 16)."""

from datetime import datetime

from pydantic import BaseModel, Field

from app.models import NotificationChannel
from app.notifications.catalog import ALLOWED, NotificationEvent


class TelegramLinkOut(BaseModel):
    url: str = Field(
        description="Ссылка на бота с одноразовым токеном: открыть в Telegram и нажать «Старт»",
        examples=["https://t.me/saleshub_bot?start=3q2-7wUn…"],
    )
    expires_at: datetime = Field(description="После этого ссылка не работает — нужна новая")


class TelegramStatusOut(BaseModel):
    linked: bool
    username: str | None = Field(description="@username в Telegram на момент привязки, без @")
    linked_at: datetime | None


class NotificationChannelIn(BaseModel):
    event_type: NotificationEvent
    channel: NotificationChannel
    enabled: bool


class NotificationChannelOut(NotificationChannelIn):
    is_default: bool = Field(description="Значение совпадает с умолчанием (своей строки нет)")


class NotificationSettingsIn(BaseModel):
    channels: list[NotificationChannelIn] = Field(
        description="Все настройки целиком: пары, которых нет в списке, возвращаются к "
        "умолчанию. Пара — только из ответа GET, без повторов",
        max_length=len(ALLOWED),
    )


class NotificationSettingsOut(BaseModel):
    telegram_linked: bool = Field(
        description="Привязан ли Telegram: без него сообщения в Telegram не приходят, даже "
        "если канал включён"
    )
    channels: list[NotificationChannelOut] = Field(description="Все разрешённые пары")
