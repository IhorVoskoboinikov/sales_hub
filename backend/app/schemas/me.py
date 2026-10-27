"""Личные настройки пользователя: /api/v1/me/* (раздел 16)."""

from datetime import datetime

from pydantic import BaseModel, Field


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
