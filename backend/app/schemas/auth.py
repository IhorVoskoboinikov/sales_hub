import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TokenOut(BaseModel):
    """Пара токенов. Формат — как требует OAuth2: с ним работает кнопка Authorize в Swagger."""

    access_token: str
    refresh_token: str = Field(description="Только для POST /auth/refresh; живёт 30 дней")
    token_type: str = "bearer"
    expires_in: int = Field(description="Сколько секунд живёт access_token", examples=[3600])


class MeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    is_superadmin: bool
    timezone: str | None = Field(description="None — как у организации")
    email_verified_at: datetime | None
    last_login_at: datetime | None
    created_at: datetime


class RefreshIn(BaseModel):
    refresh_token: str = Field(max_length=256)
