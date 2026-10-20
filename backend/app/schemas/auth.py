import uuid
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.core.security import MAX_PASSWORD_LENGTH, MIN_PASSWORD_LENGTH
from app.models import Role


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
    org_id: uuid.UUID | None
    team_id: uuid.UUID | None
    role: str | None
    permissions: dict[str, str] = Field(
        default_factory=dict,
        description="Действия роли с уровнями из data.yaml: фронт по ним прячет кнопки. "
        "У супер-админа пусто — ему доступны только /api/v1/admin/*",
        examples=[{"member:read": "team", "org:read": "org"}],
    )
    timezone: str | None = Field(description="None — как у организации")
    email_verified_at: datetime | None
    last_login_at: datetime | None
    created_at: datetime


class RefreshIn(BaseModel):
    refresh_token: str = Field(max_length=256)


class SessionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sid: uuid.UUID = Field(description="Идентификатор сессии — для DELETE /auth/sessions/{sid}")
    ip: str | None
    user_agent: str | None
    started_at: datetime = Field(description="Когда вошли")
    last_refresh_at: datetime = Field(description="Когда последний раз обновляли токены")
    expires_at: datetime = Field(description="Когда сессия закончится сама")
    current: bool = Field(description="Это сессия, из которой сделан запрос")


Password = Annotated[str, Field(min_length=MIN_PASSWORD_LENGTH, max_length=MAX_PASSWORD_LENGTH)]
EmailTokenStr = Annotated[str, Field(max_length=256, description="Токен из письма")]


class InviteIn(BaseModel):
    """Первый админ организации (супер-админ): роль всегда org_admin."""

    email: EmailStr


class MemberInviteIn(BaseModel):
    """Приглашение в свою организацию (админ организации)."""

    email: EmailStr
    role: Role
    team_id: uuid.UUID | None = Field(
        default=None, description="Обязателен для team_lead и sales_rep, у остальных — пусто"
    )


class InviteOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    email: str
    expires_at: datetime = Field(description="До какого момента действует приглашение")


class InviteAcceptIn(BaseModel):
    token: EmailTokenStr
    password: Password


class PasswordForgotIn(BaseModel):
    email: EmailStr


class PasswordResetIn(BaseModel):
    token: EmailTokenStr
    new_password: Password


class PasswordChangeIn(BaseModel):
    old_password: str = Field(max_length=MAX_PASSWORD_LENGTH)
    new_password: Password
