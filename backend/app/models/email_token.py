import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, String, func, text
from sqlalchemy.dialects.postgresql import CITEXT, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class EmailTokenPurpose(StrEnum):
    INVITE = "invite"
    RESET_PASSWORD = "reset_password"


class EmailToken(Base):
    """Одноразовый токен из письма: приглашение или сброс пароля.

    В письмо уходит случайная строка, здесь — только её SHA-256 хэш.
    """

    __tablename__ = "email_tokens"
    __table_args__ = (
        CheckConstraint(
            "purpose IN ('invite', 'reset_password')",
            name="purpose",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # У приглашения пользователя ещё нет — только email.
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    email: Mapped[str] = mapped_column(CITEXT, index=True)
    purpose: Mapped[str] = mapped_column(String(32))
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    # Для приглашения с этапа 3: организация, отдел и роль.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # Использован или погашен новым токеном той же цели.
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
