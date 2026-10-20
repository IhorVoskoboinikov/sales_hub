import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, func, text
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class User(Base):
    """Пользователь. Появляется только из приглашения или командой create-superadmin.

    Организация, отдел и роль добавляются на этапе 3 (раздел 5).
    """

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # citext: Ivan@Mail.com и ivan@mail.com — один и тот же пользователь.
    email: Mapped[str] = mapped_column(CITEXT, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))
    is_superadmin: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Входит в access-токен (claim ver). +1 — и все выданные access-токены перестают работать.
    token_version: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    # None — часовой пояс организации (появится на этапе 3).
    timezone: Mapped[str | None] = mapped_column(String(64))
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
