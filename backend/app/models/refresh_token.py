import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class RefreshToken(Base):
    """Refresh-токен. Хранится только SHA-256 хэш: утечка таблицы не даёт войти.

    Все токены одной сессии (одного входа) связаны family_id — он же sid в access-токене.
    При ротации старый токен получает revoked_at и replaced_by, новый остаётся в той же семье.
    """

    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), index=True
    )
    family_id: Mapped[uuid.UUID] = mapped_column(index=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Не продлевается при ротации: сессия живёт не дольше REFRESH_TTL_DAYS с момента входа.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Индекс — для FK: удаление истёкших токенов (maintenance.cleanup) ищет ссылки на строку.
    replaced_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("refresh_tokens.id", ondelete="SET NULL"), index=True
    )
    user_agent: Mapped[str | None] = mapped_column(String(255))
    ip: Mapped[str | None] = mapped_column(String(45))  # длина IPv6 в текстовом виде
