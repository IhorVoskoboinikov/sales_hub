"""Привязка Telegram-чата к пользователю CRM (раздел 16, «Привязка Telegram»).

Один чат ↔ один пользователь: user_id — первичный ключ (новый чат заменяет старый), chat_id —
UNIQUE (чат переходит к тому, кто привязал его последним). Строку создаёт бот по /start <token>,
удаляют DELETE /api/v1/me/telegram, /stop (шаг 5.13), бот у отключённого пользователя и отправка
при 403 от Telegram (шаг 5.12).
"""

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TelegramLink(Base):
    __tablename__ = "telegram_links"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    # В личном чате chat_id совпадает с id пользователя Telegram; бот отвечает только в личных.
    chat_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    # @username в Telegram на момент привязки — только чтобы показать в профиле; его может не быть.
    username: Mapped[str | None] = mapped_column(String(64))
    linked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
