"""Отправленные уведомления: защита от повторной отправки писем и сообщений (раздел 15,
«Идемпотентность и защита от дублей»).

Celery доставляет задачу «как минимум один раз», relay тоже может опубликовать событие дважды.
Отправитель в одной транзакции вставляет ключ дедупа и отправляет: ключ уже есть — значит, уже
отправлено. Ошибка отправки откатывает вставку — ключ исчезает, и ретрай отправит снова.
"""

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import CheckConstraint, DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class NotificationChannel(StrEnum):
    EMAIL = "email"
    TELEGRAM = "telegram"  # с шага 5.12


class SentNotification(Base):
    __tablename__ = "sent_notifications"
    __table_args__ = (CheckConstraint("channel IN ('email', 'telegram')", name="channel"),)

    # Формат ключей — app/notifications/keys.py: email:{event_id}, record.assigned:… и т. д.
    dedup_key: Mapped[str] = mapped_column(Text, primary_key=True)
    channel: Mapped[str] = mapped_column(String(16))
    # Без внешнего ключа: у приглашения пользователя ещё нет, а строка переживает удаление.
    user_id: Mapped[uuid.UUID | None]
    # Индекс — для очистки: maintenance.cleanup удаляет строки старше 30 дней (шаг 5.8).
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
