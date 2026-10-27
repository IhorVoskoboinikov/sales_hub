"""Уведомления: отправленные (защита от повторной отправки, раздел 15, «Идемпотентность и
защита от дублей») и личные настройки каналов (раздел 16, «Настройки»).

Celery доставляет задачу «как минимум один раз», relay тоже может опубликовать событие дважды.
Отправитель в одной транзакции вставляет ключ дедупа и отправляет: ключ уже есть — значит, уже
отправлено. Ошибка отправки откатывает вставку — ключ исчезает, и ретрай отправит снова.
"""

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, String, Text, func
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


class NotificationSetting(Base):
    """Отличие от умолчания: пара «событие × канал», которую пользователь включил или выключил.
    Умолчания и разрешённые пары — в коде (app/notifications/catalog.py), строк с умолчанием
    здесь нет: поменяли умолчание в коде — оно действует у всех, кто его не трогал."""

    __tablename__ = "notification_settings"
    __table_args__ = (CheckConstraint("channel IN ('email', 'telegram')", name="channel"),)

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    # Без CHECK: список событий — в каталоге кода, а не в схеме.
    event_type: Mapped[str] = mapped_column(String(64), primary_key=True)
    channel: Mapped[str] = mapped_column(String(16), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean)
