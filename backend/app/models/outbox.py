"""Outbox: события и команды, которые нужно опубликовать после коммита (раздел 6).

Сервис пишет строку в той же транзакции, что и данные: откатилась транзакция — события нет,
закоммитилась — relay (app/events/relay.py) опубликует его, даже если брокер был недоступен.
Триггер outbox_events_notify после каждого INSERT шлёт NOTIFY outbox_new — Postgres доставляет
его только после коммита, и relay просыпается сразу, а не по опросу. Триггер и его функцию
создаёт миграция: Alembic их не видит и не сравнивает.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Identity, Index, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

UNPUBLISHED = "published_at IS NULL"


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        # Очередь relay: неопубликованные по порядку. Опубликованные (их почти все строки)
        # в индекс не попадают — он остаётся крошечным, сколько бы событий ни накопилось.
        Index("ix_outbox_events_unpublished", "id", postgresql_where=text(UNPUBLISHED)),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_type: Mapped[str] = mapped_column(Text)  # email.send, deal.closed … (app/events)
    # Без внешнего ключа: событие переживает удаление организации. None — у супер-админа.
    org_id: Mapped[uuid.UUID | None]
    # Потребитель читает payload из базы по id: в брокер уходит только id события.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    request_id: Mapped[str | None] = mapped_column(String(64))  # связь с логами запроса
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
