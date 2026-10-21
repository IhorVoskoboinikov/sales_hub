"""Активности и заметки (раздел 3).

activities — самая большая таблица (20 млн строк), поэтому партиционирована по месяцам:
PARTITION BY RANGE (created_at), партиции activities_pYYYY_MM. Ключ партиционирования обязан
входить в первичный ключ — поэтому PK (id, created_at). Запрос с условием на created_at
читает только нужные месяцы (partition pruning); без него — индексы всех партиций.

Запланированная активность — с due_at, done_at пуст, пока её не отметят выполненной; без
due_at — запись о том, что уже было (звонок, письмо), done_at = created_at сразу. Поэтому
«открытые задачи» — ровно done_at IS NULL, и частичные индексы задач маленькие.
"""

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

OPEN = "done_at IS NULL"


class ActivityType(StrEnum):
    CALL = "call"
    MEETING = "meeting"
    EMAIL = "email"
    TASK = "task"


class Activity(Base):
    __tablename__ = "activities"
    __table_args__ = (
        CheckConstraint("type IN ('call', 'meeting', 'email', 'task')", name="type_valid"),
        # Открытая активность всегда со сроком: иначе её не найти в списке задач.
        CheckConstraint("due_at IS NOT NULL OR done_at IS NOT NULL", name="open_has_due"),
        # Открытые задачи по уровням Access, по сроку: «на сегодня», просроченные.
        *(
            Index(f"ix_activities_{col}_due_at", col, "due_at", "id", postgresql_where=text(OPEN))
            for col in ("org_id", "team_id", "owner_id")
        ),
        # История карточки сделки и контакта, новые сначала.
        Index("ix_activities_deal_id", "deal_id", "created_at", "id"),
        Index("ix_activities_contact_id", "contact_id", "created_at", "id"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, server_default=func.now()
    )
    # Без FK: 20 млн строк, а org/team/owner копируются сервисом (раздел 3, «Внешние ключи»).
    org_id: Mapped[uuid.UUID]
    team_id: Mapped[uuid.UUID]
    owner_id: Mapped[uuid.UUID]
    deal_id: Mapped[int | None] = mapped_column(ForeignKey("deals.id", ondelete="CASCADE"))
    contact_id: Mapped[int | None] = mapped_column(ForeignKey("contacts.id", ondelete="SET NULL"))
    type: Mapped[str] = mapped_column(String(16))
    subject: Mapped[str] = mapped_column(String(300))
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012


class Note(Base):
    """Заметка к контакту, компании или сделке. Родитель полиморфный (entity_type + entity_id),
    поэтому FK на него нет: заметки удаляет trash.purge вместе с родителем (этап 5)."""

    __tablename__ = "notes"
    __table_args__ = (
        CheckConstraint("entity_type IN ('contact', 'company', 'deal')", name="entity_type_valid"),
        Index("ix_notes_entity", "entity_type", "entity_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organizations.id"))
    entity_type: Mapped[str] = mapped_column(String(16))
    entity_id: Mapped[int] = mapped_column(BigInteger)
    author_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))
    body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012
