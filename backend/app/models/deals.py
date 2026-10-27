"""Воронки, этапы и сделки (раздел 3).

Воронка — последовательность этапов продажи («Новий → Кваліфікація → Пропозиція → …»),
её настраивает org_admin. Сделка всегда стоит на этапе своей воронки: составной FK
(stage_id, pipeline_id) → pipeline_stages (id, pipeline_id) не даст поставить этап из чужой
воронки, а pipeline_id сервис берёт из этапа.

Суммы — в USD (amount_usd), рядом то, что ввёл человек, и курс (раздел 3, «Деньги»).
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.models.crm import CrmRecord, scope_indexes, trash_index

# Открытая и живая сделка — предикат частичных индексов канбана и просроченных. В запросах
# пишется тем же литералом (OPEN_ALIVE), а не параметром: иначе в общем плане asyncpg
# Postgres не докажет, что частичный индекс подходит.
OPEN_ALIVE = "status = 'open' AND deleted_at IS NULL"


class DealStatus(StrEnum):
    OPEN = "open"
    WON = "won"
    LOST = "lost"


class Pipeline(Base):
    __tablename__ = "pipelines"
    __table_args__ = (UniqueConstraint("org_id", "name"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organizations.id"))
    name: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Этапы грузятся явно (selectinload); lazy="raise" — случайная ленивая загрузка в async
    # упала бы с MissingGreenlet, а так ошибка понятная. delete-orphan: этап, убранный из
    # списка, удаляется.
    stages: Mapped[list["PipelineStage"]] = relationship(
        order_by="PipelineStage.position", cascade="all, delete-orphan", lazy="raise"
    )

    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012 — так требует SQLAlchemy


class PipelineStage(Base):
    __tablename__ = "pipeline_stages"
    __table_args__ = (
        # Для составного FK из deals: этап и его воронка — одной парой.
        UniqueConstraint("id", "pipeline_id"),
        CheckConstraint("probability BETWEEN 0 AND 100", name="probability_range"),
        Index("ix_pipeline_stages_pipeline_id_position", "pipeline_id", "position"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"))
    name: Mapped[str] = mapped_column(String(100))
    # Порядок этапов. Без UNIQUE: при перестановке промежуточные значения совпадали бы.
    position: Mapped[int] = mapped_column(Integer)
    probability: Mapped[int] = mapped_column(Integer, server_default=text("0"))  # % выигрыша


class Deal(CrmRecord, Base):
    __tablename__ = "deals"
    __table_args__ = (
        CheckConstraint("status IN ('open', 'won', 'lost')", name="status_valid"),
        # closed_at есть ровно у закрытых сделок.
        CheckConstraint("(status = 'open') = (closed_at IS NULL)", name="closed_at_for_closed"),
        ForeignKeyConstraint(
            ["stage_id", "pipeline_id"], ["pipeline_stages.id", "pipeline_stages.pipeline_id"]
        ),
        *scope_indexes("deals", ("created_at", "id")),
        # Канбан: index-only scan — все колонки карточки и фильтра Access лежат в индексе.
        Index(
            "ix_deals_kanban",
            "pipeline_id",
            "stage_id",
            "id",
            postgresql_include=["title", "amount_usd", "owner_id", "team_id", "org_id"],
            postgresql_where=text(OPEN_ALIVE),
        ),
        # Просроченные открытые сделки: список ?overdue=true и bulk «закрыть просроченные».
        Index(
            "ix_deals_overdue",
            "org_id",
            "expected_close_date",
            "id",
            postgresql_where=text(OPEN_ALIVE),
        ),
        # Полные индексы под FK: удаление этапа и окончательное удаление контакта или
        # компании ищут и сделки из корзины. Они же — для фильтров ?stage_id=, ?contact_id=.
        Index("ix_deals_stage_id", "stage_id"),
        Index("ix_deals_contact_id", "contact_id"),
        Index("ix_deals_company_id", "company_id"),
        # Корзина: trash.purge (с этапа 5, миграция — CONCURRENTLY).
        trash_index("deals"),
    )

    contact_id: Mapped[int | None] = mapped_column(ForeignKey("contacts.id", ondelete="SET NULL"))
    company_id: Mapped[int | None] = mapped_column(ForeignKey("companies.id", ondelete="SET NULL"))
    pipeline_id: Mapped[int] = mapped_column(BigInteger)
    stage_id: Mapped[int] = mapped_column(BigInteger)
    title: Mapped[str] = mapped_column(String(200))
    amount_usd: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    amount_original: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    currency_original: Mapped[str] = mapped_column(String(3))
    fx_rate: Mapped[Decimal] = mapped_column(Numeric(14, 6))  # единиц валюты за 1 USD
    fx_date: Mapped[date] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(8), server_default=text("'open'"))
    expected_close_date: Mapped[date | None] = mapped_column(Date)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
