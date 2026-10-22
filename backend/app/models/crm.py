"""CRM-сущности (раздел 3): компании-клиенты и контакты.

У каждой строки org_id (тенант), team_id (отдел) и owner_id (ответственный) — по ним Access
решает, кто что видит (раздел 5). team_id — всегда отдел ответственного: его ставит сервис.

Внешние ключи: org_id → organizations и owner_id → users (пользователей не удаляют, только
отключают; при вставке это поиск по первичному ключу). На team_id FK нет: это копия отдела
ответственного, а FK при удалении отдела требовал бы полного индекса по team_id или прохода
по миллионам строк. Удалить отдел с записями не даёт сервис (409 team.has_records).

Удаление — мягкое: deleted_at (корзина на 30 дней). Почти все индексы частичные
WHERE deleted_at IS NULL — удалённые строки их не раздувают.
"""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    func,
    literal_column,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, CITEXT
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

ALIVE = text("deleted_at IS NULL")
# Текст поиска контакта: имя, фамилия и email одной строкой в нижнем регистре.
# first_name и last_name NOT NULL, email может быть NULL — поэтому coalesce только у него.
# Записано так, как Postgres хранит выражение индекса (со скобками и ::text), — тогда
# alembic check сравнивает его с базой без ложных расхождений.
CONTACT_SEARCH = (
    "lower((((first_name::text || ' '::text) || last_name::text) || ' '::text)"
    " || COALESCE(email::text, ''::text))"
)


class ContactStatus(StrEnum):
    LEAD = "lead"
    CUSTOMER = "customer"
    CHURNED = "churned"


class CrmRecord:
    """Общие колонки компаний, контактов и сделок."""

    # sort_order: общие колонки в таблице идут первыми (id, org_id…) и последними (даты).
    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True, sort_order=-4)
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organizations.id"), sort_order=-3)
    team_id: Mapped[uuid.UUID] = mapped_column(sort_order=-2)
    owner_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), sort_order=-1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), sort_order=1
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), sort_order=2
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), sort_order=3)
    # Оптимистическая блокировка: UPDATE … WHERE id = :id AND version = :v. Кто-то успел
    # изменить запись раньше — UPDATE ничего не найдёт, и сервис ответит 409 version_conflict.
    version: Mapped[int] = mapped_column(Integer, server_default=text("1"), sort_order=4)

    # eager_defaults: значения по умолчанию из базы (id, created_at, updated_at) возвращаются
    # тем же INSERT/UPDATE … RETURNING. Иначе SQLAlchemy прочитал бы их отдельным запросом
    # при первом обращении, а в async это ошибка (MissingGreenlet).
    __mapper_args__: ClassVar[dict[str, Any]] = {"version_id_col": version, "eager_defaults": True}


def scope_indexes(table: str, order: tuple[str, ...]) -> tuple[Index, ...]:
    """Индексы под три уровня Access (раздел 5) для списка с keyset-сортировкой order:
    org — вся организация, team — отдел, own — свои записи."""
    return tuple(
        Index(f"ix_{table}_{column}_{order[0]}", column, *order, postgresql_where=ALIVE)
        for column in ("org_id", "team_id", "owner_id")
    )


class Company(CrmRecord, Base):
    """Компания-клиент. Общий справочник: company:read у всех ролей — org."""

    __tablename__ = "companies"
    __table_args__ = (
        # Список компаний сортируется по имени: keyset (name, id).
        Index("ix_companies_org_id_name", "org_id", "name", "id", postgresql_where=ALIVE),
        # Удаление отдела и смена роли человека проверяют, что записей нет (*.has_records);
        # перевод человека ищет записи, которые догонит job move_team.
        Index("ix_companies_team_id", "team_id", postgresql_where=ALIVE),
        Index("ix_companies_owner_id", "owner_id", postgresql_where=ALIVE),
        # Поиск ?q= по подстроке: ILIKE '%acme%' через триграммы (pg_trgm).
        Index(
            "ix_companies_name_trgm",
            "name",
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
            postgresql_where=ALIVE,
        ),
    )

    name: Mapped[str] = mapped_column(String(200))
    domain: Mapped[str | None] = mapped_column(String(253))
    industry: Mapped[str | None] = mapped_column(String(100))
    size: Mapped[int | None] = mapped_column(Integer)  # число сотрудников


class Contact(CrmRecord, Base):
    """Контакт — человек, с которым работает менеджер. Главная «тяжёлая» таблица: 5 млн строк."""

    __tablename__ = "contacts"
    __table_args__ = (
        CheckConstraint("status IN ('lead', 'customer', 'churned')", name="status_valid"),
        *scope_indexes("contacts", ("created_at", "id")),
        # Дедупликация при импорте и 409 contact.duplicate_email. Частичный: email удалённого
        # контакта можно завести заново.
        Index(
            "uq_contacts_org_id_email",
            "org_id",
            "email",
            unique=True,
            postgresql_where=ALIVE,
        ),
        # Контакты компании (?company_id=) и SET NULL при окончательном удалении компании:
        # полный индекс — FK ищет и строки из корзины.
        Index("ix_contacts_company_id", "company_id", "created_at", "id"),
        # Поиск ?q=: одно выражение из имени, фамилии и email — один GIN-индекс на триграммах.
        # Запрос обязан писать то же выражение (CONTACT_SEARCH), иначе индекс не применится.
        Index(
            "ix_contacts_search_trgm",
            literal_column(CONTACT_SEARCH).label("search"),
            postgresql_using="gin",
            postgresql_ops={"search": "gin_trgm_ops"},
            postgresql_where=ALIVE,
        ),
        # Сегмент по тегу: tags @> ARRAY['vip'].
        Index("ix_contacts_tags", "tags", postgresql_using="gin", postgresql_where=ALIVE),
        # Корзина: удалённые, новые сначала.
        Index(
            "ix_contacts_trash",
            "org_id",
            "deleted_at",
            "id",
            postgresql_where=text("deleted_at IS NOT NULL"),
        ),
    )

    company_id: Mapped[int | None] = mapped_column(ForeignKey("companies.id", ondelete="SET NULL"))
    first_name: Mapped[str] = mapped_column(String(100))
    last_name: Mapped[str] = mapped_column(String(100), server_default=text("''"))
    email: Mapped[str | None] = mapped_column(CITEXT)
    phone: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16), server_default=text("'lead'"))
    source: Mapped[str | None] = mapped_column(String(64))
    tags: Mapped[list[str]] = mapped_column(ARRAY(String(64)), server_default=text("'{}'"))
