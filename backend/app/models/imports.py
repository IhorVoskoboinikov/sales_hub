"""Импорт контактов из CSV (разделы 3 и 6): рабочая таблица шагов и ошибки строк.

contacts_staging — проверенные и нормализованные строки файла одной job: шаг parse кладёт их
COPY-ем, stage проверяет SQL-ом (дубли в файле, ответственный), merge переносит чанками в
contacts. Таблица обычная (logged) и общая: любой воркер видит данные любого шага, а повтор
задачи продолжает с последнего завершённого (bulk_jobs.stage). Строки job удаляются в той же
транзакции, что её финальный переход.

email в staging — text, а не citext: у asyncpg нет бинарного кодека для типа из расширения, и
COPY упал бы. Поэтому дубли в файле и сопоставление с результатом merge сравниваются по
lower(email). Пустая ячейка — NULL: при обновлении существующего контакта она поле не трогает.

import_errors — ошибки строк: у строки одна, первая ошибка (PK job_id, row_no). raw — ячейки
строки файла (у duplicate_outside_scope — без данных существующего контакта: его не видно).
Отдаёт GET /bulk-jobs/{job_id}/errors (keyset по row_no).
"""

import uuid
from typing import Any

from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

# Колонки CSV импорта (раздел 6), имена без учёта регистра. Остальные игнорируются — в том числе
# id, created_at, company_id из нашего экспорта, поэтому свой экспорт загружается обратно.
IMPORT_COLUMNS = (
    "email",
    "first_name",
    "last_name",
    "phone",
    "status",
    "source",
    "tags",
    "owner_email",
)
REQUIRED_IMPORT_COLUMNS = ("email", "first_name")


class ContactStaging(Base):
    __tablename__ = "contacts_staging"

    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("bulk_jobs.id", ondelete="CASCADE"), primary_key=True
    )
    row_no: Mapped[int] = mapped_column(Integer, primary_key=True)  # номер строки файла
    email: Mapped[str] = mapped_column(Text)  # уже нормализован, сравнение — по lower(email)
    first_name: Mapped[str] = mapped_column(String(100))
    last_name: Mapped[str | None] = mapped_column(String(100))
    phone: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str | None] = mapped_column(String(16))  # NULL — не указан, не меняется
    source: Mapped[str | None] = mapped_column(String(64))
    tags: Mapped[list[str] | None] = mapped_column(ARRAY(String(64)))
    owner_email: Mapped[str | None] = mapped_column(Text)
    # Ответственный строки: из owner_email или default_owner_id job — ставит шаг stage.
    owner_id: Mapped[uuid.UUID | None]
    error: Mapped[str | None] = mapped_column(String(64))  # код ошибки шага stage или merge
    merged: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))


class ImportRowError(Base):
    """Ошибка строки импорта (имя не ImportError: так называется встроенное исключение)."""

    __tablename__ = "import_errors"

    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("bulk_jobs.id", ondelete="CASCADE"), primary_key=True
    )
    row_no: Mapped[int] = mapped_column(Integer, primary_key=True)
    # validation, email_required, duplicate_in_file, owner_invalid, duplicate_outside_scope…
    code: Mapped[str] = mapped_column(String(64))
    field: Mapped[str | None] = mapped_column(String(64))  # колонка, если ошибка в одном поле
    raw: Mapped[Any | None] = mapped_column(JSONB)  # ячейки строки файла
