"""Массовые операции (разделы 3, 6, 15): bulk job и учёт её чанков.

bulk_jobs — сама job: кто запустил, что делать (params, в них и Access автора), прогресс и итог.
owner_id — автор job: так Access работает для bulk_job:read без изменений (своё / отдел /
организация), а team_id — отдел автора на момент запуска. Колонки version нет: переходы статуса —
условными UPDATE … WHERE status IN (…), их выполняет репозиторий.

bulk_job_chunks — по строке на чанк: повтор чанка идемпотентен (строка уже не pending), прогресс
не считается дважды, а завершение job видно по базе — без chord и result backend (раздел 6).
"""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.imports import IMPORT_COLUMNS


class BulkJobKind(StrEnum):
    REASSIGN_OWNER = "reassign_owner"
    MOVE_TEAM = "move_team"
    IMPORT_CONTACTS = "import_contacts"
    CLOSE_OVERDUE_DEALS = "close_overdue_deals"
    ADD_TAGS = "add_tags"
    REMOVE_TAGS = "remove_tags"


class BulkJobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    DONE = "done"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Финальные статусы: дальше job не меняется, SSE отдаёт end и закрывает поток.
FINAL_STATUSES = frozenset(
    {
        BulkJobStatus.DONE,
        BulkJobStatus.PARTIAL_FAILED,
        BulkJobStatus.FAILED,
        BulkJobStatus.CANCELLED,
    }
)


class ChunkStatus(StrEnum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"  # job отменяют, или автор и получатель больше не подходят


class BulkEntity(StrEnum):
    """Таблица, которую обновляет чанк."""

    COMPANIES = "companies"
    CONTACTS = "contacts"
    DEALS = "deals"
    ACTIVITIES = "activities"


def _in(values: type[StrEnum]) -> str:
    return ", ".join(f"'{v.value}'" for v in values)


# Одна тяжёлая job вида на организацию (раздел 15): вторая получит 409 bulk.already_running.
# cancelling тоже держит слот, иначе старые и новые чанки шли бы параллельно. move_team разных
# людей идут параллельно: их строки не пересекаются.
ACTIVE_JOB = "status IN ('pending', 'running', 'cancelling') AND kind <> 'move_team'"
ACTIVE_JOB_INDEX = "uq_bulk_jobs_org_id_kind_active"
IDEMPOTENCY_CONSTRAINT = "uq_bulk_jobs_owner_id_idempotency_key"


class BulkJob(Base):
    __tablename__ = "bulk_jobs"
    __table_args__ = (
        CheckConstraint(f"kind IN ({_in(BulkJobKind)})", name="kind_valid"),
        CheckConstraint(f"status IN ({_in(BulkJobStatus)})", name="status_valid"),
        # Повтор запроса с тем же Idempotency-Key — та же job (раздел 6). NULL-ключи не мешают
        # друг другу: в UNIQUE Postgres NULL не равен NULL.
        UniqueConstraint("owner_id", "idempotency_key"),
        Index(
            ACTIVE_JOB_INDEX,
            "org_id",
            "kind",
            unique=True,
            postgresql_where=text(ACTIVE_JOB),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organizations.id"))
    team_id: Mapped[uuid.UUID | None]  # отдел автора на момент запуска, без FK
    owner_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"))  # автор job
    kind: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16), server_default=text("'pending'"))
    stage: Mapped[str | None] = mapped_column(String(16))  # последний завершённый шаг импорта
    # Параметры операции и Access автора (access.to_params()): воркер проверяет по ним каждый чанк.
    params: Mapped[dict[str, Any]] = mapped_column(JSONB)
    idempotency_key: Mapped[str | None] = mapped_column(String(64))
    request_hash: Mapped[str | None] = mapped_column(CHAR(64))  # sha256 {kind, body}
    total: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    processed: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    failed_rows: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
    chunks_total: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    failed_chunks: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)  # итоги: строки по сущностям
    error: Mapped[str | None] = mapped_column(String(64))  # код причины провала
    reaps: Mapped[int] = mapped_column(SmallInteger, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Время последнего прогресса: по нему bulk.reap_stuck находит зависшие job (шаг 5.5).
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    @property
    def import_info(self) -> dict[str, Any] | None:
        """Как прочитан файл импорта (раздел 6): разделитель, распознанные колонки в порядке
        файла и проигнорированные. Его отдаёт BulkJobOut; у других видов job — None."""
        if self.kind != BulkJobKind.IMPORT_CONTACTS:
            return None
        columns: list[str] = self.params["columns"]  # заголовок файла целиком, по порядку
        return {
            "delimiter": self.params["delimiter"],
            "columns": [c for c in columns if c in IMPORT_COLUMNS],
            "ignored_columns": [c for c in columns if c and c not in IMPORT_COLUMNS],
        }

    # Значения по умолчанию из базы (status, счётчики, created_at) — тем же INSERT … RETURNING:
    # иначе их пришлось бы дочитывать, а ленивая загрузка в async — ошибка.
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012


class BulkJobChunk(Base):
    __tablename__ = "bulk_job_chunks"
    __table_args__ = (
        CheckConstraint(f"entity IN ({_in(BulkEntity)})", name="entity_valid"),
        CheckConstraint(f"status IN ({_in(ChunkStatus)})", name="status_valid"),
    )

    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("bulk_jobs.id", ondelete="CASCADE"), primary_key=True
    )
    no: Mapped[int] = mapped_column(Integer, primary_key=True)  # номер чанка в job, с 0
    entity: Mapped[str] = mapped_column(String(16))
    lo: Mapped[int] = mapped_column(BigInteger)  # границы id, включительно
    hi: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), server_default=text("'pending'"))
    rows: Mapped[int | None] = mapped_column(Integer)  # сколько строк изменил чанк
    error: Mapped[str | None] = mapped_column(String(64))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
