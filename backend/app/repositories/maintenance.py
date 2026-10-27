"""Запросы maintenance.cleanup (раздел 15, «Расписание beat»): служебные строки, которые
отслужили своё.

Каждое удаление — чанк: DELETE … WHERE ключ IN (SELECT ключ … LIMIT n). Вызывающий коммитит
чанк и повторяет, пока чанк полный: короткие транзакции не держат блокировки и горизонт VACUUM.
"""

import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any, cast

from sqlalchemy import ColumnElement, CursorResult, Delete, delete, literal, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    FINAL_STATUSES,
    BulkJob,
    BulkJobKind,
    ContactStaging,
    EmailToken,
    ImportRowError,
    OutboxEvent,
    RefreshToken,
    SentNotification,
)

# Таблицы строк job импорта: PK (job_id, row_no), FK на bulk_jobs с CASCADE.
JobRows = type[ImportRowError] | type[ContactStaging]

# Отметка в params job импорта: файл из S3 удалён (сам ключ остаётся — для истории).
FILE_DELETED = "file_deleted_at"


def file_key() -> ColumnElement[str]:
    return cast(ColumnElement[str], BulkJob.params["file_key"].astext)


def imports() -> ColumnElement[bool]:
    return BulkJob.kind == BulkJobKind.IMPORT_CONTACTS.value


def old_job(before: datetime) -> ColumnElement[bool]:
    """Завершённая job, созданная раньше before: незавершённую не трогаем, сколько бы ей ни было."""
    return BulkJob.status.in_([s.value for s in FINAL_STATUSES]) & (BulkJob.created_at < before)


class CleanupRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _delete_chunk(self, key: Any, where: ColumnElement[bool], limit: int) -> int:
        """Удалить до limit строк таблицы ключа key по условию; сколько удалено."""
        chunk = select(key).where(where).limit(limit).scalar_subquery()
        statement: Delete = (
            delete(key.class_).where(key.in_(chunk)).execution_options(synchronize_session=False)
        )
        result = await self.session.execute(statement)
        return cast(CursorResult[Any], result).rowcount

    async def delete_expired_refresh_tokens(self, now: datetime, limit: int) -> int:
        """Истёкшие refresh-токены. Срок у токенов одной сессии общий (не продлевается при
        ротации), поэтому семья уходит целиком; отозванные, но живые остаются — по ним ловится
        повтор украденного токена."""
        return await self._delete_chunk(RefreshToken.id, RefreshToken.expires_at < now, limit)

    async def delete_expired_email_tokens(self, now: datetime, limit: int) -> int:
        return await self._delete_chunk(EmailToken.id, EmailToken.expires_at < now, limit)

    async def delete_published_outbox(self, before: datetime, limit: int) -> int:
        """Опубликованные события старше before. Неопубликованные не удаляются никогда — их
        ещё доставит relay (а застрявшие видны по алерту OutboxLag)."""
        published = OutboxEvent.published_at.is_not(None) & (OutboxEvent.published_at < before)
        return await self._delete_chunk(OutboxEvent.id, published, limit)

    async def delete_sent_notifications(self, before: datetime, limit: int) -> int:
        return await self._delete_chunk(
            SentNotification.dedup_key, SentNotification.sent_at < before, limit
        )

    async def finished_import_files(
        self, before: datetime, limit: int
    ) -> list[tuple[uuid.UUID, str]]:
        """(job_id, ключ файла) импортов, завершённых раньше before, чей файл ещё не удалён."""
        rows = await self.session.execute(
            select(BulkJob.id, file_key())
            .where(
                imports(),
                BulkJob.status.in_([s.value for s in FINAL_STATUSES]),
                BulkJob.finished_at < before,
                BulkJob.params.has_key("file_key"),
                ~BulkJob.params.has_key(FILE_DELETED),
            )
            .order_by(BulkJob.finished_at, BulkJob.id)
            .limit(limit)
        )
        return [(job_id, key) for job_id, key in rows]

    async def mark_files_deleted(self, job_ids: Sequence[uuid.UUID], now: datetime) -> None:
        await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id.in_(job_ids))
            .values(params=BulkJob.params.op("||")(literal({FILE_DELETED: now.isoformat()}, JSONB)))
            .execution_options(synchronize_session=False)
        )

    async def referenced_files(self, keys: Sequence[str]) -> set[str]:
        """Какие из ключей ещё нужны: у них есть job импорта, а файл не отмечен удалённым.
        Остальные — сироты: файл залит, а job не закоммитилась (или её уже нет)."""
        if not keys:
            return set()
        rows = await self.session.scalars(
            select(file_key()).where(
                imports(), file_key().in_(keys), ~BulkJob.params.has_key(FILE_DELETED)
            )
        )
        return set(rows)

    async def old_job_ids(self, before: datetime, limit: int) -> list[uuid.UUID]:
        """Завершённые job старше before, самые старые первыми."""
        rows = await self.session.scalars(
            select(BulkJob.id)
            .where(old_job(before))
            .order_by(BulkJob.created_at, BulkJob.id)
            .limit(limit)
        )
        return list(rows)

    async def delete_job_rows(self, model: JobRows, job_id: uuid.UUID, limit: int) -> int:
        """Чанк строк job в import_errors или contacts_staging (по PK job_id, row_no): у
        импорта их бывает до миллиона, и каскад одним DELETE был бы транзакцией на часы."""
        chunk = (
            select(model.row_no)
            .where(model.job_id == job_id)
            .order_by(model.row_no)
            .limit(limit)
            .scalar_subquery()
        )
        result = await self.session.execute(
            delete(model)
            .where(model.job_id == job_id, model.row_no.in_(chunk))
            .execution_options(synchronize_session=False)
        )
        return cast(CursorResult[Any], result).rowcount

    async def delete_jobs(self, job_ids: Sequence[uuid.UUID], before: datetime) -> int:
        """Сами job (строки staging и ошибок уже удалены); чанки job — каскадом (FK)."""
        if not job_ids:
            return 0
        result = await self.session.execute(
            delete(BulkJob)
            .where(BulkJob.id.in_(job_ids), old_job(before))
            .execution_options(synchronize_session=False)
        )
        return cast(CursorResult[Any], result).rowcount
