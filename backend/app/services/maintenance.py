"""Очистка служебных данных — задача beat maintenance.cleanup, ежедневно в 04:00 (раздел 15,
«Расписание beat»).

Шаги по порядку, каждый — в своих транзакциях, чанками. Ошибка шага (база, S3) логируется, а
остальные шаги всё равно выполняются: недоступный S3 не должен копить токены и outbox.

1. истёкшие refresh- и email-токены;
2. опубликованные outbox_events старше 7 дней (неопубликованные — никогда);
3. sent_notifications старше 30 дней: ключ дедупа живёт дольше строки outbox, которую relay
   может опубликовать повторно;
4. файлы S3 завершённых импортов старше 7 дней (job остаётся, в params — отметка);
5. сироты imports/* старше суток: файл залит, а job не закоммитилась (или её уже удалили);
6. bulk_jobs старше 90 дней: сначала ошибки строк и staging каждой job чанками по строкам, потом
   сами job (их чанки — каскадом).

Все шаги идемпотентны: повтор после сбоя продолжает с того, что осталось.
"""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from functools import partial

import structlog

from app.models import ContactStaging, ImportRowError
from app.storage import FileStorage
from app.uow import UnitOfWork

logger = structlog.get_logger("maintenance")

CLEANUP_CHUNK = 1000
JOBS_CHUNK = 100
# Строки ошибок и staging импорта (до миллиона у job) — своими чанками, до удаления самой job.
JOB_ROWS_CHUNK = 10_000
FILES_CHUNK = 100
IMPORTS_PREFIX = "imports/"

OUTBOX_KEEP = timedelta(days=7)
SENT_NOTIFICATIONS_KEEP = timedelta(days=30)
IMPORT_FILES_KEEP = timedelta(days=7)
# Файл заливается до коммита job: моложе суток — может быть, job ещё коммитится.
ORPHAN_AGE = timedelta(days=1)
BULK_JOBS_KEEP = timedelta(days=90)

Step = Callable[[datetime], Awaitable[int]]


class CleanupService:
    def __init__(
        self, uow: UnitOfWork, storage: FileStorage, *, chunk_size: int = CLEANUP_CHUNK
    ) -> None:
        self.uow = uow
        self.repo = uow.cleanup
        self.storage = storage
        self.chunk_size = chunk_size

    def steps(self) -> list[tuple[str, Step]]:
        return [
            ("refresh_tokens", self.expired_refresh_tokens),
            ("email_tokens", self.expired_email_tokens),
            ("outbox_events", self.published_outbox),
            ("sent_notifications", self.sent_notifications),
            ("import_files", self.finished_import_files),
            ("orphan_files", self.orphan_files),
            ("bulk_jobs", self.old_jobs),
        ]

    async def run(self, now: datetime | None = None) -> dict[str, int | None]:
        """Все шаги; итог — сколько удалено на каждом, None — шаг упал (ошибка в логе)."""
        now = now or datetime.now(UTC)
        result: dict[str, int | None] = {}
        for name, step in self.steps():
            try:
                result[name] = await step(now)
            except Exception as exc:
                await self.uow.rollback()
                logger.error("cleanup_step_failed", step=name, error=repr(exc))
                result[name] = None
        logger.info("cleanup_done", **result)
        return result

    async def _drain(self, chunk: Callable[[int], Awaitable[int]], size: int) -> int:
        """Чанк за чанком, каждый со своим коммитом, пока чанк полный."""
        total = 0
        while True:
            deleted = await chunk(size)
            await self.uow.commit()
            total += deleted
            if deleted < size:
                return total

    async def expired_refresh_tokens(self, now: datetime) -> int:
        return await self._drain(
            lambda n: self.repo.delete_expired_refresh_tokens(now, n), self.chunk_size
        )

    async def expired_email_tokens(self, now: datetime) -> int:
        return await self._drain(
            lambda n: self.repo.delete_expired_email_tokens(now, n), self.chunk_size
        )

    async def published_outbox(self, now: datetime) -> int:
        before = now - OUTBOX_KEEP
        return await self._drain(
            lambda n: self.repo.delete_published_outbox(before, n), self.chunk_size
        )

    async def sent_notifications(self, now: datetime) -> int:
        before = now - SENT_NOTIFICATIONS_KEEP
        return await self._drain(
            lambda n: self.repo.delete_sent_notifications(before, n), self.chunk_size
        )

    async def finished_import_files(self, now: datetime) -> int:
        """Файлы S3 завершённых импортов: сначала файлы, потом отметка в job. Упали посреди —
        отметок нет, повтор удалит те же файлы ещё раз (нет файла — не ошибка)."""
        total = 0
        while True:
            files = await self.repo.finished_import_files(now - IMPORT_FILES_KEEP, FILES_CHUNK)
            await self.uow.rollback()  # пока идут запросы к S3, транзакцию не держим
            for _, key in files:
                await self.storage.delete(key)
            if files:
                await self.repo.mark_files_deleted([job_id for job_id, _ in files], now)
                await self.uow.commit()
            total += len(files)
            if len(files) < FILES_CHUNK:
                return total

    async def orphan_files(self, now: datetime) -> int:
        """Файлы imports/* старше суток, которые не нужны ни одной job."""
        deleted = 0
        async for page in self.storage.list_files(IMPORTS_PREFIX):
            old = [f.key for f in page if f.modified_at < now - ORPHAN_AGE]
            referenced = await self.repo.referenced_files(old)
            await self.uow.rollback()
            for key in old:
                if key not in referenced:
                    await self.storage.delete(key)
                    logger.info("orphan_file_deleted", key=key)
                    deleted += 1
        return deleted

    async def old_jobs(self, now: datetime) -> int:
        """Завершённые job старше 90 дней, самые старые первыми. Ошибок строк у импорта бывает
        до миллиона: каскад одним DELETE не уложился бы в лимит задачи, откатывался бы и
        назавтра начинался заново. Поэтому строки job удаляются чанками с коммитом на чанк, а
        job — уже пустыми. Упали посреди — повтор продолжит с оставшихся строк."""
        before = now - BULK_JOBS_KEEP
        total = 0
        while True:
            job_ids = await self.repo.old_job_ids(before, JOBS_CHUNK)
            await self.uow.rollback()
            for job_id in job_ids:
                for model in (ImportRowError, ContactStaging):
                    await self._drain(
                        partial(self.repo.delete_job_rows, model, job_id), JOB_ROWS_CHUNK
                    )
            total += await self.repo.delete_jobs(job_ids, before)
            await self.uow.commit()
            if len(job_ids) < JOBS_CHUNK:
                return total
