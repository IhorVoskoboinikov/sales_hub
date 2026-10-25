"""Массовые операции (раздел 6): bulk job, её чанки и set-based SQL самих чанков.

Выборка каждой операции — одна функция (reassign_owner_filter, move_team_filter…): по ней plan
режет строки на чанки, и тем же условием чанк обновляет свой диапазон id. Поэтому повтор чанка
ничего не находит (owner_id = :from или team_id <> :team уже не выполняются), а записи, которые
вышли из выборки после plan, чанк не трогает. Снимок plan — это диапазоны id, а не список строк:
запись, которая вошла в выборку после plan (восстановлена из корзины, передана прежнему
ответственному, снова открыта) и чей id попал в диапазон ещё не выполненного чанка, тоже
обновится. Поэтому total растёт вместе с processed (add_progress) — прогресс не больше 100 %.

Переходы статуса job — условными UPDATE … WHERE status IN (…): колонки version у job нет, а два
чанка, закончившие одновременно, не должны оба завершить job (finalize).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any, cast

from sqlalchemy import (
    BindParameter,
    ColumnElement,
    CursorResult,
    Uuid,
    and_,
    bindparam,
    case,
    false,
    func,
    insert,
    literal_column,
    or_,
    select,
    text,
    true,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import String

from app.authz.access import Access
from app.errors import NotFound
from app.models import (
    Activity,
    BulkEntity,
    BulkJob,
    BulkJobChunk,
    BulkJobKind,
    BulkJobStatus,
    ChunkStatus,
    Company,
    Contact,
    Deal,
)
from app.models.bulk import ACTIVE_JOB

ENTITY_MODELS: dict[BulkEntity, Any] = {
    BulkEntity.COMPANIES: Company,
    BulkEntity.CONTACTS: Contact,
    BulkEntity.DEALS: Deal,
    BulkEntity.ACTIVITIES: Activity,
}


def transferable(model: Any) -> ColumnElement[bool]:
    """Записи, которые переходят вместе с человеком (раздел 6): живые компании и контакты,
    открытые живые сделки, открытые активности. Закрытые сделки и выполненные активности — это
    история, корзина не передаётся. Предикаты — литералами, как у частичных индексов (раздел 3):
    в общем плане asyncpg параметр $1 не дал бы Postgres выбрать частичный индекс."""
    if model is Activity:
        return Activity.done_at.is_(None)
    if model is Deal:
        return and_(Deal.status == literal_column("'open'"), Deal.deleted_at.is_(None))
    return cast(ColumnElement[bool], model.deleted_at.is_(None))


def reassign_owner_filter(
    model: Any, from_user_id: uuid.UUID, access: Access
) -> ColumnElement[bool]:
    """Выборка reassign_owner — одна для plan и для чанка: записи прежнего ответственного в
    уровне автора (bulk:run), только живые и открытые."""
    return and_(model.owner_id == from_user_id, access.sql_filter(model), transferable(model))


def visible_jobs(access: Access) -> ColumnElement[bool]:
    """Job в уровне bulk_job:read — и всегда свои. team_id job — отдел автора на момент запуска:
    руководитель, которого перевели в другой отдел, иначе перестал бы видеть свои job, а повтор
    по Idempotency-Key (по owner_id) всё равно возвращал бы их."""
    own = and_(BulkJob.org_id == access.org_id, BulkJob.owner_id == access.user_id)
    return or_(access.sql_filter(BulkJob), own)


def move_team_filter(
    model: Any, user_id: uuid.UUID, team_id: uuid.UUID | None
) -> ColumnElement[bool]:
    """Выборка move_team (раздел 6, «Смена отдела человека»): живые и открытые записи человека,
    чей team_id отстал от его отдела team_id. Одна для plan, чанка и проверки в сервисе людей
    (LiveRecords.exist_outside_team). Отдела у человека нет (стал org_admin или viewer) —
    догонять нечего: сравнение с NULL в SQLAlchemy дало бы IS NOT NULL, поэтому — явное false."""
    if team_id is None:
        return false()
    return and_(model.owner_id == user_id, model.team_id != team_id, transferable(model))


# Job ещё идёт (в частичном UNIQUE «одна job вида» те же статусы плюс pending).
ACTIVE_STATUSES = (BulkJobStatus.RUNNING, BulkJobStatus.CANCELLING)
STUCK_ERROR = "bulk.stuck"
IMPORT_KIND = BulkJobKind.IMPORT_CONTACTS


def _older_than(seconds: int) -> ColumnElement[bool]:
    """Нет прогресса дольше seconds: updated_at ставят plan, каждый чанк и сам reaper."""
    return BulkJob.updated_at < func.now() - timedelta(seconds=seconds)


def _has_pending_chunks() -> ColumnElement[bool]:
    return (
        select(BulkJobChunk.no)
        .where(BulkJobChunk.job_id == BulkJob.id, BulkJobChunk.status == ChunkStatus.PENDING)
        .exists()
    )


# Условия bulk.reap_stuck (раздел 15). Правила (1)–(3) — для чанковых job (kind <> import),
# импорт — своё правило: его ведёт одна задача imports.import_contacts, а не чанки.


def stale_pending(seconds: int) -> ColumnElement[bool]:
    """(1) pending дольше seconds — потерялась постановка bulk.plan из API."""
    return and_(
        BulkJob.kind != IMPORT_KIND,
        BulkJob.status == BulkJobStatus.PENDING,
        _older_than(seconds),
    )


def lost_finalize() -> ColumnElement[bool]:
    """(2) running / cancelling без pending-чанков — потерялся finalize (упал между коммитом
    чанка и finalize_if_complete)."""
    return and_(
        BulkJob.kind != IMPORT_KIND,
        BulkJob.status.in_(ACTIVE_STATUSES),
        ~_has_pending_chunks(),
    )


def stalled_chunks(seconds: int) -> ColumnElement[bool]:
    """(3) running / cancelling с pending-чанками и без прогресса seconds — сообщения чанков
    потерялись (жёсткий time_limit, убитый воркер)."""
    return and_(
        BulkJob.kind != IMPORT_KIND,
        BulkJob.status.in_(ACTIVE_STATUSES),
        _older_than(seconds),
        _has_pending_chunks(),
    )


def stalled_import(pending_seconds: int, stall_seconds: int) -> ColumnElement[bool]:
    """Импорт: pending дольше pending_seconds или идёт (и отменяется) без прогресса
    stall_seconds — порог больше лимита запуска задачи (12 минут)."""
    return and_(
        BulkJob.kind == IMPORT_KIND,
        or_(
            and_(BulkJob.status == BulkJobStatus.PENDING, _older_than(pending_seconds)),
            and_(BulkJob.status.in_(ACTIVE_STATUSES), _older_than(stall_seconds)),
        ),
    )


@dataclass(frozen=True)
class ChunkPlan:
    no: int
    entity: BulkEntity
    lo: int
    hi: int


@dataclass(frozen=True)
class FinishedJob:
    """Что вернул завершающий UPDATE: этого хватает для итога и журнала без чтения job."""

    status: str
    kind: str
    org_id: uuid.UUID
    owner_id: uuid.UUID
    params: dict[str, Any]


# finalize_if_complete (раздел 6, шаг 5): один запрос. Два чанка закончили одновременно —
# строку получит только один UPDATE (второй после ожидания блокировки увидит уже не running),
# поэтому итог пишется ровно один раз. chunks_total > 0: у пустой выборки чанков нет, и она done.
FINALIZE = (
    text(
        """
        UPDATE bulk_jobs SET
            status = CASE
                WHEN status = 'cancelling' THEN 'cancelled'
                WHEN error IS NOT NULL THEN 'failed'
                WHEN failed_chunks = 0 AND failed_rows = 0 THEN 'done'
                WHEN chunks_total > 0 AND failed_chunks = chunks_total THEN 'failed'
                ELSE 'partial_failed'
            END,
            finished_at = now()
        WHERE id = :id
          AND kind <> 'import_contacts'
          AND status IN ('running', 'cancelling')
          AND NOT EXISTS (
              SELECT 1 FROM bulk_job_chunks WHERE job_id = :id AND status = 'pending'
          )
        RETURNING status, kind, org_id, owner_id, params
        """
    )
    .bindparams(bindparam("id", type_=Uuid))
    .columns(status=String, kind=String, org_id=Uuid, owner_id=Uuid, params=JSONB)
)


class BulkJobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def add(self, job: BulkJob) -> None:
        self.session.add(job)

    async def get(self, job_id: uuid.UUID) -> BulkJob | None:
        """Без блокировки, но всегда свежая строка: job меняют чанки в других транзакциях."""
        return await self.session.get(BulkJob, job_id, populate_existing=True)

    async def get_visible_or_404(
        self, job_id: uuid.UUID, access: Access, *, for_update: bool = False
    ) -> BulkJob:
        """Job вне уровня bulk_job:read (и не своя) — 404, как несуществующая: owner_id — автор
        job (visible_jobs). for_update — для отмены: переход статуса не обгонит plan, который
        берёт ту же строку FOR UPDATE."""
        query = select(BulkJob).where(BulkJob.id == job_id, visible_jobs(access))
        if for_update:
            query = query.with_for_update()
        job = await self.session.scalar(query.execution_options(populate_existing=True))
        if job is None:
            raise NotFound(entity="bulk_job", id=str(job_id))
        return job

    async def get_pending_for_update(self, job_id: uuid.UUID) -> BulkJob | None:
        """Job, которую ещё не планировали: plan с этим условием повторно ничего не сделает."""
        return await self.session.scalar(
            select(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status == BulkJobStatus.PENDING)
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def find_by_key(self, owner_id: uuid.UUID, key: str) -> BulkJob | None:
        return await self.session.scalar(
            select(BulkJob)
            .where(BulkJob.owner_id == owner_id, BulkJob.idempotency_key == key)
            .execution_options(populate_existing=True)
        )

    async def find_active(self, kind: str, read: Access) -> BulkJob | None:
        """Активная job вида в организации — та, что заняла частичный UNIQUE (то же условие), —
        если она видна мне (read — уровень bulk_job:read). Чужая — None: её id ни к чему тому,
        кто не может её открыть."""
        return await self.session.scalar(
            select(BulkJob)
            .where(
                BulkJob.org_id == read.org_id,
                BulkJob.kind == kind,
                text(ACTIVE_JOB),
                visible_jobs(read),
            )
            .execution_options(populate_existing=True)
        )

    async def has_active(self, org_id: uuid.UUID, kind: str) -> bool:
        """Идёт ли в организации job этого вида (то же условие, что у частичного UNIQUE), чья
        бы она ни была: вторая всё равно упрётся в индекс."""
        found = await self.session.scalar(
            select(BulkJob.id)
            .where(BulkJob.org_id == org_id, BulkJob.kind == kind, text(ACTIVE_JOB))
            .limit(1)
        )
        return found is not None

    async def start(self, job_id: uuid.UUID, *, total: int, chunks_total: int) -> None:
        """plan закончил нарезку: job идёт (running), прогресс считается от total."""
        await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status == BulkJobStatus.PENDING)
            .values(
                status=BulkJobStatus.RUNNING,
                total=total,
                chunks_total=chunks_total,
                started_at=func.now(),
                updated_at=func.now(),
            )
        )

    async def add_progress(self, job_id: uuid.UUID, rows: int) -> None:
        """Последний шаг чанка: прогресс и время прогресса (по нему reaper ищет зависшие).
        total не меньше processed: чанк мог обновить и запись, которая вошла в выборку после
        plan (см. docstring модуля)."""
        processed = BulkJob.processed + rows
        await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id)
            .values(
                processed=processed,
                total=func.greatest(BulkJob.total, processed),
                updated_at=func.now(),
            )
        )

    async def add_failed_chunk(self, job_id: uuid.UUID) -> None:
        await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id)
            .values(failed_chunks=BulkJob.failed_chunks + 1, updated_at=func.now())
        )

    async def set_error(self, job_id: uuid.UUID, code: str) -> None:
        """Причина провала — первая: чанки, которые заметят её позже, код не перепишут."""
        await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.error.is_(None))
            .values(error=code, updated_at=func.now())
        )

    async def finalize(self, job_id: uuid.UUID) -> FinishedJob | None:
        """None — job ещё не закончена или её уже завершил кто-то другой."""
        row = (await self.session.execute(FINALIZE, {"id": job_id})).first()
        if row is None:
            return None
        return FinishedJob(row.status, row.kind, row.org_id, row.owner_id, row.params)

    async def set_result(self, job_id: uuid.UUID, result: dict[str, Any]) -> None:
        await self.session.execute(
            update(BulkJob).where(BulkJob.id == job_id).values(result=result)
        )

    # --- отмена (раздел 15, «Жизненный цикл bulk job»)

    async def cancel_pending(self, job_id: uuid.UUID) -> FinishedJob | None:
        """pending → сразу cancelled: plan с условием status = 'pending' уже ничего не сделает.
        None — job уже не pending."""
        return await self._finish(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status == BulkJobStatus.PENDING)
            .values(status=BulkJobStatus.CANCELLED, finished_at=func.now())
        )

    async def request_cancel(self, job_id: uuid.UUID) -> bool:
        """running → cancelling: следующие чанки станут skipped, finalize поставит cancelled.
        updated_at не трогаем — это время прогресса, по нему reaper ищет зависшие job."""
        result = await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status == BulkJobStatus.RUNNING)
            .values(status=BulkJobStatus.CANCELLING)
        )
        return cast(CursorResult[Any], result).rowcount > 0

    # --- bulk.reap_stuck (раздел 15)

    async def ids_where(self, where: ColumnElement[bool]) -> list[uuid.UUID]:
        """Кандидаты reaper — без блокировок: каждую job он потом берёт lock_where и проверяет
        условие ещё раз."""
        return list(
            await self.session.scalars(select(BulkJob.id).where(where).order_by(BulkJob.created_at))
        )

    async def lock_where(self, job_id: uuid.UUID, where: ColumnElement[bool]) -> BulkJob | None:
        """Строка job FOR UPDATE SKIP LOCKED, если условие reaper всё ещё выполняется. None —
        job занята (другая копия reaper, отмена, чанк пишет прогресс) или уже сдвинулась:
        после коммита первой копии вторая увидит свежий updated_at и ничего не переотправит."""
        return await self.session.scalar(
            select(BulkJob)
            .where(BulkJob.id == job_id, where)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )

    async def mark_reaped(self, job_id: uuid.UUID) -> None:
        """Ещё одна повторная отправка: reaps + 1, и отсчёт «без прогресса» начинается заново."""
        await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id)
            .values(reaps=BulkJob.reaps + 1, updated_at=func.now())
        )

    async def fail_stuck(self, job_id: uuid.UUID) -> FinishedJob | None:
        """Reaper сдался на job без чанков (pending, импорт): сразу failed. Причина — первая:
        если error уже есть, bulk.stuck её не перепишет."""
        return await self._finish(
            update(BulkJob)
            .where(
                BulkJob.id == job_id,
                BulkJob.status.in_((BulkJobStatus.PENDING, *ACTIVE_STATUSES)),
            )
            .values(
                status=BulkJobStatus.FAILED,
                error=func.coalesce(BulkJob.error, STUCK_ERROR),
                finished_at=func.now(),
            )
        )

    # --- импорт CSV (раздел 6, «Поток: импорт контактов из CSV», шаг 3): одна задача на job,
    # её шаги отмечает bulk_jobs.stage; после каждого шага и чанка — updated_at (по нему reaper
    # ищет зависший импорт). Записи шагов — условные (status IN running, cancelling): если
    # отмена pending или reaper завершили job, пока шаг шёл, шаг откатывается целиком — после
    # финала (он удалил staging) ничего не пишется.

    async def start_import(self, job_id: uuid.UUID) -> bool:
        """Задача импорта взялась за job: pending → running. False — job уже не pending
        (отменили или reaper сдался, пока задача её читала)."""
        result = await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status == BulkJobStatus.PENDING)
            .values(status=BulkJobStatus.RUNNING, started_at=func.now(), updated_at=func.now())
        )
        return cast(CursorResult[Any], result).rowcount > 0

    async def finish_import_step(
        self, job_id: uuid.UUID, stage: str, *, failed: int = 0, total: int | None = None
    ) -> bool:
        """Шаг импорта закончен (в его транзакции): stage, новые ошибки строк, после parse —
        total и начальный итог. False — job уже завершена: шаг надо откатить."""
        values: dict[str, Any] = {
            "stage": stage,
            "failed_rows": BulkJob.failed_rows + failed,
            "updated_at": func.now(),
        }
        if total is not None:
            values |= {"total": total, "result": {"created": 0, "updated": 0}}
        result = await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status.in_(ACTIVE_STATUSES))
            .values(**values)
        )
        return cast(CursorResult[Any], result).rowcount > 0

    async def add_import_progress(
        self, job_id: uuid.UUID, *, created: int, updated: int, failed: int
    ) -> bool:
        """Последний запрос чанка merge: прогресс, ошибки и итог created / updated — в той же
        транзакции, что сами строки, поэтому повтор задачи их не посчитает дважды. False —
        job уже завершена: чанк надо откатить."""

        def counter(name: str, add: int) -> ColumnElement[Any]:
            return func.coalesce(BulkJob.result[name].as_integer(), 0) + add

        result = await self.session.execute(
            update(BulkJob)
            .where(BulkJob.id == job_id, BulkJob.status.in_(ACTIVE_STATUSES))
            .values(
                processed=BulkJob.processed + created + updated,
                failed_rows=BulkJob.failed_rows + failed,
                result=func.jsonb_build_object(
                    literal_column("'created'"),
                    counter("created", created),
                    literal_column("'updated'"),
                    counter("updated", updated),
                ),
                updated_at=func.now(),
            )
        )
        return cast(CursorResult[Any], result).rowcount > 0

    async def finish_import(
        self, job_id: uuid.UUID, error: str | None = None
    ) -> FinishedJob | None:
        """Финал импорта — свой условный UPDATE (FINALIZE импорт не трогает): отменяли —
        cancelled; есть причина провала (error или уже записанная) — failed; есть ошибки
        строк — partial_failed; иначе done. None — job уже завершена или не импорт."""
        failed = BulkJob.error.is_not(None) if error is None else true()
        values: dict[str, Any] = {
            "status": case(
                (BulkJob.status == BulkJobStatus.CANCELLING, BulkJobStatus.CANCELLED.value),
                (failed, BulkJobStatus.FAILED.value),
                (BulkJob.failed_rows > 0, BulkJobStatus.PARTIAL_FAILED.value),
                else_=BulkJobStatus.DONE.value,
            ),
            "finished_at": func.now(),
            "updated_at": func.now(),
        }
        if error is not None:
            values["error"] = func.coalesce(BulkJob.error, error)  # причина — первая
        return await self._finish(
            update(BulkJob)
            .where(
                BulkJob.id == job_id,
                BulkJob.kind == IMPORT_KIND,
                BulkJob.status.in_(ACTIVE_STATUSES),
            )
            .values(**values)
        )

    async def _finish(self, statement: Any) -> FinishedJob | None:
        row = (
            await self.session.execute(
                statement.returning(
                    BulkJob.status, BulkJob.kind, BulkJob.org_id, BulkJob.owner_id, BulkJob.params
                )
            )
        ).first()
        if row is None:
            return None
        return FinishedJob(row.status, row.kind, row.org_id, row.owner_id, row.params)


class BulkChunkRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add_all(self, job_id: uuid.UUID, chunks: list[ChunkPlan]) -> None:
        if not chunks:
            return
        await self.session.execute(
            insert(BulkJobChunk),
            [
                {"job_id": job_id, "no": c.no, "entity": c.entity, "lo": c.lo, "hi": c.hi}
                for c in chunks
            ],
        )

    async def claim(self, job_id: uuid.UUID, no: int) -> BulkJobChunk | None:
        """Строка чанка FOR UPDATE, если он ещё не выполнен. None — чанк уже выполнен (или
        пропущен): повторная доставка задачи ничего не делает. Вторая копия той же задачи
        ждёт здесь, пока первая не закончит транзакцию, и потом получит None."""
        return await self.session.scalar(
            select(BulkJobChunk)
            .where(
                BulkJobChunk.job_id == job_id,
                BulkJobChunk.no == no,
                BulkJobChunk.status == ChunkStatus.PENDING,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def finish(
        self,
        job_id: uuid.UUID,
        no: int,
        status: ChunkStatus,
        *,
        rows: int | None = None,
        error: str | None = None,
    ) -> bool:
        """Отметить чанк, если он ещё pending. False — его уже отметил кто-то другой."""
        result = await self.session.execute(
            update(BulkJobChunk)
            .where(
                BulkJobChunk.job_id == job_id,
                BulkJobChunk.no == no,
                BulkJobChunk.status == ChunkStatus.PENDING,
            )
            .values(status=status, rows=rows, error=error, finished_at=datetime.now(UTC))
        )
        return cast(CursorResult[Any], result).rowcount > 0

    async def rows_by_entity(self, job_id: uuid.UUID) -> dict[str, int]:
        """Сколько строк изменили выполненные чанки — по сущностям, для итога и журнала."""
        rows = await self.session.execute(
            select(BulkJobChunk.entity, func.coalesce(func.sum(BulkJobChunk.rows), 0))
            .where(BulkJobChunk.job_id == job_id, BulkJobChunk.status == ChunkStatus.DONE)
            .group_by(BulkJobChunk.entity)
        )
        return {entity: int(total or 0) for entity, total in rows.all()}

    async def pending_numbers(self, job_id: uuid.UUID) -> list[int]:
        """Номера невыполненных чанков — reaper отправляет их снова."""
        return list(
            await self.session.scalars(
                select(BulkJobChunk.no)
                .where(BulkJobChunk.job_id == job_id, BulkJobChunk.status == ChunkStatus.PENDING)
                .order_by(BulkJobChunk.no)
            )
        )

    async def skip_pending(self, job_id: uuid.UUID, reason: str) -> int:
        """Reaper сдался: pending-чанки → skipped. SKIP LOCKED: строку чанка, который сейчас
        выполняется, держит он сам и в конце ждёт строку job — а её держит reaper; без SKIP
        LOCKED они ждали бы друг друга (deadlock). Такой чанк доработает и сам вызовет finalize."""
        pending = (
            select(BulkJobChunk.no)
            .where(BulkJobChunk.job_id == job_id, BulkJobChunk.status == ChunkStatus.PENDING)
            .with_for_update(skip_locked=True)
        )
        result = await self.session.execute(
            update(BulkJobChunk)
            .where(BulkJobChunk.job_id == job_id, BulkJobChunk.no.in_(pending))
            .values(status=ChunkStatus.SKIPPED, error=reason, finished_at=func.now())
            .execution_options(synchronize_session=False)
        )
        return cast(CursorResult[Any], result).rowcount


class BulkRepository:
    """Set-based SQL массовых операций по CRM-таблицам: нарезка на чанки и UPDATE чанка."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def chunk_bounds(
        self, model: Any, where: ColumnElement[bool], size: int
    ) -> tuple[list[tuple[int, int]], int]:
        """Границы чанков по реальным строкам выборки (раздел 6, шаг 3) и число строк.

        Нумеруем строки выборки по id и берём каждую size-ю ((rn - 1) % size = 0; не
        rn % size = 1 — при size = 1 оно не выполняется никогда) — начало чанка —
        плюс последнюю (её id — верхняя граница последнего чанка). В каждом чанке ровно size
        строк выборки (в последнем — остаток), сколько бы чужих id ни лежало между ними: у
        менеджера со 100 тыс. клиентов среди 5 млн равные диапазоны id дали бы сотни почти
        пустых чанков."""
        numbered = (
            select(
                model.id.label("id"),
                func.row_number().over(order_by=model.id).label("rn"),
                func.count().over().label("cnt"),
            )
            .where(where)
            .subquery()
        )
        size_param: BindParameter[int] = bindparam("size", size)
        rows = (
            await self.session.execute(
                select(numbered.c.id, numbered.c.rn, numbered.c.cnt)
                .where(or_((numbered.c.rn - 1) % size_param == 0, numbered.c.rn == numbered.c.cnt))
                .order_by(numbered.c.id)
            )
        ).all()
        if not rows:
            return [], 0
        starts = [row.id for row in rows if (row.rn - 1) % size == 0]
        bounds = [(lo, next_lo - 1) for lo, next_lo in pairwise(starts)]
        bounds.append((starts[-1], rows[-1].id))
        return bounds, int(rows[-1].cnt)

    async def reassign(
        self,
        model: Any,
        lo: int,
        hi: int,
        where: ColumnElement[bool],
        to_user_id: uuid.UUID,
        to_team_id: uuid.UUID,
    ) -> int:
        """UPDATE одного чанка reassign_owner: новый ответственный и его отдел (раздел 6, шаг 4).
        Возвращает число изменённых строк."""
        return await self._update_chunk(
            model, lo, hi, where, {"owner_id": to_user_id, "team_id": to_team_id}
        )

    async def move_team(
        self, model: Any, lo: int, hi: int, where: ColumnElement[bool], team_id: uuid.UUID
    ) -> int:
        """UPDATE одного чанка move_team: записи догоняют текущий отдел своего ответственного."""
        return await self._update_chunk(model, lo, hi, where, {"team_id": team_id})

    async def _update_chunk(
        self, model: Any, lo: int, hi: int, where: ColumnElement[bool], values: dict[str, Any]
    ) -> int:
        """Диапазон id чанка и выборка job. У компаний, контактов и сделок — ещё version + 1
        (открытая карточка получит 409 на PATCH) и updated_at; у активностей этих колонок нет."""
        if model is not Activity:
            values = values | {"version": model.version + 1, "updated_at": func.now()}
        result = await self.session.execute(
            update(model)
            .where(model.id.between(lo, hi), where)
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        return cast(CursorResult[Any], result).rowcount
