"""Массовые операции (раздел 6): запуск job из API, plan, чанки и завершение — без chord.

Поток «передать клиентов уволенного менеджера»:
1. API (start_reassign_owner): права и параметры → повтор по Idempotency-Key → строка
   bulk_jobs (pending) → commit → bulk.plan best-effort через TaskQueue → 202.
2. plan: границы чанков по реальным строкам выборки, строки bulk_job_chunks, job → running.
3. run_chunk: одна транзакция на чанк; повтор ничего не делает (строка чанка уже не pending).
4. finalize_if_complete: условный UPDATE завершает job ровно один раз, победитель пишет итог и
   одну строку журнала.

Закрытие просроченных сделок и теги сегмента (шаг 5.7) — те же шаги: start_close_overdue_deals
и start_tags проверяют параметры, дальше общие plan / чанк / finalize; выборка вида — одна
функция (close_overdue_filter, tag_filter) для plan и для чанка.

Смена отдела человека (move_team): job вставляет сервис людей в транзакции перевода
(new_move_team_job), дальше — те же plan, чанки и finalize. Проверки автора у неё нет: это
следствие уже сделанного изменения, а не операция от имени автора.

Задачи Celery (app/workers/tasks/bulk.py) — тонкие обёртки над этими методами.
"""

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import structlog
from sqlalchemy import ColumnElement, and_
from sqlalchemy.exc import IntegrityError

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Conflict, NotFound, ValidationFailed
from app.models import (
    BulkEntity,
    BulkJob,
    BulkJobKind,
    BulkJobStatus,
    ChunkStatus,
    Contact,
    ImportRowError,
    User,
)
from app.models.bulk import ACTIVE_JOB_INDEX, FINAL_STATUSES, IDEMPOTENCY_CONSTRAINT
from app.repositories.bulk import (
    ENTITY_MODELS,
    STUCK_ERROR,
    ChunkPlan,
    FinishedJob,
    close_overdue_filter,
    lost_finalize,
    move_team_filter,
    reassign_owner_filter,
    stale_pending,
    stalled_chunks,
    stalled_import,
    tag_filter,
)
from app.repositories.contacts import ContactFilters, ContactRepository
from app.repositories.pagination import Page, PageResult
from app.services.crm import Owner, can_own, resolve_owner
from app.services.fx import org_today
from app.uow import UnitOfWork
from app.workers.queue import TaskQueue

logger = structlog.get_logger("bulk")

# Что передаёт reassign-owner (поле include) и какая таблица за этим стоит.
REASSIGN_INCLUDE: dict[str, BulkEntity] = {
    "companies": BulkEntity.COMPANIES,
    "contacts": BulkEntity.CONTACTS,
    "open_deals": BulkEntity.DEALS,
    "open_activities": BulkEntity.ACTIVITIES,
}

PLAN_TASK = "bulk.plan"
CHUNK_TASK = "bulk.chunk"
# Задача импорта (шаг 5.6): API и reaper ставят её по имени, как bulk.plan.
IMPORT_TASK = "imports.import_contacts"
BULK_QUEUE = "bulk"  # очередь чанков: её длину reaper смотрит перед повторной отправкой

# move_team догоняет все живые и открытые записи человека — во всех четырёх таблицах.
MOVE_TEAM_ENTITIES = list(BulkEntity)
TAG_KINDS = (BulkJobKind.ADD_TAGS, BulkJobKind.REMOVE_TAGS)


@dataclass(frozen=True)
class ReassignOwnerRequest:
    from_user_id: uuid.UUID
    to_user_id: uuid.UUID
    include: tuple[str, ...] = tuple(REASSIGN_INCLUDE)


def filter_params(filters: ContactFilters) -> dict[str, Any]:
    """Фильтр сегмента для bulk_jobs.params: только заданные поля и только JSON-типы. Пустое
    значение — то же, что не заданное (как в GET /contacts), поэтому одинаковые сегменты дают
    один хэш запроса."""
    return {
        name: value if isinstance(value, int) else str(value)
        for name, value in asdict(filters).items()
        if value
    }


def filters_from_params(params: dict[str, Any]) -> ContactFilters:
    """Обратно из filter_params: чанк строит то же условие, что видел plan."""
    owner_id = params.get("owner_id")
    return ContactFilters(
        q=params.get("q"),
        status=params.get("status"),
        tag=params.get("tag"),
        owner_id=uuid.UUID(owner_id) if owner_id else None,
        company_id=params.get("company_id"),
    )


def request_hash(kind: str, body: dict[str, Any]) -> str:
    """sha256 канонического JSON {kind, body}: тот же запрос — тот же хэш, порядок ключей
    не важен (раздел 6, «Повтор запроса»)."""
    canonical = json.dumps({"kind": kind, "body": body}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def unique_constraint(exc: IntegrityError) -> str | None:
    """Имя нарушенного ограничения: SQLAlchemy оборачивает исключение asyncpg, и имя есть
    только у исходного."""
    driver_error = getattr(exc.orig, "orig", None) or exc.orig
    return getattr(driver_error, "constraint_name", None)


def new_move_team_job(access: Access, user_id: uuid.UUID) -> BulkJob:
    """Job move_team (раздел 6, «Смена отдела человека»): её добавляет в свою транзакцию сервис
    людей, а после коммита ставит bulk.plan. Автор — админ, который перевёл человека; отдела у
    него обычно нет (team_id NULL). Ключа повтора нет: повторный PATCH с тем же отделом создаёт
    новую job, если записи вне отдела остались, — так чинится упавшая."""
    return BulkJob(
        org_id=access.org_id,
        team_id=access.team_id,
        owner_id=access.user_id,
        kind=BulkJobKind.MOVE_TEAM,
        status=BulkJobStatus.PENDING,
        params={"user_id": str(user_id), "access": access.to_params()},
    )


@dataclass(frozen=True)
class ReapPolicy:
    """Пороги bulk.reap_stuck (Settings: BULK_REAP_PENDING_SECONDS, BULK_REAP_STALL_SECONDS,
    IMPORT_REAP_STALL_SECONDS, BULK_MAX_REAPS)."""

    pending_seconds: int = 120
    stall_seconds: int = 300
    import_stall_seconds: int = 900
    max_reaps: int = 3


@dataclass
class ReapReport:
    """Что сделал один проход reaper — для лога и тестов."""

    replanned: list[uuid.UUID] = field(default_factory=list)  # (1) снова bulk.plan
    finalized: list[uuid.UUID] = field(default_factory=list)  # (2) потерянный finalize
    resent: dict[uuid.UUID, list[int]] = field(default_factory=dict)  # (3) job → чанки
    reimported: list[uuid.UUID] = field(default_factory=list)  # импорт: снова задача
    stuck: list[uuid.UUID] = field(default_factory=list)  # сдались: bulk.stuck
    queue_busy: bool = False  # (3) отложено: в очереди bulk есть сообщения

    def __bool__(self) -> bool:
        return bool(
            self.replanned
            or self.finalized
            or self.resent
            or self.reimported
            or self.stuck
            or self.queue_busy
        )


# Длина очереди брокера (LLEN в db 1): в воркере — Redis, в тестах — подмена.
QueueLength = Callable[[str], Awaitable[int]]


def failure_code(exc: BaseException) -> str:
    """Код причины в bulk_job_chunks.error: код AppError или имя класса исключения."""
    return str(getattr(exc, "code", None) or type(exc).__name__)[:64]


class BulkService:
    def __init__(
        self, uow: UnitOfWork, roles: RoleCatalog, queue: TaskQueue, *, chunk_size: int
    ) -> None:
        self.uow = uow
        self.roles = roles
        self.queue = queue
        self.chunk_size = chunk_size

    # --- API

    async def get(self, access: Access, job_id: uuid.UUID) -> BulkJob:
        """Job в уровне bulk_job:read (автор — owner_id) или своя, иначе 404."""
        return await self.uow.bulk_jobs.get_visible_or_404(job_id, access)

    async def import_errors(
        self, access: Access, job_id: uuid.UUID, page: Page
    ) -> PageResult[ImportRowError]:
        """Ошибки строк импорта (keyset по row_no). Job — как в get: не видна — 404."""
        await self.uow.bulk_jobs.get_visible_or_404(job_id, access)
        return await self.uow.import_errors.list(job_id, page)

    async def cancel(self, access: Access, job_id: uuid.UUID) -> BulkJob:
        """Отменить job (раздел 15, «Жизненный цикл bulk job»). access — bulk_job:cancel.

        Job ищется по уровню bulk_job:read (не видна — 404), отменить можно в уровне
        bulk_job:cancel (иначе 403); свою job автор отменяет всегда — так же, как всегда её
        видит. pending → сразу cancelled (с итогом и строкой журнала), running → cancelling
        (чанки станут skipped, cancelled поставит finalize), cancelling / cancelled — без
        изменений; остальные финальные статусы и move_team — 409 bulk.not_cancellable."""
        read = access.for_action("bulk_job:read", self.roles)
        if read is None:  # cancel не шире read (тест политик) — это страховка
            raise NotFound(entity="bulk_job", id=str(job_id))
        try:
            job = await self.uow.bulk_jobs.get_visible_or_404(job_id, read, for_update=True)
            if job.owner_id != access.user_id:
                access.ensure_allowed(job)
            if job.kind == BulkJobKind.MOVE_TEAM:
                # Следствие уже сделанного перевода, а не операция автора: её не отменяют.
                raise Conflict("bulk.not_cancellable", status=job.status)
            if job.status == BulkJobStatus.PENDING:
                finished = await self.uow.bulk_jobs.cancel_pending(job_id)
                assert finished is not None  # строка под FOR UPDATE и pending
                await self._record_final(job_id, finished)
            elif job.status == BulkJobStatus.RUNNING:
                await self.uow.bulk_jobs.request_cancel(job_id)
            elif job.status in FINAL_STATUSES and job.status != BulkJobStatus.CANCELLED:
                raise Conflict("bulk.not_cancellable", status=job.status)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        job = await self._reload(job_id)
        logger.info("bulk_job_cancel", job_id=str(job_id), status=job.status)
        return job

    async def _reload(self, job_id: uuid.UUID) -> BulkJob:
        job = await self.uow.bulk_jobs.get(job_id)
        assert job is not None  # job не удаляется, пока она нужна
        return job

    async def start_reassign_owner(
        self, access: Access, request: ReassignOwnerRequest, idempotency_key: str | None
    ) -> BulkJob:
        """Передать записи человека другому (раздел 6, шаг 2). Счёта строк здесь нет: count
        по сегменту в миллион строк занял бы сотни миллисекунд HTTP-запроса — считает plan."""
        # Прежний ответственный должен быть виден автору по member:read: руководитель отдела
        # не передаёт клиентов человека из чужого отдела.
        member_read = access.for_action("member:read", self.roles)
        source = (
            await self.uow.users.get_visible(request.from_user_id, member_read)
            if member_read is not None
            else None
        )
        if source is None:
            raise ValidationFailed(field="from_user_id")
        if request.from_user_id == request.to_user_id:
            raise ValidationFailed("bulk.same_owner", field="to_user_id")
        # Тот же resolve_owner, что у одной записи: активный team_lead или sales_rep (422), и
        # в уровне автора (403): руководитель передаёт только внутри своего отдела.
        target = await resolve_owner(self.uow, access, request.to_user_id, field="to_user_id")
        access.ensure_allowed(target)
        params = {
            "from_user_id": str(request.from_user_id),
            "to_user_id": str(request.to_user_id),
            # В порядке REASSIGN_INCLUDE и без повторов: одинаковые запросы — один хэш.
            "include": [name for name in REASSIGN_INCLUDE if name in request.include],
        }
        return await self._start(access, BulkJobKind.REASSIGN_OWNER, params, idempotency_key)

    async def start_close_overdue_deals(
        self,
        access: Access,
        before: date | None,
        pipeline_id: int | None,
        idempotency_key: str | None,
    ) -> BulkJob:
        """Закрыть просроченные открытые сделки как lost (раздел 6, «Остальные массовые
        операции»). «Просрочена» — как у GET /deals?overdue=true: expected_close_date раньше
        before; before по умолчанию — сегодня по часовому поясу организации, позже сегодня —
        422 validation (поле before). Воронка не своей организации — 422 (поле pipeline_id).
        Дата фиксируется в params: все чанки работают с одной границей. Хэш повтора — от тела
        как пришло: before не передан — тот же запрос и завтра (в params — дата первого)."""
        today = await org_today(self.uow, access.org_id)
        if before is not None and before > today:
            raise ValidationFailed(field="before", today=today.isoformat())
        if (
            pipeline_id is not None
            and await self.uow.pipelines.get(pipeline_id, access.org_id) is None
        ):
            raise ValidationFailed(field="pipeline_id")
        request = {"before": before.isoformat() if before else None, "pipeline_id": pipeline_id}
        params = {**request, "before": (before or today).isoformat()}
        return await self._start(
            access, BulkJobKind.CLOSE_OVERDUE_DEALS, params, idempotency_key, request=request
        )

    async def start_tags(
        self,
        access: Access,
        kind: BulkJobKind,
        tag: str,
        filters: ContactFilters,
        idempotency_key: str | None,
    ) -> BulkJob:
        """Добавить (add_tags) или снять (remove_tags) тег у сегмента контактов. Сегмент —
        фильтры GET /contacts в уровне автора (bulk:run), пустой фильтр — весь уровень.
        Контакты, у которых уже 20 тегов, add_tags пропускает и считает в failed_rows — job
        закончится partial_failed."""
        assert kind in TAG_KINDS
        params = {"tag": tag, "filter": filter_params(filters)}
        return await self._start(access, kind, params, idempotency_key)

    async def _start(
        self,
        access: Access,
        kind: BulkJobKind,
        params: dict[str, Any],
        key: str | None,
        *,
        request: dict[str, Any] | None = None,
    ) -> BulkJob:
        """request — тело для хэша повтора, если оно не совпадает с params (у
        close_overdue_deals в params уже подставлена дата по умолчанию)."""
        digest = request_hash(kind, params if request is None else request)
        if key is not None:
            existing = await self.find_replay(access, key, digest)
            if existing is not None:
                return existing
        job, created = await self.insert(access, kind, params, key, digest)
        if not created:
            return job  # гонка двух одинаковых запросов: job первого
        await self.uow.commit()
        logger.info("bulk_job_created", job_id=str(job.id), kind=kind)
        # После коммита и best-effort: ошибку брокера порт только логирует.
        await self.queue.enqueue(PLAN_TASK, str(job.id))
        return job

    async def find_replay(self, access: Access, key: str, digest: str) -> BulkJob | None:
        """Повтор по Idempotency-Key: моя job с этим ключом (в любом статусе) или None. Ключ
        тот же, а запрос другой (digest) — 422 bulk.idempotency_key_reused."""
        existing = await self.uow.bulk_jobs.find_by_key(access.user_id, key)
        return self._replay(existing, digest) if existing is not None else None

    async def ensure_not_running(self, access: Access, kind: BulkJobKind) -> None:
        """Заранее, до тяжёлой работы запроса (импорт — до приёма файла): job вида уже идёт в
        организации — 409 bulk.already_running. Гонку двух запросов всё равно ловит частичный
        UNIQUE при вставке (insert)."""
        if await self.uow.bulk_jobs.has_active(access.org_id, kind):
            raise await self._already_running(access, kind)

    async def insert(
        self,
        access: Access,
        kind: BulkJobKind,
        params: dict[str, Any],
        key: str | None,
        digest: str,
    ) -> tuple[BulkJob, bool]:
        """Строка bulk_jobs (pending) в транзакции запроса, без коммита. True — job новая;
        False — гонка с тем же Idempotency-Key, это job первого запроса (транзакция откачена).
        Активная job вида уже есть — 409 bulk.already_running."""
        job = BulkJob(
            org_id=access.org_id,
            team_id=access.team_id,
            owner_id=access.user_id,
            kind=kind,
            status=BulkJobStatus.PENDING,
            params={**params, "access": access.to_params()},
            idempotency_key=key,
            request_hash=digest,
        )
        self.uow.bulk_jobs.add(job)
        try:
            await self.uow.flush()
        except IntegrityError as exc:
            await self.uow.rollback()
            return await self._after_conflict(exc, access, kind, key, digest), False
        return job, True

    def _replay(self, job: BulkJob, digest: str) -> BulkJob:
        """Тот же ключ и то же тело — та же job в любом статусе; другое тело — 422."""
        if job.request_hash != digest:
            raise ValidationFailed("bulk.idempotency_key_reused")
        logger.info("bulk_job_replayed", job_id=str(job.id))
        return job

    async def _after_conflict(
        self,
        exc: IntegrityError,
        access: Access,
        kind: BulkJobKind,
        key: str | None,
        digest: str,
    ) -> BulkJob:
        """INSERT упёрся в уникальный индекс — гонка двух запросов (транзакция уже откачена)."""
        constraint = unique_constraint(exc)
        if constraint == IDEMPOTENCY_CONSTRAINT and key is not None:
            # Два одинаковых запроса одновременно: второй возвращает job первого.
            existing = await self.uow.bulk_jobs.find_by_key(access.user_id, key)
            if existing is not None:
                return self._replay(existing, digest)
        if constraint == ACTIVE_JOB_INDEX:
            raise await self._already_running(access, kind)
        raise exc

    async def _already_running(self, access: Access, kind: BulkJobKind) -> Conflict:
        """409 bulk.already_running. job_id — только видимой мне job (bulk_job:read): чужая
        дала бы ссылку на 404, а успевшая закончиться — на уже не активную."""
        read = access.for_action("bulk_job:read", self.roles)
        active = await self.uow.bulk_jobs.find_active(kind, read) if read else None
        return Conflict("bulk.already_running", job_id=str(active.id) if active else None)

    # --- воркер

    async def plan(self, job_id: uuid.UUID) -> list[int]:
        """Нарезать выборку на чанки (раздел 6, шаг 3). Возвращает номера чанков — задача
        ставит их группой после коммита. Job уже не pending (повтор) — ничего, пустой список.
        Пустая выборка — job сразу завершается (done)."""
        try:
            job = await self.uow.bulk_jobs.get_pending_for_update(job_id)
            if job is None:
                await self.uow.rollback()
                logger.info("bulk_plan_skipped", job_id=str(job_id))
                return []
            chunks: list[ChunkPlan] = []
            total = 0
            team_id = await self._member_team(job)
            for entity in self._entities(job.kind, job.params):
                bounds, count = await self.uow.bulk.chunk_bounds(
                    ENTITY_MODELS[entity], self._selection(job, entity, team_id), self.chunk_size
                )
                chunks += [
                    ChunkPlan(len(chunks) + i, entity, lo, hi) for i, (lo, hi) in enumerate(bounds)
                ]
                total += count
            await self.uow.bulk_chunks.add_all(job.id, chunks)
            await self.uow.bulk_jobs.start(job.id, total=total, chunks_total=len(chunks))
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.info("bulk_planned", job_id=str(job_id), total=total, chunks=len(chunks))
        if not chunks:
            await self.finalize_if_complete(job_id)
        return [chunk.no for chunk in chunks]

    async def run_chunk(self, job_id: uuid.UUID, no: int) -> None:
        """Один чанк — одна транзакция (раздел 6, шаг 4), затем finalize_if_complete.

        Порядок блокировок: строка чанка → пользователи → строки данных → job. Он совместим с
        сервисом людей (организация → пользователь FOR UPDATE): перевод получателя в другой
        отдел ждёт конца чанка и видит его записи."""
        try:
            chunk = await self.uow.bulk_chunks.claim(job_id, no)
            if chunk is None:
                # Чанк уже выполнен: повторная доставка. Завершение всё равно проверяем — вдруг
                # первая копия упала между коммитом чанка и finalize.
                await self.uow.rollback()
                logger.info("bulk_chunk_already_finished", job_id=str(job_id), no=no)
            else:
                await self._run_claimed(job_id, no, BulkEntity(chunk.entity), chunk.lo, chunk.hi)
                await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        await self.finalize_if_complete(job_id)

    async def _run_claimed(
        self, job_id: uuid.UUID, no: int, entity: BulkEntity, lo: int, hi: int
    ) -> None:
        job = await self.uow.bulk_jobs.get(job_id)  # без блокировки: job меняют другие чанки
        assert job is not None  # строка чанка ссылается на job (FK)
        if job.status == BulkJobStatus.CANCELLING or job.error is not None:
            await self._skip(job_id, no, job.error or "cancelled")
            return
        failed = 0  # строки выборки, которые чанк пропустил (у контакта уже 20 тегов)
        if job.kind == BulkJobKind.MOVE_TEAM:
            rows: int | None = await self._move_team_rows(job, entity, lo, hi)
        elif not await self._author_still_allowed(job, no):
            return  # чанк пропущен: автор больше не может запускать эту job
        elif job.kind == BulkJobKind.REASSIGN_OWNER:
            rows = await self._reassign_rows(job, no, entity, lo, hi)
        elif job.kind == BulkJobKind.CLOSE_OVERDUE_DEALS:
            rows = await self.uow.bulk.close_deals(lo, hi, self._selection(job, entity))
        elif job.kind == BulkJobKind.ADD_TAGS:
            where = self._selection(job, entity)
            rows, failed = await self.uow.bulk.add_tag(lo, hi, where, job.params["tag"])
        elif job.kind == BulkJobKind.REMOVE_TAGS:
            where = self._selection(job, entity)
            rows = await self.uow.bulk.remove_tag(lo, hi, where, job.params["tag"])
        else:
            raise ValueError(f"нет чанков для вида job: {job.kind}")
        if rows is None:
            return  # чанк пропущен: получатель больше не подходит
        await self.uow.bulk_chunks.finish(job_id, no, ChunkStatus.DONE, rows=rows)
        await self.uow.bulk_jobs.add_progress(job_id, rows, failed)  # последним — строка job
        logger.info(
            "bulk_chunk_done", job_id=str(job_id), no=no, entity=entity, rows=rows, failed=failed
        )

    async def _author_still_allowed(self, job: BulkJob, no: int) -> bool:
        """Автор FOR SHARE и всё ещё может запускать эту job (раздел 6, шаг 4 чанка) — у всех
        видов, кроме move_team. Иначе причина в job.error, свой чанк skipped и False."""
        access = Access.from_params(job.params["access"])
        author = await self.uow.users.get_for_share(access.user_id)
        if self._author_unchanged(author, access):
            return True
        # Чужие строки чанков не трогаем: иначе два чанка ждали бы друг друга. Остальные
        # увидят error в начале своего чанка.
        await self.uow.bulk_jobs.set_error(job.id, "bulk.author_changed")
        await self._skip(job.id, no, "bulk.author_changed")
        return False

    async def _reassign_rows(
        self, job: BulkJob, no: int, entity: BulkEntity, lo: int, hi: int
    ) -> int | None:
        """Чанк reassign_owner (автора уже проверил _author_still_allowed): получатель FOR SHARE
        и всё ещё подходит — UPDATE. Иначе причина в job.error, свой чанк skipped и None."""
        access = Access.from_params(job.params["access"])
        target = await self.uow.users.get_for_share(uuid.UUID(job.params["to_user_id"]))
        if not self._target_unchanged(target, access):
            await self.uow.bulk_jobs.set_error(job.id, "bulk.target_changed")
            await self._skip(job.id, no, "bulk.target_changed")
            return None
        assert target is not None and target.team_id is not None
        # Отдел — из текущей строки получателя: записи уходят в его отдел на момент чанка.
        return await self.uow.bulk.reassign(
            ENTITY_MODELS[entity], lo, hi, self._selection(job, entity), target.id, target.team_id
        )

    async def _move_team_rows(self, job: BulkJob, entity: BulkEntity, lo: int, hi: int) -> int:
        """Чанк move_team: записи догоняют отдел из текущей строки человека (FOR SHARE) — при
        цепочке переводов всегда последний. Перевод, начатый во время чанка, ждёт его конца и
        потом создаёт свою job, если записи вне нового отдела остались."""
        team_id = await self._member_team(job)
        if team_id is None:
            return 0  # отдела больше нет — догонять нечего (move_team_filter даёт false)
        where = self._selection(job, entity, team_id)
        return await self.uow.bulk.move_team(ENTITY_MODELS[entity], lo, hi, where, team_id)

    async def _member_team(self, job: BulkJob) -> uuid.UUID | None:
        """Текущий отдел человека, которого перевели (move_team), — строка FOR SHARE: пока план
        или чанк идёт, отдел не поменяется. Для других видов job — None."""
        if job.kind != BulkJobKind.MOVE_TEAM:
            return None
        person = await self.uow.users.get_for_share(uuid.UUID(job.params["user_id"]))
        return person.team_id if person is not None else None

    async def _skip(self, job_id: uuid.UUID, no: int, reason: str) -> None:
        """Чанк не выполняется: job отменяют или уже провалена, автор или получатель сменились."""
        await self.uow.bulk_chunks.finish(job_id, no, ChunkStatus.SKIPPED, error=reason)
        logger.info("bulk_chunk_skipped", job_id=str(job_id), no=no, reason=reason)

    def _author_unchanged(self, author: User | None, access: Access) -> bool:
        """Автор всё ещё может запускать эту job: активен, та же организация, роль и отдел, и
        уровень bulk:run в data.yaml тот же, что при запуске."""
        return (
            author is not None
            and author.is_active
            and author.org_id == access.org_id
            and author.role == access.role
            and author.team_id == access.team_id
            and self.roles.scope_or_none(author.role, access.action) == access.scope
        )

    @staticmethod
    def _target_unchanged(target: User | None, access: Access) -> bool:
        """Получатель всё ещё ведёт записи и в уровне автора (как resolve_owner при запуске)."""
        if target is None or not can_own(target, access.org_id):
            return False
        assert target.team_id is not None
        return access.matches(Owner(access.org_id, target.team_id, target.id))

    async def fail_chunk(self, job_id: uuid.UUID, no: int, error: str) -> None:
        """Чанк упал после всех ретраев (on_failure задачи): чанк failed, failed_chunks + 1,
        затем завершение — упавший чанк даёт partial_failed, а не повисшую job."""
        try:
            if await self.uow.bulk_chunks.finish(job_id, no, ChunkStatus.FAILED, error=error):
                await self.uow.bulk_jobs.add_failed_chunk(job_id)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.warning("bulk_chunk_failed", job_id=str(job_id), no=no, error=error)
        await self.finalize_if_complete(job_id)

    async def finalize_if_complete(self, job_id: uuid.UUID) -> str | None:
        """Завершить job, если pending-чанков не осталось (раздел 6, шаг 5). Возвращает
        финальный статус — только тому вызову, который завершил job; остальным None. Итог и
        строка журнала пишутся в той же транзакции, поэтому ровно один раз."""
        try:
            finished = await self.uow.bulk_jobs.finalize(job_id)
            if finished is None:
                await self.uow.rollback()
                return None
            await self._record_final(job_id, finished)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        return finished.status

    async def _record_final(self, job_id: uuid.UUID, finished: FinishedJob) -> None:
        """Итог job и одна строка журнала — в транзакции того, кто сделал финальный переход:
        finalize, отмена pending-job, reaper, который сдался, или финал импорта. Переход —
        условный UPDATE, поэтому итог пишется ровно один раз. У импорта здесь же удаляются
        строки staging (_import_final)."""
        if finished.kind == BulkJobKind.IMPORT_CONTACTS:
            rows = await self._import_final(job_id)
        else:
            done = await self.uow.bulk_chunks.rows_by_entity(job_id)
            entities = self._entities(finished.kind, finished.params)
            rows = {e.value: done.get(e, 0) for e in entities}
            if finished.kind == BulkJobKind.ADD_TAGS:
                # Контакты, пропущенные из-за предела в 20 тегов, — в итоге и журнале.
                rows["failed"] = (await self._reload(job_id)).failed_rows
            await self.uow.bulk_jobs.set_result(job_id, {"rows": rows})
        self.uow.audit.record(
            actor_id=finished.owner_id,
            org_id=finished.org_id,
            action=f"bulk.{finished.kind}",
            entity_type="bulk_job",
            entity_id=job_id,
            changes=self._audit_changes(finished, rows),
        )
        logger.info("bulk_job_finished", job_id=str(job_id), status=finished.status, rows=rows)

    async def _import_final(self, job_id: uuid.UUID) -> dict[str, int]:
        """Финальный переход импорта — любой: его собственный финал, отмена pending, reaper,
        который сдался. Строки staging удаляются здесь, в той же транзакции. Итог created /
        updated копит сам импорт (чанки merge); до merge его нет — нули. В журнал — итог и
        число ошибок строк."""
        await self.uow.imports.delete_staging(job_id)
        job = await self._reload(job_id)
        result = job.result or {"created": 0, "updated": 0}
        if job.result is None:
            await self.uow.bulk_jobs.set_result(job_id, result)
        return {
            "created": result["created"],
            "updated": result["updated"],
            "failed": job.failed_rows,
        }

    # --- bulk.reap_stuck (раздел 15, «Жизненный цикл bulk job»)

    async def reap_stuck(self, policy: ReapPolicy, queue_length: QueueLength) -> ReapReport:
        """Подобрать job, у которых потерялась задача: API не поставил bulk.plan, finalize не
        случился, сообщения чанков пропали (жёсткий time_limit, убитый воркер), импорт встал.

        Каждая job — своя короткая транзакция: строка FOR UPDATE SKIP LOCKED и повторная
        проверка условия под блокировкой. Поэтому два прохода reaper одновременно (beat
        поставил следующий, пока медленный ещё идёт) одну job не переотправят дважды: второй
        пропустит занятую строку, а после коммита первого увидит свежий updated_at. Задачи
        ставятся после коммита: потерю постановки подберёт следующий проход, а лишняя доставка
        безопасна — plan, чанк и импорт идемпотентны."""
        report = ReapReport()
        # (1) pending дольше порога → снова bulk.plan.
        condition = stale_pending(policy.pending_seconds)
        for job_id in await self._candidates(condition):
            if await self._resend(job_id, condition, policy, report):
                await self.queue.enqueue(PLAN_TASK, str(job_id))
                report.replanned.append(job_id)
        # Импорт: одна задача на job — снова imports.import_contacts.
        condition = stalled_import(policy.pending_seconds, policy.import_stall_seconds)
        for job_id in await self._candidates(condition):
            if await self._resend(job_id, condition, policy, report):
                await self.queue.enqueue(IMPORT_TASK, str(job_id))
                report.reimported.append(job_id)
        # (2) pending-чанков нет, а job не завершена → потерянный finalize.
        for job_id in await self._candidates(lost_finalize()):
            if await self.finalize_if_complete(job_id) is not None:
                report.finalized.append(job_id)
        # (3) чанки стоят без прогресса → снова pending-чанки, если очередь bulk пуста: если в
        # ней сообщения, чанки, скорее всего, ждут своей очереди за чужой job — не отправлять
        # и не считать попытку.
        condition = stalled_chunks(policy.stall_seconds)
        stalled = await self._candidates(condition)
        if stalled and await queue_length(BULK_QUEUE) > 0:
            report.queue_busy = True
            stalled = []
        for job_id in stalled:
            numbers = await self._resend_chunks(job_id, condition, policy, report)
            for no in numbers:
                await self.queue.enqueue(CHUNK_TASK, str(job_id), no)
            if numbers:
                report.resent[job_id] = numbers
        if report:
            logger.info(
                "bulk_reaped",
                replanned=len(report.replanned),
                reimported=len(report.reimported),
                finalized=len(report.finalized),
                resent=len(report.resent),
                stuck=len(report.stuck),
                queue_busy=report.queue_busy,
            )
        return report

    async def _candidates(self, condition: ColumnElement[bool]) -> list[uuid.UUID]:
        try:
            return await self.uow.bulk_jobs.ids_where(condition)
        finally:
            await self.uow.rollback()  # не держать снимок между транзакциями по job

    async def _resend(
        self,
        job_id: uuid.UUID,
        condition: ColumnElement[bool],
        policy: ReapPolicy,
        report: ReapReport,
    ) -> bool:
        """Job без чанков (pending, импорт): True — отметить повторную отправку (reaps + 1) и
        поставить задачу. Попытки кончились — job сразу failed с bulk.stuck."""
        try:
            job = await self.uow.bulk_jobs.lock_where(job_id, condition)
            if job is None:  # занята другой копией reaper или уже сдвинулась
                await self.uow.rollback()
                return False
            if job.reaps >= policy.max_reaps:
                finished = await self.uow.bulk_jobs.fail_stuck(job_id)
                assert finished is not None  # строка под FOR UPDATE и ещё не завершена
                await self._record_final(job_id, finished)
                await self.uow.commit()
                self._stuck(job, report)
                return False
            await self.uow.bulk_jobs.mark_reaped(job_id)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.warning("bulk_job_reaped", job_id=str(job_id), reaps=job.reaps + 1)
        return True

    async def _resend_chunks(
        self,
        job_id: uuid.UUID,
        condition: ColumnElement[bool],
        policy: ReapPolicy,
        report: ReapReport,
    ) -> list[int]:
        """(3) Номера pending-чанков для повторной отправки (reaps + 1). Попытки кончились —
        error = bulk.stuck, pending-чанки skipped, и finalize ставит failed (или cancelled, если
        job отменяли)."""
        try:
            job = await self.uow.bulk_jobs.lock_where(job_id, condition)
            if job is None:
                await self.uow.rollback()
                return []
            if job.reaps >= policy.max_reaps:
                await self.uow.bulk_jobs.set_error(job_id, STUCK_ERROR)
                await self.uow.bulk_chunks.skip_pending(job_id, STUCK_ERROR)
                await self.uow.commit()
                self._stuck(job, report)
                await self.finalize_if_complete(job_id)
                return []
            numbers = await self.uow.bulk_chunks.pending_numbers(job_id)
            await self.uow.bulk_jobs.mark_reaped(job_id)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.warning(
            "bulk_job_reaped", job_id=str(job_id), reaps=job.reaps + 1, chunks=len(numbers)
        )
        return numbers

    @staticmethod
    def _stuck(job: BulkJob, report: ReapReport) -> None:
        # Алерт BulkJobStuck (этап 8) — по этой строке лога и по метрике.
        logger.error("bulk_job_stuck", job_id=str(job.id), kind=job.kind, reaps=job.reaps)
        report.stuck.append(job.id)

    @staticmethod
    def _audit_changes(finished: FinishedJob, rows: dict[str, int]) -> dict[str, Any]:
        """Одна строка журнала на job: параметры (без Access автора), итог и строки по
        сущностям. Построчного аудита у массовых операций нет (раздел 3)."""
        params = {k: v for k, v in finished.params.items() if k != "access"}
        return {**params, "status": finished.status, "rows": rows}

    # --- выборка по виду job: одна и та же для plan и чанка

    @staticmethod
    def _entities(kind: str, params: dict[str, Any]) -> list[BulkEntity]:
        if kind == BulkJobKind.REASSIGN_OWNER:
            return [REASSIGN_INCLUDE[name] for name in params["include"]]
        if kind == BulkJobKind.MOVE_TEAM:
            return MOVE_TEAM_ENTITIES
        if kind == BulkJobKind.CLOSE_OVERDUE_DEALS:
            return [BulkEntity.DEALS]
        if kind in TAG_KINDS:
            return [BulkEntity.CONTACTS]
        raise ValueError(f"нет выборки для вида job: {kind}")

    @staticmethod
    def _selection(
        job: BulkJob, entity: BulkEntity, team_id: uuid.UUID | None = None
    ) -> ColumnElement[bool]:
        """team_id — текущий отдел человека для move_team (_member_team)."""
        model = ENTITY_MODELS[entity]
        if job.kind == BulkJobKind.REASSIGN_OWNER:
            access = Access.from_params(job.params["access"])
            from_user_id = uuid.UUID(job.params["from_user_id"])
            return reassign_owner_filter(model, from_user_id, access)
        if job.kind == BulkJobKind.MOVE_TEAM:
            user_id = uuid.UUID(job.params["user_id"])
            return and_(model.org_id == job.org_id, move_team_filter(model, user_id, team_id))
        access = Access.from_params(job.params["access"])
        if job.kind == BulkJobKind.CLOSE_OVERDUE_DEALS:
            before = date.fromisoformat(job.params["before"])
            return close_overdue_filter(access, before, job.params["pipeline_id"])
        if job.kind in TAG_KINDS:
            assert model is Contact
            segment = ContactRepository.filter_conditions(filters_from_params(job.params["filter"]))
            add = job.kind == BulkJobKind.ADD_TAGS
            return tag_filter(access, job.params["tag"], add, segment)
        raise ValueError(f"нет выборки для вида job: {job.kind}")
