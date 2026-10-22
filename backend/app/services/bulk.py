"""Массовые операции (раздел 6): запуск job из API, plan, чанки и завершение — без chord.

Поток «передать клиентов уволенного менеджера»:
1. API (start_reassign_owner): права и параметры → повтор по Idempotency-Key → строка
   bulk_jobs (pending) → commit → bulk.plan best-effort через TaskQueue → 202.
2. plan: границы чанков по реальным строкам выборки, строки bulk_job_chunks, job → running.
3. run_chunk: одна транзакция на чанк; повтор ничего не делает (строка чанка уже не pending).
4. finalize_if_complete: условный UPDATE завершает job ровно один раз, победитель пишет итог и
   одну строку журнала.

Смена отдела человека (move_team): job вставляет сервис людей в транзакции перевода
(new_move_team_job), дальше — те же plan, чанки и finalize. Проверки автора у неё нет: это
следствие уже сделанного изменения, а не операция от имени автора.

Задачи Celery (app/workers/tasks/bulk.py) — тонкие обёртки над этими методами.
"""

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import ColumnElement, and_
from sqlalchemy.exc import IntegrityError

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Conflict, ValidationFailed
from app.models import BulkEntity, BulkJob, BulkJobKind, BulkJobStatus, ChunkStatus, User
from app.models.bulk import ACTIVE_JOB_INDEX, IDEMPOTENCY_CONSTRAINT
from app.repositories.bulk import (
    ENTITY_MODELS,
    ChunkPlan,
    FinishedJob,
    move_team_filter,
    reassign_owner_filter,
)
from app.services.crm import Owner, can_own, resolve_owner
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

# move_team догоняет все живые и открытые записи человека — во всех четырёх таблицах.
MOVE_TEAM_ENTITIES = list(BulkEntity)


@dataclass(frozen=True)
class ReassignOwnerRequest:
    from_user_id: uuid.UUID
    to_user_id: uuid.UUID
    include: tuple[str, ...] = tuple(REASSIGN_INCLUDE)


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

    async def _start(
        self, access: Access, kind: BulkJobKind, params: dict[str, Any], key: str | None
    ) -> BulkJob:
        digest = request_hash(kind, params)
        if key is not None:
            existing = await self.uow.bulk_jobs.find_by_key(access.user_id, key)
            if existing is not None:
                return self._replay(existing, digest)
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
            return await self._after_conflict(exc, access, kind, key, digest)
        await self.uow.commit()
        logger.info("bulk_job_created", job_id=str(job.id), kind=kind)
        # После коммита и best-effort: ошибку брокера порт только логирует.
        await self.queue.enqueue(PLAN_TASK, str(job.id))
        return job

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
            # job_id — только видимой мне job (bulk_job:read): чужая дала бы ссылку на 404.
            read = access.for_action("bulk_job:read", self.roles)
            active = await self.uow.bulk_jobs.find_active(kind, read) if read else None
            raise Conflict("bulk.already_running", job_id=str(active.id) if active else None)
        raise exc

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
        if job.kind == BulkJobKind.MOVE_TEAM:
            rows: int | None = await self._move_team_rows(job, entity, lo, hi)
        elif job.kind == BulkJobKind.REASSIGN_OWNER:
            rows = await self._reassign_rows(job, no, entity, lo, hi)
        else:
            raise ValueError(f"нет чанков для вида job: {job.kind}")
        if rows is None:
            return  # чанк пропущен: автор или получатель больше не подходят
        await self.uow.bulk_chunks.finish(job_id, no, ChunkStatus.DONE, rows=rows)
        await self.uow.bulk_jobs.add_progress(job_id, rows)  # последним — строка job
        logger.info("bulk_chunk_done", job_id=str(job_id), no=no, entity=entity, rows=rows)

    async def _reassign_rows(
        self, job: BulkJob, no: int, entity: BulkEntity, lo: int, hi: int
    ) -> int | None:
        """Чанк reassign_owner: автор и получатель FOR SHARE и всё ещё подходят — UPDATE.
        Иначе причина в job.error, свой чанк skipped и None."""
        access = Access.from_params(job.params["access"])
        author = await self.uow.users.get_for_share(access.user_id)
        if not self._author_unchanged(author, access):
            # Чужие строки чанков не трогаем: иначе два чанка ждали бы друг друга. Остальные
            # увидят error в начале своего чанка.
            await self.uow.bulk_jobs.set_error(job.id, "bulk.author_changed")
            await self._skip(job.id, no, "bulk.author_changed")
            return None
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
            done = await self.uow.bulk_chunks.rows_by_entity(job_id)
            rows = {e.value: done.get(e, 0) for e in self._entities(finished.kind, finished.params)}
            await self.uow.bulk_jobs.set_result(job_id, {"rows": rows})
            self.uow.audit.record(
                actor_id=finished.owner_id,
                org_id=finished.org_id,
                action=f"bulk.{finished.kind}",
                entity_type="bulk_job",
                entity_id=job_id,
                changes=self._audit_changes(finished, rows),
            )
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.info("bulk_job_finished", job_id=str(job_id), status=finished.status, rows=rows)
        return finished.status

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
        raise ValueError(f"нет выборки для вида job: {job.kind}")
