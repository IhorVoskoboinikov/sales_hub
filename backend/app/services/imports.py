"""Импорт контактов из CSV (раздел 6, «Поток: импорт контактов из CSV»): приём файла в API и
задача импорта.

POST /contacts/import (шаг 1): заголовок CSV проверяется по началу файла сразу — ошибки формата
дают 422 без job; файл уходит в S3 (imports/{org_id}/{uuid}.csv), строка bulk_jobs (pending)
коммитится, и после коммита best-effort ставится задача imports.import_contacts.

Повтор по Idempotency-Key проверяется до приёма файла, поэтому хэш запроса — только от вида
job: тот же ключ — та же job, какой бы файл ни пришёл.

Задача (шаг 3, ImportService.run) идёт шагами parse → stage → merge; bulk_jobs.stage хранит
последний завершённый, поэтому повтор задачи продолжает с него. Одна копия на job — сессионный
advisory lock на отдельном соединении (app/db/locks.py). Перед каждым шагом и чанком merge —
статус (cancelling → cancelled) и автор (bulk.author_changed). Финал — свой условный UPDATE;
строки staging удаляются в той же транзакции, что любой финальный переход (BulkService). Записи
шагов тоже условные (job ещё running или cancelling): шаг, который job пережила, откатывается.
"""

import asyncio
import codecs
import csv
import hashlib
import json
import os
import tempfile
import uuid
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol, TextIO

import structlog
from pydantic import ValidationError

from app.authz.access import Access
from app.errors import ValidationFailed
from app.models import BulkJob, BulkJobKind, BulkJobStatus, User
from app.models.imports import IMPORT_COLUMNS, REQUIRED_IMPORT_COLUMNS
from app.schemas.crm import ContactImportRow
from app.services.bulk import IMPORT_TASK, BulkService
from app.services.crm import Owner, can_own, resolve_owner
from app.storage import FileStorage

logger = structlog.get_logger("imports")

HEADER_BYTES = 64 * 1024  # заголовок CSV ищется в начале файла, дальше не читаем
DELIMITERS = (",", ";")  # Excel с украинской локалью сохраняет «;»
CONTENT_TYPE = "text/csv"
FILE_NAME_MAX = 255
KIND = BulkJobKind.IMPORT_CONTACTS


def import_request_hash() -> str:
    """sha256 канонического JSON {kind} — без тела: ключ проверяется до приёма файла."""
    canonical = json.dumps({"kind": KIND.value}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class CsvHeader:
    """Как читать файл: разделитель и заголовок целиком (имена в нижнем регистре, по порядку
    колонок файла — по ним задача импорта сопоставит ячейки с полями)."""

    delimiter: str
    columns: tuple[str, ...]


def read_header(head: bytes, *, whole_file: bool) -> CsvHeader:
    """Заголовок CSV по первым HEADER_BYTES файла (whole_file — файл в них целиком).

    UTF-8, BOM допускается; разделитель — «,» или «;», тот, что даёт больше колонок. Колонки —
    без учёта регистра; обязательны email и first_name, известная колонка дважды — ошибка,
    незнакомые игнорируются. Ошибки — 422: import.empty_file, import.invalid_encoding,
    import.invalid_header (в details — чего не хватает и что повторяется)."""
    line, newline, _ = head.partition(b"\n")
    if not newline and not whole_file:
        # Строки заголовка нет в первых 64 КиБ — это не заголовок CSV.
        raise ValidationFailed("import.invalid_header", reason="too_long")
    try:
        text = line.decode("utf-8-sig")  # строка целиком: до перевода строки или конца файла
    except UnicodeDecodeError:
        raise ValidationFailed("import.invalid_encoding") from None
    text = text.rstrip("\r")
    if not text.strip():
        if whole_file and not head.strip().removeprefix(codecs.BOM_UTF8).strip():
            raise ValidationFailed("import.empty_file")
        raise ValidationFailed("import.invalid_header", reason="blank")
    variants = [(d, cells) for d in DELIMITERS if (cells := _split(text, d)) is not None]
    if not variants:  # незакрытая кавычка при любом разделителе
        raise ValidationFailed("import.invalid_header", reason="malformed")
    delimiter, cells = max(variants, key=lambda v: len(v[1]))  # при равенстве — «,»
    columns = tuple(cell.strip().lower() for cell in cells)
    known = [c for c in columns if c in IMPORT_COLUMNS]
    missing = [c for c in REQUIRED_IMPORT_COLUMNS if c not in known]
    duplicates = sorted({c for c in known if known.count(c) > 1})
    if missing or duplicates:
        raise ValidationFailed("import.invalid_header", missing=missing, duplicates=duplicates)
    return CsvHeader(delimiter, columns)


def _split(line: str, delimiter: str) -> list[str] | None:
    """Ячейки заголовка при этом разделителе; None — так строка не читается (кавычки)."""
    try:
        return next(csv.reader([line], delimiter=delimiter, strict=True))
    except csv.Error:
        return None


@dataclass(frozen=True)
class ImportUpload:
    """Принятый файл формы: временный файл Starlette (поддерживает seek) и что о нём известно."""

    file: BinaryIO
    size: int
    file_name: str
    header: CsvHeader
    owner_id: uuid.UUID | None  # ответственный строк без owner_email; None — я


# --- Задача импорта (шаг 3): разбор файла

FIELD_MAX = 10_000  # символов в поле CSV; больше — csv.Error, то есть import.malformed_csv
# Символов файла на одну ячейку записи, не больше: поле FIELD_MAX в кавычках, где каждая
# кавычка сдвоена, плюс разделитель или \r\n. Запись длиннее, чем столько на колонку
# заголовка, — import.malformed_csv: иначе одна строка из 100 млн разделителей стала бы списком
# из 100 млн ячеек (около 1 ГБ памяти воркера).
CELL_MAX_CHARS = 2 * FIELD_MAX + 4
PARSE_BATCH = 5_000  # строк файла на один COPY: файл не держится в памяти целиком
MERGE_CHUNK = 5_000  # строк staging на транзакцию merge
REPLACEMENT = "�"  # так errors="replace" отмечает байты, которые не UTF-8
NUL = "\x00"  # Postgres не хранит его ни в text, ни в jsonb — та же invalid_encoding

# Шаги задачи — значения bulk_jobs.stage (последний завершённый).
STAGE_PARSE = "parse"
STAGE_STAGE = "stage"
STAGE_MERGE = "merge"

AUTHOR_CHANGED = "bulk.author_changed"
TOO_MANY_ROWS = "import.too_many_rows"
MALFORMED_CSV = "import.malformed_csv"


@dataclass(frozen=True)
class StagingRow:
    """Проверенная строка файла — в contacts_staging."""

    row_no: int
    row: ContactImportRow

    def record(self, job_id: uuid.UUID) -> tuple[Any, ...]:
        """Кортеж для COPY — порядок STAGING_COLUMNS (repositories/imports.py)."""
        r = self.row
        status = r.status.value if r.status is not None else None
        return (
            job_id,
            self.row_no,
            r.email,
            r.first_name,
            r.last_name,
            r.phone,
            status,
            r.source,
            r.tags or None,  # «,» в ячейке — тоже пусто: тегов не добавляет
            r.owner_email,
        )


@dataclass(frozen=True)
class RowError:
    """Ошибка строки — в import_errors: у строки одна, первая."""

    row_no: int
    code: str
    field: str | None
    raw: dict[str, str] | None  # ячейки известных колонок, как в файле

    def record(self, job_id: uuid.UUID) -> tuple[Any, ...]:
        raw = json.dumps(self.raw, ensure_ascii=False) if self.raw is not None else None
        return (job_id, self.row_no, self.code, self.field, raw)


ParsedRow = StagingRow | RowError


class MalformedCsv(Exception):
    """csv.Error: незакрытая кавычка, лишняя кавычка в поле, поле длиннее FIELD_MAX, запись
    длиннее предела RecordLines. Дальше файл не читается — строки после такой ошибки границ
    не имеют."""

    def __init__(self, row_no: int, message: str) -> None:
        super().__init__(message)
        self.row_no = row_no


def parse_row(row_no: int, cells: Sequence[str], columns: Sequence[str]) -> ParsedRow:
    """Строка файла → строка staging или ошибка. Ячейки сопоставляются с колонками заголовка по
    позиции, незнакомые колонки не читаются. Первая ошибка: число ячеек не как в заголовке
    (invalid_columns), не UTF-8 в ячейке известной колонки (invalid_encoding с полем), пустой
    email (email_required), поле не прошло проверку ContactImportRow (validation с полем). NUL —
    тоже invalid_encoding: COPY с ним провалил бы весь шаг parse; в raw он заменён на U+FFFD.
    Незнакомые колонки не хранятся, поэтому их байты строку не портят."""
    raw = {
        c: cell.replace(NUL, REPLACEMENT)
        for c, cell in zip(columns, cells, strict=False)
        if c in IMPORT_COLUMNS
    }
    if len(cells) != len(columns):
        return RowError(row_no, "invalid_columns", None, raw)
    broken = next(
        (
            c
            for c, cell in zip(columns, cells, strict=True)
            if c in IMPORT_COLUMNS and (REPLACEMENT in cell or NUL in cell)
        ),
        None,
    )
    if broken is not None:
        return RowError(row_no, "invalid_encoding", broken, raw)
    if not raw.get("email", "").strip():
        # Без email повторная загрузка того же файла плодила бы дубли.
        return RowError(row_no, "email_required", "email", raw)
    try:
        return StagingRow(row_no, ContactImportRow.model_validate(raw))
    except ValidationError as exc:
        loc = exc.errors()[0]["loc"]
        return RowError(row_no, "validation", str(loc[0]) if loc else None, raw)


class RecordLines:
    """Строки файла для csv.reader с пределом на запись: limit символов файла от начала записи
    (start_record перед каждой). Строка читается readline(n), поэтому даже одна строка без
    перевода строки не прочитается в память больше предела. Перебор — csv.Error: его, как и
    любую ошибку разбора, CsvRows превратит в MalformedCsv."""

    def __init__(self, file: TextIO, limit: int) -> None:
        self._file = file
        self._limit = limit
        self._left = limit

    def start_record(self) -> None:
        self._left = self._limit

    def __iter__(self) -> "RecordLines":
        return self

    def __next__(self) -> str:
        line = self._file.readline(self._left + 1)
        if not line:
            raise StopIteration
        self._left -= len(line)
        if self._left < 0:
            raise csv.Error(f"record longer than {self._limit} characters")
        return line


class CsvRows:
    """Чтение скачанного файла csv.reader-ом — синхронно (многострочные поля async-поток не
    разберёт), поэтому сервис зовёт batch() через asyncio.to_thread.

    UTF-8 с BOM или без, байты не UTF-8 — U+FFFD (строка получит invalid_encoding, а не весь
    файл провал); строка заголовка пропускается (её проверил API); пустые строки и строки из
    одних разделителей пропускаются. row_no — номер строки файла, с которой начинается запись
    (заголовок — строка 1): так её найдут в редакторе и в Excel. Запись — не длиннее
    CELL_MAX_CHARS на колонку заголовка (RecordLines)."""

    def __init__(self, path: Path, delimiter: str, columns: Sequence[str]) -> None:
        # Лимит csv — на процесс; 10 000 символов — правило формата импорта (раздел 6).
        csv.field_size_limit(FIELD_MAX)
        self.columns = tuple(columns)
        self._file = path.open(encoding="utf-8-sig", errors="replace", newline="")
        self._lines = RecordLines(self._file, len(self.columns) * CELL_MAX_CHARS)
        self._reader = csv.reader(self._lines, delimiter=delimiter, strict=True)
        self._header_read = False

    def batch(self, size: int) -> list[ParsedRow]:
        """Следующие size непустых строк (меньше — файл кончился). csv.Error — MalformedCsv
        с номером строки, на которой началась битая запись."""
        rows: list[ParsedRow] = []
        while len(rows) < size:
            row_no = self._reader.line_num + 1
            self._lines.start_record()
            try:
                cells = next(self._reader)
            except StopIteration:
                break
            except csv.Error as exc:
                raise MalformedCsv(row_no, str(exc)) from None
            if not self._header_read:
                self._header_read = True
                continue
            if not any(cell.strip() for cell in cells):
                continue
            rows.append(parse_row(row_no, cells, self.columns))
        return rows

    def close(self) -> None:
        self._file.close()


class JobLock(Protocol):
    """Одна копия задачи на job: hold(key) → True — блокировка взята и держится до выхода из
    блока; False — её держит другая копия. В воркере — AdvisoryLock (app/db/locks.py)."""

    def hold(self, key: str) -> AbstractAsyncContextManager[bool]: ...


class TooManyRows(Exception):
    """Строк в файле больше IMPORT_MAX_ROWS — job failed с import.too_many_rows."""


class ImportService:
    """Запуск импорта из API (before_upload, start) и сама задача импорта (run). Общие правила
    job (повтор по ключу, «одна job вида на организацию», вставка с разбором гонок, итог и
    журнал финального перехода) — у BulkService.

    Задача imports.import_contacts (раздел 6, шаг 3): parse → stage → merge → финал. Каждый
    шаг — своя транзакция, чанк merge — тоже; bulk_jobs.stage отмечает последний завершённый
    шаг, staging.merged — перенесённые строки. Поэтому повтор задачи (reaper, повторная
    доставка Redis, ретрай) продолжает с места остановки и ничего не считает дважды."""

    def __init__(
        self,
        bulk: BulkService,
        storage: FileStorage,
        *,
        max_rows: int = 1_000_000,
        merge_chunk: int = MERGE_CHUNK,
        parse_batch: int = PARSE_BATCH,
    ) -> None:
        self.bulk = bulk
        self.uow = bulk.uow
        self.storage = storage
        self.max_rows = max_rows  # IMPORT_MAX_ROWS — нужен только задаче
        self.merge_chunk = merge_chunk
        self.parse_batch = parse_batch

    async def before_upload(self, access: Access, key: str | None) -> BulkJob | None:
        """До приёма файла: повтор по Idempotency-Key — та же job (её и вернуть, файл не
        читать); импорт в организации уже идёт — 409 bulk.already_running. Дальше пойдёт
        загрузка файла, поэтому транзакция закрывается: пока клиент шлёт 100 МиБ, соединение с
        базой не держим."""
        if key is not None:
            existing = await self.bulk.find_replay(access, key, import_request_hash())
            if existing is not None:
                return existing  # ответ сразу — транзакция закроется вместе с запросом
        try:
            await self.bulk.ensure_not_running(access, KIND)
        finally:
            await self.uow.rollback()
        return None

    async def start(self, access: Access, upload: ImportUpload, key: str | None) -> BulkJob:
        """Ответственный по умолчанию → файл в S3 → job (pending) → commit → задача импорта."""
        # Тот же resolve_owner, что у одной записи: активный team_lead или sales_rep (422; не
        # указан у org_admin — owner.required), в уровне автора (403).
        owner = await resolve_owner(self.uow, access, upload.owner_id)
        access.ensure_allowed(owner)
        # Строку ответственного (FOR SHARE) не держим, пока файл идёт в S3: шаги stage и merge
        # проверят его ещё раз.
        await self.uow.rollback()
        file_key = f"imports/{access.org_id}/{uuid.uuid4()}.csv"
        await self.storage.put(file_key, upload.file, upload.size, CONTENT_TYPE)
        params = {
            "file_key": file_key,
            "file_name": upload.file_name[:FILE_NAME_MAX],
            "delimiter": upload.header.delimiter,
            "columns": list(upload.header.columns),
            "default_owner_id": str(owner.owner_id),
        }
        try:
            job, created = await self.bulk.insert(access, KIND, params, key, import_request_hash())
            if created:
                await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            await self._discard(file_key)  # job нет — файл никому не нужен
            raise
        if not created:
            # Гонка двух запросов с одним ключом: это job первого, файл второго не нужен.
            await self._discard(file_key)
            return job
        logger.info("import_job_created", job_id=str(job.id), size=upload.size)
        # После коммита и best-effort: потерю постановки подберёт bulk.reap_stuck.
        await self.bulk.queue.enqueue(IMPORT_TASK, str(job.id))
        return job

    async def _discard(self, file_key: str) -> None:
        """Удалить файл, для которого не стало job. Не вышло — не беда: сирот imports/*
        старше суток удаляет maintenance.cleanup (шаг 5.8)."""
        try:
            await self.storage.delete(file_key)
        except Exception as exc:
            logger.warning("import_file_discard_failed", key=file_key, error=repr(exc))

    # --- задача импорта (шаг 3)

    async def run(self, job_id: uuid.UUID, lock: JobLock) -> None:
        """Весь импорт под сессионной блокировкой job: не взялась — работает другая копия
        (reaper переотправил или Redis выдал повторно), выход без изменений."""
        async with lock.hold(str(job_id)) as taken:
            if not taken:
                logger.info("import_already_running", job_id=str(job_id))
                return
            await self._run(job_id)

    async def _run(self, job_id: uuid.UUID) -> None:
        merged_upto = 0  # row_no, до которого merge уже прошёл в этом запуске
        while (job := await self._checkpoint(job_id)) is not None:
            if job.stage is None:
                await self._parse(job)
            elif job.stage == STAGE_PARSE:
                await self._stage(job)
            elif job.stage == STAGE_STAGE:
                hi = await self._merge_chunk(job, merged_upto)
                if hi is None:
                    await self._complete(job_id)
                else:
                    merged_upto = hi
            else:  # merge отмечается вместе с финалом — сюда не попасть
                return

    async def _checkpoint(self, job_id: uuid.UUID) -> BulkJob | None:
        """Перед каждым шагом и чанком: job ещё идёт? pending → running; cancelling →
        финал cancelled; автор больше не может запускать импорт → failed с
        bulk.author_changed. None — продолжать нечего.

        Job читается без блокировки, поэтому pending → running — условный UPDATE: не прошёл —
        отмена pending или reaper успели раньше, и статус перечитывается. Если они успеют уже
        во время шага, шаг откатится сам (условные записи шагов, repositories/bulk.py)."""
        try:
            job = await self.uow.bulk_jobs.get(job_id)
            if job is None or job.kind != KIND:
                await self.uow.rollback()
                return None
            if job.status == BulkJobStatus.PENDING:
                if not await self.uow.bulk_jobs.start_import(job_id):
                    await self.uow.rollback()
                    return await self._checkpoint(job_id)  # из pending обратно не вернётся
                logger.info("import_started", job_id=str(job_id))
            elif job.status == BulkJobStatus.CANCELLING:
                await self._final(job_id)
                await self.uow.commit()
                return None
            elif job.status != BulkJobStatus.RUNNING:
                await self.uow.rollback()  # уже завершена (отменили pending, reaper сдался)
                return None
            access = Access.from_params(job.params["access"])
            author = await self.uow.users.get_for_share(access.user_id)
            if not self.bulk._author_unchanged(author, access):
                await self._final(job_id, error=AUTHOR_CHANGED)
                await self.uow.commit()
                return None
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        return job

    async def _final(self, job_id: uuid.UUID, error: str | None = None) -> bool:
        """Финальный переход в текущей транзакции (коммитит вызывающий): итог, журнал и
        удаление staging — в BulkService._record_final, как у любого финала job. False — job
        уже завершил кто-то другой."""
        finished = await self.uow.bulk_jobs.finish_import(job_id, error)
        if finished is None:
            return False
        await self.bulk._record_final(job_id, finished)
        return True

    async def _step_lost(self, job_id: uuid.UUID, step: str) -> None:
        """Условная запись шага не прошла: job завершили, пока шаг шёл (отмена pending,
        reaper). Шаг откатывается целиком — staging и ошибки строк вместе с ним; следующий
        _checkpoint увидит финальный статус и выйдет."""
        await self.uow.rollback()
        logger.info("import_step_discarded", job_id=str(job_id), step=step)

    async def _fail(self, job_id: uuid.UUID, error: str, row: RowError | None = None) -> None:
        """Job — failed с причиной (too_many_rows, malformed_csv); row — строка, на которой
        разбор остановился."""
        try:
            if row is not None:
                await self.uow.imports.add_error(job_id, row.row_no, row.code, row.raw)
            if not await self._final(job_id, error):
                await self._step_lost(job_id, STAGE_PARSE)  # без строки ошибки у чужого финала
                return
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.warning("import_failed", job_id=str(job_id), error=error)

    # --- parse

    async def _parse(self, job: BulkJob) -> None:
        """Файл из S3 — во временный файл воркера (удаляется в конце), строки — COPY в
        staging и import_errors. Одна транзакция: упадёт посреди — повтор начнёт заново (и
        на всякий случай удалит всё, что осталось от прошлой копии, — clear_parse)."""
        fd, name = await asyncio.to_thread(tempfile.mkstemp, prefix="import-", suffix=".csv")
        await asyncio.to_thread(os.close, fd)
        path = Path(name)
        try:
            await self.storage.download_to(job.params["file_key"], path)
            await self._load(job, path)
        finally:
            await asyncio.to_thread(path.unlink, missing_ok=True)

    async def _load(self, job: BulkJob, path: Path) -> None:
        # После отката объект job протухает (rollback сессии), поэтому id — заранее.
        job_id = job.id
        rows = await asyncio.to_thread(
            CsvRows, path, job.params["delimiter"], job.params["columns"]
        )
        total = failed = 0
        try:
            # Первый запрос шага — до COPY: он открывает транзакцию (см. ImportRepository).
            await self.uow.imports.clear_parse(job_id)
            while batch := await asyncio.to_thread(rows.batch, self.parse_batch):
                total += len(batch)
                if total > self.max_rows:
                    raise TooManyRows
                errors = [r.record(job_id) for r in batch if isinstance(r, RowError)]
                await self.uow.imports.copy_staging(
                    [r.record(job_id) for r in batch if isinstance(r, StagingRow)]
                )
                await self.uow.imports.copy_errors(errors)
                failed += len(errors)
            if not await self.uow.bulk_jobs.finish_import_step(
                job_id, STAGE_PARSE, failed=failed, total=total
            ):
                await self._step_lost(job_id, STAGE_PARSE)
                return
            await self.uow.commit()
        except MalformedCsv as exc:
            await self.uow.rollback()
            logger.warning("import_malformed_csv", job_id=str(job_id), row_no=exc.row_no)
            await self._fail(
                job_id, MALFORMED_CSV, RowError(exc.row_no, "malformed_csv", None, None)
            )
            return
        except TooManyRows:
            await self.uow.rollback()
            await self._fail(job_id, TOO_MANY_ROWS)
            return
        except BaseException:
            await self.uow.rollback()
            raise
        finally:
            await asyncio.to_thread(rows.close)
        logger.info("import_parsed", job_id=str(job_id), total=total, failed=failed)

    # --- stage

    async def _stage(self, job: BulkJob) -> None:
        """Проверки по файлу и базе — одна транзакция set-based UPDATE-ов (repositories)."""
        access = Access.from_params(job.params["access"])
        try:
            failed = await self.uow.imports.stage(
                job.id, access, uuid.UUID(job.params["default_owner_id"])
            )
            if not await self.uow.bulk_jobs.finish_import_step(job.id, STAGE_STAGE, failed=failed):
                await self._step_lost(job.id, STAGE_STAGE)
                return
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.info("import_staged", job_id=str(job.id), failed=failed)

    # --- merge

    async def _merge_chunk(self, job: BulkJob, after: int) -> int | None:
        """Один чанк merge — одна транзакция. Порядок блокировок: ответственные FOR SHARE →
        строки контактов → job (как у чанка bulk). Возвращает последний row_no чанка; None —
        переносить больше нечего; after — чанк откатился (job уже завершена)."""
        access = Access.from_params(job.params["access"])
        try:
            hi = await self.uow.imports.chunk_end(job.id, after, self.merge_chunk)
            if hi is None:
                await self.uow.rollback()
                return None
            lo = after + 1
            owner_ids = await self.uow.imports.chunk_owners(job.id, lo, hi)
            owners = await self.uow.imports.lock_users(owner_ids)
            valid = {u.id for u in owners if self._can_own(u, job.org_id, access)}
            invalid = [owner_id for owner_id in owner_ids if owner_id not in valid]
            rejected = (
                await self.uow.imports.reject_owners(job.id, lo, hi, invalid) if invalid else 0
            )
            counts = await self.uow.imports.merge(job.id, lo, hi, job.org_id, access)
            await self.uow.imports.mark_merged(job.id, lo, hi, counts.outside)
            if not await self.uow.bulk_jobs.add_import_progress(
                job.id,
                created=counts.created,
                updated=counts.updated,
                failed=rejected + len(counts.outside),
            ):
                await self._step_lost(job.id, STAGE_MERGE)
                return after
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        logger.info(
            "import_chunk_merged",
            job_id=str(job.id),
            upto=hi,
            created=counts.created,
            updated=counts.updated,
            failed=rejected + len(counts.outside),
        )
        return hi

    @staticmethod
    def _can_own(user: User, org_id: uuid.UUID, access: Access) -> bool:
        """Ответственный всё ещё ведёт записи и в уровне автора импорта — как resolve_owner и
        ensure_allowed при запуске (раздел 6: «стали неактивными или вышли из уровня»)."""
        if not can_own(user, org_id):
            return False
        assert user.team_id is not None
        return access.matches(Owner(org_id, user.team_id, user.id))

    async def _complete(self, job_id: uuid.UUID) -> None:
        """Все строки перенесены: stage = merge и финал (done или partial_failed)."""
        try:
            if not await self.uow.bulk_jobs.finish_import_step(job_id, STAGE_MERGE):
                await self._step_lost(job_id, STAGE_MERGE)
                return
            await self._final(job_id)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
