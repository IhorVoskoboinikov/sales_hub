"""Выгрузка контактов в CSV (раздел 6, «Остальные массовые операции и выгрузка»).

Не job, а поток в ответе: лимит частоты → count с пределом → строки серверного курсора пачками
по 2000 → CSV в UTF-8 с BOM. Тот же Access и те же фильтры, что у GET /contacts, поэтому
выгрузка не шире списка. Колонки — те, что принимает импорт (id, company_id и даты он
игнорирует), поэтому файл загружается обратно. В лог — кто, с каким фильтром и сколько строк;
audit_log выгрузка не пишет.

Курсор живёт в транзакции сессии запроса: зависимость get_session с yield (scope «request»)
FastAPI 0.151 закрывает после того, как StreamingResponse отдал тело целиком, а не после
возврата из обработчика. Транзакция открыта, пока клиент качает файл, — это и есть цена
потоковой выгрузки (снимок держит горизонт VACUUM); большие выгрузки в S3 — этап 10.

Соединение из пула выгрузка держит столько же, поэтому одновременных выгрузок на процесс API —
не больше EXPORT_MAX_CONCURRENT (ExportSlots): медленные загрузки иначе разобрали бы весь пул
(5 + 10), и остальные запросы процесса ждали бы соединение до pool_timeout.
"""

import csv
import io
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from typing import Any

import structlog

from app.authz.access import Access
from app.errors import RateLimited, ValidationFailed
from app.repositories.contacts import ContactFilters
from app.repositories.rate_limiter import RateLimiter, export_limit
from app.schemas.crm import looks_like_formula
from app.uow import UnitOfWork

logger = structlog.get_logger("export")

# Колонки файла — в этом порядке (раздел 6).
EXPORT_COLUMNS = (
    "id",
    "email",
    "first_name",
    "last_name",
    "phone",
    "status",
    "source",
    "tags",
    "owner_email",
    "company_id",
    "created_at",
    "updated_at",
)
# CSV-инъекция: Excel и LibreOffice считают формулой ячейку, начинающуюся с = + - @, табуляции
# или \r. Такое значение получает префикс «'» — импорт снимает его перед = + - @
# (looks_like_formula; значения с «'=» в начале — тоже с префиксом, чтобы вернуться как были).
CONTROL_PREFIXES = ("\t", "\r")
BOM = "\ufeff"  # Excel без BOM читает UTF-8 как cp1251
BUSY_RETRY_AFTER = 30  # секунд: Retry-After у 429 export.busy


def csv_cell(value: Any) -> str:
    """Значение колонки → ячейка: NULL — пусто, теги — через запятую (как в импорте), даты —
    ISO 8601, опасный первый символ — с апострофом."""
    if value is None:
        return ""
    if isinstance(value, list):
        value = ",".join(value)
    elif isinstance(value, datetime):
        value = value.isoformat()
    text = str(value)
    return "'" + text if looks_like_formula(text) or text.startswith(CONTROL_PREFIXES) else text


def csv_lines(rows: Sequence[Sequence[Any]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer)  # кавычки — только где нужны, конец строки \r\n (RFC 4180)
    writer.writerows([csv_cell(value) for value in row] for row in rows)
    return buffer.getvalue().encode()


class ExportSlots:
    """Счётчик идущих выгрузок процесса API (один на приложение, app.state.export_slots).
    Процесс однопоточный (asyncio), поэтому блокировка не нужна. Слот держит зависимость
    запроса, а не генератор тела: она закрывается вместе с сессией — после того, как тело
    отдано или клиент оборвал загрузку, — даже если генератор так и не начал работу."""

    def __init__(self) -> None:
        self.busy = 0

    @contextmanager
    def hold(self, limit: int) -> Iterator[None]:
        """Занять слот на время запроса; все limit заняты — 429 export.busy (Retry-After)."""
        if self.busy >= limit:
            raise RateLimited("export.busy", retry_after=BUSY_RETRY_AFTER)
        self.busy += 1
        try:
            yield
        finally:
            self.busy -= 1


class ExportService:
    def __init__(
        self, uow: UnitOfWork, limiter: RateLimiter, *, max_rows: int, rate_per_hour: int
    ) -> None:
        self.uow = uow
        self.limiter = limiter
        self.max_rows = max_rows  # EXPORT_MAX_ROWS
        self.rate_per_hour = rate_per_hour  # EXPORT_RATE_PER_HOUR

    async def contacts(self, access: Access, filters: ContactFilters) -> AsyncIterator[bytes]:
        """Проверки — до первого байта ответа, пока ещё можно ответить ошибкой: больше
        EXPORT_RATE_PER_HOUR выгрузок в час — 429; больше EXPORT_MAX_ROWS строк — 422
        export.too_large («сузьте фильтр»). Возвращает тело ответа — генератор пачек CSV."""
        await self.limiter.hit(export_limit(self.rate_per_hour), str(access.user_id))
        total = await self.uow.contacts.export_count(access, filters, self.max_rows + 1)
        if total > self.max_rows:
            raise ValidationFailed("export.too_large", max_rows=self.max_rows)
        return self._stream(access, filters)

    async def _stream(self, access: Access, filters: ContactFilters) -> AsyncIterator[bytes]:
        rows = 0
        completed = False
        try:
            yield (BOM + ",".join(EXPORT_COLUMNS) + "\r\n").encode()
            async for batch in self.uow.contacts.export_rows(access, filters):
                rows += len(batch)
                yield csv_lines(batch)
            completed = True
        finally:
            # completed = False — клиент оборвал загрузку (или упал запрос посреди потока).
            logger.info(
                "contacts_exported",
                user_id=str(access.user_id),
                filters={k: str(v) for k, v in asdict(filters).items() if v is not None},
                rows=rows,
                completed=completed,
            )
