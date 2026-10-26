"""Выгрузка CSV и сегмент тегов без базы (раздел 6, шаг 5.7): ячейки и экранирование формул
(и обратная загрузка ячейки импортом), слоты одновременных выгрузок, фильтр сегмента в params
job, типизированный массив тега и то, что сессия запроса живёт, пока StreamingResponse отдаёт
тело (FastAPI 0.151, зависимость с yield, scope «request»)."""

import csv
import io
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import StreamingResponse
from httpx import ASGITransport, AsyncClient
from sqlalchemy.dialects.postgresql import asyncpg as pg_asyncpg

from app.api.deps import export_slot, get_app_settings
from app.authz.access import Access
from app.errors import RateLimited
from app.errors.handlers import register_error_handlers
from app.models import ContactStatus
from app.repositories.bulk import tag_filter
from app.repositories.contacts import ContactFilters, ContactRepository
from app.schemas.crm import ContactImportRow
from app.services.bulk import filter_params, filters_from_params
from app.services.exports import BUSY_RETRY_AFTER, ExportSlots, csv_cell, csv_lines

ORG = uuid.uuid4()


def test_cells_escape_formulas_and_format_values() -> None:
    assert csv_cell(None) == ""
    assert csv_cell(42) == "42"
    assert csv_cell(["vip", "kyiv"]) == "vip,kyiv"
    assert csv_cell(datetime(2026, 10, 25, 12, 0, tzinfo=UTC)) == "2026-10-25T12:00:00+00:00"
    for dangerous in ("=1+2", "+380", "-5", "@SUM(A1)", "\tx", "\rx"):
        assert csv_cell(dangerous) == "'" + dangerous
    assert csv_cell(["=cmd", "vip"]) == "'=cmd,vip"  # ячейка тегов — тоже по первому символу
    assert csv_cell("a=b") == "a=b"
    # Значение, которое само начинается с «'» перед формулой, тоже получает апостроф: импорт
    # снимает ровно один, и значение возвращается как было.
    assert csv_cell("'=1") == "''=1"
    assert csv_cell("''+x") == "'''+x"
    assert csv_cell("'abc") == "'abc"  # апостроф не перед формулой — как есть


def imported(cell: str, field: str = "last_name") -> object:
    row = ContactImportRow.model_validate({"email": "a@x.com", "first_name": "A", field: cell})
    return getattr(row, field)


@pytest.mark.parametrize(
    "value", ["=1", "+380", "-5", "@x", "'=1", "''=1", "'-", "'abc", "'", "a'=b", "Коваль"]
)
def test_cell_survives_export_and_import(value: str) -> None:
    assert imported(csv_cell(value)) == value


def test_import_unescapes_one_apostrophe_before_formula() -> None:
    assert imported("'=1") == "=1"  # файл из Excel или выгрузки
    assert imported("''=1") == "'=1"
    assert imported("'abc") == "'abc"


def test_tag_with_comma_does_not_survive_round_trip() -> None:
    """Известное ограничение (раздел 6): тег может содержать запятую, а в ячейке теги — через
    запятую, поэтому «a,b» после обратной загрузки — два тега."""
    assert imported(csv_cell(["a,b", "vip"]), "tags") == ["a", "b", "vip"]


def test_export_slots_limit_and_release() -> None:
    slots = ExportSlots()
    with slots.hold(2), slots.hold(2):
        assert slots.busy == 2
        with pytest.raises(RateLimited) as exc, slots.hold(2):
            pass
        assert exc.value.code == "export.busy"
        assert exc.value.details == {"retry_after": BUSY_RETRY_AFTER}
        assert slots.busy == 2  # отказ слот не занимает
    assert slots.busy == 0
    with pytest.raises(ValueError), slots.hold(1):
        raise ValueError  # ошибка внутри запроса слот освобождает
    assert slots.busy == 0


def test_lines_quote_only_when_needed() -> None:
    data = csv_lines([[1, 'Іван "Ваня"', "a,b", None]]).decode()
    assert data == '1,"Іван ""Ваня""","a,b",\r\n'
    assert next(csv.reader(io.StringIO(data))) == ["1", 'Іван "Ваня"', "a,b", ""]


def test_filter_params_round_trip_and_drop_empty() -> None:
    owner = uuid.uuid4()
    filters = ContactFilters("петренко", ContactStatus.LEAD, "vip", owner, 7)
    params = filter_params(filters)
    assert params == {
        "q": "петренко",
        "status": "lead",
        "tag": "vip",
        "owner_id": str(owner),
        "company_id": 7,
    }
    assert filters_from_params(params) == ContactFilters("петренко", "lead", "vip", owner, 7)
    assert filter_params(ContactFilters(q=None, tag="")) == {}  # пустое — как не заданное
    assert filters_from_params({}) == ContactFilters()


def test_tag_filter_uses_typed_array_for_gin_index() -> None:
    access = Access("bulk:run", "org", uuid.uuid4(), ORG, None, "org_admin")
    segment = ContactRepository.filter_conditions(ContactFilters(tag="kyiv"))
    for add, negated in ((True, True), (False, False)):
        sql = str(tag_filter(access, "vip", add, segment).compile(dialect=pg_asyncpg.dialect()))
        assert sql.count("contacts.tags @> $") == 2
        assert sql.count("::VARCHAR(64)[]") == 2  # иначе text[] и мимо ix_contacts_tags
        assert ("NOT ((contacts.tags @>" in sql) is negated
        assert "contacts.deleted_at IS NULL" in sql


async def test_request_session_outlives_streaming_body() -> None:
    """Зависимость с yield (как get_session) закрывается после того, как тело StreamingResponse
    отдано целиком, — курсор выгрузки читает строки из ещё открытой сессии."""
    events: list[str] = []

    async def session() -> AsyncIterator[str]:
        events.append("open")
        yield "session"
        events.append("close")

    app = FastAPI()

    @app.get("/stream")
    async def stream(s: str = Depends(session)) -> StreamingResponse:
        async def body() -> AsyncIterator[bytes]:
            for i in range(3):
                events.append(f"chunk {i}: {'closed' if 'close' in events else s}")
                yield b"x"

        return StreamingResponse(body())

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/stream")

    assert resp.content == b"xxx"
    assert events == ["open", "chunk 0: session", "chunk 1: session", "chunk 2: session", "close"]


async def test_export_slot_is_held_until_body_is_sent() -> None:
    """Слот выгрузки (export_slot) держится, пока тело отдаётся, и освобождается после него —
    так же, как сессия запроса."""
    seen: list[int] = []
    app = FastAPI()
    register_error_handlers(app)
    app.state.export_slots = slots = ExportSlots()
    app.dependency_overrides[get_app_settings] = lambda: SimpleNamespace(export_max_concurrent=1)

    @app.get("/export")
    async def export(_slot: None = Depends(export_slot)) -> StreamingResponse:
        async def body() -> AsyncIterator[bytes]:
            for _ in range(2):
                seen.append(slots.busy)
                yield b"x"

        return StreamingResponse(body())

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/export")).content == b"xx"
        with slots.hold(1):  # слот занят другой выгрузкой
            busy = await client.get("/export")
        assert (busy.status_code, busy.json()["code"]) == (429, "export.busy")
        assert busy.headers["retry-after"] == str(BUSY_RETRY_AFTER)

    assert seen == [1, 1]
    assert slots.busy == 0
