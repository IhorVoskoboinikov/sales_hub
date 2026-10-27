"""Массовые операции без базы: Access в params job, хэш запроса, события SSE."""

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi.sse import KEEPALIVE_COMMENT, format_sse_event

from app.api.v1.bulk_jobs import RETRY_MS, job_events
from app.authz.access import Access
from app.errors import Conflict
from app.schemas.bulk import BulkJobOut
from app.services.bulk import failure_code, request_hash


@pytest.mark.parametrize("team_id", [uuid.uuid4(), None], ids=["team_lead", "org_admin"])
def test_access_survives_params_round_trip(team_id: uuid.UUID | None) -> None:
    access = Access("bulk:run", "team", uuid.uuid4(), uuid.uuid4(), team_id, "team_lead")

    params = access.to_params()

    assert all(isinstance(v, str | None) for v in params.values())  # только JSON-типы
    assert Access.from_params(params) == access


def test_access_from_params_rejects_unknown_scope() -> None:
    params = Access("bulk:run", "org", uuid.uuid4(), uuid.uuid4(), None).to_params()

    with pytest.raises(ValueError, match="неизвестный уровень"):
        Access.from_params({**params, "scope": "all"})


def test_request_hash_ignores_key_order_but_not_values() -> None:
    body = {"from_user_id": "a", "to_user_id": "b", "include": ["contacts"]}

    same = request_hash("reassign_owner", dict(reversed(body.items())))

    assert request_hash("reassign_owner", body) == same
    assert len(same) == 64
    assert request_hash("reassign_owner", {**body, "include": ["companies"]}) != same
    assert request_hash("close_overdue_deals", body) != same


def test_failure_code_takes_app_error_code_or_class_name() -> None:
    assert failure_code(Conflict("bulk.already_running")) == "bulk.already_running"
    assert failure_code(TimeoutError()) == "TimeoutError"


def snapshot(status: str, processed: int) -> BulkJobOut:
    return BulkJobOut(
        id=uuid.UUID(int=1),
        kind="reassign_owner",
        status=status,
        total=10,
        processed=processed,
        failed_rows=0,
        chunks_total=2,
        failed_chunks=0,
        result=None,
        error=None,
        created_at=datetime(2026, 10, 22, tzinfo=UTC),
        started_at=None,
        finished_at=None,
    )


def reader(*snapshots: BulkJobOut | None) -> "Iterator[BulkJobOut | None]":
    yield from snapshots


async def collect(
    first: BulkJobOut, *later: BulkJobOut | None, seconds: float = 60, ping: float = 60
) -> list[bytes]:
    rest = reader(*later)

    async def read() -> BulkJobOut | None:
        return next(rest)

    events = job_events(first, read, poll_seconds=0, stream_seconds=seconds, ping_seconds=ping)
    return [e async for e in events]


def progress(job: BulkJobOut, *, first: bool = False) -> bytes:
    return format_sse_event(
        event="progress", data_str=job.model_dump_json(), retry=RETRY_MS if first else None
    )


async def test_events_only_on_change_and_end_on_final_status() -> None:
    start, half, done = snapshot("running", 0), snapshot("running", 5), snapshot("done", 10)

    events = await collect(start, snapshot("running", 0), half, half, done)

    assert events == [
        progress(start, first=True),  # retry — в первом событии
        progress(half),  # одинаковые снимки событий не дают
        progress(done),
        format_sse_event(event="end", data_str=done.model_dump_json()),
    ]


async def test_ping_when_nothing_changed() -> None:
    start, done = snapshot("pending", 0), snapshot("done", 10)

    events = await collect(start, start, start, done, ping=0)  # ping после каждого пустого опроса

    assert events == [
        progress(start, first=True),
        KEEPALIVE_COMMENT,  # job ждёт в pending — прокси не должен счесть поток мёртвым
        KEEPALIVE_COMMENT,
        progress(done),
        format_sse_event(event="end", data_str=done.model_dump_json()),
    ]


async def test_finished_job_gives_snapshot_and_end_at_once() -> None:
    done = snapshot("partial_failed", 7)

    events = await collect(done)

    assert events == [
        progress(done, first=True),
        format_sse_event(event="end", data_str=done.model_dump_json()),
    ]


async def test_stream_stops_after_time_limit() -> None:
    running = snapshot("running", 1)

    events = await collect(running, seconds=0)  # предел уже вышел: читать больше нечего

    assert events == [progress(running, first=True)]


async def test_stream_stops_when_job_disappears() -> None:
    running = snapshot("running", 1)

    events = await collect(running, None)

    assert events == [progress(running, first=True)]
