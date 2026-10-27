"""Прогресс bulk job (разделы 6 и 15): снимок, поток SSE, ошибки строк импорта и отмена.

Видимость — по уровню bulk_job:read, где owner_id job — её автор: руководитель видит job
своего отдела, наблюдатель — только свои. Чужая job — 404.
"""

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

from fastapi import Depends, status
from fastapi.responses import StreamingResponse
from fastapi.sse import KEEPALIVE_COMMENT, EventSourceResponse, format_sse_event

from app.api.deps import SessionScope, get_bulk_service, get_page, get_session_scope, get_uow
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION, Responses
from app.authz.access import Access
from app.authz.deps import authorize
from app.errors import NotFound
from app.models import FINAL_STATUSES
from app.repositories.bulk import BulkJobRepository
from app.repositories.pagination import Page
from app.schemas.bulk import BulkJobOut, ImportErrorOut
from app.schemas.common import PageOut
from app.services.bulk import BulkService
from app.uow import UnitOfWork

POLL_SECONDS = 1.0  # как часто поток перечитывает job
STREAM_SECONDS = 300.0  # поток живёт не дольше 5 минут, потом клиент переподключается
RETRY_MS = 3000  # через сколько клиенту переподключиться (поле retry первого события)
# Комментарий `: ping`, если событий не было 15 с, — как у FastAPI в response_class-пути: иначе
# прокси с таймаутом простоя (60 с у nginx) рвал бы поток, пока job ждёт в pending.
PING_SECONDS = 15.0


@protected_router.get("/bulk-jobs/{job_id}", tags=["bulk"], responses={**PROTECTED, **NOT_FOUND})
async def get_bulk_job(
    job_id: uuid.UUID,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> BulkJobOut:
    return BulkJobOut.model_validate(await bulk.get(access, job_id))


@protected_router.get(
    "/bulk-jobs/{job_id}/errors",
    tags=["bulk"],
    responses={**PROTECTED, **NOT_FOUND, **VALIDATION},
)
async def bulk_job_errors(
    job_id: uuid.UUID,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> PageOut[ImportErrorOut]:
    """Ошибки строк импорта CSV по порядку файла: у строки одна, первая ошибка. Job ищется
    так же, как в `GET /bulk-jobs/{job_id}`: не видна — 404. У других видов job — пустой
    список."""
    result = await bulk.import_errors(access, job_id, page)
    return PageOut(
        items=[ImportErrorOut.model_validate(e) for e in result.items],
        next_cursor=result.next_cursor,
    )


@protected_router.post(
    "/bulk-jobs/{job_id}/cancel",
    tags=["bulk"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT},
)
async def cancel_bulk_job(
    job_id: uuid.UUID,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> BulkJobOut:
    """Отменить job. Ещё не начатая (`pending`) отменяется сразу, идущая (`running`) переходит
    в `cancelling`: начатый чанк доработает, следующие пропускаются, финал — `cancelled`.
    Уже изменённые строки остаются изменёнными — сколько успели, видно в `processed`.

    - уже отменяется или отменена — 202 без изменений;
    - завершена (`done`, `partial_failed`, `failed`) или это перенос записей при смене отдела
      (`move_team`) — 409 `bulk.not_cancellable`."""
    return BulkJobOut.model_validate(await bulk.cancel(access, job_id))


# Не response_class=EventSourceResponse: с ним FastAPI ждёт эндпоинт-генератор и запускает его
# уже внутри ответа 200 — 404 на чужую job и возврат соединения в пул до потока были бы
# невозможны. Поэтому эндпоинт возвращает готовый EventSourceResponse, а тип ответа для
# Swagger описан здесь.
SSE_RESPONSE: Responses = {
    200: {
        "description": "Поток событий progress и end; data — BulkJobOut в JSON",
        "content": {"text/event-stream": {"schema": {"type": "string"}}},
    }
}


@protected_router.get(
    "/bulk-jobs/{job_id}/events",
    tags=["bulk"],
    response_class=StreamingResponse,  # без application/json в схеме ответа 200
    responses={**SSE_RESPONSE, **PROTECTED, **NOT_FOUND},
)
async def bulk_job_events(
    job_id: uuid.UUID,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
    uow: UnitOfWork = Depends(get_uow),
    session_scope: SessionScope = Depends(get_session_scope),
) -> EventSourceResponse:
    """Прогресс потоком SSE: `event: progress` при каждом изменении job, `event: end` на
    финальном статусе — и поток закрывается. Поток живёт не дольше 5 минут (`retry: 3000` —
    клиент переподключится со свежим токеном). Аутентификация — обычный
    `Authorization: Bearer`: читать через fetch или `curl -N`, токен в URL не кладём."""
    first = BulkJobOut.model_validate(await bulk.get(access, job_id))  # чужая job — 404 сразу
    # Сессия запроса закрывается только после ответа, а поток длится минуты: вернуть её
    # соединение в пул сейчас, иначе полтора десятка открытых вкладок исчерпали бы пул.
    await uow.rollback()

    async def read() -> BulkJobOut | None:
        async with session_scope() as session:  # короткая сессия на каждый опрос
            try:
                job = await BulkJobRepository(session).get_visible_or_404(job_id, access)
            except NotFound:
                return None  # job удалила очистка (старше 90 дней)
            return BulkJobOut.model_validate(job)

    return EventSourceResponse(job_events(first, read))


async def job_events(
    first: BulkJobOut,
    read: Callable[[], Awaitable[BulkJobOut | None]],
    *,
    poll_seconds: float = POLL_SECONDS,
    stream_seconds: float = STREAM_SECONDS,
    ping_seconds: float = PING_SECONDS,
) -> AsyncIterator[bytes]:
    """События потока: progress — когда снимок job изменился, end — на финальном статусе,
    `: ping` — если ничего не отправляли ping_seconds. Без Redis pub/sub: опрос раз в секунду
    короткой сессией дешевле для десятка зрителей."""
    deadline = time.monotonic() + stream_seconds
    job: BulkJobOut | None = first
    last: BulkJobOut | None = None
    sent_at = time.monotonic()
    while job is not None:
        if job != last:
            yield format_sse_event(
                event="progress",
                data_str=job.model_dump_json(),
                retry=RETRY_MS if last is None else None,
            )
            last, sent_at = job, time.monotonic()
        elif time.monotonic() - sent_at >= ping_seconds:
            yield KEEPALIVE_COMMENT
            sent_at = time.monotonic()
        if job.status in FINAL_STATUSES:
            yield format_sse_event(event="end", data_str=job.model_dump_json())
            return
        if time.monotonic() >= deadline:
            return
        await asyncio.sleep(poll_seconds)
        job = await read()
