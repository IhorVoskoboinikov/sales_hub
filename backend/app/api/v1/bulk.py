"""Массовые операции (раздел 6): запуск job. Ответ — сразу 202 с job и Location, работу делают
воркеры; прогресс — GET /bulk-jobs/{job_id} или поток SSE …/events."""

from typing import Annotated

from fastapi import Depends, Header, Response, status

from app.api.deps import get_bulk_service
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.models import BulkJobKind
from app.repositories.contacts import ContactFilters
from app.schemas.bulk import BulkJobOut, CloseOverdueDealsIn, ReassignOwnerIn, TagIn
from app.services.bulk import REASSIGN_INCLUDE, BulkService, ReassignOwnerRequest

# Повтор запроса (раздел 6): 1–64 печатных ASCII-символа, иначе 422.
IdempotencyKey = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=64,
        pattern=r"^[ -~]+$",
        description="Необязательный ключ повтора: тот же ключ и то же тело — та же job",
    ),
]


def job_location(response: Response, job: BulkJobOut) -> BulkJobOut:
    response.headers["Location"] = f"/api/v1/bulk-jobs/{job.id}"
    return job


@protected_router.post(
    "/bulk/reassign-owner",
    tags=["bulk"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION},
)
async def reassign_owner(
    body: ReassignOwnerIn,
    response: Response,
    idempotency_key: IdempotencyKey = None,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> BulkJobOut:
    """Передать компании, контакты, открытые сделки и открытые активности человека другому.

    - `from_user_id` не виден вам по `member:read` — 422 `validation` (поле `from_user_id`);
    - тот же человек — 422 `bulk.same_owner`;
    - получатель не ведёт записи — 422 `owner.invalid`, вне вашего уровня — 403 `out_of_scope`;
    - такая операция в организации уже идёт — 409 `bulk.already_running` (`details.job_id`);
    - тот же `Idempotency-Key` с другим телом — 422 `bulk.idempotency_key_reused`."""
    request = ReassignOwnerRequest(
        from_user_id=body.from_user_id,
        to_user_id=body.to_user_id,
        include=tuple(body.include) if body.include else tuple(REASSIGN_INCLUDE),
    )
    job = await bulk.start_reassign_owner(access, request, idempotency_key)
    return job_location(response, BulkJobOut.model_validate(job))


@protected_router.post(
    "/bulk/close-overdue-deals",
    tags=["bulk"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION},
)
async def close_overdue_deals(
    body: CloseOverdueDealsIn,
    response: Response,
    idempotency_key: IdempotencyKey = None,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> BulkJobOut:
    """Закрыть как `lost` открытые сделки вашего уровня с датой закрытия раньше `before` —
    только вручную: автоматическое закрытие без человека портило бы воронку. Этап не меняется.

    - `before` позже сегодняшней даты организации — 422 `validation` (поле `before`);
    - воронки нет в организации — 422 `validation` (поле `pipeline_id`);
    - такая операция в организации уже идёт — 409 `bulk.already_running` (`details.job_id`);
    - тот же `Idempotency-Key` с другим телом — 422 `bulk.idempotency_key_reused`."""
    job = await bulk.start_close_overdue_deals(
        access, body.before, body.pipeline_id, idempotency_key
    )
    return job_location(response, BulkJobOut.model_validate(job))


async def _tags(
    kind: BulkJobKind,
    body: TagIn,
    response: Response,
    idempotency_key: str | None,
    access: Access,
    bulk: BulkService,
) -> BulkJobOut:
    f = body.filter
    filters = ContactFilters(f.q, f.status, f.tag, f.owner_id, f.company_id)
    job = await bulk.start_tags(access, kind, body.tag, filters, idempotency_key)
    return job_location(response, BulkJobOut.model_validate(job))


@protected_router.post(
    "/bulk/add-tags",
    tags=["bulk"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION},
)
async def add_tags(
    body: TagIn,
    response: Response,
    idempotency_key: IdempotencyKey = None,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> BulkJobOut:
    """Добавить тег контактам сегмента (фильтры `GET /contacts`, `{}` — весь ваш уровень).
    У кого тег уже есть — не меняются; у кого уже 20 тегов — пропускаются и считаются в
    `failed_rows` (job закончится `partial_failed`).

    - такая операция в организации уже идёт — 409 `bulk.already_running` (`details.job_id`);
    - тот же `Idempotency-Key` с другим телом — 422 `bulk.idempotency_key_reused`."""
    return await _tags(BulkJobKind.ADD_TAGS, body, response, idempotency_key, access, bulk)


@protected_router.post(
    "/bulk/remove-tags",
    tags=["bulk"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION},
)
async def remove_tags(
    body: TagIn,
    response: Response,
    idempotency_key: IdempotencyKey = None,
    access: Access = Depends(authorize),
    bulk: BulkService = Depends(get_bulk_service),
) -> BulkJobOut:
    """Снять тег с контактов сегмента (фильтры `GET /contacts`, `{}` — весь ваш уровень);
    меняются только контакты, у которых этот тег есть.

    - такая операция в организации уже идёт — 409 `bulk.already_running` (`details.job_id`);
    - тот же `Idempotency-Key` с другим телом — 422 `bulk.idempotency_key_reused`."""
    return await _tags(BulkJobKind.REMOVE_TAGS, body, response, idempotency_key, access, bulk)
