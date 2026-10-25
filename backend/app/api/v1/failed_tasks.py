"""Задачи Celery, упавшие после всех ретраев (раздел 15): только супер-админ (admin:system).

Список — новые сначала, keyset, без traceback; карточка — со всеми полями; replay ставит ту же
задачу снова (bulk и импорт — нет: их повторяют новой job).
"""

from typing import Annotated

from fastapi import Depends, Path, Query, status

from app.api.deps import current_user, get_failed_task_service, get_page
from app.api.v1 import admin_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION
from app.repositories.failed_tasks import FailedTaskFilters
from app.repositories.pagination import Page
from app.schemas.common import MAX_ID, PageOut
from app.schemas.failed_tasks import FailedTaskDetailOut, FailedTaskOut, ReplayOut
from app.services.auth import CurrentUser
from app.services.failed_tasks import FailedTaskService

FailedTaskId = Annotated[int, Path(ge=1, le=MAX_ID)]


@admin_router.get("/failed-tasks", responses={**PROTECTED, **VALIDATION})
async def list_failed_tasks(
    task_name: Annotated[str | None, Query(max_length=255, examples=["bulk.chunk"])] = None,
    replayed: Annotated[
        bool | None, Query(description="true — только перезапущенные, false — ещё нет")
    ] = None,
    page: Page = Depends(get_page),
    failed: FailedTaskService = Depends(get_failed_task_service),
) -> PageOut[FailedTaskOut]:
    """Упавшие задачи, новые сначала. traceback — в карточке задачи."""
    result = await failed.list(FailedTaskFilters(task_name, replayed), page)
    return PageOut(
        items=[FailedTaskOut.model_validate(t) for t in result.items],
        next_cursor=result.next_cursor,
    )


@admin_router.get("/failed-tasks/{failed_task_id}", responses={**PROTECTED, **NOT_FOUND})
async def get_failed_task(
    failed_task_id: FailedTaskId,
    failed: FailedTaskService = Depends(get_failed_task_service),
) -> FailedTaskDetailOut:
    return FailedTaskDetailOut.model_validate(await failed.get(failed_task_id))


@admin_router.post(
    "/failed-tasks/{failed_task_id}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT},
)
async def replay_failed_task(
    failed_task_id: FailedTaskId,
    admin: CurrentUser = Depends(current_user),
    failed: FailedTaskService = Depends(get_failed_task_service),
) -> ReplayOut:
    """Поставить ту же задачу с теми же аргументами ещё раз.

    - уже перезапущена — 409 `failed_task.already_replayed`;
    - такой задачи больше нет (переименовали) — 409 `failed_task.unknown_task`;
    - задачи bulk и импорта — 409 `failed_task.not_replayable`: их повторяют новой job;
    - брокер недоступен — 503 `unavailable`, строка не меняется."""
    return ReplayOut(task_id=await failed.replay(failed_task_id, admin.id))
