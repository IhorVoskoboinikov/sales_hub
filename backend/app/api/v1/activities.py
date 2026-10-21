"""Активности (звонки, встречи, письма, задачи) и заметки к контактам (раздел 3).

GET /activities работает в двух режимах:
- с deal_id или contact_id — история карточки: родитель проверяется по уровню его чтения,
  дальше видна вся история (раздел 5, «Активности и заметки видны по родителю»);
- без них — открытые задачи в уровне activity:read по сроку: ?due=today | overdue.
"""

from typing import Annotated, Literal

from fastapi import Depends, Query, status

from app.api.deps import get_activity_service, get_note_service, get_page
from app.api.v1 import protected_router
from app.api.v1.errors import NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.repositories.pagination import Page
from app.schemas.activities import ActivityIn, ActivityOut, ActivityUpdate, NoteIn, NoteOut
from app.schemas.common import MAX_ID, PageOut, RecordId
from app.services.activities import ActivityService, NoteService

OptionalId = Annotated[int | None, Query(ge=1, le=MAX_ID)]


@protected_router.get(
    "/activities", tags=["activities"], responses={**PROTECTED, **NOT_FOUND, **VALIDATION}
)
async def list_activities(
    deal_id: OptionalId = None,
    contact_id: OptionalId = None,
    due: Annotated[
        Literal["today", "overdue"] | None,
        Query(description="Для списка задач: на сегодня или просроченные"),
    ] = None,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    activities: ActivityService = Depends(get_activity_service),
) -> PageOut[ActivityOut]:
    """История сделки или контакта (новые сначала) либо мои открытые задачи (по сроку)."""
    if deal_id is not None or contact_id is not None:
        result = await activities.history(access, deal_id, contact_id, page)
    else:
        result = await activities.tasks(access, due, page)
    return PageOut(
        items=[ActivityOut.model_validate(a) for a in result.items],
        next_cursor=result.next_cursor,
    )


@protected_router.post(
    "/activities",
    tags=["activities"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **VALIDATION},
)
async def create_activity(
    body: ActivityIn,
    access: Access = Depends(authorize),
    activities: ActivityService = Depends(get_activity_service),
) -> ActivityOut:
    """Запланировать (со сроком due_at) или записать уже состоявшуюся активность (без срока)."""
    return ActivityOut.model_validate(await activities.create(access, body.model_dump()))


@protected_router.patch(
    "/activities/{activity_id}",
    tags=["activities"],
    responses={**PROTECTED, **NOT_FOUND, **VALIDATION},
)
async def update_activity(
    activity_id: RecordId,
    body: ActivityUpdate,
    access: Access = Depends(authorize),
    activities: ActivityService = Depends(get_activity_service),
) -> ActivityOut:
    """Изменить тему, срок, ответственного или отметить выполненной (done: true)."""
    fields = body.model_dump(exclude_unset=True)
    return ActivityOut.model_validate(await activities.update(access, activity_id, fields))


@protected_router.delete(
    "/activities/{activity_id}",
    tags=["activities"],
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**PROTECTED, **NOT_FOUND},
)
async def delete_activity(
    activity_id: RecordId,
    access: Access = Depends(authorize),
    activities: ActivityService = Depends(get_activity_service),
) -> None:
    """Удалить насовсем: у активностей корзины нет."""
    await activities.delete(access, activity_id)


@protected_router.get(
    "/contacts/{contact_id}/notes", tags=["contacts"], responses={**PROTECTED, **NOT_FOUND}
)
async def list_notes(
    contact_id: RecordId,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    notes: NoteService = Depends(get_note_service),
) -> PageOut[NoteOut]:
    """Заметки контакта, новые сначала."""
    result = await notes.list(access, contact_id, page)
    return PageOut(
        items=[NoteOut.model_validate(n) for n in result.items], next_cursor=result.next_cursor
    )


@protected_router.post(
    "/contacts/{contact_id}/notes",
    tags=["contacts"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **NOT_FOUND, **VALIDATION},
)
async def create_note(
    contact_id: RecordId,
    body: NoteIn,
    access: Access = Depends(authorize),
    notes: NoteService = Depends(get_note_service),
) -> NoteOut:
    """Добавить заметку: может тот, кто может менять контакт."""
    return NoteOut.model_validate(await notes.create(access, contact_id, body.body))
