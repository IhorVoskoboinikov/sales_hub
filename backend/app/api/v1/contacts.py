"""Контакты: люди, с которыми работают менеджеры (раздел 3).

Видимость — по уровню contact:read: менеджер видит своих, руководитель — отдел, админ и
наблюдатель — всю организацию. Чужой контакт по прямой ссылке — 404.
"""

import uuid
from typing import Annotated

from fastapi import Depends, Query, status

from app.api.deps import get_contact_service, get_page
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.models import ContactStatus
from app.repositories.contacts import SEARCH_PATTERN, ContactFilters
from app.repositories.pagination import Page
from app.schemas.common import MAX_ID, PageOut, RecordId
from app.schemas.crm import ContactIn, ContactOut, ContactUpdate, DeletedContactOut
from app.services.contacts import ContactService

Search = Annotated[
    str | None,
    Query(
        max_length=100,
        pattern=SEARCH_PATTERN,
        description="Часть имени, фамилии или email: хотя бы 3 буквы или цифры подряд",
    ),
]


@protected_router.get("/contacts", tags=["contacts"], responses={**PROTECTED, **VALIDATION})
async def list_contacts(
    q: Search = None,
    status_: Annotated[ContactStatus | None, Query(alias="status")] = None,
    tag: Annotated[str | None, Query(max_length=64)] = None,
    owner_id: uuid.UUID | None = None,
    company_id: Annotated[int | None, Query(ge=1, le=MAX_ID)] = None,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    contacts: ContactService = Depends(get_contact_service),
) -> PageOut[ContactOut]:
    """Контакты в моём уровне, новые сначала."""
    filters = ContactFilters(q, status_, tag, owner_id, company_id)
    result = await contacts.list(access, filters, page)
    return PageOut(
        items=[ContactOut.model_validate(c) for c in result.items], next_cursor=result.next_cursor
    )


# Раньше /contacts/{contact_id}: иначе Starlette сопоставил бы «trash» с {contact_id}.
@protected_router.get("/contacts/trash", tags=["contacts"], responses=PROTECTED)
async def list_trash(
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    contacts: ContactService = Depends(get_contact_service),
) -> PageOut[DeletedContactOut]:
    """Корзина: удалённые контакты в моём уровне, недавно удалённые сначала."""
    result = await contacts.trash(access, page)
    return PageOut(
        items=[DeletedContactOut.model_validate(c) for c in result.items],
        next_cursor=result.next_cursor,
    )


@protected_router.get(
    "/contacts/{contact_id}", tags=["contacts"], responses={**PROTECTED, **NOT_FOUND}
)
async def get_contact(
    contact_id: RecordId,
    access: Access = Depends(authorize),
    contacts: ContactService = Depends(get_contact_service),
) -> ContactOut:
    return ContactOut.model_validate(await contacts.get(access, contact_id))


@protected_router.post(
    "/contacts",
    tags=["contacts"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION},
)
async def create_contact(
    body: ContactIn,
    access: Access = Depends(authorize),
    contacts: ContactService = Depends(get_contact_service),
) -> ContactOut:
    """Создать контакт. Email уникален в организации: дубль — 409 contact.duplicate_email,
    а в details — кто его ведёт (если контакт вне вашего уровня, его id не раскрывается)."""
    return ContactOut.model_validate(await contacts.create(access, body.model_dump()))


@protected_router.patch(
    "/contacts/{contact_id}",
    tags=["contacts"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT, **VALIDATION},
)
async def update_contact(
    contact_id: RecordId,
    body: ContactUpdate,
    access: Access = Depends(authorize),
    contacts: ContactService = Depends(get_contact_service),
) -> ContactOut:
    """Изменить контакт. version — из последнего GET; передать owner_id может руководитель
    (в своём отделе) или админ."""
    fields = body.model_dump(exclude_unset=True, exclude={"version"})
    contact = await contacts.update(access, contact_id, body.version, fields)
    return ContactOut.model_validate(contact)


@protected_router.delete(
    "/contacts/{contact_id}",
    tags=["contacts"],
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**PROTECTED, **NOT_FOUND},
)
async def delete_contact(
    contact_id: RecordId,
    access: Access = Depends(authorize),
    service: ContactService = Depends(get_contact_service),
) -> None:
    """Удалить в корзину: запись перестаёт быть видна, но 30 дней её можно восстановить."""
    await service.delete(access, contact_id)


@protected_router.post(
    "/contacts/{contact_id}/restore",
    tags=["contacts"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT},
)
async def restore_contact(
    contact_id: RecordId,
    access: Access = Depends(authorize),
    service: ContactService = Depends(get_contact_service),
) -> ContactOut:
    """Восстановить из корзины в текущий отдел ответственного. Ответственный отключён или
    больше не ведёт записи — 409 owner.inactive."""
    return ContactOut.model_validate(await service.restore(access, contact_id))
