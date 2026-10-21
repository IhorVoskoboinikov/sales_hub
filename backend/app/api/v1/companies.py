"""Компании-клиенты: общий справочник организации (раздел 3).

Читают все роли организации, меняют — по уровню company:write: менеджер правит только свои,
руководитель — своего отдела. Чужая компания в PATCH — 403 (её видно), а не 404.
"""

from typing import Annotated

from fastapi import Depends, Query, status

from app.api.deps import get_company_service, get_page
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.repositories.contacts import SEARCH_PATTERN
from app.repositories.pagination import Page
from app.schemas.common import PageOut, RecordId
from app.schemas.crm import CompanyIn, CompanyOut, CompanyUpdate
from app.services.contacts import CompanyService

Search = Annotated[
    str | None,
    Query(
        max_length=100,
        pattern=SEARCH_PATTERN,
        description="Часть названия: хотя бы 3 буквы или цифры подряд",
    ),
]


@protected_router.get("/companies", tags=["companies"], responses={**PROTECTED, **VALIDATION})
async def list_companies(
    q: Search = None,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    companies: CompanyService = Depends(get_company_service),
) -> PageOut[CompanyOut]:
    """Компании организации по названию."""
    result = await companies.list(access, q, page)
    return PageOut(
        items=[CompanyOut.model_validate(c) for c in result.items], next_cursor=result.next_cursor
    )


@protected_router.get(
    "/companies/{company_id}", tags=["companies"], responses={**PROTECTED, **NOT_FOUND}
)
async def get_company(
    company_id: RecordId,
    access: Access = Depends(authorize),
    companies: CompanyService = Depends(get_company_service),
) -> CompanyOut:
    return CompanyOut.model_validate(await companies.get(access, company_id))


@protected_router.post(
    "/companies",
    tags=["companies"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **VALIDATION},
)
async def create_company(
    body: CompanyIn,
    access: Access = Depends(authorize),
    companies: CompanyService = Depends(get_company_service),
) -> CompanyOut:
    """Создать компанию. Ответственный по умолчанию — вы; org_admin указывает owner_id."""
    return CompanyOut.model_validate(await companies.create(access, body.model_dump()))


@protected_router.patch(
    "/companies/{company_id}",
    tags=["companies"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT, **VALIDATION},
)
async def update_company(
    company_id: RecordId,
    body: CompanyUpdate,
    access: Access = Depends(authorize),
    companies: CompanyService = Depends(get_company_service),
) -> CompanyOut:
    """Изменить компанию. Поля, которых нет в теле, не меняются; version — из последнего GET:
    если запись успели изменить — 409 version.conflict."""
    fields = body.model_dump(exclude_unset=True, exclude={"version"})
    company = await companies.update(access, company_id, body.version, fields)
    return CompanyOut.model_validate(company)


@protected_router.delete(
    "/companies/{company_id}",
    tags=["companies"],
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**PROTECTED, **NOT_FOUND},
)
async def delete_company(
    company_id: RecordId,
    access: Access = Depends(authorize),
    service: CompanyService = Depends(get_company_service),
) -> None:
    """Удалить в корзину: запись перестаёт быть видна, но 30 дней её можно восстановить."""
    await service.delete(access, company_id)


@protected_router.post(
    "/companies/{company_id}/restore",
    tags=["companies"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT},
)
async def restore_company(
    company_id: RecordId,
    access: Access = Depends(authorize),
    service: CompanyService = Depends(get_company_service),
) -> CompanyOut:
    """Восстановить из корзины в текущий отдел ответственного. Ответственный отключён или
    больше не ведёт записи — 409 owner.inactive."""
    return CompanyOut.model_validate(await service.restore(access, company_id))
