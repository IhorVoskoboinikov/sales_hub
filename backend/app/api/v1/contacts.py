"""Контакты: люди, с которыми работают менеджеры (раздел 3).

Видимость — по уровню contact:read: менеджер видит своих, руководитель — отдел, админ и
наблюдатель — всю организацию. Чужой контакт по прямой ссылке — 404.
"""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, Query, Request, Response, status
from starlette.datastructures import FormData, UploadFile
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.deps import get_app_settings, get_contact_service, get_import_service, get_page
from app.api.v1 import protected_router
from app.api.v1.bulk import IdempotencyKey, job_location
from app.api.v1.errors import (
    CONFLICT,
    LENGTH_REQUIRED,
    NOT_FOUND,
    PAYLOAD_TOO_LARGE,
    PROTECTED,
    VALIDATION,
)
from app.authz.access import Access
from app.authz.deps import authorize
from app.core.config import Settings
from app.errors import LengthRequired, PayloadTooLarge, ValidationFailed
from app.models import ContactStatus
from app.repositories.contacts import SEARCH_PATTERN, ContactFilters
from app.repositories.pagination import Page
from app.schemas.bulk import BulkJobOut
from app.schemas.common import MAX_ID, PageOut, RecordId
from app.schemas.crm import ContactIn, ContactOut, ContactUpdate, DeletedContactOut
from app.services.contacts import ContactService
from app.services.imports import HEADER_BYTES, ImportService, ImportUpload, read_header

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


# --- Импорт CSV (раздел 6, «Поток: импорт контактов из CSV»)

# Тело запроса больше файла на границы multipart, заголовки части и поле owner_id.
FORM_OVERHEAD_BYTES = 64 * 1024
FORM_FIELDS = frozenset({"file", "owner_id"})
# Схема формы для Swagger: обработчик не объявляет UploadFile (см. import_contacts).
IMPORT_FORM: dict[str, Any] = {
    "requestBody": {
        "required": True,
        "content": {
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "required": ["file"],
                    "properties": {
                        "file": {
                            "type": "string",
                            "contentMediaType": "text/csv",
                            "description": "CSV в UTF-8 (BOM можно), разделитель «,» или «;», "
                            "заголовок обязателен: email, first_name, last_name, phone, status, "
                            "source, tags, owner_email",
                        },
                        "owner_id": {
                            "type": "string",
                            "format": "uuid",
                            "description": "Ответственный строк без owner_email: активный "
                            "team_lead или sales_rep. Не указан — вы сами; org_admin обязан "
                            "указать",
                        },
                    },
                }
            }
        },
    }
}


@asynccontextmanager
async def import_form(request: Request) -> AsyncIterator[FormData]:
    """Форма запроса: временный файл Starlette закрывается при выходе. Лишний файл или поле,
    битый multipart — ошибка парсера Starlette (400) — 422 import.invalid_form."""
    try:
        form = await request.form(max_files=1, max_fields=1)
    except StarletteHTTPException as exc:
        raise ValidationFailed("import.invalid_form", reason=exc.detail) from None
    try:
        yield form
    finally:
        await form.close()


def ensure_length(request: Request, max_bytes: int) -> None:
    """До чтения тела. Без Content-Length (Transfer-Encoding: chunked) тело читалось бы без
    предела — 411. Больше лимита — 413. Больше Content-Length сервер не прочитает: uvicorn
    (h11) не отдаёт приложению байты сверх него."""
    length = request.headers.get("content-length", "")
    if not length.isdigit():
        raise LengthRequired("import.length_required")
    if int(length) > max_bytes + FORM_OVERHEAD_BYTES:
        raise PayloadTooLarge("import.file_too_large", max_bytes=max_bytes)


async def read_upload(form: FormData, max_bytes: int) -> ImportUpload:
    """Файл и owner_id из формы, заголовок CSV — по первым 64 КиБ файла (ошибки формата — 422
    сразу, без job)."""
    file = form.get("file")
    # UploadFile Starlette, а не FastAPI: форму разбирает Starlette, и файлы в ней — его класса.
    if not isinstance(file, UploadFile) or not FORM_FIELDS.issuperset(form.keys()):
        raise ValidationFailed("import.invalid_form")
    size = file.size or 0
    if size > max_bytes:
        raise PayloadTooLarge("import.file_too_large", max_bytes=max_bytes)
    head = await file.read(HEADER_BYTES)
    await file.seek(0)
    header = read_header(head, whole_file=size <= HEADER_BYTES)
    raw_owner = form.get("owner_id")
    if raw_owner is not None and not isinstance(raw_owner, str):
        raise ValidationFailed("import.invalid_form")
    try:
        owner_id = uuid.UUID(raw_owner) if raw_owner else None
    except ValueError:
        raise ValidationFailed(field="owner_id") from None
    return ImportUpload(file.file, size, file.filename or "import.csv", header, owner_id)


# Раньше /contacts/{contact_id}: так путь «import» не спутать с id контакта.
@protected_router.post(
    "/contacts/import",
    tags=["contacts"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION, **LENGTH_REQUIRED, **PAYLOAD_TOO_LARGE},
    openapi_extra=IMPORT_FORM,
)
async def import_contacts(
    request: Request,
    response: Response,
    idempotency_key: IdempotencyKey = None,
    access: Access = Depends(authorize),
    imports: ImportService = Depends(get_import_service),
    settings: Settings = Depends(get_app_settings),
) -> BulkJobOut:
    """Импорт контактов из CSV с дедупликацией по email: ответ — сразу 202 с job и Location,
    строки разбирает воркер; ошибки строк — `GET /bulk-jobs/{job_id}/errors`.

    Существующий контакт с тем же email обновляется, если он в вашем уровне: непустая ячейка
    перезаписывает поле, пустая не трогает, теги объединяются (не больше 20), ответственный не
    меняется. Незнакомые колонки игнорируются — они в `import_info.ignored_columns`.

    - нет `Content-Length` (загрузка частями) — 411 `import.length_required`;
    - файл больше 100 МиБ — 413 `import.file_too_large`;
    - тот же `Idempotency-Key` — та же job, файл не читается;
    - импорт в организации уже идёт — 409 `bulk.already_running` (`details.job_id`);
    - не та форма (нет `file`, лишнее поле или файл) — 422 `import.invalid_form`;
    - не UTF-8 — 422 `import.invalid_encoding`; нет `email` или `first_name` или колонка
      повторяется — 422 `import.invalid_header`; пустой файл — 422 `import.empty_file`;
    - `owner_id` не указан у org_admin — 422 `owner.required`, не ведёт записи — 422
      `owner.invalid`, вне вашего уровня — 403 `out_of_scope`."""
    # Форма читается здесь, а не параметром UploadFile: FastAPI прочитал бы тело до
    # зависимостей, и клиент без токена успел бы залить 100 МиБ до ответа 401.
    ensure_length(request, settings.import_max_bytes)
    existing = await imports.before_upload(access, idempotency_key)
    if existing is not None:
        return job_location(response, BulkJobOut.model_validate(existing))
    async with import_form(request) as form:
        upload = await read_upload(form, settings.import_max_bytes)
        job = await imports.start(access, upload, idempotency_key)
    return job_location(response, BulkJobOut.model_validate(job))


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
