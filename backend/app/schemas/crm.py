"""Схемы компаний и контактов.

org_id и team_id в теле запроса не принимаются: организация — всегда моя, отдел — всегда
отдел ответственного (раздел 5). owner_id — необязательный: по умолчанию ответственный — я.

Типы полей контакта (FirstName, Phone, TagList…) — общие для API и импорта CSV (раздел 6):
POST /contacts и строка файла принимают одинаковые email, телефоны и теги.
"""

import uuid
from datetime import datetime
from typing import Annotated, Any, ClassVar

from pydantic import BaseModel, ConfigDict, EmailStr, Field, ValidationInfo, field_validator

from app.models import ContactStatus
from app.schemas.common import MAX_ID

BigId = Annotated[int, Field(ge=1, le=MAX_ID)]  # ссылка на CRM-запись в теле
Text200 = Annotated[str, Field(min_length=1, max_length=200)]
# Без запятой и пробелов по краям: в ячейке CSV теги — через запятую, и тег «a,b» после
# выгрузки и загрузки обратно стал бы двумя (раздел 6). Уже сохранённые теги не меняются.
Tag = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[^,\s](?:[^,]*[^,\s])?$")]
TagList = Annotated[list[Tag], Field(max_length=20)]  # тегов у контакта — не больше 20
FirstName = Annotated[str, Field(min_length=1, max_length=100)]
LastName = Annotated[str, Field(max_length=100)]
Phone = Annotated[str, Field(max_length=32, examples=["+380671234567"])]
Source = Annotated[str, Field(max_length=64)]
Version = Annotated[
    int,
    Field(ge=1, description="Версия, которую вы видели (из ответа GET). Не совпала — 409"),
]
OwnerId = Annotated[
    uuid.UUID | None,
    Field(
        description="Ответственный: активный team_lead или sales_rep. Не указан — вы сами; "
        "org_admin обязан указать"
    ),
]


class NotNullFields(BaseModel):
    """PATCH: поле не передано — не меняется. Явный null — «очистить», но только у
    необязательных полей; у обязательных (имя, статус…) null — ошибка 422."""

    not_null: ClassVar[frozenset[str]] = frozenset()

    @field_validator("*", mode="before")
    @classmethod
    def required_not_null(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None and info.field_name in cls.not_null:
            raise ValueError("null is not allowed")
        return value


class RecordOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    team_id: uuid.UUID
    owner_id: uuid.UUID
    created_at: datetime
    updated_at: datetime
    version: int


# --- Компании


class CompanyIn(BaseModel):
    name: Text200
    domain: str | None = Field(None, max_length=253, examples=["acme.ua"])
    industry: str | None = Field(None, max_length=100)
    size: int | None = Field(None, ge=1, le=2**31 - 1, description="Число сотрудников")
    owner_id: OwnerId = None


class CompanyUpdate(NotNullFields):
    not_null = frozenset({"name", "owner_id"})

    version: Version
    name: Text200 | None = None
    domain: str | None = Field(None, max_length=253)
    industry: str | None = Field(None, max_length=100)
    size: int | None = Field(None, ge=1, le=2**31 - 1)
    owner_id: OwnerId = None


class CompanyOut(RecordOut):
    name: str
    domain: str | None
    industry: str | None
    size: int | None


# --- Контакты


class ContactIn(BaseModel):
    first_name: FirstName
    last_name: LastName = ""
    email: EmailStr | None = None
    phone: Phone | None = None
    status: ContactStatus = ContactStatus.LEAD
    source: Source | None = None
    tags: TagList = Field(default_factory=list)
    company_id: BigId | None = None
    owner_id: OwnerId = None


class ContactUpdate(NotNullFields):
    not_null = frozenset({"first_name", "last_name", "status", "tags", "owner_id"})

    version: Version
    first_name: FirstName | None = None
    last_name: LastName | None = None
    email: EmailStr | None = None
    phone: Phone | None = None
    status: ContactStatus | None = None
    source: Source | None = None
    tags: TagList | None = None
    company_id: BigId | None = None
    owner_id: OwnerId = None


# Импорт CSV (раздел 6): ведущий апостроф перед этими символами снимается — пара к экранированию
# формул в выгрузке (CSV-инъекция).
FORMULA_PREFIXES = ("=", "+", "-", "@")


def looks_like_formula(text: str) -> bool:
    """Начинается ли текст — после ведущих апострофов — с = + - @. Общее правило выгрузки и
    импорта: выгрузка добавляет такому значению апостроф, импорт снимает ровно один. Поэтому и
    значение, которое само начинается с «'=», переживает обратную загрузку: «'=1» выгружается
    как «''=1» и возвращается как «'=1»."""
    return text.lstrip("'").startswith(FORMULA_PREFIXES)


class ContactImportRow(BaseModel):
    """Строка CSV импорта: те же типы полей, что у ContactIn, но email обязателен — иначе
    повторная загрузка того же файла плодила бы дубли. Пустая ячейка — None: у существующего
    контакта такое поле не меняется. tags — через запятую в одной ячейке. Компании в импорте
    нет (имя компании не уникально), ответственный — owner_email или default_owner_id job."""

    email: EmailStr
    first_name: FirstName
    last_name: LastName | None = None
    phone: Phone | None = None
    status: ContactStatus | None = None
    source: Source | None = None
    tags: TagList | None = None
    owner_email: EmailStr | None = None

    @field_validator("*", mode="before")
    @classmethod
    def cell(cls, value: Any, info: ValidationInfo) -> Any:
        """Ячейка файла → значение поля: пробелы по краям и экранирующий апостроф снимаются,
        пустая ячейка — None, теги делятся по запятой (пустые и повторы отбрасываются)."""
        if not isinstance(value, str):
            return value
        value = value.strip()
        if value[:1] == "'" and looks_like_formula(value[1:]):
            value = value[1:]
        if not value:
            return None
        if info.field_name == "tags":
            return list(dict.fromkeys(t for t in (part.strip() for part in value.split(",")) if t))
        return value


class ContactOut(RecordOut):
    first_name: str
    last_name: str
    email: str | None
    phone: str | None
    status: str
    source: str | None
    tags: list[str]
    company_id: int | None


class DeletedContactOut(ContactOut):
    deleted_at: datetime
