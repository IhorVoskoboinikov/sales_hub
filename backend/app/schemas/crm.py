"""Схемы компаний и контактов.

org_id и team_id в теле запроса не принимаются: организация — всегда моя, отдел — всегда
отдел ответственного (раздел 5). owner_id — необязательный: по умолчанию ответственный — я.
"""

import uuid
from datetime import datetime
from typing import Annotated, Any, ClassVar

from pydantic import BaseModel, ConfigDict, EmailStr, Field, ValidationInfo, field_validator

from app.models import ContactStatus
from app.schemas.common import MAX_ID

BigId = Annotated[int, Field(ge=1, le=MAX_ID)]  # ссылка на CRM-запись в теле
Text200 = Annotated[str, Field(min_length=1, max_length=200)]
Tag = Annotated[str, Field(min_length=1, max_length=64)]
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
    first_name: Annotated[str, Field(min_length=1, max_length=100)]
    last_name: str = Field("", max_length=100)
    email: EmailStr | None = None
    phone: str | None = Field(None, max_length=32, examples=["+380671234567"])
    status: ContactStatus = ContactStatus.LEAD
    source: str | None = Field(None, max_length=64)
    tags: list[Tag] = Field(default_factory=list, max_length=20)
    company_id: BigId | None = None
    owner_id: OwnerId = None


class ContactUpdate(NotNullFields):
    not_null = frozenset({"first_name", "last_name", "status", "tags", "owner_id"})

    version: Version
    first_name: Annotated[str, Field(min_length=1, max_length=100)] | None = None
    last_name: str | None = Field(None, max_length=100)
    email: EmailStr | None = None
    phone: str | None = Field(None, max_length=32)
    status: ContactStatus | None = None
    source: str | None = Field(None, max_length=64)
    tags: list[Tag] | None = Field(None, max_length=20)
    company_id: BigId | None = None
    owner_id: OwnerId = None


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
