"""Схемы массовых операций и bulk job (раздел 6, «API этапа 5»)."""

import uuid
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.models import ContactStatus
from app.repositories.contacts import SEARCH_PATTERN
from app.schemas.crm import BigId, Tag

Include = Literal["companies", "contacts", "open_deals", "open_activities"]


class ReassignOwnerIn(BaseModel):
    """Передать записи человека другому. Закрытые сделки и выполненные активности остаются у
    прежнего ответственного (это история), корзина не передаётся. Незнакомое поле — 422, как у
    всех тел массовых операций: опечатка в необязательном поле не должна молча расширять
    операцию до значения по умолчанию."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "from_user_id": "7b0c1f8e-2f4a-4f7e-9a51-0d2d6f1c3a10",
                    "to_user_id": "1c9e4b2a-5d3f-4c8b-8e7a-6f0b9d2e4c21",
                    "include": ["contacts", "open_deals"],
                }
            ]
        },
    )

    from_user_id: uuid.UUID = Field(description="Чьи записи передать (виден вам по member:read)")
    to_user_id: uuid.UUID = Field(
        description="Новый ответственный: активный team_lead или sales_rep в вашем уровне"
    )
    include: list[Include] | None = Field(
        None,
        min_length=1,
        max_length=4,
        description="Что передать; по умолчанию — всё: компании, контакты, открытые сделки и "
        "открытые активности",
    )


class CloseOverdueDealsIn(BaseModel):
    """Закрыть просроченные открытые сделки как lost. «Просрочена» — как у
    GET /deals?overdue=true: дата закрытия раньше before. Этап сделки не меняется.
    Незнакомое поле — 422: `{"pipelineId": 3}` иначе закрыл бы просроченные во всех воронках."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"before": "2026-10-01", "pipeline_id": 1}]},
    )

    before: date | None = Field(
        None,
        description="Граница: сделки с expected_close_date раньше этой даты. По умолчанию — "
        "сегодня по часовому поясу организации; позже сегодня — 422",
    )
    pipeline_id: BigId | None = Field(None, description="Только эта воронка; по умолчанию — все")


class ContactFilterIn(BaseModel):
    """Сегмент контактов — те же поля и правила, что у GET /contacts. Пустой объект — все
    контакты вашего уровня. Незнакомое поле — 422: опечатка не должна превращать сегмент во
    весь уровень."""

    model_config = ConfigDict(extra="forbid")

    q: str | None = Field(
        None,
        max_length=100,
        pattern=SEARCH_PATTERN,
        description="Часть имени, фамилии или email: хотя бы 3 буквы или цифры подряд",
    )
    status: ContactStatus | None = None
    tag: str | None = Field(None, max_length=64, description="У контакта есть этот тег")
    owner_id: uuid.UUID | None = None
    company_id: BigId | None = None


class TagIn(BaseModel):
    """Добавить или снять тег у сегмента контактов. Незнакомое поле — 422."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"tag": "vip", "filter": {"status": "customer"}}]},
    )

    tag: Tag
    filter: ContactFilterIn = Field(description="Сегмент — фильтры GET /contacts; {} — все")


class ImportInfo(BaseModel):
    """Как прочитан файл импорта: разделитель и колонки заголовка (имена — в нижнем регистре)."""

    delimiter: str = Field(description="Разделитель: «,» или «;»")
    columns: list[str] = Field(description="Распознанные колонки в порядке файла")
    ignored_columns: list[str] = Field(
        description="Незнакомые колонки (id, created_at из выгрузки…) — их значения не читаются"
    )


class BulkJobOut(BaseModel):
    """Прогресс job: processed из total строк. Финальные статусы — done, partial_failed,
    failed, cancelled; причина провала — в error. У импорта CSV ещё import_info."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    kind: str
    status: str
    total: int
    processed: int
    failed_rows: int
    chunks_total: int
    failed_chunks: int
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    import_info: ImportInfo | None = None


class ImportErrorOut(BaseModel):
    """Ошибка строки импорта: у строки одна, первая."""

    model_config = ConfigDict(from_attributes=True)

    row_no: int = Field(description="Номер строки файла")
    code: str = Field(
        description="validation, email_required, duplicate_in_file, owner_invalid, "
        "owner_out_of_scope, duplicate_outside_scope…"
    )
    field: str | None = Field(description="Колонка, если ошибка в одном поле")
    raw: Any | None = Field(description="Ячейки строки файла")
