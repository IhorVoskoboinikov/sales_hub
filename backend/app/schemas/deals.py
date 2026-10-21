"""Схемы воронок и сделок."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from app.models import DealStatus
from app.schemas.crm import BigId, NotNullFields, OwnerId, RecordOut, Text200, Version

Amount = Annotated[Decimal, Field(ge=0, max_digits=14, decimal_places=2, examples=["100000.00"])]
Currency = Annotated[
    str, Field(pattern=r"^[A-Z]{3}$", examples=["UAH"], description="ISO 4217, курс — НБУ")
]


class StageIn(BaseModel):
    id: int | None = Field(None, description="Есть — изменить этап, нет — добавить новый")
    name: Annotated[str, Field(min_length=1, max_length=100)]
    probability: int = Field(0, ge=0, le=100, description="Вероятность выигрыша, %")


class StageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    position: int
    probability: int


class PipelineIn(BaseModel):
    name: Text200
    stages: list[StageIn] = Field(min_length=1, max_length=20)


class PipelineUpdate(NotNullFields):
    not_null = frozenset({"name", "stages"})

    name: Text200 | None = None
    stages: list[StageIn] | None = Field(
        None,
        min_length=1,
        max_length=20,
        description="Этапы целиком, в нужном порядке. Пропавший этап удаляется — если на нём "
        "нет сделок (в том числе в корзине)",
    )


class PipelineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    stages: list[StageOut]
    created_at: datetime


class DealIn(BaseModel):
    title: Text200
    amount: Amount
    currency: Currency = "UAH"
    stage_id: BigId
    status: DealStatus = DealStatus.OPEN
    contact_id: BigId | None = None
    company_id: BigId | None = None
    expected_close_date: date | None = None
    owner_id: OwnerId = None


class DealUpdate(NotNullFields):
    not_null = frozenset({"title", "amount", "currency", "stage_id", "status", "owner_id"})

    version: Version
    title: Text200 | None = None
    amount: Amount | None = None
    currency: Currency | None = None
    stage_id: BigId | None = None
    status: DealStatus | None = None
    contact_id: BigId | None = None
    company_id: BigId | None = None
    expected_close_date: date | None = None
    owner_id: OwnerId = None


class DealOut(RecordOut):
    title: str
    pipeline_id: int
    stage_id: int
    status: str
    contact_id: int | None
    company_id: int | None
    amount_usd: Decimal
    amount_original: Decimal
    currency_original: str
    fx_rate: Decimal
    fx_date: date
    expected_close_date: date | None
    closed_at: datetime | None


class FxOut(BaseModel):
    """По какому курсу посчитаны суммы ответа (поле amount)."""

    currency: str
    rate: Decimal = Field(description="Единиц валюты за 1 USD по курсу НБУ")
    date: date
    stale: bool = Field(description="true — НБУ недоступен, взят последний известный курс")


class DealListItem(DealOut):
    amount: Decimal = Field(description="amount_usd в валюте ответа (см. fx)")


class DealPageOut(BaseModel):
    items: list[DealListItem]
    next_cursor: str | None
    fx: FxOut


class KanbanCard(BaseModel):
    id: int
    title: str
    amount_usd: Decimal
    amount: Decimal
    owner_id: uuid.UUID


class KanbanStage(StageOut):
    deals_count: int
    amount_usd: Decimal
    amount: Decimal
    deals: list[KanbanCard]


class KanbanOut(BaseModel):
    pipeline_id: int
    stages: list[KanbanStage]
    fx: FxOut
