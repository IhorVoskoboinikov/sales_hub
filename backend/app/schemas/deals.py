"""Схемы воронок и сделок."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.models import DealStatus
from app.schemas.crm import NotNullFields, OwnerId, RecordOut, Text200, Version

Amount = Annotated[Decimal, Field(ge=0, max_digits=14, decimal_places=2, examples=["100000.00"])]
# Этап 4.3: суммы только в долларах; любая валюта по курсу НБУ — шаг 4.4.
Currency = Annotated[Literal["USD"], Field(description="ISO 4217")]


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
    currency: Currency = "USD"
    stage_id: int
    status: DealStatus = DealStatus.OPEN
    contact_id: int | None = None
    company_id: int | None = None
    expected_close_date: date | None = None
    owner_id: OwnerId = None


class DealUpdate(NotNullFields):
    not_null = frozenset({"title", "amount", "currency", "stage_id", "status", "owner_id"})

    version: Version
    title: Text200 | None = None
    amount: Amount | None = None
    currency: Currency | None = None
    stage_id: int | None = None
    status: DealStatus | None = None
    contact_id: int | None = None
    company_id: int | None = None
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


class KanbanCard(BaseModel):
    id: int
    title: str
    amount_usd: Decimal
    owner_id: uuid.UUID


class KanbanStage(StageOut):
    deals_count: int
    amount_usd: Decimal
    deals: list[KanbanCard]


class KanbanOut(BaseModel):
    pipeline_id: int
    stages: list[KanbanStage]
