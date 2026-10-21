import uuid
from datetime import date
from decimal import Decimal

from pydantic import BaseModel

from app.schemas.deals import FxOut


class StageReportRow(BaseModel):
    stage_id: int
    name: str
    position: int
    deals: int
    amount_usd: Decimal
    amount: Decimal


class PipelineReportOut(BaseModel):
    pipeline_id: int
    stages: list[StageReportRow]
    fx: FxOut


class ManagerReportRow(BaseModel):
    owner_id: uuid.UUID
    email: str
    open_deals: int
    open_amount: Decimal
    won_deals: int
    won_amount: Decimal
    lost_deals: int
    lost_amount: Decimal


class ManagersReportOut(BaseModel):
    month_from: date
    month_to: date
    managers: list[ManagerReportRow]
    fx: FxOut
