import uuid
from datetime import datetime
from typing import Annotated
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


def _check_timezone(name: str) -> str:
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError("unknown timezone") from None
    return name


Name = Annotated[str, Field(min_length=1, max_length=200)]
Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$", examples=["UAH"], description="ISO 4217")]
Timezone = Annotated[
    str, Field(max_length=64, examples=["Europe/Kyiv"]), AfterValidator(_check_timezone)
]


class OrgCreate(BaseModel):
    name: Name
    slug: str = Field(
        pattern=r"^[a-z0-9][a-z0-9-]{1,63}$",
        examples=["acme"],
        description="Короткое имя: латиница, цифры, дефис",
    )
    display_currency: Currency = "UAH"
    timezone: Timezone = "Europe/Kyiv"


class OrgUpdate(BaseModel):
    """Поля, которых нет в запросе, не меняются."""

    name: Name | None = None
    display_currency: Currency | None = None
    timezone: Timezone | None = None


class OrgOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    slug: str
    plan: str
    display_currency: str
    timezone: str
    created_at: datetime


class TeamIn(BaseModel):
    name: Name


class TeamOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    created_at: datetime
