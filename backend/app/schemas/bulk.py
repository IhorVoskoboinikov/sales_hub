"""Схемы массовых операций и bulk job (раздел 6, «API этапа 5»)."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Include = Literal["companies", "contacts", "open_deals", "open_activities"]


class ReassignOwnerIn(BaseModel):
    """Передать записи человека другому. Закрытые сделки и выполненные активности остаются у
    прежнего ответственного (это история), корзина не передаётся."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "from_user_id": "7b0c1f8e-2f4a-4f7e-9a51-0d2d6f1c3a10",
                    "to_user_id": "1c9e4b2a-5d3f-4c8b-8e7a-6f0b9d2e4c21",
                    "include": ["contacts", "open_deals"],
                }
            ]
        }
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


class BulkJobOut(BaseModel):
    """Прогресс job: processed из total строк. Финальные статусы — done, partial_failed,
    failed, cancelled; причина провала — в error."""

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
