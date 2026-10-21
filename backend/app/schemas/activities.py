"""Схемы активностей, заметок и журнала."""

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.models import ActivityType
from app.schemas.crm import BigId, NotNullFields, OwnerId

Subject = Annotated[str, Field(min_length=1, max_length=300)]


class ActivityIn(BaseModel):
    type: ActivityType
    subject: Subject
    # AwareDatetime: время только с часовым поясом (…+03:00 или …Z). Без него «10:00»
    # означало бы время часов сервера — у каждого стенда своё.
    due_at: AwareDatetime | None = Field(
        None, description="Срок. Не указан — активность уже состоялась (звонок, письмо)"
    )
    deal_id: BigId | None = None
    contact_id: BigId | None = None
    owner_id: OwnerId = None


class ActivityUpdate(NotNullFields):
    not_null = frozenset({"subject", "due_at", "done", "owner_id"})

    subject: Subject | None = None
    due_at: AwareDatetime | None = None
    done: bool | None = Field(None, description="true — выполнено сейчас, false — снова открыть")
    owner_id: OwnerId = None


class ActivityOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    type: str
    subject: str
    deal_id: int | None
    contact_id: int | None
    team_id: uuid.UUID
    owner_id: uuid.UUID
    due_at: datetime | None
    done_at: datetime | None
    created_at: datetime


class NoteIn(BaseModel):
    body: Annotated[str, Field(min_length=1, max_length=10_000)]


class NoteOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    author_id: uuid.UUID
    body: str
    created_at: datetime


class AuditOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    occurred_at: datetime
    actor_id: uuid.UUID | None
    action: str
    entity_type: str
    entity_id: str
    changes: dict[str, Any]
    request_id: str | None
