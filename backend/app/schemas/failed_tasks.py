"""Схемы упавших задач Celery (раздел 15, «Задачи, упавшие после всех ретраев»)."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# Игнор mypy: поле kwargs плагин pydantic путает с **kwargs своего __init__.
class FailedTaskOut(BaseModel):  # type: ignore[no-redef]
    """Строка списка — без traceback: он бывает до 20 000 символов."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    task_id: str
    task_name: str
    queue: str | None
    args: list[Any]
    kwargs: dict[str, Any]
    request_id: str | None = Field(description="По нему ищутся строки лога задачи и запроса")
    user_id: uuid.UUID | None
    exc_type: str
    exc_message: str
    retries: int
    failed_at: datetime
    replayed_at: datetime | None
    replayed_by: uuid.UUID | None
    replay_task_id: str | None


class FailedTaskDetailOut(FailedTaskOut):  # type: ignore[no-redef]
    traceback: str | None


class ReplayOut(BaseModel):
    task_id: str = Field(description="id новой задачи Celery — её видно во Flower")
