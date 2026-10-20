from typing import Any

from pydantic import BaseModel, Field


class ErrorOut(BaseModel):
    """Тело любой ошибки (по мотивам RFC 9457 problem+json)."""

    type: str = Field(examples=["https://saleshub.local/errors/not_found"])
    code: str = Field(description="Стабильный английский ключ: по нему работают клиенты и тесты")
    title: str = Field(description="Текст для человека, по-украински")
    status: int
    request_id: str | None = Field(description="Тот же, что в заголовке X-Request-ID")
    details: dict[str, Any] = Field(default_factory=dict)
