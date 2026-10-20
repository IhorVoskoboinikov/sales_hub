"""Типовые ответы с ошибками для документации операций в Swagger."""

from typing import Any

from app.schemas.errors import ErrorOut

Responses = dict[int | str, dict[str, Any]]

PROTECTED: Responses = {
    401: {"model": ErrorOut, "description": "Нет или истёк access-токен"},
    403: {"model": ErrorOut, "description": "Роли не разрешено это действие"},
    503: {"model": ErrorOut, "description": "OPA недоступен"},
}
NOT_FOUND: Responses = {404: {"model": ErrorOut, "description": "Нет такой записи в организации"}}
CONFLICT: Responses = {409: {"model": ErrorOut}}
