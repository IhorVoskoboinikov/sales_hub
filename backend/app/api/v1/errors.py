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
LENGTH_REQUIRED: Responses = {411: {"model": ErrorOut, "description": "Нет Content-Length"}}
PAYLOAD_TOO_LARGE: Responses = {413: {"model": ErrorOut, "description": "Файл больше лимита"}}
RATE_LIMITED: Responses = {429: {"model": ErrorOut, "description": "Слишком часто"}}
VALIDATION: Responses = {422: {"model": ErrorOut}}
