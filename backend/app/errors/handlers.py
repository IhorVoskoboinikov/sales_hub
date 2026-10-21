"""Превращение исключений в ответ единого формата (ErrorOut).

Неожиданные исключения (500) обрабатывает не здесь, а UnhandledErrorMiddleware — почему,
написано в app/middleware/unhandled_errors.py.
"""

from collections.abc import Mapping
from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm.exc import StaleDataError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.context import get_request_id
from app.errors import AppError, Conflict
from app.schemas.errors import ErrorOut
from app.texts import uk

logger = structlog.get_logger("errors")

# Ошибки Starlette (нет маршрута, не тот метод) — тоже в общий формат.
HTTP_STATUS_CODES = {
    401: "auth.invalid_token",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
}

UNIQUE_VIOLATION = "23505"  # SQLSTATE Postgres: нарушение UNIQUE

# Уникальные ограничения, у которых есть свой код ошибки. Сервис проверяет их заранее, а база
# ловит гонку двух одновременных запросов — ответ должен быть тем же.
UNIQUE_CODES = {
    "uq_contacts_org_id_email": "contact.duplicate_email",
    "uq_pipelines_org_id_name": "pipeline.name_taken",
}


def problem(
    status: int,
    code: str,
    details: dict[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    title: str | None = None,
) -> JSONResponse:
    body = ErrorOut(
        type=f"https://saleshub.local/errors/{code}",
        code=code,
        title=title or uk.error_title(code),
        status=status,
        request_id=get_request_id(),
        details=details or {},
    )
    return JSONResponse(
        body.model_dump(mode="json"),
        status_code=status,
        media_type="application/problem+json",
        headers=headers,
    )


async def app_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AppError)
    log = logger.warning if exc.status < 500 else logger.error
    log("app_error", code=exc.code, status=exc.status)
    return problem(
        exc.status, exc.code, exc.details, headers=exc.response_headers(), title=exc.title
    )


async def validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    # Поле input не отдаём: в нём может быть пароль из тела запроса.
    errors = [
        {
            "loc": list(err["loc"]),
            "type": err["type"],
            "message": uk.validation_message(err["type"], err.get("ctx")),
        }
        for err in exc.errors()
    ]
    logger.info("validation_error", errors=[(e["loc"], e["type"]) for e in errors])
    return problem(422, "validation", {"errors": errors})


async def http_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    code = HTTP_STATUS_CODES.get(exc.status_code, "http_error")
    return problem(exc.status_code, code, headers=exc.headers)


async def integrity_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, IntegrityError)
    if getattr(exc.orig, "sqlstate", None) != UNIQUE_VIOLATION:
        raise exc  # другие нарушения (FK, CHECK) — ошибка в коде: пусть станет 500
    # SQLAlchemy оборачивает исключение asyncpg; имя constraint есть только у исходного.
    driver_error = getattr(exc.orig, "orig", None) or exc.orig
    constraint = getattr(driver_error, "constraint_name", None)
    logger.warning("unique_violation", constraint=constraint)
    return problem(Conflict.status, UNIQUE_CODES.get(constraint or "", Conflict.code))


async def data_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Postgres не принял значение (SQLSTATE класса 22: число вне диапазона, символ NUL в
    тексте…). Это ввод клиента, который пропустила валидация схемы, — 422, а не 500."""
    assert isinstance(exc, DBAPIError)
    sqlstate = str(getattr(exc.orig, "sqlstate", "") or "")
    if not sqlstate.startswith("22"):
        raise exc  # остальное (соединение, синтаксис) — ошибка сервера: пусть станет 500
    logger.info("data_error", sqlstate=sqlstate)
    return problem(422, "validation")


async def stale_data_handler(request: Request, exc: Exception) -> JSONResponse:
    """UPDATE … WHERE id = :id AND version = :v не нашёл строку: между чтением и записью её
    изменил другой запрос (оптимистическая блокировка, version_id_col у CRM-моделей)."""
    logger.warning("version_conflict", error=str(exc))
    return problem(Conflict.status, "version.conflict")


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, app_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_error_handler)
    app.add_exception_handler(IntegrityError, integrity_error_handler)
    app.add_exception_handler(DBAPIError, data_error_handler)
    app.add_exception_handler(StaleDataError, stale_data_handler)
