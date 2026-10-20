"""Ошибки приложения. Сервисы и репозитории бросают только их — без HTTP-исключений FastAPI.

Код ошибки можно уточнить при создании: Conflict("member.last_admin"), а details попадут в ответ:
NotFound(entity="deal", id=123).
"""

from typing import Any

from app.texts import uk


class AppError(Exception):
    status: int = 500
    code: str = "internal"

    def __init__(self, code: str | None = None, **details: Any) -> None:
        if code is not None:
            self.code = code
        self.details = details
        super().__init__(self.code)

    @property
    def title(self) -> str:
        # Текст своего кода, иначе — ближайшего родителя, у которого он есть:
        # Conflict("member.last_admin") → «Конфлікт даних». У AppError текст есть всегда.
        codes = [self.code, *(c.code for c in type(self).__mro__ if issubclass(c, AppError))]
        return next(uk.ERRORS[code] for code in codes if code in uk.ERRORS)


class NotFound(AppError):
    status = 404
    code = "not_found"


class Forbidden(AppError):
    status = 403
    code = "forbidden"


class Unauthorized(AppError):
    status = 401
    code = "auth.invalid_token"


class Conflict(AppError):
    status = 409
    code = "conflict"  # конфликт версии, дубль


class ValidationFailed(AppError):
    status = 422
    code = "validation"


class RateLimited(AppError):
    status = 429
    code = "rate_limited"


class ServiceUnavailable(AppError):
    status = 503
    code = "unavailable"  # не отвечают Postgres, Redis, OPA, SMTP
