"""Данные текущего запроса, доступные из любого слоя без передачи через аргументы."""

from contextvars import ContextVar

# Ставит RequestIdMiddleware; читают обработчики ошибок (поле request_id в ответе).
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


def get_request_id() -> str | None:
    return request_id_var.get()
