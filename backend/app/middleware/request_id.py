import re
import uuid

import structlog
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.context import request_id_var

# Чужой X-Request-ID принимаем, только если он короткий и без спецсимволов:
# иначе через него можно подсунуть в логи что угодно.
VALID_REQUEST_ID = re.compile(r"[A-Za-z0-9._-]{1,64}")


class RequestIdMiddleware:
    """Даёт каждому запросу request_id: берёт из X-Request-ID или создаёт новый.

    Кладёт его в contextvars (попадает в каждую строку лога и в тело ошибки)
    и возвращает в заголовке ответа X-Request-ID.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = Headers(scope=scope).get("x-request-id", "")
        request_id = incoming if VALID_REQUEST_ID.fullmatch(incoming) else uuid.uuid4().hex

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message).append("X-Request-ID", request_id)
            await send(message)

        token = request_id_var.set(request_id)
        try:
            # user_id привяжет current_user; на выходе оба поля вернутся к прежним значениям,
            # и следующий запрос в том же контексте не унаследует чужого пользователя.
            with structlog.contextvars.bound_contextvars(request_id=request_id, user_id=None):
                await self.app(scope, receive, send_with_request_id)
        finally:
            request_id_var.reset(token)
