import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.errors import AppError
from app.errors.handlers import problem

logger = structlog.get_logger("errors")


class UnhandledErrorMiddleware:
    """Последний рубеж: неожиданное исключение → 500 в общем формате, traceback — только в лог.

    Почему не app.add_exception_handler(Exception, ...): такой обработчик Starlette вызывает
    снаружи всех middleware. Ответ 500 ушёл бы без заголовка X-Request-ID, в логе не было бы
    request_id, а uvicorn напечатал бы traceback второй раз. Этот middleware стоит внутри
    RequestIdMiddleware и AccessLogMiddleware, поэтому ошибка видна обоим.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False

        async def send_and_track(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, send_and_track)
        except Exception:
            logger.exception("unhandled_error")
            if response_started:
                raise  # часть ответа уже ушла клиенту — исправить нельзя, сервер закроет соединение
            response = problem(AppError.status, AppError.code)
            await response(scope, receive, send)
