import time

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = structlog.get_logger("access")


class AccessLogMiddleware:
    """Одна строка лога на запрос: метод, шаблон пути, статус, длительность.

    Путь пишем шаблоном (/api/v1/deals/{deal_id}), а не реальным URL: так по логам
    удобно группировать запросы, и в лог не попадают id и query-параметры.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = time.perf_counter()
        status = 500  # если ответ так и не начался — значит, упали

        async def send_and_remember_status(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_and_remember_status)
        finally:
            # Маршрут FastAPI кладёт в scope["route"] после того, как нашёл его.
            # Нет маршрута (404) — пишем заглушку: в сыром URL могут быть id и мусор от сканеров.
            path = getattr(scope.get("route"), "path", None) or "<unmatched>"
            # Пробы Kubernetes и compose дёргают health каждые несколько секунд — не засоряем лог.
            log = logger.debug if path.startswith("/health/") else logger.info
            log(
                "request",
                method=scope["method"],
                path=path,
                status=status,
                duration_ms=round((time.perf_counter() - started) * 1000, 1),
            )
