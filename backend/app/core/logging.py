"""Логи через structlog: JSON в stdout (в dev — цветной консольный вывод).

Логи uvicorn и библиотек идут через тот же форматтер, поэтому формат у всех строк один.
"""

import logging
import sys

import structlog
from structlog.typing import Processor


def configure_logging(level: str, *, json: bool) -> None:
    # Общие шаги для наших логов и для логов библиотек (stdlib logging).
    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,  # request_id и другие поля запроса
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
    ]
    renderer: Processor
    if json:
        renderer = structlog.processors.JSONRenderer()
        shared.append(structlog.processors.format_exc_info)  # traceback — строкой в поле exception
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # Без кэша: structlog.testing.capture_logs в тестах видит все логгеры.
        cache_logger_on_first_use=False,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared,
            processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
        )
    )
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)

    # uvicorn настраивает свои логгеры сам — отправляем их в общий обработчик.
    for name in ("uvicorn", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True
        uvicorn_logger.setLevel(logging.NOTSET)  # уровень — общий, из LOG_LEVEL
    # Access-лог пишет AccessLogMiddleware (с шаблоном пути и request_id).
    logging.getLogger("uvicorn.access").disabled = True
