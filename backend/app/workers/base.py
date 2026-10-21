"""Базовый класс задач (раздел 15, «Настройки надёжности»): что повторять и с какой паузой.

Повторяем только временные сбои — Postgres перезапускается, нет соединения, сеть, SMTP, Redis.
Остальные ошибки (404 / 409 / 422, ошибка в коде) — сразу провал: повтор дал бы тот же результат.
"""

import random
import socket
from typing import TYPE_CHECKING, Any, ClassVar

import aiosmtplib
import httpx
import redis.exceptions
import structlog
from celery import Task
from sqlalchemy.exc import DBAPIError, InterfaceError

from app.errors import AppError, ServiceUnavailable

logger = structlog.get_logger("workers")

# У asyncpg ошибки Postgres приходят как DBAPIError, а не OperationalError, поэтому решаем по
# SQLSTATE: 40001 — serialization failure, 40P01 — deadlock, 57P01–57P03 — Postgres
# перезапускается, 53300 — нет свободных соединений. Плюс весь класс 08 — соединение.
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01", "57P01", "57P02", "57P03", "53300"})
CONNECTION_SQLSTATE_CLASS = "08"
# fx.unavailable — курсов НБУ нет совсем. authz.policy_out_of_sync — ошибка конфигурации,
# её не повторяем.
RETRYABLE_UNAVAILABLE_CODES = frozenset({"unavailable", "fx.unavailable"})

MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2.0
# Больше 60 с не ждём: countdown держит сам воркер, и он всегда меньше visibility_timeout.
BACKOFF_MAX_SECONDS = 60.0


def is_retryable(exc: BaseException) -> bool:
    """Временный ли это сбой, после которого повтор задачи имеет смысл."""
    if isinstance(exc, InterfaceError):  # соединение с базой закрыто или сломано
        return True
    if isinstance(exc, DBAPIError):
        sqlstate = str(getattr(exc.orig, "sqlstate", "") or "")
        return sqlstate.startswith(CONNECTION_SQLSTATE_CLASS) or sqlstate in RETRYABLE_SQLSTATES
    if isinstance(exc, ServiceUnavailable):
        return exc.code in RETRYABLE_UNAVAILABLE_CODES
    # SMTP ответил 4xx — «попробуйте позже»; 5xx (адрес отклонён, нет авторизации) — навсегда.
    # Обрыв и таймаут SMTP — наследники ConnectionError и TimeoutError, их ловит проверка ниже.
    if isinstance(exc, aiosmtplib.SMTPResponseException):
        return 400 <= exc.code < 500
    # Не весь OSError: FileNotFoundError, PermissionError и TemplateNotFound тоже его
    # наследники, а это ошибка в коде или настройке. Повторяем только сеть: отказ и обрыв
    # соединения, таймаут, не разрешилось имя хоста (сервис в compose ещё не поднялся).
    # Redis бросает свои ConnectionError и TimeoutError, не встроенные.
    return isinstance(
        exc,
        ConnectionError
        | TimeoutError
        | socket.gaierror
        | httpx.TransportError
        | redis.exceptions.ConnectionError
        | redis.exceptions.TimeoutError,
    )


if TYPE_CHECKING:
    _Task = Task[Any, Any]
else:
    _Task = Task  # обобщённый Task есть только в заглушках celery-types, не в самом Celery


def backoff(retries: int) -> float:
    """Пауза перед повтором номер retries + 1: 2, 4, 8… с, не больше 60, со случайной
    добавкой (jitter) — чтобы сотня задач, упавших вместе, не вернулась в одну секунду."""
    delay = min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * 2**retries)
    return random.uniform(delay / 2, delay)


class BaseTask(_Task):
    """Задача, которая повторяет себя при временных сбоях: max_retries раз с паузой backoff.

    После последнего повтора ошибка уходит дальше — задача провалена (в 5.5 — строка
    failed_tasks). replayable=False — задачу нельзя перезапустить из failed_tasks руками:
    bulk и импорт повторяют новой job.
    """

    max_retries = MAX_RETRIES
    # Ожидаемые отказы: Celery пишет их провал строкой INFO без traceback, а не ERROR.
    # Сигнал task_failure (в 5.5 — строка failed_tasks) приходит всё равно.
    throws = (AppError,)
    replayable: ClassVar[bool] = True

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # self.run, а не super().__call__: тот положил бы в стек новый request без
        # заголовков и номера повтора.
        try:
            return self.run(*args, **kwargs)
        except Exception as exc:
            if not is_retryable(exc):
                if isinstance(exc, AppError):
                    # Ожидаемый отказ (нет записи, конфликт) — без traceback, повтор не поможет.
                    logger.warning("task_rejected", code=exc.code, details=exc.details)
                raise
            retries = self.request.retries
            if self.max_retries is not None and retries >= self.max_retries:
                # Повторы кончились — задача провалена. Строка error и для ServiceUnavailable:
                # его провал Celery запишет как ожидаемый, на уровне INFO.
                logger.error("task_retries_exhausted", error=repr(exc), retries=retries)
                raise
            countdown = backoff(retries)
            logger.warning(
                "task_retry", error=repr(exc), retries=retries, countdown=round(countdown, 1)
            )
            raise self.retry(exc=exc, countdown=countdown) from exc
