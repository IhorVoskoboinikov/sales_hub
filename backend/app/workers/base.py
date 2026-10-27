"""Базовый класс задач (раздел 15, «Настройки надёжности»): что повторять и с какой паузой.

Повторяем только временные сбои — Postgres перезапускается, нет соединения, сеть, SMTP, Redis.
Остальные ошибки (404 / 409 / 422, ошибка в коде) — сразу провал: повтор дал бы тот же результат.

Провал после всех ретраев (и сразу — для неповторяемой ошибки) записывает в failed_tasks
обработчик сигнала task_failure (раздел 15, «Задачи, упавшие после всех ретраев»): у Celery с
Redis нет dead-letter очереди. Жёсткий time_limit убивает процесс пула: on_failure нет, а
task_failure Celery шлёт уже в главном процессе воркера (prefork) — там нет контекста задачи, и
обработчик его пропускает: иначе главный процесс поднял бы свой пул Postgres и Redis вне бюджета
соединений. Такой провал виден в логе (Hard time limit exceeded) и во Flower; чанки после него
подбирает bulk.reap_stuck.
"""

import json
import random
import socket
import uuid
from typing import TYPE_CHECKING, Any, ClassVar

import aiosmtplib
import httpx
import redis.exceptions
import structlog
from celery import Task
from celery.signals import task_failure
from sqlalchemy.exc import DBAPIError, InterfaceError

from app.core.context import request_id_var
from app.errors import AppError, ServiceUnavailable
from app.models.failed_tasks import EXC_MESSAGE_MAX, TRACEBACK_MAX
from app.notifications.telegram import TelegramApiError
from app.repositories.failed_tasks import FailedTaskRepository
from app.workers.runtime import resources, run_async

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
    # Bot API: 429 (подождать retry_after) и 5xx — повторяем; прочие 4xx — ошибка в запросе.
    # Сеть до Telegram — TelegramUnavailable, наследник ConnectionError (проверка ниже).
    if isinstance(exc, TelegramApiError):
        return exc.retryable
    # SMTP ответил 4xx — «попробуйте позже»; 5xx (адрес отклонён, нет авторизации) — навсегда.
    # Обрыв и таймаут SMTP — наследники ConnectionError и TimeoutError, их ловит проверка ниже.
    # Отказ на RCPT TO aiosmtplib бросает списком SMTPRecipientsRefused — он не наследник
    # SMTPResponseException, а коды внутри часто 4xx (greylisting 451, ящик переполнен 452).
    # Повторяем, если все отказы временные; у нас один получатель на письмо — решает его код.
    if isinstance(exc, aiosmtplib.SMTPRecipientsRefused):
        return bool(exc.recipients) and all(400 <= r.code < 500 for r in exc.recipients)
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


def retry_countdown(exc: BaseException, retries: int) -> float:
    """Пауза перед повтором: сервис сам сказал, сколько ждать (429 Telegram, retry_after), —
    столько плюс секунда, но не больше 60 с; иначе — backoff."""
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, int | float) and retry_after >= 0:
        return min(float(retry_after) + 1, BACKOFF_MAX_SECONDS)
    return backoff(retries)


class BaseTask(_Task):
    """Задача, которая повторяет себя при временных сбоях: max_retries раз с паузой backoff.

    После последнего повтора ошибка уходит дальше — задача провалена: строка failed_tasks
    (record_failed_task). replayable=False — задачу нельзя перезапустить из failed_tasks руками:
    bulk и импорт повторяют новой job.
    """

    max_retries = MAX_RETRIES
    # Ожидаемые отказы: Celery пишет их провал строкой INFO без traceback, а не ERROR.
    # Сигнал task_failure (строка failed_tasks) приходит всё равно.
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
            countdown = retry_countdown(exc, retries)
            logger.warning(
                "task_retry", error=repr(exc), retries=retries, countdown=round(countdown, 1)
            )
            raise self.retry(exc=exc, countdown=countdown) from exc


def failed_task_values(
    task: "_Task",
    task_id: str,
    exc: BaseException,
    args: Any,
    kwargs: Any,
    traceback: str | None,
) -> dict[str, Any]:
    """Строка failed_tasks из того, что пришло с сигналом task_failure. request_id и user_id —
    из контекста задачи (его восстановил task_prerun, а task_postrun ещё не очистил)."""
    request = task.request
    user_id = structlog.contextvars.get_contextvars().get("user_id")
    try:
        user = uuid.UUID(str(user_id)) if user_id is not None else None
    except ValueError:
        user = None  # чужой формат в заголовке — не повод терять строку
    exc_type = type(exc)
    return {
        "task_id": task_id,
        "task_name": task.name,
        # У транспорта Redis routing_key — имя очереди, из которой взята задача.
        "queue": (request.delivery_info or {}).get("routing_key"),
        # Аргументы уже прошли JSON-сериализацию Celery; default=str — страховка.
        "args": json.loads(json.dumps(list(args or ()), default=str)),
        "kwargs": json.loads(json.dumps(dict(kwargs or {}), default=str)),
        "request_id": request_id_var.get(),
        "user_id": user,
        "exc_type": f"{exc_type.__module__}.{exc_type.__qualname__}"[:255],
        "exc_message": str(exc)[:EXC_MESSAGE_MAX],
        # Конец traceback, а не начало: внизу — само место ошибки.
        "traceback": traceback[-TRACEBACK_MAX:] if traceback else None,
        "retries": request.retries or 0,
    }


@task_failure.connect
def record_failed_task(
    sender: "_Task | None" = None,
    task_id: str | None = None,
    exception: BaseException | None = None,
    args: Any = None,
    kwargs: Any = None,
    einfo: Any = None,
    **_: Any,
) -> None:
    """Задача провалена (после всех ретраев; на Retry сигнала нет) → строка failed_tasks своей
    сессией. ON CONFLICT (task_id) DO NOTHING: повторно доставленная задача дубля не даёт.
    Ошибка записи только логируется — провал задачи и так уже случился.

    Eager-вызовы (task.apply() в unit-тестах) не записываются: их не брал воркер из очереди,
    и у такого процесса может не быть своей базы."""
    if sender is None or task_id is None or exception is None or sender.request.is_eager:
        return
    if sender.request.id != task_id:
        # Сигнал пришёл не из выполнения задачи: жёсткий time_limit в prefork шлёт task_failure
        # в главном процессе, где request — пустой контекст по умолчанию. Не пишем (см. выше).
        logger.warning("failed_task_skipped_outside_task", task_id=task_id, error=repr(exception))
        return
    try:
        values = failed_task_values(
            sender, task_id, exception, args, kwargs, getattr(einfo, "traceback", None)
        )
        recorded = run_async(_record_failed_task(values))
    except Exception as error:
        logger.error("failed_task_not_recorded", task_id=task_id, error=repr(error))
        return
    logger.info("failed_task_recorded" if recorded else "failed_task_duplicate", task_id=task_id)


async def _record_failed_task(values: dict[str, Any]) -> bool:
    async with resources().session_factory() as session:  # своя сессия, не сессия задачи
        recorded = await FailedTaskRepository(session).record(values)
        await session.commit()
        return recorded
