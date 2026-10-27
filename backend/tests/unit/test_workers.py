"""Воркеры без брокера: что повторять, пауза между повторами, контекст задачи, строка
failed_tasks, перезапуск по имени, расписание beat (раздел 15)."""

import socket
import uuid
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import aiosmtplib
import httpx
import jinja2
import pytest
import redis.exceptions
import structlog
from billiard.einfo import ExceptionInfo
from celery.app.task import Context
from celery.app.trace import get_log_policy, log_policy_expected, log_policy_unexpected
from celery.schedules import crontab
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError

from app.core.config import Settings
from app.core.context import request_id_var
from app.core.resources import Resources
from app.errors import Conflict, NotFound, ServiceUnavailable
from app.workers import base
from app.workers.base import (
    BACKOFF_MAX_SECONDS,
    BaseTask,
    backoff,
    failed_task_values,
    is_retryable,
    record_failed_task,
)
from app.workers.celery_app import TASK_ROUTES, celery_app
from app.workers.context import clear_context, put_context_headers, restore_context
from app.workers.queue import CeleryTaskSender
from app.workers.schedule import BEAT_SCHEDULE


class DriverError(Exception):
    """Как ошибка asyncpg внутри DBAPIError: SQLSTATE — в атрибуте sqlstate."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


def db_error(sqlstate: str) -> DBAPIError:
    return DBAPIError("UPDATE contacts SET ...", {}, DriverError(sqlstate))


@pytest.mark.parametrize(
    "sqlstate",
    [
        "08000",  # класс 08 — соединение
        "08006",
        "40001",  # serialization failure
        "40P01",  # deadlock
        "57P01",  # admin shutdown — Postgres перезапускается
        "57P02",
        "57P03",
        "53300",  # too many connections
    ],
)
def test_temporary_postgres_errors_are_retried(sqlstate: str) -> None:
    assert is_retryable(db_error(sqlstate))


@pytest.mark.parametrize(
    "sqlstate",
    [
        "23505",  # unique violation — повтор даст тот же дубль
        "22003",  # число вне диапазона
        "42P01",  # нет таблицы — ошибка в коде
        "57014",  # query canceled (statement_timeout)
    ],
)
def test_other_postgres_errors_are_not_retried(sqlstate: str) -> None:
    assert not is_retryable(db_error(sqlstate))


def test_db_error_without_sqlstate_is_not_retried() -> None:
    assert not is_retryable(DBAPIError("SELECT 1", {}, Exception("boom")))
    assert not is_retryable(IntegrityError("INSERT ...", {}, Exception("no sqlstate")))


@pytest.mark.parametrize(
    "exc",
    [
        InterfaceError("SELECT 1", {}, Exception("connection is closed")),
        ConnectionRefusedError("postgres:5432"),
        TimeoutError(),  # asyncpg: хост не отвечает
        socket.gaierror(-2, "Name or service not known"),  # postgres ещё не в DNS compose
        httpx.ConnectError("nbu"),
        httpx.ReadTimeout("opa"),
        aiosmtplib.SMTPServerDisconnected("mailpit"),
        aiosmtplib.SMTPConnectTimeoutError("mailpit"),
        aiosmtplib.SMTPResponseException(421, "try again later"),
        # Отказ на RCPT TO с 4xx — greylisting: через несколько минут адрес примут.
        aiosmtplib.SMTPRecipientsRefused(
            [aiosmtplib.SMTPRecipientRefused(451, "Greylisted", "alice@corp.example")]
        ),
        redis.exceptions.ConnectionError("redis:6379"),
        redis.exceptions.TimeoutError("Timeout connecting to server"),  # не TimeoutError Python
        ServiceUnavailable(failed=["redis"]),
        ServiceUnavailable("fx.unavailable"),
    ],
    ids=repr,
)
def test_temporary_failures_are_retried(exc: BaseException) -> None:
    assert is_retryable(exc)


@pytest.mark.parametrize(
    "exc",
    [
        NotFound(entity="deal", id=1),
        Conflict("bulk.already_running"),
        ServiceUnavailable("authz.policy_out_of_sync"),  # ошибка конфигурации
        ValueError("bug"),
        KeyError("bug"),
        redis.exceptions.ResponseError("WRONGTYPE"),
        # Наследники OSError, но не сеть: опечатка в имени шаблона или файла, права.
        FileNotFoundError("data.yaml"),
        PermissionError("data.yaml"),
        jinja2.TemplateNotFound("invite.html"),
        # SMTP 5xx — отказ навсегда: повтор получит тот же ответ.
        aiosmtplib.SMTPRecipientsRefused(
            [aiosmtplib.SMTPRecipientRefused(550, "no such user", "alice@corp.example")]
        ),
        aiosmtplib.SMTPRecipientsRefused([]),
        aiosmtplib.SMTPResponseException(550, "mailbox unavailable"),
        aiosmtplib.SMTPAuthenticationError(535, "bad credentials"),
    ],
    ids=repr,
)
def test_permanent_failures_are_not_retried(exc: BaseException) -> None:
    assert not is_retryable(exc)


@celery_app.task(base=BaseTask, name="tests.unit.always_down", bind=True)
def always_down(self: BaseTask) -> None:
    raise ConnectionRefusedError("postgres:5432")


def test_expected_failure_is_logged_without_traceback() -> None:
    # throws: провал с AppError Celery пишет как ожидаемый (INFO), а не ERROR с traceback.
    def policy(exc: Exception) -> Any:
        try:
            raise exc
        except Exception:
            return get_log_policy(always_down, ExceptionInfo(), exc)  # einfo из sys.exc_info()

    assert policy(NotFound(entity="deal", id=1)) is log_policy_expected
    assert policy(ValueError("bug")) is log_policy_unexpected


def test_last_attempt_is_not_logged_as_retry() -> None:
    # apply — без брокера: повтор выполняется сразу, в этом же потоке.
    with structlog.testing.capture_logs() as logs:
        result = always_down.apply(retries=BaseTask.max_retries - 1)

    assert isinstance(result.result, ConnectionRefusedError)
    events = [(log["event"], log["retries"]) for log in logs if log["event"].startswith("task_")]
    assert events == [
        ("task_retry", BaseTask.max_retries - 1),
        ("task_retries_exhausted", BaseTask.max_retries),
    ]


@pytest.mark.parametrize(
    ("retries", "longest"), [(0, 2), (1, 4), (2, 8), (3, 16), (4, 32), (5, 60), (50, 60)]
)
def test_backoff_doubles_with_jitter_and_is_capped(retries: int, longest: float) -> None:
    for _ in range(100):  # jitter — случайная часть: пауза от половины до целого
        assert longest / 2 <= backoff(retries) <= longest
    assert longest <= BACKOFF_MAX_SECONDS


def test_routes_by_full_task_name() -> None:
    router = celery_app.amqp.router

    def queue(name: str) -> str:
        return str(router.route({}, name)["queue"].name)

    for name, route in TASK_ROUTES.items():
        assert queue(name) == route["queue"]
    # Короткие задачи bulk — не в очередь за тысячей чанков.
    assert queue("bulk.plan") == "default"
    assert queue("bulk.reap_stuck") == "default"
    assert queue("workers.heartbeat") == "default"


@pytest.mark.parametrize("for_worker", [False, True])
async def test_email_sender_only_for_workers(settings: Settings, for_worker: bool) -> None:
    # С шага 5.3 API почту не отправляет: SMTP-отправитель собирается только у воркеров.
    resources = Resources.create(settings, for_worker=for_worker)
    try:
        assert (resources.email_sender is not None) is for_worker
    finally:
        await resources.aclose()


def test_broker_io_is_bounded() -> None:
    # Зависший Redis не держит send_task (и батч relay) бесконечно, а BRPOP воркера (1 с)
    # укладывается в таймаут чтения.
    options = celery_app.conf.broker_transport_options
    assert 1 < options["socket_timeout"] <= 10
    assert 0 < options["socket_connect_timeout"] <= 10


@pytest.fixture(autouse=True)
def clean_context() -> Iterator[None]:
    # Обработчики сигналов меняют контекст потока — не оставляем его следующим тестам.
    yield
    request_id_var.set(None)
    structlog.contextvars.clear_contextvars()


def test_publish_puts_request_and_user_into_headers() -> None:
    request_id_var.set("req-1")
    structlog.contextvars.bind_contextvars(user_id="user-1")
    headers: dict[str, Any] = {"task": "workers.heartbeat"}

    put_context_headers(headers=headers)

    assert headers["saleshub_request_id"] == "req-1"
    assert headers["saleshub_user_id"] == "user-1"


def test_publish_without_context_adds_nothing() -> None:
    headers: dict[str, Any] = {"task": "workers.heartbeat"}

    put_context_headers(headers=headers)

    assert headers == {"task": "workers.heartbeat"}


def fake_task(**headers: Any) -> Any:
    return SimpleNamespace(name="workers.heartbeat", request=Context(headers))


def test_prerun_restores_context_and_postrun_clears_it() -> None:
    structlog.contextvars.bind_contextvars(job_id="from-previous-task")
    task = fake_task(saleshub_request_id="req-1", saleshub_user_id="user-1")

    restore_context(task_id="task-1", task=task)

    assert request_id_var.get() == "req-1"
    assert structlog.contextvars.get_contextvars() == {
        "request_id": "req-1",
        "user_id": "user-1",
        "task_id": "task-1",
        "task_name": "workers.heartbeat",
    }

    clear_context(task_id="task-1", task=task)

    assert request_id_var.get() is None
    assert structlog.contextvars.get_contextvars() == {}


def test_task_without_request_uses_task_id() -> None:
    # Задачи beat: запроса нет, request_id = task_id.
    restore_context(task_id="task-1", task=fake_task())

    assert request_id_var.get() == "task-1"
    assert structlog.contextvars.get_contextvars()["request_id"] == "task-1"
    assert structlog.contextvars.get_contextvars()["user_id"] is None


def failed_task(**request: Any) -> Any:
    request.setdefault("id", "t-1")  # сигнал из выполнения задачи: request.id = task_id
    request.setdefault("retries", 5)
    request.setdefault("delivery_info", {"routing_key": "notifications"})
    return SimpleNamespace(name="notifications.send_email", request=Context(request))


def test_failed_task_row_from_signal() -> None:
    user_id = uuid.uuid4()
    request_id_var.set("req-1")
    structlog.contextvars.bind_contextvars(user_id=str(user_id))
    job_id = uuid.uuid4()

    values = failed_task_values(
        failed_task(),
        "task-1",
        ConnectionRefusedError("smtp:25"),
        (42, job_id),
        {"force": True},
        "Traceback ...",
    )

    assert values == {
        "task_id": "task-1",
        "task_name": "notifications.send_email",
        "queue": "notifications",
        "args": [42, str(job_id)],  # только JSON-типы
        "kwargs": {"force": True},
        "request_id": "req-1",
        "user_id": user_id,
        "exc_type": "builtins.ConnectionRefusedError",
        "exc_message": "smtp:25",
        "traceback": "Traceback ...",
        "retries": 5,
    }


def test_failed_task_texts_are_truncated() -> None:
    structlog.contextvars.bind_contextvars(user_id="not-a-uuid")
    traceback = "head " + "x" * 30_000 + " place of error"

    values = failed_task_values(
        failed_task(delivery_info=None), "task-1", ValueError("m" * 5_000), None, None, traceback
    )

    assert len(values["exc_message"]) == 2_000
    assert len(values["traceback"]) == 20_000
    assert values["traceback"].endswith("place of error")  # конец, а не начало
    assert (values["args"], values["kwargs"]) == ([], {})
    assert (values["queue"], values["user_id"]) == (None, None)


def test_eager_failure_is_not_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    # task.apply() в тестах: задачу не брали из очереди, базы у процесса может и не быть.
    def must_not_run(coro: Any) -> None:
        coro.close()
        raise AssertionError("run_async вызван для eager-задачи")

    monkeypatch.setattr(base, "run_async", must_not_run)

    record_failed_task(
        sender=failed_task(is_eager=True), task_id="t-1", exception=ValueError("x"), args=[]
    )


def test_failure_outside_task_is_not_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    # Жёсткий time_limit в prefork: task_failure приходит в главный процесс, где request —
    # пустой контекст. Там нельзя поднимать свой пул Postgres — строку не пишем.
    def must_not_run(coro: Any) -> None:
        coro.close()
        raise AssertionError("run_async вызван вне выполнения задачи")

    monkeypatch.setattr(base, "run_async", must_not_run)

    with structlog.testing.capture_logs() as logs:
        record_failed_task(
            sender=failed_task(id=None), task_id="t-1", exception=TimeoutError("hard limit")
        )

    assert [log["event"] for log in logs] == ["failed_task_skipped_outside_task"]


def test_recording_error_is_logged_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    def database_down(coro: Any) -> None:
        coro.close()
        raise ConnectionRefusedError("postgres:5432")

    monkeypatch.setattr(base, "run_async", database_down)

    with structlog.testing.capture_logs() as logs:
        record_failed_task(sender=failed_task(), task_id="t-1", exception=ValueError("x"))

    [log] = [log for log in logs if log["event"] == "failed_task_not_recorded"]
    assert log["task_id"] == "t-1"
    assert "postgres:5432" in log["error"]


def test_replayable_comes_from_registered_task() -> None:
    sender = CeleryTaskSender(celery_app)

    # bulk и импорт повторяют новой job; остальные задачи BaseTask перезапускаются.
    assert sender.replayable("bulk.plan") is False
    assert sender.replayable("bulk.chunk") is False
    assert sender.replayable("bulk.reap_stuck") is True
    assert sender.replayable("notifications.send_email") is True
    assert sender.replayable("workers.heartbeat") is True
    assert sender.replayable("celery.backend_cleanup") is False  # встроенная, не BaseTask
    assert sender.replayable("no.such_task") is None


async def test_send_error_is_503() -> None:
    def broker_down(*args: Any, **kwargs: Any) -> Any:
        raise ConnectionRefusedError("redis:6379")

    sender = CeleryTaskSender(celery_app)
    stub = SimpleNamespace(send_task=broker_down)
    sender.celery_app = stub  # type: ignore[assignment]

    with pytest.raises(ServiceUnavailable):
        await sender.send("workers.heartbeat", [], {})


async def test_send_returns_new_task_id() -> None:
    calls: list[tuple[Any, ...]] = []

    def send_task(name: str, **kwargs: Any) -> Any:
        calls.append((name, kwargs))
        return SimpleNamespace(id="new-task")

    sender = CeleryTaskSender(celery_app)
    sender.celery_app = SimpleNamespace(send_task=send_task)  # type: ignore[assignment]

    assert await sender.send("workers.heartbeat", [1], {"a": 2}) == "new-task"
    assert calls == [("workers.heartbeat", {"args": [1], "kwargs": {"a": 2}, "retry": False})]


def test_reaper_runs_every_minute() -> None:
    entry = BEAT_SCHEDULE["bulk.reap_stuck"]

    assert entry["task"] == "bulk.reap_stuck"
    assert entry["schedule"] == crontab()
    assert entry["options"] == {"expires": 60}  # пропущенные запуски не копятся


def test_beat_schedule_has_all_rows_with_expires_on_default_queue() -> None:
    """Все строки таблицы «Расписание beat» (обслуживание — шаг 5.8, напоминания и дайджест —
    5.14). Время — по Киеву; expires — интервал между запусками; очередь — default."""
    hour, day = 3600, 86400
    expected = {
        "workers.heartbeat": (crontab(), 60),
        "bulk.reap_stuck": (crontab(), 60),
        "notifications.send_reminders": (crontab(), 60),
        "notifications.morning_digest": (crontab(minute="*/15"), 900),
        "reports.refresh": (crontab(minute="*/5"), 300),
        "fx.fetch_nbu_rates": (crontab(minute=30, hour="18,19"), hour),
        "maintenance.create_partitions": (crontab(minute=0, hour=1), day),
        "trash.purge": (crontab(minute=0, hour=3), day),
        "maintenance.cleanup": (crontab(minute=0, hour=4), day),
    }
    celery_app.loader.import_default_modules()  # задачи регистрируются при импорте модулей

    assert set(BEAT_SCHEDULE) == set(expected)
    assert celery_app.conf.timezone == "Europe/Kyiv"
    for name, (schedule, expires) in expected.items():
        entry = BEAT_SCHEDULE[name]
        assert entry["task"] == name
        assert entry["schedule"] == schedule, name
        assert entry["options"] == {"expires": expires}, name
        assert name in celery_app.tasks, f"задачи {name} нет в celery_app"
        assert name not in TASK_ROUTES  # → task_default_queue
    assert celery_app.conf.task_default_queue == "default"
