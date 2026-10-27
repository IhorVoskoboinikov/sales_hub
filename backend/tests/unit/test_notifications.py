"""Уведомления без базы (раздел 16): каталог каналов, ключи дедупа, шаблоны, клиент Bot API
через respx и то, что токен бота не попадает ни в лог, ни в исключение, ни в failed_tasks."""

import logging
import traceback
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx
import structlog
from celery.app.task import Context

from app.core.logging import QUIET_LOGGERS, configure_logging
from app.emails.render import render_email, render_text
from app.models import NotificationChannel
from app.notifications import catalog
from app.notifications.catalog import ALLOWED, DEFAULTS, NotificationEvent
from app.notifications.keys import (
    digest_key,
    event_subject,
    job_subject,
    notify_key,
    reminder_key,
)
from app.notifications.telegram import TelegramApiError, TelegramClient, TelegramUnavailable
from app.workers.base import BaseTask, failed_task_values, is_retryable, retry_countdown
from app.workers.celery_app import TASK_ROUTES, celery_app
from app.workers.tasks import notifications as notification_tasks

TOKEN = "777000:SECRET-token-value"
BASE_URL = "http://telegram.unit"
SEND_URL = f"{BASE_URL}/bot{TOKEN}/sendMessage"
TELEGRAM, EMAIL = NotificationChannel.TELEGRAM, NotificationChannel.EMAIL


# --- каталог и ключи


def test_catalog_has_eight_allowed_pairs_with_defaults() -> None:
    # Таблица «События и каналы» раздела 16: email у напоминания и дайджеста не бывает.
    assert DEFAULTS == {
        (NotificationEvent.RECORD_ASSIGNED, TELEGRAM): True,
        (NotificationEvent.RECORD_ASSIGNED, EMAIL): True,
        (NotificationEvent.DEAL_CLOSED, TELEGRAM): True,
        (NotificationEvent.DEAL_CLOSED, EMAIL): False,
        (NotificationEvent.ACTIVITY_REMINDER, TELEGRAM): True,
        (NotificationEvent.DIGEST_MORNING, TELEGRAM): True,
        (NotificationEvent.BULK_JOB_COMPLETED, TELEGRAM): True,
        (NotificationEvent.BULK_JOB_COMPLETED, EMAIL): True,
    }
    assert tuple(DEFAULTS) == ALLOWED
    assert catalog.channels(NotificationEvent.DIGEST_MORNING) == [TELEGRAM]


def test_user_row_wins_over_default() -> None:
    overrides = {("deal.closed", "email"): True, ("record.assigned", "telegram"): False}

    assert catalog.enabled(overrides, NotificationEvent.DEAL_CLOSED, EMAIL) is True
    assert catalog.enabled(overrides, NotificationEvent.RECORD_ASSIGNED, TELEGRAM) is False
    assert catalog.enabled(overrides, NotificationEvent.RECORD_ASSIGNED, EMAIL) is True
    # Неразрешённая пара выключена, даже если строка каким-то образом есть.
    stray = {("digest.morning", "email"): True}
    assert catalog.enabled(stray, NotificationEvent.DIGEST_MORNING, EMAIL) is False


def test_notifier_dedup_key_format() -> None:
    # Формат — часть данных: строки sent_notifications живут 30 дней.
    user_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
    job_id = uuid.UUID("00000000-0000-0000-0000-0000000000aa")

    assert notify_key("deal.closed", event_subject(42), user_id, "telegram") == (
        "deal.closed:42:00000000-0000-0000-0000-000000000001:telegram"
    )
    assert notify_key("bulk_job.completed", job_subject(job_id), user_id, "email") == (
        "bulk_job.completed:job:00000000-0000-0000-0000-0000000000aa:"
        "00000000-0000-0000-0000-000000000001:email"
    )


def test_reminder_and_digest_key_format() -> None:
    user_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
    # Срок — unix-время в секундах, доли отбрасываются.
    due_at = datetime(2026, 10, 27, 12, 15, 0, 999_999, tzinfo=UTC)

    assert reminder_key(7, user_id, due_at) == (
        "activity.reminder:7:00000000-0000-0000-0000-000000000001:1793103300:telegram"
    )
    # Перенос встречи и передача звонка другому — другой ключ.
    assert reminder_key(7, user_id, due_at + timedelta(minutes=30)) != reminder_key(
        7, user_id, due_at
    )
    assert reminder_key(7, uuid.uuid4(), due_at) != reminder_key(7, user_id, due_at)
    assert digest_key(user_id, date(2026, 10, 27)) == (
        "digest.morning:00000000-0000-0000-0000-000000000001:2026-10-27:telegram"
    )


def test_notify_and_telegram_tasks_go_to_default_queue() -> None:
    celery_app.loader.import_default_modules()
    router = celery_app.amqp.router
    for name in (
        "notifications.notify",
        "notifications.send_telegram",
        "notifications.send_reminders",
        "notifications.morning_digest",
    ):
        assert name in celery_app.tasks
        assert name not in TASK_ROUTES
        assert router.route({}, name)["queue"].name == "default"
    assert celery_app.tasks["notifications.send_telegram"].rate_limit is None


async def test_reminder_tasks_skip_without_bot_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Нет токена бота — тики напоминаний и дайджеста ничего не пишут: send_telegram такие
    команды всё равно пропустил бы. Сессию задача даже не открывает."""

    def no_session() -> None:
        raise AssertionError("сессия не нужна")

    res = SimpleNamespace(telegram=None, session_factory=no_session)
    monkeypatch.setattr(notification_tasks, "resources", lambda: res)

    with structlog.testing.capture_logs() as logs:
        await notification_tasks._send_reminders()
        await notification_tasks._morning_digest()

    assert [log["event"] for log in logs] == ["reminders_skipped", "reminders_skipped"]


# --- шаблоны: письмо и текст Telegram из одного .txt


@pytest.mark.parametrize(
    ("template", "context", "expected"),
    [
        (
            "record_assigned",
            {"entity": "контакт", "title": "Іван <Петренко>", "actor": "boss@example.com"},
            "Вам призначено контакт «Іван <Петренко>».\nПризначив: boss@example.com.",
        ),
        (
            "deal_closed",
            {
                "title": "Угода",
                "result": "виграно",
                "amount": "1000.00 EUR",
                "owner": "rep@example.com",
                "actor": "lead@example.com",
            },
            "Угоду «Угода» закрито: виграно.\nСума: 1000.00 EUR.\n"
            "Відповідальний: rep@example.com. Закрив: lead@example.com.",
        ),
        (
            "bulk_job_completed",
            {
                "operation": "Імпорт контактів",
                "status": "виконано",
                "processed": "3",
                "total": "3",
                "failed_rows": "0",
                "failed_chunks": "0",
                "rows": "",
            },
            "Масову операцію «Імпорт контактів» завершено: виконано.\n"
            "Оброблено записів: 3 з 3.\nПомилок у рядках: 0, невдалих частин: 0.",
        ),
        (
            "bulk_reassigned",
            {"count": "120", "rows": "контакти — 100, компанії — 20", "author": "a@b.c"},
            "Вам передано клієнтів: 120 (контакти — 100, компанії — 20).\nПередав: a@b.c.",
        ),
    ],
)
def test_notification_templates(template: str, context: dict[str, str], expected: str) -> None:
    assert render_text(template, **context) == expected

    email = render_email(template, "to@example.com", **context)
    assert email.text.strip() == expected  # Telegram получает ровно текст письма
    assert email.subject
    assert "PUT /api/v1/me/notifications" in email.html  # как выключить такие письма
    assert "<Петренко>" not in email.html  # HTML экранируется, текст Telegram — нет


# --- TelegramClient


@pytest.fixture
def api() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as router:
        yield router


async def send(text: str = "Привіт") -> None:
    async with httpx.AsyncClient() as http:
        await TelegramClient(http, TOKEN, BASE_URL + "/").send_message(42, text)


async def test_send_message_is_plain_text_without_preview(api: respx.MockRouter) -> None:
    route = api.post(SEND_URL).mock(return_value=httpx.Response(200, json={"ok": True}))

    await send("Угода <b>«X»</b>")

    request = route.calls.last.request
    assert request.url.path == f"/bot{TOKEN}/sendMessage"
    assert httpx.Response(200, content=request.content).json() == {
        "chat_id": 42,
        "text": "Угода <b>«X»</b>",
        "link_preview_options": {"is_disabled": True},
    }  # без parse_mode: разметку Telegram не разбирает


def error(status: int, description: str | None, **body: Any) -> httpx.Response:
    if description is None:
        return httpx.Response(status, text="<html>Bad Gateway</html>")
    return httpx.Response(
        status, json={"ok": False, "error_code": status, "description": description, **body}
    )


@pytest.mark.parametrize(
    ("response", "retryable", "chat_unavailable", "retry_after"),
    [
        (
            error(429, "Too Many Requests: retry after 7", parameters={"retry_after": 7}),
            True,
            False,
            7,
        ),
        (error(403, "Forbidden: bot was blocked by the user"), False, True, None),
        (error(400, "Bad Request: chat not found"), False, True, None),
        (error(400, "Bad Request: message text is empty"), False, False, None),
        (error(401, "Unauthorized"), False, False, None),
        (error(500, "Internal Server Error"), True, False, None),
        (error(502, None), True, False, None),  # ответил прокси, а не Telegram: тела нет
    ],
)
async def test_api_errors(
    api: respx.MockRouter,
    response: httpx.Response,
    retryable: bool,
    chat_unavailable: bool,
    retry_after: int | None,
) -> None:
    api.post(SEND_URL).mock(return_value=response)

    with pytest.raises(TelegramApiError) as caught:
        await send()

    exc = caught.value
    assert exc.status == response.status_code
    assert (exc.retryable, exc.chat_unavailable, exc.retry_after) == (
        retryable,
        chat_unavailable,
        retry_after,
    )
    assert is_retryable(exc) is retryable
    assert TOKEN not in str(exc) and TOKEN not in repr(exc)


@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), httpx.RemoteProtocolError("x")],
)
async def test_network_error_is_retried_without_url(
    api: respx.MockRouter, failure: httpx.TransportError
) -> None:
    api.post(SEND_URL).mock(side_effect=failure)

    with pytest.raises(TelegramUnavailable) as caught:
        await send()

    assert is_retryable(caught.value)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert TOKEN not in str(caught.value)


def test_429_waits_retry_after_plus_one_but_not_longer_than_a_minute() -> None:
    assert retry_countdown(TelegramApiError(429, 429, "x", retry_after=7), 0) == 8
    assert retry_countdown(TelegramApiError(429, 429, "x", retry_after=300), 0) == 60
    assert 1 <= retry_countdown(TelegramApiError(500), 0) <= 2  # без retry_after — backoff


@celery_app.task(base=BaseTask, name="tests.unit.telegram_flood", bind=True)
def telegram_flood(self: BaseTask) -> None:
    raise TelegramApiError(429, 429, "Too Many Requests: retry after 4", retry_after=4)


def test_task_retries_429_after_retry_after() -> None:
    with structlog.testing.capture_logs() as logs:
        telegram_flood.apply(retries=BaseTask.max_retries - 1)

    [retry] = [log for log in logs if log["event"] == "task_retry"]
    assert retry["countdown"] == 5.0


def test_httpx_logs_are_quiet() -> None:
    # На INFO httpx пишет «HTTP Request: POST …/bot<token>/sendMessage» — токен попал бы в Loki.
    configure_logging("INFO", json=True)

    for name in QUIET_LOGGERS:
        assert logging.getLogger(name).getEffectiveLevel() == logging.WARNING


async def test_token_never_reaches_logs_exception_or_failed_tasks(
    api: respx.MockRouter, caplog: pytest.LogCaptureFixture
) -> None:
    api.post(SEND_URL).mock(return_value=error(400, "Bad Request: can't parse entities"))

    configure_logging("DEBUG", json=True)  # даже на DEBUG у приложения
    try:
        with (
            caplog.at_level(logging.DEBUG),
            structlog.testing.capture_logs() as logs,
            pytest.raises(TelegramApiError) as caught,
        ):
            await send()
    finally:
        configure_logging("INFO", json=True)  # DEBUG не должен достаться следующим тестам

    assert TOKEN not in caplog.text
    assert all(TOKEN not in str(log) for log in logs)
    # Строка failed_tasks из этого провала: ни в тексте ошибки, ни в traceback токена нет.
    trace = "".join(traceback.format_exception(caught.value))
    task = SimpleNamespace(
        name="notifications.send_telegram",
        request=Context({"id": "t-1", "retries": 0, "delivery_info": {"routing_key": "default"}}),
    )
    values = failed_task_values(task, "t-1", caught.value, (1,), {}, trace)  # type: ignore[arg-type]
    assert TOKEN not in str(values)
    assert values["exc_type"] == "app.notifications.telegram.TelegramApiError"
    async with httpx.AsyncClient() as http:
        assert TOKEN not in repr(TelegramClient(http, TOKEN, BASE_URL))
