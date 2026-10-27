"""Процесс бота и роутер /api/v1/me без Telegram, Postgres и OPA (раздел 16)."""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from aiogram import Bot
from aiogram.client.session.base import BaseSession
from aiogram.exceptions import TelegramNetworkError
from aiogram.methods import DeleteWebhook, GetMe, GetUpdates, SendMessage, TelegramMethod
from aiogram.types import Update, User
from aiogram.utils.backoff import BackoffConfig
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute
from pydantic import SecretStr
from redis.asyncio import Redis
from structlog.testing import capture_logs

from app.api.deps import current_member
from app.api.v1 import me
from app.authz.deps import opa_decision
from app.bot import __main__ as bot_main
from app.bot import dispatcher as bot_dispatcher
from app.bot.dispatcher import create_dispatcher
from app.bot.handlers import commands_text
from app.core.config import Settings
from app.core.resources import Resources


async def test_bot_exits_without_token(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_resources(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("без токена ресурсы не создаются")

    monkeypatch.setattr(Resources, "create", no_resources)
    monkeypatch.setattr(bot_main, "configure_logging", lambda *args, **kwargs: None)

    with capture_logs() as logs:
        code = await bot_main.main(settings.model_copy(update={"telegram_bot_token": None}))

    assert code == 1
    assert [(log["event"], log["log_level"]) for log in logs] == [
        ("telegram_bot_token_missing", "error")
    ]


def test_bot_is_disabled_without_token_or_username(settings: Settings) -> None:
    token, username = SecretStr("42:x"), "saleshub_bot"

    assert not settings.telegram_enabled
    assert not settings.model_copy(update={"telegram_bot_username": username}).telegram_enabled
    assert not settings.model_copy(update={"telegram_bot_token": token}).telegram_enabled
    assert settings.model_copy(
        update={"telegram_bot_token": token, "telegram_bot_username": username}
    ).telegram_enabled


def calls(dependant: Dependant) -> set[Any]:
    found: set[Any] = set()
    for dep in dependant.dependencies:
        found |= {dep.call} | calls(dep)
    return found


def test_me_routes_have_no_opa_and_need_member() -> None:
    """/api/v1/me/* — без OPA (раздел 5): их нет в routes.rego, а test_routes_match_policy
    проверяет только роутеры protected и admin. Вместо OPA — current_member."""
    routes = [route for route in me.router.routes if isinstance(route, APIRoute)]

    assert {f"{m} {r.path}" for r in routes for m in r.methods or ()} == {
        "POST /api/v1/me/telegram/link",
        "GET /api/v1/me/telegram",
        "DELETE /api/v1/me/telegram",
    }
    for route in routes:
        assert opa_decision not in calls(route.dependant), route.path
        assert current_member in calls(route.dependant), route.path


# --- процесс бота без Telegram: Bot API подменён сессией aiogram


class FakeSession(BaseSession):
    """Bot API в памяти: getUpdates отдаёт заготовленные апдейты и дальше «висит», как long
    polling; ответы бота (sendMessage) копятся в sent. fail — сколько первых deleteWebhook
    упадут сетевой ошибкой."""

    def __init__(self, updates: list[Update] | None = None, *, fail: int = 0) -> None:
        super().__init__()
        self.updates = updates or []
        self.fail = fail
        self.sent: list[str] = []
        self.calls: list[str] = []

    async def make_request(
        self,
        bot: Bot,
        method: TelegramMethod[Any],
        timeout: int | None = None,  # noqa: ASYNC109
    ) -> Any:
        self.calls.append(type(method).__name__)
        if isinstance(method, DeleteWebhook) and self.fail:
            self.fail -= 1
            raise TelegramNetworkError(method=method, message="ClientConnectorError")
        if isinstance(method, GetMe):
            return User(id=42, is_bot=True, first_name="SalesHub", username="saleshub_bot")
        if isinstance(method, GetUpdates):
            if self.updates:
                updates, self.updates = self.updates, []
                return updates
            await asyncio.Event().wait()  # ждём до отмены polling
        if isinstance(method, SendMessage):
            self.sent.append(method.text)
        return True

    async def close(self) -> None:
        return None

    async def stream_content(self, *args: Any, **kwargs: Any) -> AsyncGenerator[bytes]:
        yield b""


def text_update(text: str) -> Update:
    return Update.model_validate(
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "date": int(datetime.now(UTC).timestamp()),
                "chat": {"id": 7, "type": "private"},
                "from": {"id": 7, "is_bot": False, "first_name": "Іван"},
                "text": text,
            },
        }
    )


async def run_until_update_started(
    settings: Settings, session: FakeSession, work: asyncio.Event
) -> tuple[asyncio.Event, asyncio.Event]:
    """Polling с одним апдейтом, «сессия БД» которого ждёт work; как только апдейт начался —
    stop_polling (как SIGINT). Возвращает события «апдейт начался» и «апдейт закончил с БД»."""
    started, finished = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def slow_scope() -> AsyncIterator[Any]:
        started.set()
        await work.wait()
        yield None  # обработчик «любого текста» в БД не ходит
        finished.set()

    # Redis не нужен: UserCache только хранит клиента, а обработчик списка команд его не трогает.
    dp = create_dispatcher(slow_scope, cast(Redis, None), settings)
    bot = Bot("42:TEST-token", session=session)

    async def stop() -> None:
        await started.wait()
        await dp.stop_polling()

    await asyncio.gather(
        dp.start_polling(bot, handle_signals=False, tasks_concurrency_limit=4), stop()
    )
    return started, finished


async def test_stop_waits_for_update_in_flight(settings: Settings) -> None:
    """aiogram сам задачи апдейтов не ждёт: без InFlightUpdates start_polling вернулся бы
    сразу, а ответ не ушёл бы никогда."""
    session = FakeSession([text_update("привіт")])
    work = asyncio.Event()
    asyncio.get_running_loop().call_later(0.3, work.set)  # обработка дольше остановки

    _, finished = await run_until_update_started(settings, session, work)

    assert finished.is_set()
    assert session.sent == [commands_text()]  # ответ ушёл до закрытия сессии Bot API


async def test_stop_cancels_update_after_drain_timeout(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bot_dispatcher, "DRAIN_TIMEOUT", 0.2)
    session = FakeSession([text_update("привіт")])

    with capture_logs() as logs:
        _, finished = await run_until_update_started(settings, session, asyncio.Event())

    assert not finished.is_set()
    assert session.sent == []
    assert {"event": "bot_updates_cancelled", "count": 1, "log_level": "warning"} in logs


async def test_prepare_retries_network_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        bot_main,
        "STARTUP_BACKOFF",
        BackoffConfig(min_delay=0.01, max_delay=0.02, factor=1.5, jitter=0),
    )
    session = FakeSession(fail=2)
    bot = Bot("42:TEST-token", session=session)

    with capture_logs() as logs:
        me = await bot_main.prepare(bot)

    assert me.username == "saleshub_bot"
    assert session.calls == [
        "DeleteWebhook",
        "DeleteWebhook",
        "DeleteWebhook",
        "SetMyCommands",
        "GetMe",
    ]
    retries = [log for log in logs if log["event"] == "telegram_bot_startup_retry"]
    assert [log["error"] for log in retries] == ["TelegramNetworkError"] * 2
