"""Middleware бота (раздел 16): контекст и ошибки апдейта, сессия БД, привязанный пользователь.

Порядок: InFlightUpdates и UpdateContextMiddleware (внешние, на каждый апдейт) → фильтр «только
личные чаты» →
SessionMiddleware (внутренний: только когда нашёлся обработчик) → LinkedUserMiddleware
(только у команд привязанного пользователя) → обработчик.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Any, cast

import structlog
from aiogram import BaseMiddleware
from aiogram.dispatcher.event.bases import CancelHandler, SkipHandler
from aiogram.methods import SendMessage
from aiogram.types import Message, TelegramObject, Update
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.context import request_id_var
from app.errors import AppError
from app.repositories.user_cache import UserCache
from app.services.telegram import TelegramService
from app.texts import uk
from app.uow import UnitOfWork

logger = structlog.get_logger("bot")

Handler = Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]]
# Сессия на апдейт: `async with scope() as session` — async_sessionmaker процесса, в тестах —
# сессия теста (как SessionScope в api/deps.py; бот API-зависимости не импортирует).
SessionScope = Callable[[], AbstractAsyncContextManager[AsyncSession]]


class InFlightUpdates(BaseMiddleware):
    """Апдейты, которые ещё обрабатываются, и ожидание их при остановке (dp.shutdown).

    aiogram 3.31 на SIGINT/SIGTERM отменяет только задачу polling: задачи апдейтов он не ждёт,
    start_polling сразу закрывает сессию Bot API и возвращается, а процесс закрыл бы пул БД под
    работающим обработчиком. Ответ бы не ушёл, а Telegram апдейт не повторит: offset следующего
    getUpdates его уже подтвердил. Поэтому middleware запоминает задачу каждого апдейта (в ней
    же aiogram потом отправляет ответ-метод), а drain ждёт их до timeout и отменяет оставшиеся.
    """

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self.tasks: set[asyncio.Task[Any]] = set()

    async def __call__(self, handler: Handler, event: TelegramObject, data: dict[str, Any]) -> Any:
        task = asyncio.current_task()
        if task is not None and task not in self.tasks:
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)
        return await handler(event, data)

    async def drain(self) -> None:
        """Обработчик dp.shutdown: aiogram вызывает его до закрытия сессии Bot API, поэтому
        ответы дожидающихся апдейтов ещё уходят."""
        tasks = self.tasks - {asyncio.current_task()}
        if not tasks:
            return
        logger.info("bot_updates_draining", count=len(tasks))
        _, pending = await asyncio.wait(tasks, timeout=self.timeout)
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.wait(pending)
            logger.warning("bot_updates_cancelled", count=len(pending))


class UpdateContextMiddleware(BaseMiddleware):
    """Как RequestId + UnhandledError у API: у апдейта свой request_id (в логах и, через
    request_id_var, в outbox), а ошибка становится ответом пользователю. AppError — текст по
    коду из app/texts/uk.py, остальное — общий текст и лог с traceback. Текст сообщения
    пользователя в лог не пишется: в нём бывают токены и личные данные."""

    async def __call__(self, handler: Handler, event: TelegramObject, data: dict[str, Any]) -> Any:
        update = cast(Update, event)
        request_id = uuid.uuid4().hex
        reset_token = request_id_var.set(request_id)
        try:
            with structlog.contextvars.bound_contextvars(
                request_id=request_id, update_id=update.update_id
            ):
                try:
                    return await handler(event, data)
                except (SkipHandler, CancelHandler):
                    raise  # управление потоком aiogram, не ошибки
                except AppError as exc:
                    logger.info("bot_app_error", code=exc.code, status=exc.status)
                    return reply(update, uk.bot_error_text(exc.code, exc.title))
                except Exception:
                    logger.exception("bot_update_failed")
                    return reply(update, uk.BOT["error"])
        finally:
            request_id_var.reset(reset_token)


def reply(update: Update, text: str) -> SendMessage | None:
    """Ответ методом, а не вызовом: aiogram отправит его сам (в тестах — сравнят текст)."""
    return update.message.answer(text) if update.message is not None else None


class SessionMiddleware(BaseMiddleware):
    """Сессия БД на апдейт — как сессия на запрос в API — и сервисы для обработчиков.
    Соединение берётся из пула при первом запросе, а не здесь."""

    def __init__(self, session_scope: SessionScope, redis: Redis, *, link_ttl: timedelta) -> None:
        self.session_scope = session_scope
        self.cache = UserCache(redis)
        self.link_ttl = link_ttl

    async def __call__(self, handler: Handler, event: TelegramObject, data: dict[str, Any]) -> Any:
        async with self.session_scope() as session:
            uow = UnitOfWork(session)
            data["uow"] = uow
            data["telegram"] = TelegramService(uow, self.cache, link_ttl=self.link_ttl)
            return await handler(event, data)


class LinkedUserMiddleware(BaseMiddleware):
    """Перед каждой командой привязанного пользователя (/today, /deals — шаг 5.13): привязка по
    chat_id → пользователь из кэша Redis (как у current_user) → активен, email подтверждён, есть
    организация. Отключённому — «доступ закрито» и привязка удаляется (TelegramService), без
    привязки — как привязать. Обработчик получает пользователя в аргументе user."""

    async def __call__(self, handler: Handler, event: TelegramObject, data: dict[str, Any]) -> Any:
        message = cast(Message, event)
        telegram = cast(TelegramService, data["telegram"])
        user = await telegram.linked_user(message.chat.id)
        if user is None:
            return message.answer(uk.BOT["how_to_link"])
        data["user"] = user
        with structlog.contextvars.bound_contextvars(user_id=str(user.id)):
            return await handler(event, data)
