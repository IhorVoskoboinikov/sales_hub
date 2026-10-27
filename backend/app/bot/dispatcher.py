"""Сборка диспетчера aiogram: её вызывает и процесс бота (__main__.py), и тесты (dp.feed_update
с сессией теста, без Telegram)."""

from datetime import timedelta

from aiogram import Dispatcher, F
from aiogram.enums import ChatType
from redis.asyncio import Redis

from app.bot.handlers import create_routers
from app.bot.middlewares import (
    InFlightUpdates,
    SessionMiddleware,
    SessionScope,
    UpdateContextMiddleware,
)
from app.core.config import Settings

# Сколько при остановке ждать начатые апдейты: меньше --sigint-timeout 25 у watchfiles и
# stop_grace_period 30s в compose, иначе процесс убьют раньше.
DRAIN_TIMEOUT = 20.0


def create_dispatcher(session_scope: SessionScope, redis: Redis, settings: Settings) -> Dispatcher:
    # FSM не используется (раздел 16): без него и хранилища состояний.
    dp = Dispatcher(disable_fsm=True)
    # Первым: задача апдейта запоминается до всего остального; на остановке — ждать её.
    in_flight = InFlightUpdates(DRAIN_TIMEOUT)
    dp.update.outer_middleware(in_flight)
    dp.shutdown.register(in_flight.drain)
    dp.update.outer_middleware(UpdateContextMiddleware())
    # Только личные чаты: в группе личные данные CRM попали бы в общий чат. Фильтр корневого
    # роутера отсекает апдейт до всех вложенных роутеров и до сессии БД.
    dp.message.filter(F.chat.type == ChatType.PRIVATE)
    # Внутренний middleware корневого роутера действует и на обработчики вложенных.
    dp.message.middleware(
        SessionMiddleware(
            session_scope,
            redis,
            link_ttl=timedelta(minutes=settings.telegram_link_ttl_minutes),
        )
    )
    dp.include_routers(*create_routers())
    return dp
