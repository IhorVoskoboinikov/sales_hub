"""Процесс бота: python -m app.bot (раздел 16, «Запуск»).

Long polling — внешний адрес не нужен; webhook появится при переходе в облако (этап 10.7).
Копия процесса должна быть одна: две копии polling конфликтуют (409 у getUpdates).
"""

import asyncio
import sys

import structlog
from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.types import BotCommand, User
from aiogram.utils.backoff import Backoff, BackoffConfig

from app.bot.dispatcher import create_dispatcher
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.resources import Resources
from app.texts import uk

logger = structlog.get_logger("bot")

# Одновременно обрабатываемых апдейтов; пул Postgres бота — на одно соединение больше
# (бюджет соединений, раздел 15: «бот 5»).
CONCURRENCY = 4
POOL_SIZE = CONCURRENCY + 1
# Повторы запуска — как у polling aiogram (DEFAULT_BACKOFF_CONFIG): 1 с, рост в 1,3 раза, до 5 с.
STARTUP_BACKOFF = BackoffConfig(min_delay=1.0, max_delay=5.0, factor=1.3, jitter=0.1)
# Временные ошибки Bot API: сеть, 5xx, 429. Неверный токен (401) повтор не исправит.
RETRYABLE = (TelegramNetworkError, TelegramServerError, TelegramRetryAfter)


async def prepare(bot: Bot) -> User:
    """delete_webhook, set_my_commands и getMe до polling — с повторами, как сам polling.
    Под watchfiles упавший процесс никто не перезапустит (он ждёт только правок файлов), и
    короткий сбой сети на старте оставил бы контейнер Up с мёртвым ботом."""
    backoff = Backoff(STARTUP_BACKOFF)
    while True:
        try:
            # Webhook и polling взаимоисключают друг друга: снимаем webhook, если он остался.
            await bot.delete_webhook()
            await bot.set_my_commands(
                [
                    BotCommand(command=name, description=text)
                    for name, text in uk.BOT_COMMANDS.items()
                ]
            )
            return await bot.me()
        except RETRYABLE as exc:
            # Только класс ошибки: в тексте сетевой ошибки aiohttp бывает URL с токеном бота.
            delay = next(backoff)
            logger.warning(
                "telegram_bot_startup_retry", error=type(exc).__name__, delay=round(delay, 1)
            )
            await asyncio.sleep(delay)


async def main(settings: Settings) -> int:
    configure_logging(settings.log_level, json=settings.log_json)
    if settings.telegram_bot_token is None:
        # make bot без токена: понятная строка в логе вместо traceback, ресурсы не создаются.
        logger.error("telegram_bot_token_missing")
        return 1
    resources = Resources.create(settings, pool_size=POOL_SIZE, max_overflow=0)
    bot = Bot(
        settings.telegram_bot_token.get_secret_value(),
        session=AiohttpSession(api=TelegramAPIServer.from_base(settings.telegram_api_url)),
    )
    dp = create_dispatcher(resources.session_factory, resources.redis, settings)
    try:
        me = await prepare(bot)
        if settings.telegram_bot_username and me.username != settings.telegram_bot_username:
            # Ссылка из профиля вела бы в чужого бота.
            logger.warning("telegram_bot_username_mismatch", actual=me.username)
        logger.info("telegram_bot_started", username=me.username)
        # SIGTERM и SIGINT обрабатывает aiogram: останавливает polling, а начатые апдейты
        # дожидается dp.shutdown (InFlightUpdates, до DRAIN_TIMEOUT) — до закрытия ресурсов.
        await dp.start_polling(
            bot, allowed_updates=["message"], tasks_concurrency_limit=CONCURRENCY
        )
    finally:
        await bot.session.close()  # повторное закрытие после start_polling ничего не делает
        await resources.aclose()
    logger.info("telegram_bot_stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(get_settings())))
