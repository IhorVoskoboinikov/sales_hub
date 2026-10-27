"""Обработчики команд бота (раздел 16, «Команды бота»).

Обработчик не отправляет ответ сам, а возвращает метод (message.answer(...)): aiogram выполнит
его после обработки, а тест через dp.feed_update сравнит текст без Telegram. Ошибки сервисов
(AppError) превращает в ответ UpdateContextMiddleware.
"""

from aiogram import Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.methods import SendMessage
from aiogram.types import Message

from app.bot.middlewares import LinkedUserMiddleware
from app.repositories.user_cache import CachedUser
from app.services.agenda import AgendaService, render_agenda, render_deals
from app.services.telegram import TelegramService
from app.texts import uk


async def start_with_token(
    message: Message, command: CommandObject, telegram: TelegramService
) -> SendMessage:
    """/start <token> — по ссылке из профиля: привязать этот чат к владельцу токена."""
    username = message.from_user.username if message.from_user is not None else None
    user = await telegram.link(command.args or "", message.chat.id, username)
    return message.answer(uk.BOT["linked"].format(email=user.email))


async def start(message: Message, telegram: TelegramService) -> SendMessage:
    """/start без токена: кто подключён к этому чату, иначе — как подключить."""
    user = await telegram.linked_user(message.chat.id)
    if user is None:
        return message.answer(uk.BOT["how_to_link"])
    return message.answer(uk.BOT["connected_as"].format(email=user.email))


async def stop(message: Message, telegram: TelegramService) -> SendMessage:
    """/stop — отвязать чат. Стоит вне LinkedUserMiddleware: отключённый пользователь тоже
    может отвязаться, а повтор без привязки — не ошибка."""
    stopped = await telegram.stop(message.chat.id)
    return message.answer(uk.BOT["stopped"] if stopped else uk.BOT["not_linked"])


async def today(message: Message, user: CachedUser, agenda: AgendaService) -> SendMessage:
    """/today — мои задачи на сегодня и просроченные (AgendaService.day — тот же, что у
    утреннего дайджеста)."""
    day = await agenda.day(user)
    if day.empty:
        return message.answer(uk.BOT["today_empty"])
    title = uk.BOT["today_title"].format(date=day.date.strftime("%d.%m.%Y"))
    return message.answer(render_agenda(day, title))


async def deals(message: Message, user: CachedUser, agenda: AgendaService) -> SendMessage:
    """/deals — мои открытые сделки и итог по ним в валюте организации."""
    summary = await agenda.deals(user)
    if summary is None:
        return message.answer(uk.BOT["deals_empty"])
    return message.answer(render_deals(summary))


async def unknown(message: Message) -> SendMessage:
    """Любой другой текст — список команд."""
    return message.answer(commands_text())


def commands_text() -> str:
    lines = (f"/{command} — {description}" for command, description in uk.BOT_COMMANDS.items())
    return uk.BOT["commands"].format(commands="\n".join(lines))


def create_routers() -> list[Router]:
    """Роутеры в порядке проверки: команды без привязки (/start, /stop), команды
    привязанного пользователя, «всё остальное». Новые на каждый вызов: роутер нельзя
    подключить к двум диспетчерам, а тесты создают диспетчер на тест."""
    public = Router(name="public")
    public.message.register(start_with_token, CommandStart(deep_link=True))
    public.message.register(start, CommandStart())
    public.message.register(stop, Command("stop"))

    # Команды привязанного пользователя: обработчик получает user из LinkedUserMiddleware.
    # Внутренний middleware работает, только когда нашёлся обработчик, — любой другой текст
    # проходит к fallback без запросов к привязкам.
    linked = Router(name="linked")
    linked.message.middleware(LinkedUserMiddleware())
    linked.message.register(today, Command("today"))
    linked.message.register(deals, Command("deals"))

    fallback = Router(name="fallback")
    fallback.message.register(unknown)
    return [public, linked, fallback]
