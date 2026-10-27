"""Telegram-бот без Telegram (разделы 10, 16): create_dispatcher с сессией теста и
dp.feed_update. Обработчики возвращают метод SendMessage — тест сравнивает его текст.
"""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import Any

import pytest
import structlog
from aiogram import Bot, Dispatcher
from aiogram.dispatcher.event.bases import UNHANDLED
from aiogram.methods import SendMessage
from aiogram.types import Update
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.testing import capture_logs

from app.bot.dispatcher import create_dispatcher
from app.bot.handlers import commands_text
from app.bot.middlewares import LinkedUserMiddleware
from app.core.config import Settings
from app.core.security import hash_token
from app.errors import Forbidden
from app.models import EmailToken, EmailTokenPurpose, TelegramLink, User
from app.repositories.user_cache import CachedUser, UserCache
from app.services.email_tokens import issue_email_token
from app.services.telegram import TelegramService
from app.texts import uk
from app.uow import UnitOfWork
from tests.integration.conftest import MakeOrg, MakeUser, member

CHAT = 2_000_001
_update_ids = count(1)


@pytest.fixture
async def redis(redis_url: str) -> AsyncIterator[Redis]:
    client = Redis.from_url(redis_url)
    yield client
    await client.aclose()


@pytest.fixture
async def bot() -> AsyncIterator[Bot]:
    # Токен нужного формата, но сети нет: обработчики только возвращают методы.
    bot = Bot("42:TEST-token")
    yield bot
    await bot.session.close()


@pytest.fixture
def dp(db_session: AsyncSession, redis: Redis, settings: Settings) -> Dispatcher:
    @asynccontextmanager
    async def scope() -> AsyncIterator[AsyncSession]:
        # Как подменённая get_session у API: ошибка — откат SAVEPOINT апдейта, данные теста целы.
        try:
            yield db_session
        except Exception:
            await db_session.rollback()
            raise

    return create_dispatcher(scope, redis, settings)


def update(text: str, *, chat_id: int = CHAT, chat_type: str = "private") -> Update:
    return Update.model_validate(
        {
            "update_id": next(_update_ids),
            "message": {
                "message_id": 1,
                "date": int(datetime.now(UTC).timestamp()),
                "chat": {"id": chat_id, "type": chat_type},
                "from": {"id": chat_id, "is_bot": False, "first_name": "Іван", "username": "ivan"},
                "text": text,
            },
        }
    )


async def send(dp: Dispatcher, bot: Bot, text: str, **kwargs: Any) -> Any:
    return await dp.feed_update(bot, update(text, **kwargs))


async def answer(dp: Dispatcher, bot: Bot, text: str, **kwargs: Any) -> str:
    result = await send(dp, bot, text, **kwargs)
    assert isinstance(result, SendMessage), result
    assert result.chat_id == kwargs.get("chat_id", CHAT)
    return result.text


async def link_token(uow: UnitOfWork, user: User) -> str:
    token = await issue_email_token(
        uow,
        EmailTokenPurpose.TELEGRAM_LINK,
        user.email,
        timedelta(minutes=10),
        user_id=user.id,
        payload={"token_version": user.token_version},  # как TelegramService.issue_link
    )
    await uow.commit()
    return token.raw


async def link_of(uow: UnitOfWork, user_id: uuid.UUID) -> TelegramLink | None:
    return await uow.telegram_links.get(user_id)


async def test_start_with_token_links_chat(
    dp: Dispatcher, bot: Bot, uow: UnitOfWork, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    raw = await link_token(uow, user)

    text = await answer(dp, bot, f"/start {raw}")

    assert text == uk.BOT["linked"].format(email=user.email)
    link = await link_of(uow, user.id)
    assert link is not None
    assert (link.chat_id, link.username) == (CHAT, "ivan")
    # Та же ссылка второй раз — текст бота по коду ошибки.
    assert await answer(dp, bot, f"/start {raw}") == uk.BOT_ERRORS["email_token.invalid"]


async def test_start_with_token_of_disabled_user_is_denied(
    dp: Dispatcher, bot: Bot, uow: UnitOfWork, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "sales_rep", is_active=False)
    user_id = user.id
    raw = await link_token(uow, user)

    assert await answer(dp, bot, f"/start {raw}") == uk.ERRORS["telegram.access_denied"]
    assert await link_of(uow, user_id) is None
    token = await uow.session.scalar(
        select(EmailToken)
        .where(EmailToken.token_hash == hash_token(raw))
        .execution_options(populate_existing=True)
    )
    assert token is not None
    assert token.used_at is None  # откат сессии апдейта


async def test_start_with_token_does_not_take_chat_of_other_user(
    dp: Dispatcher, bot: Bot, uow: UnitOfWork, make_org: MakeOrg, make_user: MakeUser
) -> None:
    owner = await member(make_user, await make_org(), "sales_rep")
    stranger = await member(make_user, await make_org(), "sales_rep")
    owner_id, stranger_id = owner.id, stranger.id
    await answer(dp, bot, f"/start {await link_token(uow, owner)}")

    text = await answer(dp, bot, f"/start {await link_token(uow, stranger)}")

    assert text == uk.ERRORS["telegram.chat_taken"]
    link = await link_of(uow, owner_id)
    assert link is not None
    assert link.chat_id == CHAT
    assert await link_of(uow, stranger_id) is None


async def test_start_without_token(
    dp: Dispatcher, bot: Bot, uow: UnitOfWork, make_org: MakeOrg, make_user: MakeUser
) -> None:
    assert await answer(dp, bot, "/start") == uk.BOT["how_to_link"]

    user = await member(make_user, await make_org(), "team_lead")
    await answer(dp, bot, f"/start {await link_token(uow, user)}")

    assert await answer(dp, bot, "/start") == uk.BOT["connected_as"].format(email=user.email)


async def test_start_without_token_drops_link_of_disabled_user(
    dp: Dispatcher,
    bot: Bot,
    uow: UnitOfWork,
    redis: Redis,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    await answer(dp, bot, f"/start {await link_token(uow, user)}")
    user.is_active = False
    await uow.commit()
    await UserCache(redis).invalidate(user.id)

    assert await answer(dp, bot, "/start") == uk.ERRORS["telegram.access_denied"]
    assert await link_of(uow, user.id) is None
    assert await answer(dp, bot, "/start") == uk.BOT["how_to_link"]


@pytest.mark.parametrize("text", ["привіт", "/today", "/unknown"])
async def test_other_text_lists_commands(dp: Dispatcher, bot: Bot, text: str) -> None:
    assert await answer(dp, bot, text) == commands_text()
    assert "/start" in commands_text()


@pytest.mark.parametrize("chat_type", ["group", "supergroup", "channel"])
async def test_bot_ignores_non_private_chats(
    dp: Dispatcher,
    bot: Bot,
    uow: UnitOfWork,
    chat_type: str,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    raw = await link_token(uow, user)

    assert await send(dp, bot, f"/start {raw}", chat_id=-100, chat_type=chat_type) is UNHANDLED
    assert await send(dp, bot, "привіт", chat_id=-100, chat_type=chat_type) is UNHANDLED
    assert await link_of(uow, user.id) is None


async def test_unexpected_error_is_generic_text_and_logged_without_message_text(
    dp: Dispatcher,
    bot: Bot,
    uow: UnitOfWork,
    make_org: MakeOrg,
    make_user: MakeUser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    raw = await link_token(uow, user)

    async def broken(self: TelegramService, *args: Any) -> CachedUser:
        raise RuntimeError("boom")

    monkeypatch.setattr(TelegramService, "link", broken)

    # merge_contextvars: в строке лога — request_id и update_id апдейта, как в настоящем логе.
    with capture_logs(processors=[structlog.contextvars.merge_contextvars]) as logs:
        text = await answer(dp, bot, f"/start {raw}")

    assert text == uk.BOT["error"]
    failed = next(log for log in logs if log["event"] == "bot_update_failed")
    assert failed["log_level"] == "error"
    assert len(failed["request_id"]) == 32
    assert isinstance(failed["update_id"], int)
    assert raw not in repr(logs)  # ни токена, ни текста сообщения
    assert structlog.contextvars.get_contextvars().get("request_id") is None  # не утекает


async def test_linked_user_middleware(
    uow: UnitOfWork,
    redis: Redis,
    dp: Dispatcher,
    bot: Bot,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    """LinkedUserMiddleware — для команд шага 5.13: вызывается напрямую с тестовым обработчиком."""
    telegram = TelegramService(uow, UserCache(redis), link_ttl=timedelta(minutes=10))
    middleware = LinkedUserMiddleware()
    seen: list[CachedUser] = []

    async def handler(event: Any, data: dict[str, Any]) -> str:
        seen.append(data["user"])
        return "handled"

    message = update("/today").message
    assert message is not None

    # Не привязан — как привязать, обработчик не вызывается.
    result = await middleware(handler, message, {"telegram": telegram})
    assert isinstance(result, SendMessage)
    assert result.text == uk.BOT["how_to_link"]
    assert seen == []

    user = await member(make_user, await make_org(), "sales_rep")
    await answer(dp, bot, f"/start {await link_token(uow, user)}")
    assert await middleware(handler, message, {"telegram": telegram}) == "handled"
    assert [u.id for u in seen] == [user.id]

    # Отключили — Forbidden (бот ответит «доступ закрито»), привязка удалена.
    user.is_active = False
    await uow.commit()
    await UserCache(redis).invalidate(user.id)
    with pytest.raises(Forbidden) as error:
        await middleware(handler, message, {"telegram": telegram})
    assert error.value.code == "telegram.access_denied"
    assert await link_of(uow, user.id) is None
    assert len(seen) == 1
