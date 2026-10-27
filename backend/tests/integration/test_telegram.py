"""Привязка Telegram (раздел 16): TelegramService.link, /api/v1/me/telegram и отвязка при
logout-all, смене и сбросе пароля и отключении.

Сервис — в транзакции теста; гонка двух /start из одного чата воспроизводится подменой:
get_by_chat «не видит» чужую привязку, как параллельная транзакция до её коммита.
"""

import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy import func, select

from app.core.config import Settings
from app.core.security import hash_token
from app.errors import Conflict, Forbidden, ValidationFailed
from app.models import EmailToken, EmailTokenPurpose, TelegramLink, User
from app.repositories.telegram_links import TelegramLinkRepository
from app.repositories.user_cache import UserCache
from app.services.email_tokens import issue_email_token
from app.services.telegram import TelegramService
from app.uow import UnitOfWork
from tests.integration.conftest import (
    PASSWORD,
    MakeOrg,
    MakeUser,
    bearer,
    login,
    member,
    tokens_for,
)

BOT_USERNAME = "saleshub_test_bot"
CHAT = 1_000_001
OTHER_CHAT = 1_000_002
NEW_PASSWORD = "a brand new passphrase"
LINK_URL = re.compile(rf"^https://t\.me/{BOT_USERNAME}\?start=([A-Za-z0-9_-]{{43}})$")


@pytest.fixture
def settings(settings: Settings) -> Settings:
    # Бот настроен: иначе POST /me/telegram/link — 503 telegram.disabled.
    return settings.model_copy(
        update={
            "telegram_bot_token": SecretStr("42:TEST-token"),
            "telegram_bot_username": BOT_USERNAME,
        }
    )


@pytest.fixture
async def redis(redis_url: str) -> AsyncIterator[Redis]:
    client = Redis.from_url(redis_url)
    yield client
    await client.aclose()


@pytest.fixture
def telegram(uow: UnitOfWork, redis: Redis) -> TelegramService:
    return TelegramService(
        uow, UserCache(redis), link_ttl=timedelta(minutes=10), bot_username=BOT_USERNAME
    )


async def link_token(uow: UnitOfWork, user: User, ttl: timedelta = timedelta(minutes=10)) -> str:
    """Как issue_link: в payload — token_version, с которой ссылку выдали."""
    token = await issue_email_token(
        uow,
        EmailTokenPurpose.TELEGRAM_LINK,
        user.email,
        ttl,
        user_id=user.id,
        payload={"token_version": user.token_version},
    )
    await uow.commit()
    return token.raw


def raw_from(resp: Response) -> str:
    """Токен из ссылки ответа POST /me/telegram/link: 43 символа [A-Za-z0-9_-]."""
    match = LINK_URL.match(resp.json()["url"])
    assert match is not None, resp.text
    return match.group(1)


async def links(uow: UnitOfWork) -> dict[uuid.UUID, tuple[int, str | None]]:
    rows = await uow.session.scalars(select(TelegramLink).execution_options(populate_existing=True))
    return {row.user_id: (row.chat_id, row.username) for row in rows}


async def token_used(uow: UnitOfWork, raw: str) -> bool:
    token = await uow.session.scalar(
        select(EmailToken)
        .where(EmailToken.token_hash == hash_token(raw))
        .execution_options(populate_existing=True)
    )
    assert token is not None
    return token.used_at is not None


# --- TelegramService.link


async def test_link_binds_chat_and_uses_token(
    uow: UnitOfWork, telegram: TelegramService, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    raw = await link_token(uow, user)

    linked = await telegram.link(raw, CHAT, "ivan_tg")

    assert linked.id == user.id
    assert await links(uow) == {user.id: (CHAT, "ivan_tg")}
    assert await token_used(uow, raw)
    with pytest.raises(ValidationFailed) as error:  # одноразовый
        await telegram.link(raw, CHAT, "ivan_tg")
    assert error.value.code == "email_token.invalid"


async def test_link_rejects_expired_and_unknown_token(
    uow: UnitOfWork, telegram: TelegramService, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    expired = await link_token(uow, user, ttl=timedelta(seconds=-1))

    with pytest.raises(ValidationFailed) as error:
        await telegram.link(expired, CHAT, None)
    assert error.value.code == "email_token.expired"
    with pytest.raises(ValidationFailed) as error:
        await telegram.link("x" * 43, CHAT, None)
    assert error.value.code == "email_token.invalid"
    assert await links(uow) == {}


async def test_link_token_of_other_purpose_is_invalid(
    uow: UnitOfWork, telegram: TelegramService, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    reset = await issue_email_token(
        uow, EmailTokenPurpose.RESET_PASSWORD, user.email, timedelta(hours=1), user_id=user.id
    )
    await uow.commit()

    with pytest.raises(ValidationFailed) as error:
        await telegram.link(reset.raw, CHAT, None)
    assert error.value.code == "email_token.invalid"


@pytest.mark.parametrize("who", ["inactive", "unverified", "superadmin"])
async def test_link_denied_to_inactive_unverified_and_user_without_org(
    who: str,
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    if who == "inactive":
        user = await member(make_user, await make_org(), "sales_rep", is_active=False)
    elif who == "unverified":
        user = await member(make_user, await make_org(), "sales_rep", email_verified_at=None)
    else:
        user = await make_user(f"admin-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    raw = await link_token(uow, user)

    with pytest.raises(Forbidden) as error:
        await telegram.link(raw, CHAT, None)
    await uow.rollback()  # как сессия бота после ошибки

    assert error.value.code == "telegram.access_denied"
    assert await links(uow) == {}
    assert not await token_used(uow, raw)


async def test_chat_of_other_user_is_not_taken(
    uow: UnitOfWork, telegram: TelegramService, make_org: MakeOrg, make_user: MakeUser
) -> None:
    """Ссылку на свой аккаунт может прислать кто угодно: открыв её, владелец чата не должен
    молча лишиться своей привязки (login CSRF)."""
    first = await member(make_user, await make_org(), "sales_rep")
    stranger = await member(make_user, await make_org(), "sales_rep")  # другая организация
    first_id = first.id
    await telegram.link(await link_token(uow, first), CHAT, "victim")
    raw = await link_token(uow, stranger)

    with pytest.raises(Conflict) as error:
        await telegram.link(raw, CHAT, "victim")
    await uow.rollback()  # как сессия бота после ошибки

    assert error.value.code == "telegram.chat_taken"
    assert await links(uow) == {first_id: (CHAT, "victim")}
    assert not await token_used(uow, raw)


async def test_chat_of_disabled_user_moves_to_token_owner(
    uow: UnitOfWork,
    telegram: TelegramService,
    redis: Redis,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    org = await make_org()
    first, second = [await member(make_user, org, "sales_rep") for _ in range(2)]
    await telegram.link(await link_token(uow, first), CHAT, "shared")
    # Отключили мимо MemberService: привязка осталась, как у отключённого до шага с отвязкой.
    first.is_active = False
    await uow.commit()
    await UserCache(redis).invalidate(first.id)

    await telegram.link(await link_token(uow, second), CHAT, "shared")

    assert await links(uow) == {second.id: (CHAT, "shared")}


async def test_new_chat_replaces_old_one(
    uow: UnitOfWork, telegram: TelegramService, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "team_lead")
    await telegram.link(await link_token(uow, user), CHAT, "old_name")
    first = await uow.telegram_links.get(user.id)
    assert first is not None
    first_linked_at = first.linked_at

    await telegram.link(await link_token(uow, user), OTHER_CHAT, "new_name")

    assert await links(uow) == {user.id: (OTHER_CHAT, "new_name")}
    again = await uow.telegram_links.get(user.id)
    assert again is not None
    assert again.linked_at >= first_linked_at


async def test_race_of_two_starts_from_one_chat_asks_to_retry(
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org = await make_org()
    first, second = [await member(make_user, org, "sales_rep") for _ in range(2)]
    first_id = first.id  # откат «протухнет» объекты сессии
    await telegram.link(await link_token(uow, first), CHAT, None)
    raw = await link_token(uow, second)

    # Параллельный /start из того же чата: его привязка ещё не закоммичена, и SELECT её не видит.
    async def blind_get_by_chat(self: TelegramLinkRepository, chat_id: int) -> None:
        return None

    monkeypatch.setattr(TelegramLinkRepository, "get_by_chat", blind_get_by_chat)

    with pytest.raises(Conflict) as error:
        await telegram.link(raw, CHAT, None)

    assert error.value.code == "telegram.link_conflict"
    assert await links(uow) == {first_id: (CHAT, None)}
    assert not await token_used(uow, raw)  # откат: токен цел
    monkeypatch.undo()
    # Повтор той же ссылкой уже видит победителя гонки: чат занят — chat_taken, токен цел.
    with pytest.raises(Conflict) as error:
        await telegram.link(raw, CHAT, None)
    assert error.value.code == "telegram.chat_taken"
    await uow.rollback()
    assert not await token_used(uow, raw)
    assert await links(uow) == {first_id: (CHAT, None)}


@pytest.mark.parametrize("change", ["inactive", "unverified"])
async def test_linked_user_drops_link_of_disabled_user(
    change: str,
    uow: UnitOfWork,
    telegram: TelegramService,
    redis: Redis,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    await telegram.link(await link_token(uow, user), CHAT, None)
    found = await telegram.linked_user(CHAT)
    assert found is not None
    assert found.id == user.id
    assert await telegram.linked_user(OTHER_CHAT) is None

    if change == "inactive":
        user.is_active = False
    else:
        user.email_verified_at = None
    await uow.commit()
    await UserCache(redis).invalidate(user.id)  # как MemberService после отключения

    with pytest.raises(Forbidden) as error:
        await telegram.linked_user(CHAT)
    assert error.value.code == "telegram.access_denied"
    assert await links(uow) == {}


# --- отвязка вместе с сессиями: чат мог привязать тот, у кого был украденный токен


async def revoke_by(how: str, client: AsyncClient, uow: UnitOfWork, user: User) -> None:
    """Операция, после которой старые токены не работают (token_version + 1)."""
    if how == "logout_all":
        headers = await tokens_for(client, user)
        resp = await client.post("/api/v1/auth/logout-all", headers=headers)
    elif how == "password_change":
        headers = await tokens_for(client, user)
        resp = await client.post(
            "/api/v1/auth/password/change",
            json={"old_password": PASSWORD, "new_password": NEW_PASSWORD},
            headers=headers,
        )
    elif how == "password_reset":
        reset = await issue_email_token(
            uow, EmailTokenPurpose.RESET_PASSWORD, user.email, timedelta(hours=1), user_id=user.id
        )
        await uow.commit()
        resp = await client.post(
            "/api/v1/auth/password/reset", json={"token": reset.raw, "new_password": NEW_PASSWORD}
        )
    else:
        raise AssertionError(how)
    assert resp.status_code in (200, 204), resp.text


REVOKE = ["logout_all", "password_change", "password_reset"]


@pytest.mark.parametrize("how", REVOKE)
async def test_revoking_sessions_unlinks_telegram(
    how: str,
    client: AsyncClient,
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    org = await make_org()
    user, colleague = [await member(make_user, org, "sales_rep") for _ in range(2)]
    user_id = user.id
    await telegram.link(await link_token(uow, user), CHAT, "attacker")
    await telegram.link(await link_token(uow, colleague), OTHER_CHAT, None)

    await revoke_by(how, client, uow, user)

    assert await links(uow) == {colleague.id: (OTHER_CHAT, None)}
    assert await telegram.linked_user(CHAT) is None
    assert await uow.telegram_links.get(user_id) is None


@pytest.mark.parametrize("how", REVOKE)
async def test_link_issued_before_revoking_sessions_is_dead(
    how: str,
    client: AsyncClient,
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    """Ссылку, выданную по украденному access-токену, нельзя доиспользовать после того, как
    хозяин вышел везде или сменил пароль: в ней старая token_version."""
    org = await make_org()
    user = await member(make_user, org, "sales_rep")
    user_id, email = user.id, user.email
    resp = await client.post("/api/v1/me/telegram/link", headers=await tokens_for(client, user))
    raw = raw_from(resp)

    await revoke_by(how, client, uow, user)

    with pytest.raises(ValidationFailed) as error:
        await telegram.link(raw, CHAT, None)
    await uow.rollback()
    assert error.value.code == "email_token.invalid"
    assert await uow.telegram_links.get(user_id) is None
    # Новая ссылка — с новой версией — работает.
    password = PASSWORD if how == "logout_all" else NEW_PASSWORD
    headers = bearer(await login(client, email, password))
    fresh = await client.post("/api/v1/me/telegram/link", headers=headers)
    await telegram.link(raw_from(fresh), CHAT, None)
    assert await uow.telegram_links.get(user_id) is not None


async def test_disabling_member_unlinks_telegram(
    client: AsyncClient,
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    org = await make_org()
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    await telegram.link(await link_token(uow, rep), CHAT, None)

    resp = await client.patch(
        f"/api/v1/users/{rep_id}",
        json={"is_active": False},
        headers=await tokens_for(client, admin),
    )

    assert resp.status_code == 200, resp.text
    assert await links(uow) == {}


# --- /api/v1/me/telegram


async def test_link_endpoint_issues_one_time_url(
    client: AsyncClient, uow: UnitOfWork, make_org: MakeOrg, make_user: MakeUser
) -> None:
    user = await member(make_user, await make_org(), "viewer")
    headers = await tokens_for(client, user)
    before = datetime.now(UTC)

    resp = await client.post("/api/v1/me/telegram/link", headers=headers)

    assert resp.status_code == 201, resp.text
    expires_at = datetime.fromisoformat(resp.json()["expires_at"])
    assert before + timedelta(minutes=10) <= expires_at <= datetime.now(UTC) + timedelta(minutes=10)
    token = await uow.session.scalar(
        select(EmailToken).where(EmailToken.token_hash == hash_token(raw_from(resp)))
    )
    assert token is not None
    assert (token.purpose, token.user_id, token.used_at) == ("telegram_link", user.id, None)


async def test_new_link_revokes_previous(
    client: AsyncClient,
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    headers = await tokens_for(client, user)
    first_raw, second_raw = [
        raw_from(await client.post("/api/v1/me/telegram/link", headers=headers)) for _ in range(2)
    ]

    with pytest.raises(ValidationFailed):
        await telegram.link(first_raw, CHAT, None)
    await telegram.link(second_raw, CHAT, None)
    assert await links(uow) == {user.id: (CHAT, None)}


@pytest.mark.parametrize("missing", ["telegram_bot_token", "telegram_bot_username"])
async def test_link_endpoint_is_503_when_bot_is_not_configured(
    missing: str,
    app: Any,
    client: AsyncClient,
    uow: UnitOfWork,
    settings: Settings,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    user = await member(make_user, await make_org(), "sales_rep")
    headers = await tokens_for(client, user)
    app.state.settings = settings.model_copy(update={missing: None})

    resp = await client.post("/api/v1/me/telegram/link", headers=headers)

    assert resp.status_code == 503
    assert resp.json()["code"] == "telegram.disabled"
    count = await uow.session.scalar(
        select(func.count()).select_from(EmailToken).where(EmailToken.user_id == user.id)
    )
    assert count == 0
    # Статус и отвязка от настройки бота не зависят.
    assert (await client.get("/api/v1/me/telegram", headers=headers)).status_code == 200
    assert (await client.delete("/api/v1/me/telegram", headers=headers)).status_code == 204


async def test_status_and_unlink(
    client: AsyncClient,
    uow: UnitOfWork,
    telegram: TelegramService,
    make_org: MakeOrg,
    make_user: MakeUser,
) -> None:
    org = await make_org()
    user, colleague = [await member(make_user, org, "sales_rep") for _ in range(2)]
    headers = await tokens_for(client, user)
    empty = {"linked": False, "username": None, "linked_at": None}
    assert (await client.get("/api/v1/me/telegram", headers=headers)).json() == empty

    await telegram.link(await link_token(uow, user), CHAT, "ivan_tg")
    await telegram.link(await link_token(uow, colleague), OTHER_CHAT, None)
    body = (await client.get("/api/v1/me/telegram", headers=headers)).json()
    assert body["linked"] is True
    assert body["username"] == "ivan_tg"
    assert datetime.fromisoformat(body["linked_at"]) <= datetime.now(UTC)

    assert (await client.delete("/api/v1/me/telegram", headers=headers)).status_code == 204
    assert (await client.get("/api/v1/me/telegram", headers=headers)).json() == empty
    assert await links(uow) == {colleague.id: (OTHER_CHAT, None)}  # чужую привязку не тронули
    # Повтор без привязки — тоже 204.
    assert (await client.delete("/api/v1/me/telegram", headers=headers)).status_code == 204


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/api/v1/me/telegram/link"),
        ("GET", "/api/v1/me/telegram"),
        ("DELETE", "/api/v1/me/telegram"),
    ],
)
async def test_me_requires_member(
    method: str, path: str, client: AsyncClient, make_user: MakeUser
) -> None:
    resp = await client.request(method, path)
    assert resp.status_code == 401
    assert resp.json()["code"] == "auth.invalid_token"

    # Супер-админ без организации: личных настроек CRM у него нет.
    admin = await make_user(f"admin-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    resp = await client.request(method, path, headers=await tokens_for(client, admin))
    assert resp.status_code == 403
    assert resp.json()["code"] == "forbidden"
