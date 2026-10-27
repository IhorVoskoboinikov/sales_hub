"""Привязка Telegram (раздел 16, «Привязка Telegram»): ссылка на бота из профиля, /start <token>
в боте, статус и отвязка. Сервис общий для API (/api/v1/me/telegram) и процесса бота.

Токен ссылки — тот же механизм, что у писем: email_tokens с целью telegram_link, в базе только
хэш, новый запрос гасит предыдущий. 43 символа [A-Za-z0-9_-] укладываются в лимит Telegram для
параметра start (64). В payload токена — token_version из access-токена, которым ссылку
запросили: после logout-all, смены или сброса пароля ссылка мертва, а привязку эти операции
удаляют сами (AuthService, PasswordResetService, MemberService при отключении).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy.exc import IntegrityError

from app.errors import Conflict, Forbidden, ServiceUnavailable, ValidationFailed
from app.models import EmailTokenPurpose
from app.repositories.telegram_links import CHAT_CONSTRAINT
from app.repositories.user_cache import CachedUser, UserCache
from app.services.auth import CurrentUser, cached_user
from app.services.bulk import unique_constraint
from app.services.email_tokens import issue_email_token, use_email_token
from app.uow import UnitOfWork

logger = structlog.get_logger("telegram")


@dataclass(frozen=True)
class LinkOffer:
    url: str  # https://t.me/<бот>?start=<токен>
    expires_at: datetime


@dataclass(frozen=True)
class TelegramStatus:
    linked: bool
    username: str | None
    linked_at: datetime | None


def can_use_bot(user: CachedUser) -> bool:
    """Боту — только активному пользователю организации с подтверждённым email. У супер-админа
    организации нет: данных CRM для бота у него нет."""
    return user.is_active and user.email_verified and user.org_id is not None


class TelegramService:
    def __init__(
        self,
        uow: UnitOfWork,
        cache: UserCache,
        *,
        link_ttl: timedelta,
        bot_username: str | None = None,
    ) -> None:
        """bot_username — имя бота для ссылки; None — бот не настроен (нет токена или имени):
        ссылку выдать нельзя, а статус и отвязка работают."""
        self.uow = uow
        self.cache = cache
        self.link_ttl = link_ttl
        self.bot_username = bot_username

    # --- API: /api/v1/me/telegram

    async def issue_link(self, user: CurrentUser) -> LinkOffer:
        """Ссылка на бота с одноразовым токеном; предыдущая ссылка перестаёт работать.

        Строку пользователя не блокируем (как и при сбросе пароля): link() держит токен и
        вставляет привязку с проверкой FK на users, и блокировка users здесь при гашении его
        токенов дала бы взаимную блокировку. Два одновременных запроса могут оставить две живые
        ссылки — обе доказывают владение тем же аккаунтом и живут TELEGRAM_LINK_TTL_MINUTES."""
        if self.bot_username is None:
            raise ServiceUnavailable("telegram.disabled")
        token = await issue_email_token(
            self.uow,
            EmailTokenPurpose.TELEGRAM_LINK,
            user.email,
            self.link_ttl,
            user_id=user.id,
            payload={"token_version": user.token_version},
        )
        await self.uow.commit()
        return LinkOffer(
            url=f"https://t.me/{self.bot_username}?start={token.raw}",
            expires_at=token.row.expires_at,
        )

    async def status(self, user: CurrentUser) -> TelegramStatus:
        link = await self.uow.telegram_links.get(user.id)
        if link is None:
            return TelegramStatus(linked=False, username=None, linked_at=None)
        return TelegramStatus(linked=True, username=link.username, linked_at=link.linked_at)

    async def unlink(self, user: CurrentUser) -> None:
        """Отвязать. Без привязки — тоже успех: результат тот же."""
        await self.uow.telegram_links.delete_for_user(user.id)
        await self.uow.commit()

    # --- бот

    async def link(self, raw_token: str, chat_id: int, username: str | None) -> CachedUser:
        """/start <token>: привязать чат к владельцу токена, одна транзакция (раздел 16, шаг 3).

        Токен доказывает владение аккаунтом CRM, апдейт — владение чатом, но не согласие отдать
        чат: ссылку на свой аккаунт может прислать кто угодно. Поэтому чат, привязанный к
        другому пользователю, который ещё может пользоваться ботом, не перепривязывается —
        Conflict telegram.chat_taken; привязку отключённого чат забирает. Новый чат пользователя
        заменяет старый. Гонка двух /start из одного чата — откат (токен не погашен) и Conflict
        «спробуйте ще раз»."""
        token = await use_email_token(self.uow, EmailTokenPurpose.TELEGRAM_LINK, raw_token)
        # FOR SHARE: отключение, logout-all, смена и сброс пароля (FOR UPDATE) ждут привязку и
        # затем удаляют её, или она ждёт их и видит is_active = false и новую token_version.
        row = await self.uow.users.get_for_share(token.user_id) if token.user_id else None
        user = cached_user(row) if row is not None else None
        if user is None or not can_use_bot(user):
            raise Forbidden("telegram.access_denied")  # коммита нет — токен не погашен
        if token.payload.get("token_version") != user.token_version:
            # Ссылку выдали до logout-all, смены или сброса пароля — возможно, по украденному
            # токену: она мертва, как и сессии того времени.
            raise ValidationFailed("email_token.invalid")
        await self._take_chat(chat_id, user.id)
        try:
            await self.uow.telegram_links.upsert(user.id, chat_id, username, datetime.now(UTC))
            await self.uow.commit()
        except IntegrityError as exc:
            await self.uow.rollback()
            if unique_constraint(exc) != CHAT_CONSTRAINT:
                raise
            # Параллельный /start из этого же чата успел привязать его к другому аккаунту.
            logger.info("telegram_link_race", user_id=str(user.id))
            raise Conflict("telegram.link_conflict") from exc
        logger.info("telegram_linked", user_id=str(user.id))
        return user

    async def _take_chat(self, chat_id: int, user_id: uuid.UUID) -> None:
        """Чат свободен или уже этого пользователя — ничего не делать. Привязан к другому:
        тот ещё может пользоваться ботом — Conflict (сначала отвязать: /stop или профиль того
        аккаунта), отключён — удалить его привязку. Удаляется именно увиденная пара: привязку,
        которую параллельная транзакция сделала после чтения, не трогаем — её поймает upsert."""
        current = await self.uow.telegram_links.get_by_chat(chat_id)
        if current is None or current.user_id == user_id:
            return
        owner = await self._load_user(current.user_id)
        if owner is not None and can_use_bot(owner):
            raise Conflict("telegram.chat_taken")  # коммита нет — токен не погашен
        await self.uow.telegram_links.delete(current.user_id, chat_id, current.linked_at)
        logger.info("telegram_link_revoked", user_id=str(current.user_id), reason="access_denied")

    async def linked_user(self, chat_id: int) -> CachedUser | None:
        """Чей это чат: None — не привязан. Привязан к отключённому (неактивен, email не
        подтверждён, нет организации) — привязка удаляется и Forbidden: бот отвечает «доступ
        закрито». Пользователь — из того же кэша Redis, что у current_user."""
        link = await self.uow.telegram_links.get_by_chat(chat_id)
        if link is None:
            return None
        user = await self._load_user(link.user_id)
        if user is None or not can_use_bot(user):
            await self.uow.telegram_links.delete(link.user_id, chat_id, link.linked_at)
            await self.uow.commit()
            logger.info("telegram_link_revoked", user_id=str(link.user_id), reason="access_denied")
            raise Forbidden("telegram.access_denied")
        return user

    async def _load_user(self, user_id: uuid.UUID) -> CachedUser | None:
        user = await self.cache.get(user_id)
        if user is None:
            row = await self.uow.users.get(user_id)
            if row is None:
                return None
            user = cached_user(row)
            await self.cache.set(user)
        return user
