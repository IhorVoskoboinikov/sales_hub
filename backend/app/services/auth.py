"""Вход, сессии и проверка access-токена (раздел 4)."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog

from app.core.jwt import KeySet
from app.core.security import hash_token, new_token, verify_dummy_password, verify_password
from app.errors import Forbidden, Unauthorized
from app.models import RefreshToken, User
from app.repositories.user_cache import CachedUser, UserCache
from app.uow import UnitOfWork

logger = structlog.get_logger("auth")


@dataclass(frozen=True)
class CurrentUser:
    """Кто делает запрос. Организация, отдел и роль добавятся на этапе 3."""

    id: uuid.UUID
    email: str
    is_superadmin: bool
    session_id: uuid.UUID  # sid из access-токена: по нему logout знает, какую сессию закрыть


@dataclass(frozen=True)
class ClientInfo:
    """Откуда пришёл запрос — показывается в списке сессий."""

    ip: str | None
    user_agent: str | None


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str
    expires_in: int  # секунды жизни access-токена


class AuthService:
    def __init__(
        self,
        uow: UnitOfWork,
        keys: KeySet,
        cache: UserCache,
        refresh_ttl: timedelta,
    ) -> None:
        self.uow = uow
        self.keys = keys
        self.cache = cache
        self.refresh_ttl = refresh_ttl

    async def login(self, email: str, password: str, client: ClientInfo) -> TokenPair:
        user = await self.uow.users.get_by_email(email)
        if user is None:
            await verify_dummy_password(password)  # то же время ответа, что и с настоящим email
            raise Unauthorized("auth.invalid_credentials")
        ok, new_hash = await verify_password(password, user.password_hash)
        if not ok:
            raise Unauthorized("auth.invalid_credentials")
        # Причину отказа говорим только тому, кто знает пароль.
        ensure_can_sign_in(cached_user(user))
        if new_hash is not None:
            user.password_hash = new_hash  # параметры argon2 выросли — перехэшировали
        user.last_login_at = datetime.now(UTC)
        pair = self._start_session(user, client)
        await self.uow.commit()
        return pair

    async def refresh(self, raw_token: str, client: ClientInfo) -> TokenPair:
        """Ротация: старый refresh-токен отзывается, выдаётся новая пара в той же сессии."""
        now = datetime.now(UTC)
        token = await self.uow.refresh_tokens.get_by_hash_for_update(hash_token(raw_token))
        if token is None:
            raise Unauthorized()
        if token.revoked_at is not None:
            # Отозванный токен пришёл ещё раз — его украли: им уже воспользовался либо вор,
            # либо хозяин. Кто из них сейчас, не понять, поэтому закрываем всю сессию.
            await self.uow.refresh_tokens.revoke_family(token.family_id, now)
            await self.uow.commit()
            logger.warning(
                "refresh_token_reused", user_id=str(token.user_id), sid=str(token.family_id)
            )
            raise Unauthorized()
        if token.expires_at <= now:
            raise Unauthorized()
        user = await self.uow.users.get(token.user_id)
        if user is None:
            raise Unauthorized()
        ensure_can_sign_in(cached_user(user))

        # Срок не продлевается: сессия живёт не дольше refresh_ttl с момента входа.
        new = self._new_refresh_token(user.id, token.family_id, token.expires_at, client)
        # Сначала INSERT нового токена, потом ссылка на него: replaced_by — внешний ключ.
        await self.uow.flush()
        token.revoked_at = now
        token.replaced_by = new[0].id
        pair = self._token_pair(user, token.family_id, new)
        await self.uow.commit()
        return pair

    async def authenticate(self, access_token: str) -> CurrentUser:
        """Зависимость current_user: подпись и сроки токена, затем пользователь (кэш → БД)."""
        claims = self.keys.decode_access_token(access_token)
        user = await self.cache.get(claims.user_id)
        if user is None:
            row = await self.uow.users.get(claims.user_id)
            if row is None:
                raise Unauthorized()
            user = cached_user(row)
            await self.cache.set(user)
        # Версия выросла (logout-all, смена или сброс пароля) — все старые access-токены мертвы.
        if user.token_version != claims.token_version:
            raise Unauthorized()
        ensure_can_sign_in(user)
        return CurrentUser(
            id=user.id,
            email=user.email,
            is_superadmin=user.is_superadmin,
            session_id=claims.session_id,
        )

    def _start_session(self, user: User, client: ClientInfo) -> TokenPair:
        """Новая сессия (семья refresh-токенов). Коммит — за вызывающим."""
        family_id = uuid.uuid4()
        refresh = self._new_refresh_token(
            user.id, family_id, datetime.now(UTC) + self.refresh_ttl, client
        )
        return self._token_pair(user, family_id, refresh)

    def _new_refresh_token(
        self,
        user_id: uuid.UUID,
        family_id: uuid.UUID,
        expires_at: datetime,
        client: ClientInfo,
    ) -> tuple[RefreshToken, str]:
        raw = new_token()
        token = RefreshToken(
            id=uuid.uuid4(),
            user_id=user_id,
            family_id=family_id,
            token_hash=hash_token(raw),
            created_at=datetime.now(UTC),
            expires_at=expires_at,
            user_agent=client.user_agent,
            ip=client.ip,
        )
        self.uow.refresh_tokens.add(token)
        return token, raw

    def _token_pair(
        self, user: User, family_id: uuid.UUID, refresh: tuple[RefreshToken, str]
    ) -> TokenPair:
        access = self.keys.issue_access_token(user.id, family_id, user.token_version)
        return TokenPair(
            access_token=access,
            refresh_token=refresh[1],
            expires_in=int(self.keys.access_ttl.total_seconds()),
        )


def cached_user(user: User) -> CachedUser:
    return CachedUser(
        id=user.id,
        email=user.email,
        is_active=user.is_active,
        is_superadmin=user.is_superadmin,
        email_verified=user.email_verified_at is not None,
        token_version=user.token_version,
    )


def ensure_can_sign_in(user: CachedUser) -> None:
    if not user.is_active:
        raise Forbidden("auth.user_inactive")
    if not user.email_verified:
        raise Forbidden("auth.email_not_verified")
