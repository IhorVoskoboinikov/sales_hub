"""Вход, сессии и проверка access-токена (раздел 4)."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog

from app.core.jwt import KeySet
from app.core.security import (
    check_password_rules,
    hash_password,
    hash_token,
    new_token,
    verify_dummy_password,
    verify_password,
)
from app.errors import Forbidden, NotFound, Unauthorized, ValidationFailed
from app.models import RefreshToken, User
from app.repositories import rate_limiter
from app.repositories.rate_limiter import RateLimiter
from app.repositories.user_cache import CachedUser, UserCache
from app.uow import UnitOfWork

logger = structlog.get_logger("auth")


@dataclass(frozen=True)
class CurrentUser:
    """Кто делает запрос. Роль, организация и отдел — из users (кэш 60 с), не из токена:
    они могут поменяться, а токен живёт час."""

    id: uuid.UUID
    email: str
    is_superadmin: bool
    session_id: uuid.UUID  # sid из access-токена: по нему logout знает, какую сессию закрыть
    org_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None
    role: str | None = None


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


@dataclass(frozen=True)
class Session:
    """Один вход на одном устройстве."""

    sid: uuid.UUID
    ip: str | None
    user_agent: str | None
    started_at: datetime
    last_refresh_at: datetime
    expires_at: datetime
    current: bool


class AuthService:
    def __init__(
        self,
        uow: UnitOfWork,
        keys: KeySet,
        cache: UserCache,
        limiter: RateLimiter,
        refresh_ttl: timedelta,
    ) -> None:
        self.uow = uow
        self.keys = keys
        self.cache = cache
        self.limiter = limiter
        self.refresh_ttl = refresh_ttl

    async def login(self, email: str, password: str, client: ClientInfo) -> TokenPair:
        # 5 неудачных попыток за 15 минут на пару email + IP; успешный вход обнуляет счётчик.
        attempt = f"{email.casefold()}:{client.ip}"
        await self.limiter.hit(rate_limiter.LOGIN, attempt)
        found = await self.uow.users.get_by_email(email)
        if found is None:
            await verify_dummy_password(password)  # то же время ответа, что и с настоящим email
            raise Unauthorized("auth.invalid_credentials")
        ok, new_hash = await verify_password(password, found.password_hash)
        if not ok:
            raise Unauthorized("auth.invalid_credentials")
        await self.limiter.reset(rate_limiter.LOGIN, attempt)
        user = await self._lock_user(found.id)
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
        token_hash = hash_token(raw_token)
        found = await self.uow.refresh_tokens.get_by_hash(token_hash)
        if found is None:
            raise Unauthorized()
        # Порядок блокировок везде один: сначала пользователь, потом его токены.
        user = await self._lock_user(found.user_id)
        token = await self.uow.refresh_tokens.get_by_hash(token_hash, for_update=True)
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

    async def logout(self, user: CurrentUser) -> None:
        """Закрыть текущую сессию. Access-токен доживёт свой час: он stateless."""
        await self._lock_user(user.id)
        await self.uow.refresh_tokens.revoke_family(user.session_id, datetime.now(UTC))
        await self.uow.commit()

    async def logout_all(self, user: CurrentUser) -> None:
        """Закрыть все сессии и убить все живые access-токены (token_version + 1)."""
        row = await self._lock_user(user.id)
        await self.uow.refresh_tokens.revoke_all_for_user(user.id, datetime.now(UTC))
        row.token_version += 1
        await self.uow.commit()
        await self.cache.invalidate(user.id)

    async def sessions(self, user: CurrentUser) -> list[Session]:
        rows = await self.uow.refresh_tokens.list_active(user.id, datetime.now(UTC))
        return [
            Session(
                sid=token.family_id,
                ip=token.ip,
                user_agent=token.user_agent,
                started_at=started_at,
                last_refresh_at=token.created_at,
                expires_at=token.expires_at,
                current=token.family_id == user.session_id,
            )
            for token, started_at in rows
        ]

    async def revoke_session(self, user: CurrentUser, sid: uuid.UUID) -> None:
        """Выйти на конкретном устройстве."""
        await self._lock_user(user.id)
        if not await self.uow.refresh_tokens.revoke_user_session(user.id, sid, datetime.now(UTC)):
            raise NotFound(entity="session", id=str(sid))
        await self.uow.commit()

    async def change_password(
        self, current: CurrentUser, old_password: str, new_password: str, client: ClientInfo
    ) -> TokenPair:
        """Сменить пароль: все остальные сессии закрываются, их access-токены умирают сразу
        (token_version + 1). Текущая сессия продолжается — с новой парой токенов."""
        # Иначе с украденным access-токеном старый пароль можно было бы перебирать без конца.
        attempt = str(current.id)
        await self.limiter.hit(rate_limiter.PASSWORD_CHANGE, attempt)
        user = await self._lock_user(current.id)
        ok, _ = await verify_password(old_password, user.password_hash)
        if not ok:
            raise ValidationFailed("auth.wrong_password")
        await self.limiter.reset(rate_limiter.PASSWORD_CHANGE, attempt)
        check_password_rules(new_password, user.email)
        user.password_hash = await hash_password(new_password)
        user.token_version += 1

        now = datetime.now(UTC)
        session = await self.uow.refresh_tokens.get_active_in_family(
            user.id, current.session_id, now
        )
        await self.uow.refresh_tokens.revoke_all_for_user(user.id, now)
        if session is None:  # сессию уже закрыли, а access ещё жив — начинаем новую
            pair = self._start_session(user, client)
        else:
            new = self._new_refresh_token(user.id, session.family_id, session.expires_at, client)
            pair = self._token_pair(user, session.family_id, new)
        await self.uow.commit()
        await self.cache.invalidate(user.id)
        return pair

    async def _lock_user(self, user_id: uuid.UUID) -> User:
        user = await self.uow.users.get(user_id, for_update=True)
        if user is None:
            raise Unauthorized()
        return user

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
            org_id=user.org_id,
            team_id=user.team_id,
            role=user.role,
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
        org_id=user.org_id,
        team_id=user.team_id,
        role=user.role,
    )


def ensure_can_sign_in(user: CachedUser) -> None:
    if not user.is_active:
        raise Forbidden("auth.user_inactive")
    if not user.email_verified:
        raise Forbidden("auth.email_not_verified")
