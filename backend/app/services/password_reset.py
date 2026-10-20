"""Сброс забытого пароля письмом (раздел 4)."""

from datetime import UTC, datetime, timedelta

from app.core.security import check_password_rules, hash_password
from app.emails.render import render_email
from app.emails.sender import Mailer
from app.errors import ValidationFailed
from app.models import EmailTokenPurpose
from app.repositories.user_cache import UserCache
from app.services.auth import cached_user, ensure_can_sign_in
from app.services.email_tokens import format_expiry, issue_email_token, use_email_token
from app.uow import UnitOfWork


class PasswordResetService:
    def __init__(
        self,
        uow: UnitOfWork,
        mailer: Mailer,
        cache: UserCache,
        ttl: timedelta,
        docs_url: str,
    ) -> None:
        self.uow = uow
        self.mailer = mailer
        self.cache = cache
        self.ttl = ttl
        self.docs_url = docs_url

    async def forgot(self, email: str) -> None:
        """Письмо со ссылкой — только существующему и активному пользователю. Снаружи разницы
        не видно: ответ всегда 202, а письмо уходит после ответа."""
        user = await self.uow.users.get_by_email(email)
        if user is None or not user.is_active:
            return
        token = await issue_email_token(
            self.uow, EmailTokenPurpose.RESET_PASSWORD, user.email, self.ttl, user_id=user.id
        )
        await self.uow.commit()
        self.mailer.send_later(
            render_email(
                "reset_password",
                user.email,
                token=token.raw,
                docs_url=self.docs_url,
                expires_at=format_expiry(token.row.expires_at),
            )
        )

    async def reset(self, raw_token: str, new_password: str) -> None:
        """Новый пароль по токену из письма. Все сессии закрываются, все access-токены умирают."""
        token = await use_email_token(self.uow, EmailTokenPurpose.RESET_PASSWORD, raw_token)
        user = await self.uow.users.get(token.user_id) if token.user_id else None
        if user is None:
            raise ValidationFailed("email_token.invalid")
        ensure_can_sign_in(cached_user(user))
        check_password_rules(new_password, user.email)
        user.password_hash = await hash_password(new_password)
        user.token_version += 1
        await self.uow.refresh_tokens.revoke_all_for_user(user.id, datetime.now(UTC))
        await self.uow.commit()
        await self.cache.invalidate(user.id)
