"""Приглашения (раздел 4). Этап 2: только супер-админ и без организации — организация, отдел
и роль появятся в приглашении на этапе 3."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.core.security import check_password_rules, hash_password
from app.emails.render import render_email
from app.emails.sender import Mailer
from app.errors import Conflict
from app.models import EmailTokenPurpose, User
from app.services.email_tokens import format_expiry, issue_email_token, use_email_token
from app.uow import UnitOfWork


@dataclass(frozen=True)
class Invite:
    email: str
    expires_at: datetime


class InviteService:
    def __init__(self, uow: UnitOfWork, mailer: Mailer, ttl: timedelta, docs_url: str) -> None:
        self.uow = uow
        self.mailer = mailer
        self.ttl = ttl
        self.docs_url = docs_url

    async def invite(self, email: str) -> Invite:
        if await self.uow.users.get_by_email(email) is not None:
            raise Conflict("invite.email_taken")
        token = await issue_email_token(self.uow, EmailTokenPurpose.INVITE, email, self.ttl)
        await self.uow.commit()
        self.mailer.send_later(
            render_email(
                "invite",
                email,
                token=token.raw,
                docs_url=self.docs_url,
                expires_at=format_expiry(token.row.expires_at),
            )
        )
        return Invite(email=email, expires_at=token.row.expires_at)

    async def accept(self, raw_token: str, password: str) -> User:
        """Приглашённый задаёт пароль — и этим же подтверждает, что email его."""
        token = await use_email_token(self.uow, EmailTokenPurpose.INVITE, raw_token)
        if await self.uow.users.get_by_email(token.email) is not None:
            raise Conflict("invite.email_taken")  # успели пригласить и принять другим письмом
        check_password_rules(password, token.email)
        now = datetime.now(UTC)
        user = User(
            email=token.email,
            password_hash=await hash_password(password),
            email_verified_at=now,
            created_at=now,  # заполняем сами: после коммита поле читается без запроса в базу
        )
        self.uow.users.add(user)
        await self.uow.commit()
        return user
