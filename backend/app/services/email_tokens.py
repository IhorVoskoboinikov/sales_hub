"""Одноразовые токены из писем: выпуск и проверка (раздел 4)."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.core.security import hash_token, new_token
from app.errors import ValidationFailed
from app.models import EmailToken, EmailTokenPurpose
from app.uow import UnitOfWork


@dataclass(frozen=True)
class IssuedToken:
    row: EmailToken
    raw: str  # уходит в письмо; в базе только хэш


async def issue_email_token(
    uow: UnitOfWork,
    purpose: EmailTokenPurpose,
    email: str,
    ttl: timedelta,
    *,
    user_id: uuid.UUID | None = None,
) -> IssuedToken:
    """Новый токен для письма; предыдущие той же цели на этот email перестают работать.
    Коммит — за вызывающим."""
    now = datetime.now(UTC)
    await uow.email_tokens.invalidate_active(email, purpose, now)
    raw = new_token()
    row = EmailToken(
        user_id=user_id,
        email=email,
        purpose=purpose,
        token_hash=hash_token(raw),
        created_at=now,
        expires_at=now + ttl,
    )
    uow.email_tokens.add(row)
    return IssuedToken(row, raw)


async def use_email_token(uow: UnitOfWork, purpose: EmailTokenPurpose, raw: str) -> EmailToken:
    """Найти живой токен и пометить использованным. Коммит — за вызывающим."""
    now = datetime.now(UTC)
    token = await uow.email_tokens.get_by_hash_for_update(hash_token(raw), purpose)
    if token is None or token.used_at is not None:
        raise ValidationFailed("email_token.invalid")
    if token.expires_at <= now:
        raise ValidationFailed("email_token.expired")
    token.used_at = now
    return token


def format_expiry(moment: datetime) -> str:
    """Срок в письме: 27.10.2026 12:00 UTC."""
    return moment.astimezone(UTC).strftime("%d.%m.%Y %H:%M UTC")
