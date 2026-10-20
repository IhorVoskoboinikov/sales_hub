import uuid
from datetime import datetime
from typing import Any, cast

from sqlalchemy import CursorResult, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RefreshToken


class RefreshTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def add(self, token: RefreshToken) -> None:
        self.session.add(token)

    async def get_by_hash(
        self, token_hash: str, *, for_update: bool = False
    ) -> RefreshToken | None:
        query = select(RefreshToken).where(RefreshToken.token_hash == token_hash)
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        return await self.session.scalar(query)

    async def revoke_family(self, family_id: uuid.UUID, now: datetime) -> None:
        """Закрыть сессию: отозвать все ещё живые токены семьи."""
        await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.family_id == family_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=now)
        )

    async def revoke_user_session(
        self, user_id: uuid.UUID, family_id: uuid.UUID, now: datetime
    ) -> bool:
        """Закрыть одну сессию пользователя. False — такой живой сессии у него нет
        (чужую сессию так закрыть нельзя: фильтр по user_id)."""
        result = await self.session.execute(
            update(RefreshToken)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.family_id == family_id,
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at > now,
            )
            .values(revoked_at=now)
        )
        return cast(CursorResult[Any], result).rowcount > 0

    async def revoke_all_for_user(
        self, user_id: uuid.UUID, now: datetime, *, keep_family: uuid.UUID | None = None
    ) -> None:
        """Закрыть все сессии пользователя, кроме keep_family (если задана)."""
        query = (
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=now)
        )
        if keep_family is not None:
            query = query.where(RefreshToken.family_id != keep_family)
        await self.session.execute(query)

    async def list_active(
        self, user_id: uuid.UUID, now: datetime
    ) -> list[tuple[RefreshToken, datetime]]:
        """Живые сессии: в каждой семье ровно один неотозванный токен — последний выданный.
        Второй элемент — начало сессии (первый токен семьи, то есть момент входа)."""
        started = (
            select(
                RefreshToken.family_id,
                func.min(RefreshToken.created_at).label("started_at"),
            )
            .where(RefreshToken.user_id == user_id)
            .group_by(RefreshToken.family_id)
            .subquery()
        )
        rows = await self.session.execute(
            select(RefreshToken, started.c.started_at)
            .join(started, started.c.family_id == RefreshToken.family_id)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at > now,
            )
            .order_by(RefreshToken.created_at.desc())
        )
        return [(token, started_at) for token, started_at in rows]

    async def get_active_in_family(
        self, user_id: uuid.UUID, family_id: uuid.UUID, now: datetime
    ) -> RefreshToken | None:
        """Текущий (последний неотозванный) токен сессии."""
        return await self.session.scalar(
            select(RefreshToken).where(
                RefreshToken.user_id == user_id,
                RefreshToken.family_id == family_id,
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at > now,
            )
        )
