import uuid
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EmailToken, EmailTokenPurpose


class EmailTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def add(self, token: EmailToken) -> None:
        self.session.add(token)

    async def get_by_hash_for_update(
        self, token_hash: str, purpose: EmailTokenPurpose
    ) -> EmailToken | None:
        """FOR UPDATE: один токен нельзя использовать дважды двумя одновременными запросами."""
        return await self.session.scalar(
            select(EmailToken)
            .where(EmailToken.token_hash == token_hash, EmailToken.purpose == purpose)
            .with_for_update()
        )

    async def invalidate_active(
        self,
        email: str,
        purpose: EmailTokenPurpose,
        now: datetime,
        *,
        org_id: uuid.UUID | None = None,
    ) -> None:
        """Новый токен гасит предыдущие той же цели: работает только ссылка из последнего письма.
        Для приглашений — только приглашения той же организации: иначе админ одной организации
        мог бы гасить чужие приглашения, просто пригласив тот же email."""
        query = (
            update(EmailToken)
            .where(
                EmailToken.email == email,
                EmailToken.purpose == purpose,
                EmailToken.used_at.is_(None),
            )
            .values(used_at=now)
        )
        if org_id is not None:
            query = query.where(EmailToken.payload["org_id"].astext == str(org_id))
        await self.session.execute(query)
