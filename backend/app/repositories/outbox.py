import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.context import get_request_id
from app.models import OutboxEvent


@dataclass(frozen=True)
class ClaimedEvent:
    """Строка, которую relay взял на публикацию: payload ему не нужен — его читает потребитель."""

    id: int
    event_type: str
    request_id: str | None


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def add(
        self, event_type: str, payload: dict[str, Any], *, org_id: uuid.UUID | None = None
    ) -> OutboxEvent:
        """Событие в той же транзакции, что и данные: откатились данные — откатилось и оно.
        request_id — текущего запроса или задачи: по нему потребитель продолжит ту же цепочку
        в логах."""
        event = OutboxEvent(
            event_type=event_type,
            org_id=org_id,
            # UUID, date, Decimal — в строки: jsonb принимает только JSON-типы.
            payload=json.loads(json.dumps(payload, default=str)),
            request_id=get_request_id(),
        )
        self.session.add(event)
        return event

    async def get(self, event_id: int) -> OutboxEvent | None:
        return await self.session.get(OutboxEvent, event_id)

    async def claim_batch(self, limit: int) -> list[ClaimedEvent]:
        """Самые старые неопубликованные события, под блокировкой до конца транзакции.

        SKIP LOCKED: строки, которые уже взял другой relay, пропускаются, а не ждут его
        коммита, — две копии relay публикуют разные события и не мешают друг другу."""
        rows = await self.session.execute(
            select(OutboxEvent.id, OutboxEvent.event_type, OutboxEvent.request_id)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [ClaimedEvent(*row) for row in rows]

    async def mark_published(self, ids: Sequence[int]) -> None:
        await self.session.execute(
            update(OutboxEvent).where(OutboxEvent.id.in_(ids)).values(published_at=func.now())
        )
