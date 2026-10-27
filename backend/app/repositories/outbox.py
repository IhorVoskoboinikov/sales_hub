import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, literal, select, update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.types import Text

from app.core.context import get_request_id
from app.events.types import EventType, TelegramSend
from app.models import OutboxEvent

# Где в payload команды email.send лежит одноразовый токен: context.token. Под этим именем его
# подставляют шаблоны (app/texts/emails/*: {{ token }}), и только его стирает scrub_token —
# поэтому секрет в контексте письма кладётся только сюда, а не, например, в готовую ссылку.
TOKEN_PATH = ("context", "token")


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

    def add_email(
        self,
        to: str,
        template: str,
        context: Mapping[str, str],
        *,
        org_id: uuid.UUID | None = None,
        user_id: uuid.UUID | None = None,
        dedup_key: str | None = None,
    ) -> OutboxEvent:
        """Команда «отправить письмо» (раздел 4): её выполнит notifications.send_email.

        Пишется до коммита, в той же транзакции, что токен и строка журнала: откат — письма
        нет, коммит — письмо уйдёт, даже если SMTP или Redis сейчас недоступны. context — только
        строки, готовые для шаблона; токен — под ключом token (TOKEN_PATH), его стирают после
        отправки. dedup_key None — ключ email:{event_id} (app/notifications/keys.py)."""
        return self.add(
            EventType.EMAIL_SEND,
            {
                "to": to,
                "template": template,
                "context": dict(context),
                "user_id": user_id,
                "dedup_key": dedup_key,
            },
            org_id=org_id,
        )

    def add_telegram(
        self,
        user_id: uuid.UUID,
        text: str,
        dedup_key: str,
        *,
        org_id: uuid.UUID | None = None,
    ) -> OutboxEvent:
        """Команда «отправить сообщение в Telegram» (раздел 16): её выполнит
        notifications.send_telegram. Чат не записывается — его берут из telegram_links при
        отправке: отвязанный за это время чат сообщения не получит."""
        payload: TelegramSend = {"user_id": str(user_id), "text": text, "dedup_key": dedup_key}
        return self.add(EventType.TELEGRAM_SEND, dict(payload), org_id=org_id)

    async def scrub_token(self, event_id: int) -> None:
        """Стереть сырой токен из payload после отправки письма: payload #- '{context,token}'.
        Коммит — за вызывающим, в той же транзакции, что отметка в sent_notifications."""
        path = literal(list(TOKEN_PATH), ARRAY(Text))
        await self.session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(payload=OutboxEvent.payload.op("#-")(path))
            .execution_options(synchronize_session=False)
        )

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
