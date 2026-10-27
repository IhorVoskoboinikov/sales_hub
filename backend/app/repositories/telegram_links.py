import uuid
from collections.abc import Collection
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import TelegramLink

# UNIQUE (chat_id): в него упирается гонка двух /start из одного чата (раздел 16).
CHAT_CONSTRAINT = "uq_telegram_links_chat_id"


class TelegramLinkRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # populate_existing: строки меняет upsert мимо ORM — объект из сессии мог устареть.
    async def get(self, user_id: uuid.UUID) -> TelegramLink | None:
        return await self.session.get(TelegramLink, user_id, populate_existing=True)

    async def get_by_chat(self, chat_id: int) -> TelegramLink | None:
        return await self.session.scalar(
            select(TelegramLink)
            .where(TelegramLink.chat_id == chat_id)
            .execution_options(populate_existing=True)
        )

    async def linked(self, user_ids: Collection[uuid.UUID]) -> set[uuid.UUID]:
        """Кто из этих пользователей привязал Telegram — notifier пишет telegram.send только им."""
        if not user_ids:
            return set()
        return set(
            await self.session.scalars(
                select(TelegramLink.user_id).where(TelegramLink.user_id.in_(list(user_ids)))
            )
        )

    async def upsert(
        self, user_id: uuid.UUID, chat_id: int, username: str | None, now: datetime
    ) -> None:
        """Привязать чат. У пользователя уже есть привязка — новый чат заменяет старый.
        Чат занят другим пользователем, которого TelegramService.link ещё не видел
        (параллельная транзакция), — IntegrityError по CHAT_CONSTRAINT."""
        query = insert(TelegramLink).values(
            user_id=user_id, chat_id=chat_id, username=username, linked_at=now
        )
        await self.session.execute(
            query.on_conflict_do_update(
                index_elements=[TelegramLink.user_id],
                set_={
                    "chat_id": query.excluded.chat_id,
                    "username": query.excluded.username,
                    "linked_at": query.excluded.linked_at,
                },
            )
        )

    async def delete_for_user(self, user_id: uuid.UUID) -> None:
        await self.session.execute(delete(TelegramLink).where(TelegramLink.user_id == user_id))

    async def delete(self, user_id: uuid.UUID, chat_id: int, linked_at: datetime) -> None:
        """Удалить именно ту привязку, которую прочитали: пару (user_id, chat_id) и её
        linked_at. Свежую перепривязку это не задевает — ни к другому чату, ни к тому же: upsert
        тот же chat_id оставляет, а linked_at меняет."""
        await self.session.execute(
            delete(TelegramLink).where(
                TelegramLink.user_id == user_id,
                TelegramLink.chat_id == chat_id,
                TelegramLink.linked_at == linked_at,
            )
        )
