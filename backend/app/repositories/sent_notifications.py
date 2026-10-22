import uuid

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import NotificationChannel, SentNotification


class SentNotificationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def claim(
        self, dedup_key: str, channel: NotificationChannel, user_id: uuid.UUID | None = None
    ) -> bool:
        """Занять ключ перед отправкой: True — ключ наш, отправляем; False — уже отправлено.

        INSERT … ON CONFLICT DO NOTHING RETURNING: ничего не вернулось — строка уже есть.
        Параллельная копия задачи с тем же ключом ждёт на уникальном индексе, пока первая не
        закончит транзакцию: коммит — получит False, откат (отправка упала) — займёт ключ сама.
        Коммит — за вызывающим, после успешной отправки."""
        claimed = await self.session.scalar(
            insert(SentNotification)
            .values(dedup_key=dedup_key, channel=channel.value, user_id=user_id)
            .on_conflict_do_nothing(index_elements=[SentNotification.dedup_key])
            .returning(SentNotification.dedup_key)
        )
        return claimed is not None
