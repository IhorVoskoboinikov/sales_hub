"""Unit of Work: одна транзакция на сценарий. Коммитит только сервис, репозитории — никогда."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.email_tokens import EmailTokenRepository
from app.repositories.refresh_tokens import RefreshTokenRepository
from app.repositories.users import UserRepository


class UnitOfWork:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.users = UserRepository(session)
        self.refresh_tokens = RefreshTokenRepository(session)
        self.email_tokens = EmailTokenRepository(session)

    async def flush(self) -> None:
        """Отправить изменения в базу без коммита: проверить ограничения, получить значения
        по умолчанию из базы."""
        await self.session.flush()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
