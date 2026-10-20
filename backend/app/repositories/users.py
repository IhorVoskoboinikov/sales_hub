import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import User


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, user_id: uuid.UUID, *, for_update: bool = False) -> User | None:
        """for_update — заблокировать строку до конца транзакции (SELECT ... FOR UPDATE).

        Все операции, которые выдают или отзывают refresh-токены пользователя, сначала
        блокируют его строку в users. Так они идут по очереди: иначе refresh, начатый до
        logout-all, мог бы выдать токен, которого UPDATE в logout-all уже не увидит.
        populate_existing — перечитать строку, даже если объект уже есть в сессии."""
        return await self.session.get(
            User, user_id, with_for_update=for_update, populate_existing=for_update
        )

    async def get_by_email(self, email: str) -> User | None:
        # email — citext, поэтому регистр не важен и индекс используется.
        return await self.session.scalar(select(User).where(User.email == email))

    def add(self, user: User) -> None:
        self.session.add(user)
