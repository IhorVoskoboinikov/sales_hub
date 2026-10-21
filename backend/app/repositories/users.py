import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.access import Access
from app.models import Role, User


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

    async def get_for_share(self, user_id: uuid.UUID) -> User | None:
        """FOR SHARE — человек становится ответственным за запись. Конфликтует с FOR UPDATE
        в смене роли и отдела: перевод подождёт, пока запись создастся, и увидит её
        (409 member.has_records), а не оставит запись в старом отделе."""
        return await self.session.scalar(
            select(User)
            .where(User.id == user_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )

    async def get_by_email(self, email: str) -> User | None:
        # email — citext, поэтому регистр не важен и индекс используется.
        return await self.session.scalar(select(User).where(User.email == email))

    def add(self, user: User) -> None:
        self.session.add(user)

    async def list_visible(self, access: Access) -> list[User]:
        """Люди, которых видит роль: вся организация или свой отдел (member:read)."""
        return list(
            await self.session.scalars(
                select(User).where(access.sql_filter(User)).order_by(User.email)
            )
        )

    async def get_visible(
        self, user_id: uuid.UUID, access: Access, *, for_update: bool = False
    ) -> User | None:
        """Пользователь вне уровня — как несуществующий (404, а не 403)."""
        query = select(User).where(User.id == user_id, access.sql_filter(User))
        if for_update:
            query = query.with_for_update().execution_options(populate_existing=True)
        return await self.session.scalar(query)

    async def count_active_admins(self, org_id: uuid.UUID) -> int:
        return (
            await self.session.scalar(
                select(func.count())
                .select_from(User)
                .where(User.org_id == org_id, User.role == Role.ORG_ADMIN, User.is_active)
            )
            or 0
        )
