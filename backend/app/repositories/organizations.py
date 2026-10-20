import uuid
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Organization, Team, User


class OrganizationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, org_id: uuid.UUID, *, for_update: bool = False) -> Organization | None:
        return await self.session.get(
            Organization, org_id, with_for_update=for_update, populate_existing=for_update
        )

    async def get_by_slug(self, slug: str) -> Organization | None:
        return await self.session.scalar(select(Organization).where(Organization.slug == slug))

    async def list(self) -> list[Organization]:
        return list(await self.session.scalars(select(Organization).order_by(Organization.name)))

    def add(self, org: Organization) -> None:
        self.session.add(org)


class TeamRepository:
    """Отделы всегда ищутся внутри организации: чужой отдел для запроса — как несуществующий."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list(self, org_id: uuid.UUID) -> list[Team]:
        return list(
            await self.session.scalars(
                select(Team).where(Team.org_id == org_id).order_by(Team.name)
            )
        )

    async def get(
        self,
        team_id: uuid.UUID,
        org_id: uuid.UUID,
        *,
        lock: Literal["update", "key_share"] | None = None,
    ) -> Team | None:
        """lock="update" — для удаления отдела; lock="key_share" — для того, кто кладёт в отдел
        человека (перевод, принятие приглашения). Эти блокировки конфликтуют: удаление ждёт
        перевод и видит нового человека (409), а перевод ждёт удаление и видит, что отдела
        нет (422), — вместо 500 от внешнего ключа."""
        query = select(Team).where(Team.id == team_id, Team.org_id == org_id)
        if lock == "update":
            query = query.with_for_update()
        elif lock == "key_share":
            query = query.with_for_update(read=True, key_share=True)
        return await self.session.scalar(query)

    async def get_by_name(self, org_id: uuid.UUID, name: str) -> Team | None:
        return await self.session.scalar(
            select(Team).where(Team.org_id == org_id, Team.name == name)
        )

    async def count_members(self, team_id: uuid.UUID) -> int:
        return (
            await self.session.scalar(
                select(func.count()).select_from(User).where(User.team_id == team_id)
            )
            or 0
        )

    def add(self, team: Team) -> None:
        self.session.add(team)

    async def delete(self, team: Team) -> None:
        await self.session.delete(team)
