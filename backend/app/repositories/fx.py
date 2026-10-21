from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import FxRate


class FxRateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, currency: str, on: date) -> FxRate | None:
        return await self.session.get(FxRate, (on, currency))

    async def latest(self, currency: str, on: date) -> FxRate | None:
        """Последний известный курс не позже даты — резерв, если НБУ недоступен."""
        return await self.session.scalar(
            select(FxRate)
            .where(FxRate.currency == currency, FxRate.rate_date <= on)
            .order_by(FxRate.rate_date.desc())
            .limit(1)
        )

    async def upsert(self, on: date, rates: dict[str, Decimal], fetched_at: datetime) -> None:
        """INSERT … ON CONFLICT DO UPDATE: повторная загрузка того же дня безопасна."""
        statement = insert(FxRate).values(
            [
                {"rate_date": on, "currency": cur, "rate_uah": rate, "fetched_at": fetched_at}
                for cur, rate in rates.items()
            ]
        )
        await self.session.execute(
            statement.on_conflict_do_update(
                index_elements=[FxRate.rate_date, FxRate.currency],
                set_={
                    "rate_uah": statement.excluded.rate_uah,
                    "fetched_at": statement.excluded.fetched_at,
                },
            )
        )
