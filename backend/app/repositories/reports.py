"""Отчёты по сделкам из materialized view deal_stats.

Access применяется к view так же, как к таблице: у него есть org_id, team_id и owner_id.
Суммы в SQL — в USD, в валюту ответа пересчитываются один раз в сервисе (раздел 3).
"""

import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import Select, and_, func, literal_column, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.access import Access
from app.models import User
from app.models.reports import deal_stats

STATS = deal_stats.c
# Ключ — 64-битный хэш имени, как у остальных advisory lock приложения (app/db/locks.py).
REFRESH_LOCK = text("SELECT pg_try_advisory_xact_lock(hashtextextended('reports.refresh', 0))")


@dataclass(frozen=True)
class StageRow:
    stage_id: int
    deals: int
    amount_usd: Decimal


@dataclass(frozen=True)
class ManagerRow:
    owner_id: uuid.UUID
    email: str
    open_deals: int
    open_usd: Decimal
    won_deals: int
    won_usd: Decimal
    lost_deals: int
    lost_usd: Decimal


def status_is(value: str) -> Any:
    # Литералом: так же, как в остальных запросах с частичными условиями.
    return STATS.status == literal_column(f"'{value}'")


class ReportRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def pipeline_query(self, access: Access, pipeline_id: int) -> Select[Any]:
        return (
            select(STATS.stage_id, func.sum(STATS.deals), func.sum(STATS.amount_usd))
            .where(STATS.pipeline_id == pipeline_id, status_is("open"), access.sql_filter(STATS))
            .group_by(STATS.stage_id)
        )

    async def pipeline(self, access: Access, pipeline_id: int) -> list[StageRow]:
        rows = await self.session.execute(self.pipeline_query(access, pipeline_id))
        return [StageRow(stage, int(n), amount) for stage, n, amount in rows]

    def managers_query(self, access: Access, month_from: date, month_to: date) -> Select[Any]:
        """Открытые сделки — все, выигранные и проигранные — закрытые в месяцы [from, to]."""
        in_period = and_(STATS.month >= month_from, STATS.month <= month_to)

        def total(column: Any, condition: Any) -> Any:
            return func.coalesce(func.sum(column).filter(condition), 0)

        won, lost = and_(status_is("won"), in_period), and_(status_is("lost"), in_period)
        return (
            select(
                STATS.owner_id,
                User.email,
                total(STATS.deals, status_is("open")),
                total(STATS.amount_usd, status_is("open")),
                total(STATS.deals, won),
                total(STATS.amount_usd, won),
                total(STATS.deals, lost),
                total(STATS.amount_usd, lost),
            )
            .join(User, User.id == STATS.owner_id)
            .where(access.sql_filter(STATS))
            .group_by(STATS.owner_id, User.email)
            .order_by(User.email)
        )

    async def managers(self, access: Access, month_from: date, month_to: date) -> list[ManagerRow]:
        rows = await self.session.execute(self.managers_query(access, month_from, month_to))
        return [
            ManagerRow(owner, email, int(o), ou, int(w), wu, int(lo), lu)
            for owner, email, o, ou, w, wu, lo, lu in rows
        ]

    async def refresh(self) -> bool:
        """Обновить deal_stats. CONCURRENTLY: читатели не ждут обновления (нужен UNIQUE-индекс
        на view).

        Под pg_try_advisory_xact_lock: обновление уже идёт (медленный прошлый запуск beat или
        make refresh-reports) — False и пропуск, а не второй REFRESH в очередь за первым.
        Блокировка транзакционная — отпускается коммитом или откатом вызывающего."""
        if not await self.session.scalar(REFRESH_LOCK):
            return False
        await self.session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY deal_stats"))
        return True
