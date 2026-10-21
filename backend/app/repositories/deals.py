"""Воронки и сделки: списки, канбан, проверки ссылок."""

import builtins
import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import Select, func, literal_column, select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.authz.access import Access
from app.models import Deal, Pipeline, PipelineStage
from app.models.deals import OPEN_ALIVE
from app.repositories.crm import SoftDeleteRepository
from app.repositories.pagination import Key, Page, PageResult, fetch_page, paginate

DEAL_ORDER = (Key(Deal.created_at, desc=True), Key(Deal.id, desc=True))
OVERDUE_ORDER = (Key(Deal.expected_close_date), Key(Deal.id))


def open_alive() -> Any:
    """Тот же предикат, что у частичных индексов канбана и просроченных, — литералом."""
    return literal_column(OPEN_ALIVE)


class PipelineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list(self, org_id: uuid.UUID) -> list[Pipeline]:
        return list(
            await self.session.scalars(
                select(Pipeline)
                .where(Pipeline.org_id == org_id)
                .options(selectinload(Pipeline.stages))
                .order_by(Pipeline.name)
            )
        )

    async def get(self, pipeline_id: int, org_id: uuid.UUID) -> Pipeline | None:
        """Воронка чужой организации — как несуществующая."""
        return await self.session.scalar(
            select(Pipeline)
            .where(Pipeline.id == pipeline_id, Pipeline.org_id == org_id)
            .options(selectinload(Pipeline.stages))
        )

    async def get_by_name(self, org_id: uuid.UUID, name: str) -> Pipeline | None:
        return await self.session.scalar(
            select(Pipeline).where(Pipeline.org_id == org_id, Pipeline.name == name)
        )

    async def get_stage(self, stage_id: int, org_id: uuid.UUID) -> PipelineStage | None:
        """Этап своей организации (у этапа нет ответственного — проверяем воронку).

        FOR KEY SHARE — как у FK-проверки: если админ в этот момент удаляет этап (FOR UPDATE
        в lock_stages), сделка дождётся его коммита и увидит, что этапа нет (422), а не упадёт
        на внешнем ключе с 500."""
        return await self.session.scalar(
            select(PipelineStage)
            .join(Pipeline, Pipeline.id == PipelineStage.pipeline_id)
            .where(PipelineStage.id == stage_id, Pipeline.org_id == org_id)
            .with_for_update(read=True, key_share=True, of=PipelineStage)
        )

    async def lock_stages(self, stage_ids: builtins.list[int]) -> None:
        """FOR UPDATE на удаляемые этапы до проверки «на этапе нет сделок»: сделка, которая
        как раз ставится на этот этап, либо успеет раньше (и проверка её увидит — 409), либо
        дождётся удаления (и получит 422)."""
        if stage_ids:
            await self.session.execute(
                select(PipelineStage.id)
                .where(PipelineStage.id.in_(stage_ids))
                .order_by(PipelineStage.id)  # один порядок блокировок — без deadlock
                .with_for_update()
            )

    async def stage_has_deals(self, stage_id: int) -> bool:
        """Сделки на этапе, включая корзину: FK держат и удалённые строки."""
        query = select(Deal.id).where(Deal.stage_id == stage_id).limit(1)
        return await self.session.scalar(query) is not None

    def add(self, pipeline: Pipeline) -> None:
        self.session.add(pipeline)


@dataclass(frozen=True)
class DealFilters:
    status: str | None = None
    pipeline_id: int | None = None
    stage_id: int | None = None
    owner_id: uuid.UUID | None = None
    contact_id: int | None = None
    company_id: int | None = None
    overdue_on: date | None = None  # открытые с expected_close_date раньше этой даты


@dataclass(frozen=True)
class StageTotal:
    stage_id: int
    deals: int
    amount_usd: Decimal


class DealRepository(SoftDeleteRepository[Deal]):
    model = Deal
    entity = "deal"

    def list_query(self, access: Access, filters: DealFilters, page: Page) -> Select[Any]:
        query = self.visible_query(access)
        if filters.overdue_on is not None:
            query = query.where(open_alive(), Deal.expected_close_date < filters.overdue_on)
        if filters.status:
            query = query.where(Deal.status == filters.status)
        for column, value in (
            (Deal.pipeline_id, filters.pipeline_id),
            (Deal.stage_id, filters.stage_id),
            (Deal.owner_id, filters.owner_id),
            (Deal.contact_id, filters.contact_id),
            (Deal.company_id, filters.company_id),
        ):
            if value is not None:
                query = query.where(column == value)
        return paginate(query, self.order(filters), page)

    @staticmethod
    def order(filters: DealFilters) -> tuple[Key, ...]:
        return OVERDUE_ORDER if filters.overdue_on is not None else DEAL_ORDER

    async def list(self, access: Access, filters: DealFilters, page: Page) -> PageResult[Deal]:
        query = self.list_query(access, filters, page)
        return await fetch_page(self.session, query, self.order(filters), page)

    def kanban_totals_query(self, access: Access, pipeline_id: int) -> Select[int, int, Decimal]:
        """Число и сумма открытых сделок по этапам — index-only scan по ix_deals_kanban."""
        return (
            select(Deal.stage_id, func.count(), func.coalesce(func.sum(Deal.amount_usd), 0))
            .where(Deal.pipeline_id == pipeline_id, open_alive(), access.sql_filter(Deal))
            .group_by(Deal.stage_id)
        )

    def kanban_cards_query(self, access: Access, pipeline_id: int, per_stage: int) -> Select[Any]:
        """Первые per_stage открытых сделок каждого этапа (новые сначала): LATERAL — для
        каждого этапа свой короткий проход по индексу, без чтения всей воронки."""
        cards = (
            select(Deal.id, Deal.title, Deal.amount_usd, Deal.owner_id)
            .where(
                Deal.pipeline_id == pipeline_id,
                Deal.stage_id == PipelineStage.id,
                open_alive(),
                access.sql_filter(Deal),
            )
            .order_by(Deal.id.desc())
            .limit(per_stage)
            .lateral("cards")
        )
        return (
            select(
                PipelineStage.id.label("stage_id"),
                cards.c.id,
                cards.c.title,
                cards.c.amount_usd,
                cards.c.owner_id,
            )
            .select_from(PipelineStage)
            .join(cards, true())
            .where(PipelineStage.pipeline_id == pipeline_id)
            .order_by(PipelineStage.position, PipelineStage.id, cards.c.id.desc())
        )

    async def kanban(
        self, access: Access, pipeline_id: int, per_stage: int
    ) -> tuple[builtins.list[StageTotal], builtins.list[Any]]:
        totals = [
            StageTotal(stage_id, count, amount)
            for stage_id, count, amount in await self.session.execute(
                self.kanban_totals_query(access, pipeline_id)
            )
        ]
        cards = list(
            await self.session.execute(self.kanban_cards_query(access, pipeline_id, per_stage))
        )
        return totals, cards
