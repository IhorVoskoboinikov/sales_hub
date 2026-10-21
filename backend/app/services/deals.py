"""Воронки (настраивает org_admin) и сделки (раздел 3, правила доступа — раздел 5)."""

import builtins
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.errors import Conflict, NotFound, ValidationFailed
from app.models import Deal, DealStatus, Pipeline, PipelineStage
from app.repositories.crm import SoftDeleteRepository
from app.repositories.deals import DealFilters, StageTotal
from app.repositories.pagination import Page, PageResult
from app.services.contacts import CrmService, check_version
from app.services.crm import ensure_reference
from app.services.fx import CENT, Display, FxService
from app.uow import UnitOfWork


@dataclass(frozen=True)
class StageIn:
    name: str
    probability: int
    id: int | None = None


class PipelineService:
    """Воронки — ресурс уровня организации (org:read / org:manage), как отделы."""

    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def list(self, access: Access) -> list[Pipeline]:
        return await self.uow.pipelines.list(access.org_id)

    async def get(self, access: Access, pipeline_id: int) -> Pipeline:
        pipeline = await self.uow.pipelines.get(pipeline_id, access.org_id)
        if pipeline is None:
            raise NotFound(entity="pipeline", id=pipeline_id)
        return pipeline

    async def create(self, access: Access, name: str, stages: Sequence[StageIn]) -> Pipeline:
        await self._ensure_name_free(access, name)
        pipeline = Pipeline(org_id=access.org_id, name=name, stages=[])
        for position, stage in enumerate(stages):
            pipeline.stages.append(
                PipelineStage(name=stage.name, probability=stage.probability, position=position)
            )
        self.uow.pipelines.add(pipeline)
        await self.uow.flush()
        self._audit(access, "pipeline.created", pipeline, {"name": name, "stages": len(stages)})
        await self.uow.commit()
        return pipeline

    async def update(
        self, access: Access, pipeline_id: int, name: str | None, stages: Sequence[StageIn] | None
    ) -> Pipeline:
        """stages — целиком: с id — изменить, без id — добавить, пропавший — удалить."""
        pipeline = await self.get(access, pipeline_id)
        changes: dict[str, Any] = {}
        if name is not None and name != pipeline.name:
            await self._ensure_name_free(access, name)
            changes["name"] = [pipeline.name, name]
            pipeline.name = name
        if stages is not None:
            changes.update(await self._replace_stages(pipeline, stages))
        if changes:
            self._audit(access, "pipeline.updated", pipeline, changes)
            await self.uow.commit()
        return pipeline

    async def _replace_stages(
        self, pipeline: Pipeline, stages: Sequence[StageIn]
    ) -> dict[str, Any]:
        current = {stage.id: stage for stage in pipeline.stages}
        unknown = [s.id for s in stages if s.id is not None and s.id not in current]
        if unknown:
            raise ValidationFailed("pipeline.stage_not_found", field="stages", ids=unknown)
        kept = {s.id for s in stages if s.id is not None}
        removed = [stage for stage_id, stage in current.items() if stage_id not in kept]
        for stage in removed:
            if await self.uow.pipelines.stage_has_deals(stage.id):
                raise Conflict("pipeline.stage_has_deals", stage_id=stage.id)
        # Проверки закончены — дальше только изменения объектов (запросов до commit нет).
        for stage in removed:
            pipeline.stages.remove(stage)  # delete-orphan: DELETE при commit
        result: builtins.list[PipelineStage] = []
        for position, item in enumerate(stages):
            if item.id is None:
                stage = PipelineStage(name=item.name, probability=item.probability)
                pipeline.stages.append(stage)
            else:
                stage = current[item.id]
                stage.name, stage.probability = item.name, item.probability
            stage.position = position
            result.append(stage)
        pipeline.stages = result
        return {"stages": [s.name for s in result], "removed": [s.id for s in removed]}

    async def _ensure_name_free(self, access: Access, name: str) -> None:
        if await self.uow.pipelines.get_by_name(access.org_id, name) is not None:
            raise Conflict("pipeline.name_taken")

    def _audit(self, access: Access, action: str, pipeline: Pipeline, changes: Any) -> None:
        self.uow.audit.record(
            actor_id=access.user_id,
            org_id=access.org_id,
            action=action,
            entity_type="pipeline",
            entity_id=pipeline.id,
            changes=changes,
        )


@dataclass(frozen=True)
class Money:
    """Сумма сделки во всех пяти полях (раздел 3, «Деньги»)."""

    amount_usd: Decimal
    amount_original: Decimal
    currency_original: str
    fx_rate: Decimal
    fx_date: date


class DealService(CrmService[Deal]):
    """Суммы: клиент присылает {amount, currency}, в базу пишутся amount_usd, исходная сумма,
    валюта, курс и дата курса (раздел 3, «Деньги»). Курс берётся до любых других проверок:
    первая за день загрузка курса из НБУ коммитит транзакцию (FxService._fetch)."""

    entity = "deal"

    def __init__(self, uow: UnitOfWork, roles: RoleCatalog, fx: FxService) -> None:
        super().__init__(uow, roles)
        self.fx = fx

    @property
    def repo(self) -> SoftDeleteRepository[Deal]:
        return self.uow.deals

    async def list(self, access: Access, filters: DealFilters, page: Page) -> PageResult[Deal]:
        return await self.uow.deals.list(access, filters, page)

    async def create(self, access: Access, fields: dict[str, Any]) -> Deal:
        money = await self._money(access, fields.pop("amount"), fields.pop("currency"))
        stage = await self._stage(access, fields.pop("stage_id"))
        await self._check_refs(access, fields)
        status = fields.pop("status", DealStatus.OPEN)
        deal = Deal(
            pipeline_id=stage.pipeline_id,
            stage_id=stage.id,
            status=status,
            closed_at=None if status == DealStatus.OPEN else datetime.now(UTC),
            **vars(money),
        )
        return await self._create(access, deal, fields)

    async def update(
        self, access: Access, deal_id: int, version: int, fields: dict[str, Any]
    ) -> Deal:
        deal = await self._for_change(access, deal_id)
        check_version(deal, version)
        if "amount" in fields or "currency" in fields:
            amount = fields.pop("amount", deal.amount_original)
            currency = fields.pop("currency", deal.currency_original)
            if (amount, currency) != (deal.amount_original, deal.currency_original):
                # Курс пересчитывается, только если поменялись сумма или валюта.
                fields.update(vars(await self._money(access, amount, currency)))
        if "stage_id" in fields:
            stage = await self._stage(access, fields["stage_id"])
            fields["pipeline_id"] = stage.pipeline_id
        await self._check_refs(
            access, {k: v for k, v in fields.items() if getattr(deal, k, None) != v}
        )
        if "status" in fields and fields["status"] != deal.status:
            closed = fields["status"] != DealStatus.OPEN
            fields["closed_at"] = datetime.now(UTC) if closed else None
        return await self._update(access, deal, version, fields)

    async def kanban(
        self, access: Access, pipeline_id: int, per_stage: int
    ) -> tuple[Pipeline, builtins.list[StageTotal], builtins.list[Any]]:
        pipeline = await PipelineService(self.uow).get(access, pipeline_id)
        totals, cards = await self.uow.deals.kanban(access, pipeline_id, per_stage)
        return pipeline, totals, cards

    async def _stage(self, access: Access, stage_id: int) -> PipelineStage:
        stage = await self.uow.pipelines.get_stage(stage_id, access.org_id)
        if stage is None:
            raise ValidationFailed(field="stage_id")
        return stage

    async def _check_refs(self, access: Access, fields: dict[str, Any]) -> None:
        await ensure_reference(
            self.uow.contacts,
            fields.get("contact_id"),
            access,
            self.roles,
            "contact:read",
            "contact_id",
        )
        await ensure_reference(
            self.uow.companies,
            fields.get("company_id"),
            access,
            self.roles,
            "company:read",
            "company_id",
        )

    async def _money(self, access: Access, amount: Decimal, currency: str) -> Money:
        amount = amount.quantize(CENT)  # 100000 → 100000.00, как в numeric(14,2)
        converted = await self.fx.to_usd(amount, currency, await self.today(access))
        return Money(converted.amount_usd, amount, currency, converted.fx_rate, converted.fx_date)

    async def today(self, access: Access) -> date:
        """Сегодня по часовому поясу организации: курс НБУ — на её календарную дату."""
        org = await self.uow.organizations.get(access.org_id)
        zone = ZoneInfo(org.timezone if org else "UTC")
        return datetime.now(zone).date()

    async def display(self, access: Access, currency: str | None) -> Display:
        """Курс для ответа: ?currency= или валюта организации по умолчанию."""
        if currency is None:
            org = await self.uow.organizations.get(access.org_id)
            currency = org.display_currency if org else "USD"
        return await self.fx.display(currency, await self.today(access))
