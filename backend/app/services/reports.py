"""Отчёты (раздел 6): воронка по этапам и итоги по ответственным — в уровне report:read.

Данные — из materialized view deal_stats: отчёт отстаёт от сделок на интервал обновления.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime

import structlog

from app.authz.access import Access
from app.errors import NotFound, ValidationFailed
from app.models import Pipeline
from app.repositories.reports import ManagerRow, StageRow
from app.services.fx import Display, FxService
from app.uow import UnitOfWork

logger = structlog.get_logger("reports")


async def refresh_reports(uow: UnitOfWork) -> bool:
    """Обновить отчёты — один путь для задачи beat reports.refresh (каждые 5 минут) и make
    refresh-reports. False — обновление уже идёт в другой сессии, этот запуск пропущен."""
    refreshed = await uow.reports.refresh()
    await uow.commit()  # и отпустить блокировку
    if refreshed:
        logger.info("reports_refreshed")
    else:
        logger.info("reports_refresh_skipped", reason="already_running")
    return refreshed


@dataclass(frozen=True)
class PipelineReport:
    pipeline: Pipeline
    stages: list[StageRow]
    display: Display


@dataclass(frozen=True)
class ManagersReport:
    month_from: date
    month_to: date
    rows: list[ManagerRow]
    display: Display


class ReportService:
    def __init__(self, uow: UnitOfWork, fx: FxService) -> None:
        self.uow = uow
        self.fx = fx

    async def pipeline(
        self, access: Access, pipeline_id: int, currency: str | None
    ) -> PipelineReport:
        pipeline = await self.uow.pipelines.get(pipeline_id, access.org_id)
        if pipeline is None:  # воронка чужой организации — как несуществующая
            raise NotFound(entity="pipeline", id=pipeline_id)
        display = await self.fx.display_for(access.org_id, currency)
        stages = await self.uow.reports.pipeline(access, pipeline_id)
        return PipelineReport(pipeline, stages, display)

    async def managers(
        self,
        access: Access,
        month_from: date | None,
        month_to: date | None,
        currency: str | None,
    ) -> ManagersReport:
        """Период — месяцы по UTC, как в deal_stats; по умолчанию — с января текущего года
        по текущий месяц."""
        today = datetime.now(UTC).date()
        month_to = month_to or today.replace(day=1)
        month_from = month_from or month_to.replace(month=1)
        if month_from > month_to:
            raise ValidationFailed(field="from")
        display = await self.fx.display_for(access.org_id, currency)
        rows = await self.uow.reports.managers(access, month_from, month_to)
        return ManagersReport(month_from, month_to, rows, display)
