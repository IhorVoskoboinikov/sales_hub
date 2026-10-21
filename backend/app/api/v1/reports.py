"""Отчёты по сделкам (раздел 6): из materialized view deal_stats, в уровне report:read —
менеджер видит свои цифры, руководитель — отдела, админ и наблюдатель — всей организации."""

from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import Depends, Query

from app.api.deps import get_report_service
from app.api.v1 import protected_router
from app.api.v1.deals import CurrencyParam, fx_out
from app.api.v1.errors import NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.schemas.common import MAX_ID
from app.schemas.reports import (
    ManagerReportRow,
    ManagersReportOut,
    PipelineReportOut,
    StageReportRow,
)
from app.services.reports import ReportService

Month = Annotated[
    str | None,
    Query(pattern=r"^[1-9]\d{3}-(0[1-9]|1[0-2])$", examples=["2026-01"], description="YYYY-MM"),
]


def first_day(month: str | None) -> date | None:
    return date.fromisoformat(f"{month}-01") if month else None


@protected_router.get(
    "/reports/pipeline", tags=["reports"], responses={**PROTECTED, **NOT_FOUND, **VALIDATION}
)
async def pipeline_report(
    pipeline_id: Annotated[int, Query(ge=1, le=MAX_ID)],
    currency: CurrencyParam = None,
    access: Access = Depends(authorize),
    reports: ReportService = Depends(get_report_service),
) -> PipelineReportOut:
    """Воронка: число и сумма открытых сделок на каждом этапе."""
    report = await reports.pipeline(access, pipeline_id, currency)
    by_stage = {row.stage_id: row for row in report.stages}
    stages = []
    for stage in report.pipeline.stages:
        row = by_stage.get(stage.id)
        amount_usd = row.amount_usd if row else Decimal(0)
        stages.append(
            StageReportRow(
                stage_id=stage.id,
                name=stage.name,
                position=stage.position,
                deals=row.deals if row else 0,
                amount_usd=amount_usd,
                amount=report.display.amount(amount_usd),
            )
        )
    return PipelineReportOut(
        pipeline_id=report.pipeline.id, stages=stages, fx=fx_out(report.display)
    )


@protected_router.get("/reports/managers", tags=["reports"], responses={**PROTECTED, **VALIDATION})
async def managers_report(
    from_: Annotated[Month, Query(alias="from")] = None,
    to: Month = None,
    currency: CurrencyParam = None,
    access: Access = Depends(authorize),
    reports: ReportService = Depends(get_report_service),
) -> ManagersReportOut:
    """По ответственным: открытые сделки и выигранные / проигранные за месяцы from–to
    (по умолчанию — с января по текущий месяц)."""
    report = await reports.managers(access, first_day(from_), first_day(to), currency)
    amount = report.display.amount
    return ManagersReportOut(
        month_from=report.month_from,
        month_to=report.month_to,
        managers=[
            ManagerReportRow(
                owner_id=r.owner_id,
                email=r.email,
                open_deals=r.open_deals,
                open_amount=amount(r.open_usd),
                won_deals=r.won_deals,
                won_amount=amount(r.won_usd),
                lost_deals=r.lost_deals,
                lost_amount=amount(r.lost_usd),
            )
            for r in report.rows
        ],
        fx=fx_out(report.display),
    )
