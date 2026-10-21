"""Сделки и канбан воронки (раздел 3).

Видимость — по уровню deal:read: менеджер видит свои сделки, руководитель — отдела, админ и
наблюдатель — всей организации. /deals/kanban объявлен раньше /deals/{deal_id}: иначе
Starlette сопоставил бы «kanban» с {deal_id} (раздел 5, «Сторона FastAPI»).
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from fastapi import Depends, Query, status

from app.api.deps import get_deal_service, get_page
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.models import DealStatus
from app.repositories.deals import DealFilters
from app.repositories.pagination import Page
from app.schemas.common import MAX_ID, RecordId
from app.schemas.deals import (
    DealIn,
    DealListItem,
    DealOut,
    DealPageOut,
    DealUpdate,
    FxOut,
    KanbanCard,
    KanbanOut,
    KanbanStage,
)
from app.services.deals import DealService
from app.services.fx import Display

OptionalId = Annotated[int | None, Query(ge=1, le=MAX_ID)]
CurrencyParam = Annotated[
    str | None,
    Query(
        pattern=r"^[A-Z]{3}$",
        description="Валюта сумм в ответе (ISO 4217); по умолчанию — валюта организации",
    ),
]


def fx_out(display: Display) -> FxOut:
    return FxOut(
        currency=display.currency, rate=display.rate, date=display.date, stale=display.stale
    )


@protected_router.get("/deals", tags=["deals"], responses={**PROTECTED, **VALIDATION})
async def list_deals(
    status_: Annotated[DealStatus | None, Query(alias="status")] = None,
    pipeline_id: OptionalId = None,
    stage_id: OptionalId = None,
    owner_id: uuid.UUID | None = None,
    contact_id: OptionalId = None,
    company_id: OptionalId = None,
    overdue: Annotated[
        bool, Query(description="Только открытые с прошедшей датой закрытия, по этой дате")
    ] = False,
    currency: CurrencyParam = None,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    deals: DealService = Depends(get_deal_service),
) -> DealPageOut:
    """Сделки в моём уровне, новые сначала (с overdue=true — самые просроченные сначала)."""
    filters = DealFilters(
        status=status_,
        pipeline_id=pipeline_id,
        stage_id=stage_id,
        owner_id=owner_id,
        contact_id=contact_id,
        company_id=company_id,
        overdue_on=datetime.now(UTC).date() if overdue else None,
    )
    display = await deals.display(access, currency)
    result = await deals.list(access, filters, page)
    items = [
        DealListItem(**DealOut.model_validate(d).model_dump(), amount=display.amount(d.amount_usd))
        for d in result.items
    ]
    return DealPageOut(items=items, next_cursor=result.next_cursor, fx=fx_out(display))


@protected_router.get(
    "/deals/kanban", tags=["deals"], responses={**PROTECTED, **NOT_FOUND, **VALIDATION}
)
async def kanban(
    pipeline_id: Annotated[int, Query(ge=1, le=MAX_ID)],
    per_stage: Annotated[int, Query(ge=1, le=50, description="Карточек на этап")] = 20,
    currency: CurrencyParam = None,
    access: Access = Depends(authorize),
    deals: DealService = Depends(get_deal_service),
) -> KanbanOut:
    """Канбан воронки: по каждому этапу — число и сумма открытых сделок в моём уровне и первые
    per_stage карточек (новые сначала)."""
    display = await deals.display(access, currency)
    pipeline, totals, cards = await deals.kanban(access, pipeline_id, per_stage)
    by_stage = {t.stage_id: t for t in totals}
    stages = []
    for stage in pipeline.stages:
        total = by_stage.get(stage.id)
        amount_usd = total.amount_usd if total else Decimal(0)
        stages.append(
            KanbanStage(
                id=stage.id,
                name=stage.name,
                position=stage.position,
                probability=stage.probability,
                deals_count=total.deals if total else 0,
                amount_usd=amount_usd,
                amount=display.amount(amount_usd),
                deals=[
                    KanbanCard(
                        id=c.id,
                        title=c.title,
                        amount_usd=c.amount_usd,
                        amount=display.amount(c.amount_usd),
                        owner_id=c.owner_id,
                    )
                    for c in cards
                    if c.stage_id == stage.id
                ],
            )
        )
    return KanbanOut(pipeline_id=pipeline.id, stages=stages, fx=fx_out(display))


@protected_router.get("/deals/{deal_id}", tags=["deals"], responses={**PROTECTED, **NOT_FOUND})
async def get_deal(
    deal_id: RecordId,
    access: Access = Depends(authorize),
    deals: DealService = Depends(get_deal_service),
) -> DealOut:
    return DealOut.model_validate(await deals.get(access, deal_id))


@protected_router.post(
    "/deals",
    tags=["deals"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **VALIDATION},
)
async def create_deal(
    body: DealIn,
    access: Access = Depends(authorize),
    deals: DealService = Depends(get_deal_service),
) -> DealOut:
    """Создать сделку на этапе воронки (воронка берётся из этапа). Сумма — в любой валюте
    НБУ: в базе сохраняются она сама, курс и сумма в USD. Контакт и компания должны быть видны
    вам, иначе 422."""
    return DealOut.model_validate(await deals.create(access, body.model_dump()))


@protected_router.patch(
    "/deals/{deal_id}",
    tags=["deals"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT, **VALIDATION},
)
async def update_deal(
    deal_id: RecordId,
    body: DealUpdate,
    access: Access = Depends(authorize),
    deals: DealService = Depends(get_deal_service),
) -> DealOut:
    """Изменить сделку: этап, сумму, статус (won / lost ставят дату закрытия), ответственного.
    version — из последнего GET."""
    fields = body.model_dump(exclude_unset=True, exclude={"version"})
    return DealOut.model_validate(await deals.update(access, deal_id, body.version, fields))
