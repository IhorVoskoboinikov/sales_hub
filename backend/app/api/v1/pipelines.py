"""Воронки продаж и их этапы: читают все, настраивает org_admin (org:read / org:manage)."""

from fastapi import Depends, status

from app.api.deps import get_pipeline_service
from app.api.v1 import protected_router
from app.api.v1.errors import CONFLICT, NOT_FOUND, PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.schemas.common import RecordId
from app.schemas.deals import PipelineIn, PipelineOut, PipelineUpdate
from app.services.deals import PipelineService, StageIn


def stages_in(body: PipelineIn | PipelineUpdate) -> list[StageIn] | None:
    if body.stages is None:
        return None
    return [StageIn(s.name, s.probability, s.id) for s in body.stages]


@protected_router.get("/pipelines", tags=["pipelines"], responses=PROTECTED)
async def list_pipelines(
    access: Access = Depends(authorize),
    pipelines: PipelineService = Depends(get_pipeline_service),
) -> list[PipelineOut]:
    """Воронки организации с этапами по порядку."""
    return [PipelineOut.model_validate(p) for p in await pipelines.list(access)]


@protected_router.post(
    "/pipelines",
    tags=["pipelines"],
    status_code=status.HTTP_201_CREATED,
    responses={**PROTECTED, **CONFLICT, **VALIDATION},
)
async def create_pipeline(
    body: PipelineIn,
    access: Access = Depends(authorize),
    pipelines: PipelineService = Depends(get_pipeline_service),
) -> PipelineOut:
    """Создать воронку. Порядок этапов в списке — их порядок на канбане."""
    pipeline = await pipelines.create(access, body.name, stages_in(body) or [])
    return PipelineOut.model_validate(pipeline)


@protected_router.patch(
    "/pipelines/{pipeline_id}",
    tags=["pipelines"],
    responses={**PROTECTED, **NOT_FOUND, **CONFLICT, **VALIDATION},
)
async def update_pipeline(
    pipeline_id: RecordId,
    body: PipelineUpdate,
    access: Access = Depends(authorize),
    pipelines: PipelineService = Depends(get_pipeline_service),
) -> PipelineOut:
    """Переименовать воронку и/или заменить этапы целиком: этап с id — изменить, без id —
    добавить, пропавший — удалить (если на нём есть сделки, в том числе в корзине, — 409
    pipeline.stage_has_deals)."""
    pipeline = await pipelines.update(access, pipeline_id, body.name, stages_in(body))
    return PipelineOut.model_validate(pipeline)
