"""Журнал изменений организации (раздел 3): только org_admin (audit:read)."""

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import Depends, Query

from app.api.deps import get_audit_service, get_page
from app.api.v1 import protected_router
from app.api.v1.errors import PROTECTED, VALIDATION
from app.authz.access import Access
from app.authz.deps import authorize
from app.repositories.activities import AuditFilters
from app.repositories.pagination import Page
from app.schemas.activities import AuditOut
from app.schemas.common import PageOut
from app.services.activities import AuditService


@protected_router.get("/audit", tags=["audit"], responses={**PROTECTED, **VALIDATION})
async def list_audit(
    entity_type: Annotated[str | None, Query(max_length=32, examples=["contact"])] = None,
    entity_id: Annotated[str | None, Query(max_length=64)] = None,
    actor_id: uuid.UUID | None = None,
    action: Annotated[str | None, Query(max_length=64, examples=["member.updated"])] = None,
    from_: Annotated[datetime | None, Query(alias="from")] = None,
    to: datetime | None = None,
    page: Page = Depends(get_page),
    access: Access = Depends(authorize),
    audit: AuditService = Depends(get_audit_service),
) -> PageOut[AuditOut]:
    """Кто, что и когда изменил в организации, новые сначала. С from/to читаются только
    нужные месячные партиции."""
    filters = AuditFilters(entity_type, entity_id, actor_id, action, from_, to)
    result = await audit.list(access, filters, page)
    return PageOut(
        items=[AuditOut.model_validate(e) for e in result.items], next_cursor=result.next_cursor
    )
