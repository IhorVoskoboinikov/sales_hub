import json
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.context import get_request_id
from app.models import AuditEntry


class AuditRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def record(
        self,
        *,
        actor_id: uuid.UUID | None,
        org_id: uuid.UUID | None,
        action: str,
        entity_type: str,
        entity_id: object,
        changes: dict[str, Any] | None = None,
    ) -> None:
        """Записать в той же транзакции, что и само изменение: откатилось изменение —
        откатилась и запись в журнале."""
        self.session.add(
            AuditEntry(
                actor_id=actor_id,
                org_id=org_id,
                action=action,
                entity_type=entity_type,
                entity_id=str(entity_id),
                # Decimal, date, UUID — в строки: jsonb принимает только JSON-типы.
                changes=json.loads(json.dumps(changes or {}, default=str)),
                request_id=get_request_id(),
            )
        )


def diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, list[Any]]:
    """{"role": ["viewer", "org_admin"]} — только поля, которые поменялись."""
    return {
        key: [_plain(before.get(key)), _plain(value)]
        for key, value in after.items()
        if before.get(key) != value
    }


def _plain(value: Any) -> Any:
    return str(value) if isinstance(value, uuid.UUID) else value
