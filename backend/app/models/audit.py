import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Identity, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class AuditEntry(Base):
    """Журнал: кто, что и когда изменил (раздел 3). Пока одна таблица; партиции по месяцам
    (PARTITION BY RANGE (occurred_at)) — на этапе 4, когда записей станет много.

    actor_id и org_id — без внешних ключей: журнал должен пережить удаление того, о ком он.
    """

    __tablename__ = "audit_log"
    __table_args__ = (Index("ix_audit_log_org_id_occurred_at", "org_id", "occurred_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    org_id: Mapped[uuid.UUID | None]  # None — действие вне организаций (супер-админ)
    actor_id: Mapped[uuid.UUID | None]  # None — система (например, команда create-superadmin)
    action: Mapped[str] = mapped_column(String(64))  # member.updated, team.created …
    entity_type: Mapped[str] = mapped_column(String(32))  # user, team, organization, invite
    entity_id: Mapped[str] = mapped_column(String(64))
    # Что поменялось: {"role": ["viewer", "org_admin"]} — было и стало.
    changes: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    request_id: Mapped[str | None] = mapped_column(String(64))  # связь с логами запроса
