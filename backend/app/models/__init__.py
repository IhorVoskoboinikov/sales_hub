"""Все модели импортируются здесь — так Alembic видит их через Base.metadata."""

from app.db.base import Base
from app.models.activities import Activity, ActivityType, Note
from app.models.audit import AuditEntry
from app.models.bulk import (
    FINAL_STATUSES,
    BulkEntity,
    BulkJob,
    BulkJobChunk,
    BulkJobKind,
    BulkJobStatus,
    ChunkStatus,
)
from app.models.crm import Company, Contact, ContactStatus
from app.models.deals import Deal, DealStatus, Pipeline, PipelineStage
from app.models.email_token import EmailToken, EmailTokenPurpose
from app.models.failed_tasks import FailedTask
from app.models.fx import FxRate
from app.models.imports import ContactStaging, ImportRowError
from app.models.notifications import NotificationChannel, NotificationSetting, SentNotification
from app.models.organization import Organization, Team
from app.models.outbox import OutboxEvent
from app.models.refresh_token import RefreshToken
from app.models.telegram import TelegramLink
from app.models.user import ROLES_WITH_TEAM, Role, User

__all__ = [
    "FINAL_STATUSES",
    "ROLES_WITH_TEAM",
    "Activity",
    "ActivityType",
    "AuditEntry",
    "Base",
    "BulkEntity",
    "BulkJob",
    "BulkJobChunk",
    "BulkJobKind",
    "BulkJobStatus",
    "ChunkStatus",
    "Company",
    "Contact",
    "ContactStaging",
    "ContactStatus",
    "Deal",
    "DealStatus",
    "EmailToken",
    "EmailTokenPurpose",
    "FailedTask",
    "FxRate",
    "ImportRowError",
    "Note",
    "NotificationChannel",
    "NotificationSetting",
    "Organization",
    "OutboxEvent",
    "Pipeline",
    "PipelineStage",
    "RefreshToken",
    "Role",
    "SentNotification",
    "Team",
    "TelegramLink",
    "User",
]
