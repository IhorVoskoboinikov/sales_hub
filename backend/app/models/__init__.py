"""Все модели импортируются здесь — так Alembic видит их через Base.metadata."""

from app.db.base import Base
from app.models.audit import AuditEntry
from app.models.crm import Company, Contact, ContactStatus
from app.models.deals import Deal, DealStatus, Pipeline, PipelineStage
from app.models.email_token import EmailToken, EmailTokenPurpose
from app.models.fx import FxRate
from app.models.organization import Organization, Team
from app.models.refresh_token import RefreshToken
from app.models.user import ROLES_WITH_TEAM, Role, User

__all__ = [
    "ROLES_WITH_TEAM",
    "AuditEntry",
    "Base",
    "Company",
    "Contact",
    "ContactStatus",
    "Deal",
    "DealStatus",
    "EmailToken",
    "EmailTokenPurpose",
    "FxRate",
    "Organization",
    "Pipeline",
    "PipelineStage",
    "RefreshToken",
    "Role",
    "Team",
    "User",
]
