import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Role(StrEnum):
    """Роли организации (раздел 5). Что может каждая — в policies/saleshub/data.yaml.
    super_admin — не роль, а флаг users.is_superadmin."""

    ORG_ADMIN = "org_admin"
    TEAM_LEAD = "team_lead"
    SALES_REP = "sales_rep"
    VIEWER = "viewer"


# Роли, которым отдел обязателен; остальным он не положен.
ROLES_WITH_TEAM = frozenset({Role.TEAM_LEAD, Role.SALES_REP})


class User(Base):
    """Пользователь. Появляется только из приглашения или командой create-superadmin.

    Один человек — одна организация и одна роль (раздел 5): org_id, team_id и role прямо здесь.
    CHECK-и написаны без сравнений с NULL: CHECK, который дал NULL, Postgres считает пройденным.
    """

    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint(
            "role IN ('org_admin', 'team_lead', 'sales_rep', 'viewer')", name="role_valid"
        ),
        # Роль есть ровно тогда, когда есть организация.
        CheckConstraint("(role IS NULL) = (org_id IS NULL)", name="role_with_org"),
        # У супер-админа организации нет: в данные организаций он не ходит.
        CheckConstraint(
            "NOT (is_superadmin AND org_id IS NOT NULL)", name="superadmin_without_org"
        ),
        # Отдел есть ровно у team_lead и sales_rep.
        CheckConstraint(
            "(team_id IS NOT NULL) = (role IS NOT NULL AND role IN ('team_lead', 'sales_rep'))",
            name="team_for_role",
        ),
        # Отдел — только из своей организации.
        ForeignKeyConstraint(["team_id", "org_id"], ["teams.id", "teams.org_id"]),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # citext: Ivan@Mail.com и ivan@mail.com — один и тот же пользователь.
    email: Mapped[str] = mapped_column(CITEXT, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    is_superadmin: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=text("false")
    )
    org_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("organizations.id"), index=True)
    team_id: Mapped[uuid.UUID | None] = mapped_column(index=True)
    role: Mapped[str | None] = mapped_column(String(32))
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Входит в access-токен (claim ver). +1 — и все выданные access-токены перестают работать.
    token_version: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    # None — часовой пояс организации (появится на этапе 3).
    timezone: Mapped[str | None] = mapped_column(String(64))
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
