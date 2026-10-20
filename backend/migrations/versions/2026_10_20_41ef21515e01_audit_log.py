"""audit log — журнал изменений (раздел 3); партиции по месяцам — на этапе 4.

Revision ID: 41ef21515e01
Revises: a2ff1f74f48b
Create Date: 2026-10-20 18:35:01.555974
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "41ef21515e01"
down_revision: str | Sequence[str] | None = "a2ff1f74f48b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("org_id", sa.Uuid(), nullable=True),
        sa.Column("actor_id", sa.Uuid(), nullable=True),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("changes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_log")),
    )
    op.create_index(
        "ix_audit_log_org_id_occurred_at", "audit_log", ["org_id", "occurred_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_audit_log_org_id_occurred_at", table_name="audit_log")
    op.drop_table("audit_log")
