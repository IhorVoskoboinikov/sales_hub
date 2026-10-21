"""pipelines and deals — воронки, этапы и сделки (раздел 3, этап 4.3).

Индексы — обычным CREATE INDEX: таблица deals новая и пустая.

Revision ID: 81ac47d69553
Revises: 12be1d080637
Create Date: 2026-10-21 03:25:07.867594
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "81ac47d69553"
down_revision: str | Sequence[str] | None = "12be1d080637"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_pipelines_org_id_organizations")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_pipelines")),
        sa.UniqueConstraint("org_id", "name", name=op.f("uq_pipelines_org_id_name")),
    )
    op.create_table(
        "pipeline_stages",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("pipeline_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("probability", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.CheckConstraint(
            "probability BETWEEN 0 AND 100", name=op.f("ck_pipeline_stages_probability_range")
        ),
        sa.ForeignKeyConstraint(
            ["pipeline_id"], ["pipelines.id"], name=op.f("fk_pipeline_stages_pipeline_id_pipelines")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_pipeline_stages")),
        sa.UniqueConstraint("id", "pipeline_id", name=op.f("uq_pipeline_stages_id_pipeline_id")),
    )
    op.create_index(
        "ix_pipeline_stages_pipeline_id_position",
        "pipeline_stages",
        ["pipeline_id", "position"],
        unique=False,
    )
    op.create_table(
        "deals",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("team_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("contact_id", sa.BigInteger(), nullable=True),
        sa.Column("company_id", sa.BigInteger(), nullable=True),
        sa.Column("pipeline_id", sa.BigInteger(), nullable=False),
        sa.Column("stage_id", sa.BigInteger(), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("amount_usd", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.Column("amount_original", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.Column("currency_original", sa.String(length=3), nullable=False),
        sa.Column("fx_rate", sa.Numeric(precision=14, scale=6), nullable=False),
        sa.Column("fx_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=8), server_default=sa.text("'open'"), nullable=False),
        sa.Column("expected_close_date", sa.Date(), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.CheckConstraint(
            "(status = 'open') = (closed_at IS NULL)", name=op.f("ck_deals_closed_at_for_closed")
        ),
        sa.CheckConstraint("status IN ('open', 'won', 'lost')", name=op.f("ck_deals_status_valid")),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name=op.f("fk_deals_company_id_companies"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["contact_id"],
            ["contacts.id"],
            name=op.f("fk_deals_contact_id_contacts"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_deals_org_id_organizations")
        ),
        sa.ForeignKeyConstraint(["owner_id"], ["users.id"], name=op.f("fk_deals_owner_id_users")),
        sa.ForeignKeyConstraint(
            ["stage_id", "pipeline_id"],
            ["pipeline_stages.id", "pipeline_stages.pipeline_id"],
            name=op.f("fk_deals_stage_id_pipeline_stages"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_deals")),
    )
    op.create_index("ix_deals_company_id", "deals", ["company_id"], unique=False)
    op.create_index("ix_deals_contact_id", "deals", ["contact_id"], unique=False)
    op.create_index(
        "ix_deals_kanban",
        "deals",
        ["pipeline_id", "stage_id", "id"],
        unique=False,
        postgresql_include=["title", "amount_usd", "owner_id", "team_id", "org_id"],
        postgresql_where=sa.text("status = 'open' AND deleted_at IS NULL"),
    )
    op.create_index(
        "ix_deals_org_id_created_at",
        "deals",
        ["org_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_deals_overdue",
        "deals",
        ["org_id", "expected_close_date", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'open' AND deleted_at IS NULL"),
    )
    op.create_index(
        "ix_deals_owner_id_created_at",
        "deals",
        ["owner_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index("ix_deals_stage_id", "deals", ["stage_id"], unique=False)
    op.create_index(
        "ix_deals_team_id_created_at",
        "deals",
        ["team_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_deals_team_id_created_at",
        table_name="deals",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index("ix_deals_stage_id", table_name="deals")
    op.drop_index(
        "ix_deals_owner_id_created_at",
        table_name="deals",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_deals_overdue",
        table_name="deals",
        postgresql_where=sa.text("status = 'open' AND deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_deals_org_id_created_at",
        table_name="deals",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_deals_kanban",
        table_name="deals",
        postgresql_include=["title", "amount_usd", "owner_id", "team_id", "org_id"],
        postgresql_where=sa.text("status = 'open' AND deleted_at IS NULL"),
    )
    op.drop_index("ix_deals_contact_id", table_name="deals")
    op.drop_index("ix_deals_company_id", table_name="deals")
    op.drop_table("deals")
    op.drop_index("ix_pipeline_stages_pipeline_id_position", table_name="pipeline_stages")
    op.drop_table("pipeline_stages")
    op.drop_table("pipelines")
