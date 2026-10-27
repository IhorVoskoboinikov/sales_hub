"""bulk jobs — массовые операции и учёт их чанков (разделы 3, 6, 15, этап 5.4).

Частичный UNIQUE (org_id, kind) по активным статусам — одна тяжёлая job вида на организацию
(409 bulk.already_running); move_team в него не входит. UNIQUE (owner_id, idempotency_key) —
повтор запроса с тем же Idempotency-Key возвращает ту же job. Таблицы новые и пустые, поэтому
индексы — обычным CREATE INDEX, а не CONCURRENTLY.

Revision ID: 1ffac39b6f31
Revises: a7db8b638684
Create Date: 2026-10-22 10:28:01.695368
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "1ffac39b6f31"
down_revision: str | Sequence[str] | None = "a7db8b638684"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "bulk_jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("team_id", sa.Uuid(), nullable=True),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column(
            "status", sa.String(length=16), server_default=sa.text("'pending'"), nullable=False
        ),
        sa.Column("stage", sa.String(length=16), nullable=True),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=True),
        sa.Column("request_hash", sa.CHAR(length=64), nullable=True),
        sa.Column("total", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("processed", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("failed_rows", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("chunks_total", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("failed_chunks", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.String(length=64), nullable=True),
        sa.Column("reaps", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "kind IN ('reassign_owner', 'move_team', 'import_contacts', 'close_overdue_deals',"
            " 'add_tags', 'remove_tags')",
            name=op.f("ck_bulk_jobs_kind_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'cancelling', 'done', 'partial_failed', 'failed',"
            " 'cancelled')",
            name=op.f("ck_bulk_jobs_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_bulk_jobs_org_id_organizations")
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["users.id"], name=op.f("fk_bulk_jobs_owner_id_users")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_bulk_jobs")),
        sa.UniqueConstraint(
            "owner_id", "idempotency_key", name=op.f("uq_bulk_jobs_owner_id_idempotency_key")
        ),
    )
    op.create_index(
        "uq_bulk_jobs_org_id_kind_active",
        "bulk_jobs",
        ["org_id", "kind"],
        unique=True,
        postgresql_where=sa.text(
            "status IN ('pending', 'running', 'cancelling') AND kind <> 'move_team'"
        ),
    )
    op.create_table(
        "bulk_job_chunks",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("no", sa.Integer(), nullable=False),
        sa.Column("entity", sa.String(length=16), nullable=False),
        sa.Column("lo", sa.BigInteger(), nullable=False),
        sa.Column("hi", sa.BigInteger(), nullable=False),
        sa.Column(
            "status", sa.String(length=16), server_default=sa.text("'pending'"), nullable=False
        ),
        sa.Column("rows", sa.Integer(), nullable=True),
        sa.Column("error", sa.String(length=64), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "entity IN ('companies', 'contacts', 'deals', 'activities')",
            name=op.f("ck_bulk_job_chunks_entity_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'done', 'failed', 'skipped')",
            name=op.f("ck_bulk_job_chunks_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["bulk_jobs.id"],
            name=op.f("fk_bulk_job_chunks_job_id_bulk_jobs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("job_id", "no", name=op.f("pk_bulk_job_chunks")),
    )


def downgrade() -> None:
    op.drop_table("bulk_job_chunks")
    op.drop_table("bulk_jobs")  # индексы удаляются вместе с таблицей
