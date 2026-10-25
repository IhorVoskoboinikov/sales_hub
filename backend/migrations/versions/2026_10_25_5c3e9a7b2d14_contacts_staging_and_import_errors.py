"""contacts staging and import errors — рабочая таблица импорта CSV и ошибки строк (разделы 3,
6, этап 5.6).

contacts_staging — обычная (logged) таблица шагов импорта, общая для всех job: PK (job_id,
row_no), email — text (COPY через asyncpg не знает бинарного кодека citext), сравнение — по
lower(email). import_errors — у строки одна, первая ошибка: PK (job_id, row_no). Обе удаляются
вместе с job (ON DELETE CASCADE). Таблицы новые и пустые — индексов, кроме PK, нет.

Revision ID: 5c3e9a7b2d14
Revises: 07d269f319fb
Create Date: 2026-10-25 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "5c3e9a7b2d14"
down_revision: str | Sequence[str] | None = "07d269f319fb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "contacts_staging",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("row_no", sa.Integer(), nullable=False),
        sa.Column("email", sa.Text(), nullable=False),
        sa.Column("first_name", sa.String(length=100), nullable=False),
        sa.Column("last_name", sa.String(length=100), nullable=True),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=True),
        sa.Column("tags", postgresql.ARRAY(sa.String(length=64)), nullable=True),
        sa.Column("owner_email", sa.Text(), nullable=True),
        sa.Column("owner_id", sa.Uuid(), nullable=True),
        sa.Column("error", sa.String(length=64), nullable=True),
        sa.Column("merged", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["bulk_jobs.id"],
            name=op.f("fk_contacts_staging_job_id_bulk_jobs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("job_id", "row_no", name=op.f("pk_contacts_staging")),
    )
    op.create_table(
        "import_errors",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("row_no", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("field", sa.String(length=64), nullable=True),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["bulk_jobs.id"],
            name=op.f("fk_import_errors_job_id_bulk_jobs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("job_id", "row_no", name=op.f("pk_import_errors")),
    )


def downgrade() -> None:
    op.drop_table("import_errors")
    op.drop_table("contacts_staging")
