"""failed tasks — задачи Celery, упавшие после всех ретраев (разделы 3, 15, этап 5.5).

UNIQUE (task_id) — повторно доставленная и снова упавшая задача не даёт дубля (ON CONFLICT DO
NOTHING). Индекс (failed_at, id) — keyset-список супер-админа. Таблица новая и пустая, поэтому
индексы — обычным CREATE INDEX, а не CONCURRENTLY.

Revision ID: 07d269f319fb
Revises: 1ffac39b6f31
Create Date: 2026-10-22 16:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "07d269f319fb"
down_revision: str | Sequence[str] | None = "1ffac39b6f31"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "failed_tasks",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("task_id", sa.String(length=255), nullable=False),
        sa.Column("task_name", sa.String(length=255), nullable=False),
        sa.Column("queue", sa.String(length=64), nullable=True),
        sa.Column("args", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("kwargs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.Column("user_id", sa.Uuid(), nullable=True),
        sa.Column("exc_type", sa.String(length=255), nullable=False),
        sa.Column("exc_message", sa.String(length=2000), nullable=False),
        sa.Column("traceback", sa.Text(), nullable=True),
        sa.Column("retries", sa.Integer(), nullable=False),
        sa.Column(
            "failed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("replayed_by", sa.Uuid(), nullable=True),
        sa.Column("replay_task_id", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_failed_tasks")),
        sa.UniqueConstraint("task_id", name=op.f("uq_failed_tasks_task_id")),
    )
    op.create_index(
        "ix_failed_tasks_failed_at_id", "failed_tasks", ["failed_at", "id"], unique=False
    )


def downgrade() -> None:
    op.drop_table("failed_tasks")  # индекс удаляется вместе с таблицей
