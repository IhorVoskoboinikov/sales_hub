"""sent notifications — ключи дедупа отправленных писем и сообщений (разделы 3, 15, этап 5.3).

Revision ID: a7db8b638684
Revises: fb3ea21ceb61
Create Date: 2026-10-22 10:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7db8b638684"
down_revision: str | Sequence[str] | None = "fb3ea21ceb61"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sent_notifications",
        sa.Column("dedup_key", sa.Text(), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=True),
        sa.Column(
            "sent_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "channel IN ('email', 'telegram')", name=op.f("ck_sent_notifications_channel")
        ),
        sa.PrimaryKeyConstraint("dedup_key", name=op.f("pk_sent_notifications")),
    )
    op.create_index(
        op.f("ix_sent_notifications_sent_at"), "sent_notifications", ["sent_at"], unique=False
    )


def downgrade() -> None:
    op.drop_table("sent_notifications")  # индекс удаляется вместе с таблицей
