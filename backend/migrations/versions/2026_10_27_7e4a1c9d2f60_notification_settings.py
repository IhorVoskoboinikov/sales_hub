"""notification settings — личные настройки каналов уведомлений (разделы 3, 16, этап 5.12).

Только отличия от умолчаний из кода (app/notifications/catalog.py): пустая таблица — у всех
умолчания. Таблица маленькая (не больше восьми строк на пользователя) и новая — обычный
CREATE TABLE.

Revision ID: 7e4a1c9d2f60
Revises: 3b7d9e2c5a41
Create Date: 2026-10-27 18:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7e4a1c9d2f60"
down_revision: str | Sequence[str] | None = "3b7d9e2c5a41"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notification_settings",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.CheckConstraint(
            "channel IN ('email', 'telegram')", name=op.f("ck_notification_settings_channel")
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_notification_settings_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "user_id", "event_type", "channel", name=op.f("pk_notification_settings")
        ),
    )


def downgrade() -> None:
    op.drop_table("notification_settings")
