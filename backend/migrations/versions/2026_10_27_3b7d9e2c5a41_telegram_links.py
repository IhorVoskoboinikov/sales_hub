"""telegram links — привязка Telegram и цель токена telegram_link (разделы 3, 16, этап 5.11).

email_tokens — маленькая таблица (живые токены и история за 30 дней до maintenance.cleanup),
поэтому CHECK меняется обычным DROP + ADD: проверка всех строк занимает миллисекунды.
downgrade сначала удаляет токены telegram_link — иначе старый CHECK не примет таблицу.

Revision ID: 3b7d9e2c5a41
Revises: 15d23d520063
Create Date: 2026-10-27 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3b7d9e2c5a41"
down_revision: str | Sequence[str] | None = "15d23d520063"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(op.f("ck_email_tokens_purpose"), "email_tokens", type_="check")
    op.create_check_constraint(
        op.f("ck_email_tokens_purpose"),
        "email_tokens",
        "purpose IN ('invite', 'reset_password', 'telegram_link')",
    )
    op.create_table(
        "telegram_links",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("username", sa.String(length=64), nullable=True),
        sa.Column(
            "linked_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_telegram_links_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("user_id", name=op.f("pk_telegram_links")),
        sa.UniqueConstraint("chat_id", name=op.f("uq_telegram_links_chat_id")),
    )


def downgrade() -> None:
    op.drop_table("telegram_links")
    op.execute("DELETE FROM email_tokens WHERE purpose = 'telegram_link'")
    op.drop_constraint(op.f("ck_email_tokens_purpose"), "email_tokens", type_="check")
    op.create_check_constraint(
        op.f("ck_email_tokens_purpose"),
        "email_tokens",
        "purpose IN ('invite', 'reset_password')",
    )
