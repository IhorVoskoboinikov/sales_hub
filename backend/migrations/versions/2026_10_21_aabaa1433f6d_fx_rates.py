"""fx rates — курсы НБУ для конвертера валют (раздел 3, этап 4.4).

Revision ID: aabaa1433f6d
Revises: 81ac47d69553
Create Date: 2026-10-21 03:32:01.010717
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "aabaa1433f6d"
down_revision: str | Sequence[str] | None = "81ac47d69553"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fx_rates",
        sa.Column("rate_date", sa.Date(), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("rate_uah", sa.Numeric(precision=14, scale=6), nullable=False),
        sa.Column("source", sa.String(length=16), server_default=sa.text("'NBU'"), nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("rate_date", "currency", name=op.f("pk_fx_rates")),
    )


def downgrade() -> None:
    op.drop_table("fx_rates")
