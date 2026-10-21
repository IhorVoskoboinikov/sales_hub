"""deal stats materialized view — отчёты по сделкам (раздел 6, этап 4.7).

SQL записан здесь, а не импортирован из app: миграция должна создавать ровно то, что было
на момент её написания, даже если модель потом изменится.

Revision ID: 602391a6401b
Revises: 4341cd0e0a19
Create Date: 2026-10-21 09:13:29.006954
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "602391a6401b"
down_revision: str | Sequence[str] | None = "4341cd0e0a19"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE MATERIALIZED VIEW deal_stats AS
        SELECT org_id, team_id, owner_id, pipeline_id, stage_id, status,
               date_trunc('month', coalesce(closed_at, created_at) AT TIME ZONE 'UTC')::date
                   AS month,
               count(*) AS deals,
               sum(amount_usd) AS amount_usd
        FROM deals
        WHERE deleted_at IS NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7
        """
    )
    # UNIQUE по ключу группировки — без него REFRESH … CONCURRENTLY не работает. Он же —
    # индекс для отчётов: все запросы начинаются с org_id.
    op.execute(
        "CREATE UNIQUE INDEX uq_deal_stats ON deal_stats"
        " (org_id, pipeline_id, stage_id, status, team_id, owner_id, month)"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW deal_stats")
