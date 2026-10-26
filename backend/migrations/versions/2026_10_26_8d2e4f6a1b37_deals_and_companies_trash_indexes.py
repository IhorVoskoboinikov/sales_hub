"""deals and companies trash indexes — индексы корзины сделок и компаний (разделы 3, 9, этап 5.8).

(org_id, deleted_at, id) WHERE deleted_at IS NOT NULL — как ix_contacts_trash: по ним trash.purge
находит строки корзины организации старше срока, не читая таблицу. Таблицы уже заполнены
(1 млн сделок), поэтому CONCURRENTLY: обычный CREATE INDEX держал бы блокировку записи всё
время построения. CONCURRENTLY не работает в транзакции — отсюда autocommit_block. Если
построение прервалось, остаётся INVALID-индекс: IF NOT EXISTS его не пересоздаст — такой
индекс удаляют руками (DROP INDEX CONCURRENTLY) и запускают миграцию снова.

Revision ID: 8d2e4f6a1b37
Revises: 5c3e9a7b2d14
Create Date: 2026-10-26 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "8d2e4f6a1b37"
down_revision: str | Sequence[str] | None = "5c3e9a7b2d14"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = ("deals", "companies")


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES:
            op.create_index(
                f"ix_{table}_trash",
                table,
                ["org_id", "deleted_at", "id"],
                unique=False,
                postgresql_where=sa.text("deleted_at IS NOT NULL"),
                postgresql_concurrently=True,
                if_not_exists=True,
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES:
            op.drop_index(
                f"ix_{table}_trash",
                table_name=table,
                postgresql_concurrently=True,
                if_exists=True,
            )
