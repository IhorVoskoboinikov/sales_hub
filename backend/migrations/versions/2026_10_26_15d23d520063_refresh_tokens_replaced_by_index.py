"""refresh tokens replaced_by index — индекс по FK replaced_by (разделы 3, 9, этап 5.8).

replaced_by ссылается на refresh_tokens(id) с ON DELETE SET NULL. С этапа 5.8 maintenance.cleanup
удаляет истёкшие токены, и на каждую удалённую строку FK ищет ссылающиеся на неё — без индекса
по колонке это проход по всей таблице (раздел 3: у FK нужен полный индекс по колонке). Таблица
уже заполнена (новая строка на каждую ротацию), поэтому CONCURRENTLY в autocommit_block, как у
ix_*_trash. Прерванное построение оставляет INVALID-индекс: его удаляют руками
(DROP INDEX CONCURRENTLY) и запускают миграцию снова.

Revision ID: 15d23d520063
Revises: 8d2e4f6a1b37
Create Date: 2026-10-26 18:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "15d23d520063"
down_revision: str | Sequence[str] | None = "8d2e4f6a1b37"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            op.f("ix_refresh_tokens_replaced_by"),
            "refresh_tokens",
            ["replaced_by"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            op.f("ix_refresh_tokens_replaced_by"),
            table_name="refresh_tokens",
            postgresql_concurrently=True,
            if_exists=True,
        )
