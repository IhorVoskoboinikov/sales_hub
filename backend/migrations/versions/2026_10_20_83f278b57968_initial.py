"""initial — пустая миграция: проверяет, что цепочка Alembic работает.

Таблицы появятся с этапа 2.

Revision ID: 83f278b57968
Revises:
Create Date: 2026-10-20 14:12:18.790935
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "83f278b57968"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
