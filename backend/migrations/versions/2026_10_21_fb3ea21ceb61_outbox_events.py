"""outbox events — события и команды для relay, NOTIFY после вставки (разделы 3, 6, 15,
этап 5.2).

Триггер AFTER INSERT … FOR EACH STATEMENT шлёт pg_notify('outbox_new', ''): одно уведомление на
INSERT, а не на строку, — relay всё равно читает пачкой. Postgres доставляет NOTIFY только
после коммита, поэтому relay не увидит событие раньше, чем его можно прочитать. Функцию и
триггер Alembic не видит — они записаны здесь SQL-ем.

Revision ID: fb3ea21ceb61
Revises: 602391a6401b
Create Date: 2026-10-21 20:54:21.391641
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "fb3ea21ceb61"
down_revision: str | Sequence[str] | None = "602391a6401b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


OUTBOX_NOTIFY = """
CREATE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Пустой payload: relay сам читает неопубликованные строки, ему нужно только «проснись».
    PERFORM pg_notify('outbox_new', '');
    RETURN NULL;
END
$$
"""


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_events")),
    )
    op.create_index(
        "ix_outbox_events_unpublished",
        "outbox_events",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.execute(OUTBOX_NOTIFY)
    op.execute(
        "CREATE TRIGGER outbox_events_notify AFTER INSERT ON outbox_events"
        " FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify()"
    )


def downgrade() -> None:
    op.drop_table("outbox_events")  # индекс и триггер удаляются вместе с таблицей
    op.execute("DROP FUNCTION outbox_notify()")
