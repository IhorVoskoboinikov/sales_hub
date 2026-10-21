"""activities notes and partitioned audit log — активности, заметки и журнал на месячных
партициях (разделы 3 и 9, этап 4.5).

Партиции создаёт SQL-функция ensure_monthly_partitions(таблица, с, по): она идемпотентна
(существующую партицию пропускает), её вызывают эта миграция, команда
python -m app.cli ensure-partitions (сервис migrate), seed_big.sql и, с этапа 5, задача beat.

Revision ID: 4341cd0e0a19
Revises: aabaa1433f6d
Create Date: 2026-10-21 03:37:32.827021
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "4341cd0e0a19"
down_revision: str | Sequence[str] | None = "aabaa1433f6d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


ENSURE_PARTITIONS = """
-- from_day и to_day — даты по UTC: границы партиций — полночь UTC. Вызывающий переводит
-- timestamptz в дату через AT TIME ZONE 'UTC', а не ::date (тот берёт часовой пояс сессии).
CREATE OR REPLACE FUNCTION ensure_monthly_partitions(parent regclass, from_day date, to_day date)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    month date := date_trunc('month', from_day);
    name text;
    created integer := 0;
BEGIN
    WHILE month <= date_trunc('month', to_day) LOOP
        name := format('%s_p%s', parent::text, to_char(month, 'YYYY_MM'));
        IF to_regclass(name) IS NULL THEN
            -- Границы — полночь UTC: время в базе хранится в UTC (раздел 1).
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                name, parent,
                month::timestamp AT TIME ZONE 'UTC',
                (month + interval '1 month')::timestamp AT TIME ZONE 'UTC'
            );
            created := created + 1;
        END IF;
        month := month + interval '1 month';
    END LOOP;
    RETURN created;
END
$$
"""


def upgrade() -> None:
    op.execute(ENSURE_PARTITIONS)

    op.create_table(
        "activities",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("team_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("deal_id", sa.BigInteger(), nullable=True),
        sa.Column("contact_id", sa.BigInteger(), nullable=True),
        sa.Column("type", sa.String(length=16), nullable=False),
        sa.Column("subject", sa.String(length=300), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "due_at IS NOT NULL OR done_at IS NOT NULL", name=op.f("ck_activities_open_has_due")
        ),
        sa.CheckConstraint(
            "type IN ('call', 'meeting', 'email', 'task')", name=op.f("ck_activities_type_valid")
        ),
        sa.ForeignKeyConstraint(
            ["contact_id"],
            ["contacts.id"],
            name=op.f("fk_activities_contact_id_contacts"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["deal_id"], ["deals.id"], name=op.f("fk_activities_deal_id_deals"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", "created_at", name=op.f("pk_activities")),
        postgresql_partition_by="RANGE (created_at)",
    )
    # Индекс на родителе Postgres создаёт и на каждой партиции, в том числе будущих.
    for column in ("org_id", "team_id", "owner_id"):
        op.create_index(
            f"ix_activities_{column}_due_at",
            "activities",
            [column, "due_at", "id"],
            postgresql_where=sa.text("done_at IS NULL"),
        )
    op.create_index("ix_activities_deal_id", "activities", ["deal_id", "created_at", "id"])
    op.create_index("ix_activities_contact_id", "activities", ["contact_id", "created_at", "id"])
    op.execute(
        "SELECT ensure_monthly_partitions('activities', (now() AT TIME ZONE 'UTC')::date,"
        " ((now() + interval '2 months') AT TIME ZONE 'UTC')::date)"
    )

    op.create_table(
        "notes",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("entity_type", sa.String(length=16), nullable=False),
        sa.Column("entity_id", sa.BigInteger(), nullable=False),
        sa.Column("author_id", sa.Uuid(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "entity_type IN ('contact', 'company', 'deal')", name=op.f("ck_notes_entity_type_valid")
        ),
        sa.ForeignKeyConstraint(["author_id"], ["users.id"], name=op.f("fk_notes_author_id_users")),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_notes_org_id_organizations")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notes")),
    )
    op.create_index("ix_notes_entity", "notes", ["entity_type", "entity_id", "created_at", "id"])

    # --- audit_log → партиционированная таблица. Обычную таблицу нельзя «превратить» в
    # партиционированную: создаём новую, переносим строки и удаляем старую.
    op.execute("ALTER TABLE audit_log RENAME TO audit_log_old")
    op.execute("ALTER TABLE audit_log_old RENAME CONSTRAINT pk_audit_log TO pk_audit_log_old")
    op.drop_index("ix_audit_log_org_id_occurred_at", table_name="audit_log_old")
    create_audit_log(partitioned=True)
    op.execute(
        "SELECT ensure_monthly_partitions('audit_log',"
        " (coalesce((SELECT min(occurred_at) FROM audit_log_old), now()) AT TIME ZONE 'UTC')::date,"
        " ((now() + interval '2 months') AT TIME ZONE 'UTC')::date)"
    )
    copy_audit_rows()


def create_audit_log(*, partitioned: bool) -> None:
    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("org_id", sa.Uuid(), nullable=True),
        sa.Column("actor_id", sa.Uuid(), nullable=True),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=False),
        sa.Column("changes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint(
            *(("id", "occurred_at") if partitioned else ("id",)), name=op.f("pk_audit_log")
        ),
        **({"postgresql_partition_by": "RANGE (occurred_at)"} if partitioned else {}),
    )
    if partitioned:
        op.create_index(
            "ix_audit_log_org_id_occurred_at", "audit_log", ["org_id", "occurred_at", "id"]
        )
        op.create_index(
            "ix_audit_log_entity",
            "audit_log",
            ["org_id", "entity_type", "entity_id", "occurred_at", "id"],
        )
    else:
        op.create_index("ix_audit_log_org_id_occurred_at", "audit_log", ["org_id", "occurred_at"])


def copy_audit_rows() -> None:
    """Строки — вместе с id; затем последовательность id догоняет максимум, иначе следующая
    вставка получила бы уже занятый id."""
    op.execute("INSERT INTO audit_log SELECT * FROM audit_log_old")
    op.execute("DROP TABLE audit_log_old")
    op.execute(
        "SELECT setval(pg_get_serial_sequence('audit_log', 'id'),"
        " coalesce((SELECT max(id) FROM audit_log), 0) + 1, false)"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE audit_log RENAME TO audit_log_old")
    op.execute("ALTER TABLE audit_log_old RENAME CONSTRAINT pk_audit_log TO pk_audit_log_old")
    op.drop_index("ix_audit_log_entity", table_name="audit_log_old")
    op.drop_index("ix_audit_log_org_id_occurred_at", table_name="audit_log_old")
    create_audit_log(partitioned=False)
    copy_audit_rows()  # партиции удаляются вместе с родителем

    op.drop_index("ix_notes_entity", table_name="notes")
    op.drop_table("notes")
    op.drop_table("activities")  # вместе с партициями и индексами
    op.execute("DROP FUNCTION ensure_monthly_partitions(regclass, date, date)")
