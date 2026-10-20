"""companies and contacts — компании и контакты с корзиной и версией (раздел 3, этап 4.1).

Индексы создаются обычным CREATE INDEX: таблицы новые и пустые. Индекс, который добавляется
к уже заполненной большой таблице, — только CONCURRENTLY (раздел 9).

Revision ID: 12be1d080637
Revises: 41ef21515e01
Create Date: 2026-10-20 23:02:32.561102
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "12be1d080637"
down_revision: str | Sequence[str] | None = "41ef21515e01"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # pg_trgm — поиск по подстроке (ILIKE '%ivan%') через GIN-индексы на триграммах.
    # pg_stat_statements — статистика запросов; сами данные копятся, только если расширение
    # загружено в shared_preload_libraries (compose), без этого CREATE EXTENSION тоже работает.
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    op.create_table(
        "companies",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("team_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("domain", sa.String(length=253), nullable=True),
        sa.Column("industry", sa.String(length=100), nullable=True),
        sa.Column("size", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_companies_org_id_organizations")
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["users.id"], name=op.f("fk_companies_owner_id_users")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_companies")),
    )
    op.create_index(
        "ix_companies_name_trgm",
        "companies",
        ["name"],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_companies_org_id_name",
        "companies",
        ["org_id", "name", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_companies_owner_id",
        "companies",
        ["owner_id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_companies_team_id",
        "companies",
        ["team_id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_table(
        "contacts",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("team_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("company_id", sa.BigInteger(), nullable=True),
        sa.Column("first_name", sa.String(length=100), nullable=False),
        sa.Column("last_name", sa.String(length=100), server_default=sa.text("''"), nullable=False),
        sa.Column("email", postgresql.CITEXT(), nullable=True),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("status", sa.String(length=16), server_default=sa.text("'lead'"), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=True),
        sa.Column(
            "tags",
            postgresql.ARRAY(sa.String(length=64)),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.CheckConstraint(
            "status IN ('lead', 'customer', 'churned')", name=op.f("ck_contacts_status_valid")
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name=op.f("fk_contacts_company_id_companies"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_contacts_org_id_organizations")
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"], ["users.id"], name=op.f("fk_contacts_owner_id_users")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_contacts")),
    )
    op.create_index(
        "ix_contacts_company_id", "contacts", ["company_id", "created_at", "id"], unique=False
    )
    op.create_index(
        "ix_contacts_org_id_created_at",
        "contacts",
        ["org_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_contacts_owner_id_created_at",
        "contacts",
        ["owner_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_contacts_search_trgm",
        "contacts",
        [
            sa.literal_column(
                "lower((((first_name::text || ' '::text) || last_name::text) || ' '::text)"
                " || COALESCE(email::text, ''::text))"
            ).label("search")
        ],
        unique=False,
        postgresql_using="gin",
        postgresql_ops={"search": "gin_trgm_ops"},
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_contacts_tags",
        "contacts",
        ["tags"],
        unique=False,
        postgresql_using="gin",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_contacts_team_id_created_at",
        "contacts",
        ["team_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_contacts_trash",
        "contacts",
        ["org_id", "deleted_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NOT NULL"),
    )
    op.create_index(
        "uq_contacts_org_id_email",
        "contacts",
        ["org_id", "email"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_contacts_org_id_email",
        table_name="contacts",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_contacts_trash",
        table_name="contacts",
        postgresql_where=sa.text("deleted_at IS NOT NULL"),
    )
    op.drop_index(
        "ix_contacts_team_id_created_at",
        table_name="contacts",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_contacts_tags",
        table_name="contacts",
        postgresql_using="gin",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_contacts_search_trgm",
        table_name="contacts",
        postgresql_using="gin",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_contacts_owner_id_created_at",
        table_name="contacts",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_contacts_org_id_created_at",
        table_name="contacts",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index("ix_contacts_company_id", table_name="contacts")
    op.drop_table("contacts")
    op.drop_index(
        "ix_companies_team_id",
        table_name="companies",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_companies_owner_id",
        table_name="companies",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_companies_org_id_name",
        table_name="companies",
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_index(
        "ix_companies_name_trgm",
        table_name="companies",
        postgresql_using="gin",
        postgresql_ops={"name": "gin_trgm_ops"},
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.drop_table("companies")
    # Расширения не удаляем: pg_stat_statements мог включить администратор базы, а pg_trgm
    # безвреден.
