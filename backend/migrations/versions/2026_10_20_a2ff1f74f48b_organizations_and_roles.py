"""organizations and roles — организации, отделы, роль пользователя (раздел 5).

Пользователи этапа 2 без организации (не супер-админы) и их неиспользованные приглашения
удаляются: это только dev-данные, а приглашению с этапа 3 нужны организация и роль.
downgrade их не возвращает.

Revision ID: a2ff1f74f48b
Revises: 4f5134da8453
Create Date: 2026-10-20 18:09:49.282131
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a2ff1f74f48b"
down_revision: str | Sequence[str] | None = "4f5134da8453"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Удаляем до новых колонок: потом у всех таких пользователей был бы org_id IS NULL и так.
    op.execute("DELETE FROM users WHERE NOT is_superadmin")  # refresh_tokens — каскадом
    op.execute("DELETE FROM email_tokens WHERE purpose = 'invite' AND used_at IS NULL")

    op.create_table(
        "organizations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("plan", sa.String(length=32), server_default=sa.text("'free'"), nullable=False),
        sa.Column(
            "display_currency", sa.String(length=3), server_default=sa.text("'UAH'"), nullable=False
        ),
        sa.Column(
            "timezone",
            sa.String(length=64),
            server_default=sa.text("'Europe/Kyiv'"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_organizations")),
        sa.UniqueConstraint("slug", name=op.f("uq_organizations_slug")),
    )
    op.create_table(
        "teams",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["org_id"], ["organizations.id"], name=op.f("fk_teams_org_id_organizations")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_teams")),
        sa.UniqueConstraint("id", "org_id", name=op.f("uq_teams_id_org_id")),
        sa.UniqueConstraint("org_id", "name", name=op.f("uq_teams_org_id_name")),
    )
    op.add_column("users", sa.Column("org_id", sa.Uuid(), nullable=True))
    op.add_column("users", sa.Column("team_id", sa.Uuid(), nullable=True))
    op.add_column("users", sa.Column("role", sa.String(length=32), nullable=True))
    op.create_index(op.f("ix_users_org_id"), "users", ["org_id"], unique=False)
    op.create_index(op.f("ix_users_team_id"), "users", ["team_id"], unique=False)
    op.create_foreign_key(
        op.f("fk_users_team_id_teams"), "users", "teams", ["team_id", "org_id"], ["id", "org_id"]
    )
    op.create_foreign_key(
        op.f("fk_users_org_id_organizations"), "users", "organizations", ["org_id"], ["id"]
    )
    # CHECK-и на существующей таблице autogenerate не видит — добавлены руками.
    op.create_check_constraint(
        op.f("ck_users_role_valid"),
        "users",
        "role IN ('org_admin', 'team_lead', 'sales_rep', 'viewer')",
    )
    op.create_check_constraint(
        op.f("ck_users_role_with_org"), "users", "(role IS NULL) = (org_id IS NULL)"
    )
    op.create_check_constraint(
        op.f("ck_users_superadmin_without_org"),
        "users",
        "NOT (is_superadmin AND org_id IS NOT NULL)",
    )
    op.create_check_constraint(
        op.f("ck_users_team_for_role"),
        "users",
        "(team_id IS NOT NULL) = (role IS NOT NULL AND role IN ('team_lead', 'sales_rep'))",
    )


def downgrade() -> None:
    for name in ("team_for_role", "superadmin_without_org", "role_with_org", "role_valid"):
        op.drop_constraint(op.f(f"ck_users_{name}"), "users", type_="check")
    op.drop_constraint(op.f("fk_users_org_id_organizations"), "users", type_="foreignkey")
    op.drop_constraint(op.f("fk_users_team_id_teams"), "users", type_="foreignkey")
    op.drop_index(op.f("ix_users_team_id"), table_name="users")
    op.drop_index(op.f("ix_users_org_id"), table_name="users")
    op.drop_column("users", "role")
    op.drop_column("users", "team_id")
    op.drop_column("users", "org_id")
    op.drop_table("teams")
    op.drop_table("organizations")
