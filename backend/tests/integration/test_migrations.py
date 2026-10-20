"""Проверки миграций (раздел 9): одна голова, модели совпадают с миграциями, есть downgrade."""

from alembic import command
from alembic.script import ScriptDirectory

from tests.integration.conftest import alembic_config


def test_single_head(postgres_url: str) -> None:
    heads = ScriptDirectory.from_config(alembic_config(postgres_url)).get_heads()

    assert len(heads) == 1, f"несколько голов: {heads} — нужна merge-миграция"


def test_models_match_migrations(postgres_url: str) -> None:
    # Падает, если модель изменили, а миграцию не написали.
    command.check(alembic_config(postgres_url))


def test_downgrade_and_upgrade_again(postgres_url: str) -> None:
    config = alembic_config(postgres_url)

    command.downgrade(config, "base")
    command.upgrade(config, "head")
