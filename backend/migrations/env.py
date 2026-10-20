"""Запуск миграций Alembic в async-режиме.

Адрес базы — из той же Settings, что у приложения. Тесты передают свой адрес через
config.attributes["database_url"], чтобы не трогать переменные окружения.
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

import app.models  # noqa: F401 — регистрирует все модели в Base.metadata
from app.core.config import get_settings
from app.db.base import Base

config = context.config
if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
database_url: str = config.attributes.get("database_url") or get_settings().database_url


def run_migrations_offline() -> None:
    """alembic upgrade --sql: напечатать SQL, не подключаясь к базе."""
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    # NullPool: миграции — одно соединение на один запуск, пул не нужен.
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
