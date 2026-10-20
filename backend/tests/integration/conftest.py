"""Настоящие Postgres и Redis в Docker (testcontainers): одни контейнеры на весь запуск.

Изоляция (раздел 10): каждый тест работает во внешней транзакции, которая откатывается после
него. Коммиты сервисов внутри теста становятся SAVEPOINT-ами — таблицы чистить не нужно.
"""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from app.api.deps import get_session
from app.core.config import Settings
from app.uow import UnitOfWork

BACKEND_DIR = Path(__file__).resolve().parents[2]

# Те же версии, что в deploy/compose/docker-compose.yml.
POSTGRES_IMAGE = "postgres:18.6"
REDIS_IMAGE = "redis:8.10"


def alembic_config(database_url: str) -> Config:
    config = Config(str(BACKEND_DIR / "alembic.ini"))
    config.attributes["database_url"] = database_url
    config.attributes["configure_logger"] = False  # не сбрасывать настройки логов pytest
    return config


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as postgres:
        url = postgres.get_connection_url()
        command.upgrade(alembic_config(url), "head")
        yield url


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer(REDIS_IMAGE) as redis:
        yield f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"


@pytest.fixture
def settings(settings: Settings, postgres_url: str, redis_url: str) -> Settings:
    # Те же настройки, что у unit-тестов, но с адресами настоящих контейнеров.
    return settings.model_copy(update={"database_url": postgres_url, "redis_url": redis_url})


@pytest.fixture
async def db_session(postgres_url: str) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    async with engine.connect() as connection:
        transaction = await connection.begin()
        session = AsyncSession(
            bind=connection,
            join_transaction_mode="create_savepoint",  # commit() сервиса → RELEASE SAVEPOINT
            expire_on_commit=False,
        )
        try:
            yield session
        finally:
            await session.close()
            await transaction.rollback()
    await engine.dispose()


@pytest.fixture
def uow(db_session: AsyncSession) -> UnitOfWork:
    return UnitOfWork(db_session)


@pytest.fixture
def app(app: FastAPI, db_session: AsyncSession) -> FastAPI:
    # Запросы API идут в ту же транзакцию теста: после теста всё откатится.
    async def test_session() -> AsyncIterator[AsyncSession]:
        yield db_session

    app.dependency_overrides[get_session] = test_session
    return app
