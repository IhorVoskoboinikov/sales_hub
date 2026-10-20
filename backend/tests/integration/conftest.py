"""Настоящие Postgres и Redis в Docker (testcontainers): одни контейнеры на весь запуск.

Изоляция (раздел 10): каждый тест работает во внешней транзакции, которая откатывается после
него. Коммиты сервисов внутри теста становятся SAVEPOINT-ами — таблицы чистить не нужно.
"""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from app.api.deps import get_session
from app.core.config import Settings
from app.core.security import hash_password
from app.models import User
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


PASSWORD = "correct horse battery"  # пароль всех пользователей из make_user

MakeUser = Callable[..., Awaitable[User]]


@pytest.fixture
def make_user(uow: UnitOfWork) -> MakeUser:
    """Создать пользователя с подтверждённым email и паролем PASSWORD."""

    async def make(email: str = "ivan@example.com", **fields: object) -> User:
        fields.setdefault("email_verified_at", datetime.now(UTC))
        user = User(email=email, password_hash=await hash_password(PASSWORD), **fields)
        uow.users.add(user)
        await uow.commit()
        return user

    return make


async def login(client: AsyncClient, email: str, password: str = PASSWORD) -> dict[str, Any]:
    resp = await client.post("/api/v1/auth/login", data={"username": email, "password": password})
    assert resp.status_code == 200, resp.text
    return cast(dict[str, Any], resp.json())


def bearer(tokens: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}
