"""Настоящие Postgres и Redis в Docker (testcontainers): одни контейнеры на весь запуск."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from app.core.config import Settings

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
def settings(postgres_url: str, redis_url: str) -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        database_url=postgres_url,
        redis_url=redis_url,
    )
