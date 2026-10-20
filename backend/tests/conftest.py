"""Общие фикстуры.

Unit-тесты не ходят ни в Postgres, ни в Redis: адреса ведут на закрытый порт.
Интеграционные тесты подменяют фикстуру settings на адреса настоящих контейнеров
(tests/integration/conftest.py).
"""

from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from app.main import create_app

CLOSED_PORT = 9  # на этом порту никто не слушает — соединение сразу отклоняется


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,  # не читать локальный .env разработчика
        app_env="test",
        database_url=f"postgresql+asyncpg://test:test@127.0.0.1:{CLOSED_PORT}/test",
        redis_url=f"redis://127.0.0.1:{CLOSED_PORT}/0",
    )


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    # ASGITransport не запускает lifespan сам — запускаем вручную, как это делает uvicorn.
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
