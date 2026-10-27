"""Общие фикстуры.

Unit-тесты не ходят ни в Postgres, ни в Redis: адреса ведут на закрытый порт.
Интеграционные тесты подменяют фикстуру settings на адреса настоящих контейнеров
(tests/integration/conftest.py).
"""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.core.config import Settings
from app.core.jwt import generate_private_key, private_key_to_pem
from app.main import create_app

CLOSED_PORT = 9  # на этом порту никто не слушает — соединение сразу отклоняется
TESTS_DIR = Path(__file__).resolve().parent
POLICIES_DIR = TESTS_DIR.parents[1] / "policies"
LEVELS = ("unit", "integration")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Уровень теста — его каталог (раздел 10): tests/unit/… → unit, tests/integration/… →
    # integration. Руками маркер не ставится, поэтому -m "integration and not worker" надёжен.
    for item in items:
        level = item.path.relative_to(TESTS_DIR).parts[0]
        if level in LEVELS:
            item.add_marker(level)


@pytest.fixture(scope="session")
def jwt_key_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    # Один ключ на весь запуск: RSA-ключ генерируется заметное время.
    path = tmp_path_factory.mktemp("keys") / "jwt_private.pem"
    path.write_bytes(private_key_to_pem(generate_private_key()))
    return path


@pytest.fixture
def settings(jwt_key_file: Path) -> Settings:
    return Settings(
        _env_file=None,  # не читать локальный .env разработчика
        app_env="test",
        database_url=f"postgresql+asyncpg://test:test@127.0.0.1:{CLOSED_PORT}/test",
        redis_url=f"redis://127.0.0.1:{CLOSED_PORT}/0",
        opa_url=f"http://127.0.0.1:{CLOSED_PORT}",
        opa_startup_attempts=1,  # OPA в unit-тестах нет — не ждём его при старте
        policies_dir=POLICIES_DIR,
        jwt_private_key_file=jwt_key_file,
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
