import re

import pytest
import structlog
from fastapi import APIRouter, FastAPI
from httpx import AsyncClient
from structlog.testing import capture_logs

from app.core.config import Settings
from app.main import create_app


async def test_request_id_is_generated(client: AsyncClient) -> None:
    resp = await client.get("/health/live")

    assert re.fullmatch(r"[0-9a-f]{32}", resp.headers["x-request-id"])


async def test_incoming_request_id_is_kept(client: AsyncClient) -> None:
    resp = await client.get("/health/live", headers={"X-Request-ID": "abc-123"})

    assert resp.headers["x-request-id"] == "abc-123"


@pytest.mark.parametrize("bad", ["a b", "x" * 65, "id%0Ainjected"])
async def test_unsafe_request_id_is_replaced(client: AsyncClient, bad: str) -> None:
    resp = await client.get("/health/live", headers={"X-Request-ID": bad})

    assert re.fullmatch(r"[0-9a-f]{32}", resp.headers["x-request-id"])


# Роутер по правилу из app/api/v1/__init__.py: полный префикс, подключается прямо в app.
router = APIRouter(prefix="/api/v1/test")


@router.get("/items/{item_id}")
async def get_item(item_id: int) -> dict[str, int]:
    return {"item_id": item_id}


@router.get("/as-user")
async def as_user() -> dict[str, str]:
    # Так делает current_user: привязывает user_id ко всем строкам лога запроса.
    structlog.contextvars.bind_contextvars(user_id="u-1")
    return {"ok": "yes"}


@pytest.fixture
def app(app: FastAPI) -> FastAPI:
    app.include_router(router)
    return app


async def test_access_log_uses_full_path_template(client: AsyncClient) -> None:
    with capture_logs() as logs:
        await client.get("/api/v1/test/items/42?token=secret")

    entry = next(e for e in logs if e["event"] == "request")
    assert entry["method"] == "GET"
    assert entry["path"] == "/api/v1/test/items/{item_id}"  # шаблон: без id и без query
    assert entry["status"] == 200
    assert entry["log_level"] == "info"


async def test_access_log_writes_health_at_debug(client: AsyncClient) -> None:
    with capture_logs() as logs:
        await client.get("/health/live")

    entry = next(e for e in logs if e["event"] == "request")
    assert entry["log_level"] == "debug"  # пробы не засоряют лог


async def test_access_log_hides_unmatched_url(client: AsyncClient) -> None:
    with capture_logs() as logs:
        await client.get("/wp-admin/users/42")

    entry = next(e for e in logs if e["event"] == "request")
    assert entry["path"] == "<unmatched>"
    assert entry["status"] == 404


def test_docs_are_hidden_in_prod() -> None:
    prod = create_app(Settings(_env_file=None, app_env="prod"))
    paths = {getattr(route, "path", None) for route in prod.routes}

    assert "/docs" not in paths
    assert "/openapi.json" not in paths


def test_docs_flag_overrides_profile() -> None:
    assert Settings(_env_file=None, app_env="prod", docs_enabled=True).show_docs
    assert not Settings(_env_file=None, app_env="dev", docs_enabled=False).show_docs


async def test_user_id_does_not_leak_into_next_request(client: AsyncClient) -> None:
    await client.get("/api/v1/test/as-user")

    with capture_logs() as logs:
        await client.get("/api/v1/test/items/1")

    entry = next(e for e in logs if e["event"] == "request")
    assert structlog.contextvars.get_contextvars().get("user_id") is None
    assert entry.get("user_id") is None
