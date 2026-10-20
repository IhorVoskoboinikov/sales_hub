import pytest
from fastapi import APIRouter, Depends, FastAPI
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session

router = APIRouter(prefix="/test")


@router.get("/db")
async def query_db(session: AsyncSession = Depends(get_session)) -> dict[str, str]:
    version = await session.scalar(text("SHOW server_version"))
    return {"server_version": str(version)}


@pytest.fixture
def app(app: FastAPI) -> FastAPI:
    app.include_router(router)
    return app


async def test_ready_when_postgres_and_redis_answer(client: AsyncClient) -> None:
    resp = await client.get("/health/ready")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_session_per_request_reaches_postgres_18(client: AsyncClient) -> None:
    resp = await client.get("/test/db")

    assert resp.status_code == 200
    assert resp.json()["server_version"].startswith("18.")
