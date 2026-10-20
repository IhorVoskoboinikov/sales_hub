from httpx import AsyncClient


async def test_live_answers_without_dependencies(client: AsyncClient) -> None:
    resp = await client.get("/health/live")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_ready_is_503_when_postgres_and_redis_are_down(client: AsyncClient) -> None:
    resp = await client.get("/health/ready")

    assert resp.status_code == 503
    assert resp.headers["content-type"] == "application/problem+json"
    body = resp.json()
    assert body["code"] == "unavailable"
    assert body["details"] == {"failed": ["postgres", "redis"]}
