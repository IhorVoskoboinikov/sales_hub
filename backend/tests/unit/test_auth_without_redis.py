"""Без Redis вход закрыт: без счётчика попыток пароли можно было бы перебирать бесконечно."""

from httpx import AsyncClient


async def test_login_is_503_when_redis_is_down(client: AsyncClient) -> None:
    resp = await client.post(
        "/api/v1/auth/login", data={"username": "ivan@example.com", "password": "whatever-1"}
    )

    assert resp.status_code == 503
    assert resp.json()["details"] == {"failed": ["redis"]}
