"""Выход и сессии: logout, logout-all, список сессий, выход на конкретном устройстве."""

from typing import Any

from httpx import AsyncClient

from tests.integration.conftest import MakeUser, bearer, login


async def refresh_status(client: AsyncClient, tokens: dict[str, Any]) -> int:
    resp = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
    )
    return resp.status_code


async def test_logout_closes_only_current_session(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user()
    laptop = await login(client, user.email)
    phone = await login(client, user.email)

    resp = await client.post("/api/v1/auth/logout", headers=bearer(laptop))

    assert resp.status_code == 204
    assert await refresh_status(client, laptop) == 401
    assert await refresh_status(client, phone) == 200


async def test_logout_all_kills_refresh_and_live_access_tokens(
    client: AsyncClient, make_user: MakeUser
) -> None:
    user = await make_user()
    laptop = await login(client, user.email)
    phone = await login(client, user.email)
    assert (await client.get("/api/v1/auth/me", headers=bearer(phone))).status_code == 200

    resp = await client.post("/api/v1/auth/logout-all", headers=bearer(laptop))

    assert resp.status_code == 204
    assert await refresh_status(client, laptop) == 401
    assert await refresh_status(client, phone) == 401
    # access-токен ещё не истёк, но token_version вырос — и кэш пользователя сброшен
    assert (await client.get("/api/v1/auth/me", headers=bearer(phone))).status_code == 401
    # заново войти можно
    await login(client, user.email)


async def test_sessions_list_marks_current(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user()
    laptop = await login(client, user.email)
    await login(client, user.email)
    await refresh_status(client, laptop)  # ротация не плодит сессии

    resp = await client.get("/api/v1/auth/sessions", headers=bearer(laptop))

    assert resp.status_code == 200
    sessions = resp.json()
    assert len(sessions) == 2
    assert sum(s["current"] for s in sessions) == 1
    current = next(s for s in sessions if s["current"])
    assert current["ip"] == "127.0.0.1"
    assert current["started_at"] <= current["last_refresh_at"]


async def test_revoke_other_session(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user()
    laptop = await login(client, user.email)
    phone = await login(client, user.email)
    sessions = (await client.get("/api/v1/auth/sessions", headers=bearer(laptop))).json()
    phone_sid = next(s["sid"] for s in sessions if not s["current"])

    resp = await client.delete(f"/api/v1/auth/sessions/{phone_sid}", headers=bearer(laptop))

    assert resp.status_code == 204
    assert await refresh_status(client, phone) == 401
    assert await refresh_status(client, laptop) == 200
    again = await client.delete(f"/api/v1/auth/sessions/{phone_sid}", headers=bearer(laptop))
    assert again.status_code == 404


async def test_cannot_revoke_someone_elses_session(
    client: AsyncClient, make_user: MakeUser
) -> None:
    ivan = await login(client, (await make_user("ivan@example.com")).email)
    olena = await login(client, (await make_user("olena@example.com")).email)
    olena_sid = (await client.get("/api/v1/auth/sessions", headers=bearer(olena))).json()[0]["sid"]

    resp = await client.delete(f"/api/v1/auth/sessions/{olena_sid}", headers=bearer(ivan))

    assert resp.status_code == 404
    assert await refresh_status(client, olena) == 200
