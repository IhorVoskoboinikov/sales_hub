"""Rate limit на Redis: логин, password/forgot, приглашения (раздел 4)."""

import uuid

import pytest
from httpx import AsyncClient

from app.repositories import rate_limiter
from app.repositories.rate_limiter import Limit
from tests.integration.conftest import PASSWORD, MakeUser, bearer, login


def unique_email() -> str:
    # Redis общий на весь запуск: у каждого теста свои счётчики.
    return f"rl-{uuid.uuid4().hex[:8]}@example.com"


async def test_login_is_blocked_after_five_failures(
    client: AsyncClient, make_user: MakeUser
) -> None:
    email = unique_email()
    await make_user(email)

    for _ in range(5):
        resp = await client.post(
            "/api/v1/auth/login", data={"username": email, "password": "wrong password"}
        )
        assert resp.status_code == 401

    # Шестая попытка — даже с верным паролем — отклоняется до проверки пароля.
    blocked = await client.post(
        "/api/v1/auth/login", data={"username": email, "password": PASSWORD}
    )
    assert blocked.status_code == 429
    assert blocked.json()["code"] == "rate_limited"
    retry_after = int(blocked.headers["retry-after"])
    assert 0 < retry_after <= 15 * 60
    assert blocked.json()["details"]["retry_after"] == retry_after


async def test_successful_logins_are_not_counted(client: AsyncClient, make_user: MakeUser) -> None:
    email = unique_email()
    await make_user(email)

    for _ in range(6):
        await login(client, email)


async def test_login_limit_is_per_email(client: AsyncClient, make_user: MakeUser) -> None:
    victim, other = unique_email(), unique_email()
    await make_user(other)
    for _ in range(5):
        await client.post("/api/v1/auth/login", data={"username": victim, "password": "x" * 10})

    await login(client, other)


async def test_forgot_is_limited_even_for_unknown_email(client: AsyncClient) -> None:
    email = unique_email()  # такого пользователя нет — ответы всё равно одинаковые
    statuses = [
        (await client.post("/api/v1/auth/password/forgot", json={"email": email})).status_code
        for _ in range(4)
    ]

    assert statuses == [202, 202, 202, 429]


async def test_invites_are_limited_per_inviter(
    client: AsyncClient, make_user: MakeUser, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(rate_limiter, "INVITE", Limit("invite", 2, 3600))
    admin = await make_user(unique_email(), is_superadmin=True)
    tokens = await login(client, admin.email)

    statuses = [
        (
            await client.post(
                "/api/v1/admin/invites", json={"email": unique_email()}, headers=bearer(tokens)
            )
        ).status_code
        for _ in range(3)
    ]

    assert statuses == [202, 202, 429]
