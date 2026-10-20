"""Логин, current_user и /auth/me (раздел 4)."""

from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy import select
from structlog.testing import capture_logs

from app.models import RefreshToken
from app.uow import UnitOfWork
from tests.integration.conftest import PASSWORD, MakeUser, bearer, login


async def test_login_returns_token_pair_and_me_works(
    client: AsyncClient, make_user: MakeUser
) -> None:
    user = await make_user("ivan@example.com")

    tokens = await login(client, "ivan@example.com")

    assert tokens["token_type"] == "bearer"
    assert tokens["expires_in"] == 3600
    me = await client.get("/api/v1/auth/me", headers=bearer(tokens))
    assert me.status_code == 200
    assert me.json()["id"] == str(user.id)
    assert me.json()["email"] == "ivan@example.com"
    assert me.json()["last_login_at"] is not None


async def test_email_is_case_insensitive(client: AsyncClient, make_user: MakeUser) -> None:
    await make_user("ivan@example.com")

    await login(client, "  Ivan@Example.COM ")


async def test_login_stores_only_hash_of_refresh_token(
    client: AsyncClient, make_user: MakeUser, uow: UnitOfWork
) -> None:
    user = await make_user()

    tokens = await login(client, user.email)

    row = await uow.session.scalar(select(RefreshToken).where(RefreshToken.user_id == user.id))
    assert row is not None
    assert row.token_hash != tokens["refresh_token"]
    assert row.ip == "127.0.0.1"
    assert row.user_agent is not None and row.user_agent.startswith("python-httpx")


async def test_wrong_password_and_unknown_email_look_the_same(
    client: AsyncClient, make_user: MakeUser
) -> None:
    await make_user("ivan@example.com")

    wrong_password = await client.post(
        "/api/v1/auth/login", data={"username": "ivan@example.com", "password": "nope-nope-1"}
    )
    unknown = await client.post(
        "/api/v1/auth/login", data={"username": "ghost@example.com", "password": PASSWORD}
    )

    for resp in (wrong_password, unknown):
        assert resp.status_code == 401
        assert resp.json()["code"] == "auth.invalid_credentials"
        assert resp.headers["www-authenticate"] == "Bearer"


async def test_inactive_user_gets_reason_only_with_right_password(
    client: AsyncClient, make_user: MakeUser
) -> None:
    await make_user("ivan@example.com", is_active=False)

    resp = await client.post(
        "/api/v1/auth/login", data={"username": "ivan@example.com", "password": PASSWORD}
    )

    assert resp.status_code == 403
    assert resp.json()["code"] == "auth.user_inactive"


async def test_unverified_email_cannot_log_in(client: AsyncClient, make_user: MakeUser) -> None:
    await make_user("ivan@example.com", email_verified_at=None)

    resp = await client.post(
        "/api/v1/auth/login", data={"username": "ivan@example.com", "password": PASSWORD}
    )

    assert resp.status_code == 403
    assert resp.json()["code"] == "auth.email_not_verified"


async def test_me_requires_valid_token(client: AsyncClient) -> None:
    missing = await client.get("/api/v1/auth/me")
    garbage = await client.get("/api/v1/auth/me", headers={"Authorization": "Bearer garbage"})

    for resp in (missing, garbage):
        assert resp.status_code == 401
        assert resp.json()["code"] == "auth.invalid_token"


async def test_old_token_version_is_rejected(
    app: FastAPI, client: AsyncClient, make_user: MakeUser, uow: UnitOfWork
) -> None:
    user = await make_user()
    tokens = await login(client, user.email)
    assert (await client.get("/api/v1/auth/me", headers=bearer(tokens))).status_code == 200

    user.token_version += 1
    await uow.commit()
    await app.state.redis.delete(f"user:{user.id}")  # иначе до 60 с работает кэш

    resp = await client.get("/api/v1/auth/me", headers=bearer(tokens))
    assert resp.status_code == 401


async def test_access_log_has_user_id(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user()
    tokens = await login(client, user.email)

    with capture_logs() as logs:
        await client.get("/api/v1/auth/me", headers=bearer(tokens))

    entry = next(e for e in logs if e["event"] == "request")
    assert entry["path"] == "/api/v1/auth/me"
    assert entry["user_id"] == str(user.id)
