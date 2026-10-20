"""Ротация refresh-токенов и защита от повторного использования (раздел 4)."""

from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient, Response
from sqlalchemy import select, update

from app.models import RefreshToken
from app.uow import UnitOfWork
from tests.integration.conftest import MakeUser, bearer, login


async def refresh(client: AsyncClient, token: str) -> Response:
    return await client.post("/api/v1/auth/refresh", json={"refresh_token": token})


async def test_refresh_rotates_pair(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user()
    first = await login(client, user.email)

    resp = await refresh(client, first["refresh_token"])

    assert resp.status_code == 200
    second: dict[str, Any] = resp.json()
    assert second["refresh_token"] != first["refresh_token"]
    assert (await client.get("/api/v1/auth/me", headers=bearer(second))).status_code == 200


async def test_rotation_keeps_session_and_absolute_expiry(
    client: AsyncClient, make_user: MakeUser, uow: UnitOfWork
) -> None:
    user = await make_user()
    first = await login(client, user.email)
    await refresh(client, first["refresh_token"])

    rows = (
        await uow.session.scalars(
            select(RefreshToken)
            .where(RefreshToken.user_id == user.id)
            .order_by(RefreshToken.created_at)
        )
    ).all()

    old, new = rows
    assert new.family_id == old.family_id  # та же сессия
    assert new.expires_at == old.expires_at  # ротация не продлевает сессию
    assert old.revoked_at is not None
    assert old.replaced_by == new.id
    assert new.revoked_at is None


async def test_reused_refresh_revokes_whole_session(
    client: AsyncClient, make_user: MakeUser
) -> None:
    user = await make_user()
    stolen = await login(client, user.email)
    owner = (await refresh(client, stolen["refresh_token"])).json()

    reuse = await refresh(client, stolen["refresh_token"])

    assert reuse.status_code == 401
    # Сессия закрыта целиком: свежий refresh хозяина тоже больше не работает.
    assert (await refresh(client, owner["refresh_token"])).status_code == 401


async def test_reuse_does_not_touch_other_sessions(
    client: AsyncClient, make_user: MakeUser
) -> None:
    user = await make_user()
    laptop = await login(client, user.email)
    phone = await login(client, user.email)
    await refresh(client, laptop["refresh_token"])

    await refresh(client, laptop["refresh_token"])  # повтор — закрывает только ноутбук

    assert (await refresh(client, phone["refresh_token"])).status_code == 200


async def test_expired_refresh_is_rejected(
    client: AsyncClient, make_user: MakeUser, uow: UnitOfWork
) -> None:
    user = await make_user()
    tokens = await login(client, user.email)
    await uow.session.execute(
        update(RefreshToken)
        .where(RefreshToken.user_id == user.id)
        .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )

    resp = await refresh(client, tokens["refresh_token"])

    assert resp.status_code == 401
    assert resp.json()["code"] == "auth.invalid_token"


async def test_unknown_refresh_is_rejected(client: AsyncClient) -> None:
    assert (await refresh(client, "no-such-token")).status_code == 401


async def test_blocked_user_cannot_refresh(
    client: AsyncClient, make_user: MakeUser, uow: UnitOfWork
) -> None:
    user = await make_user()
    tokens = await login(client, user.email)
    user.is_active = False
    await uow.commit()

    resp = await refresh(client, tokens["refresh_token"])

    assert resp.status_code == 403
    assert resp.json()["code"] == "auth.user_inactive"
