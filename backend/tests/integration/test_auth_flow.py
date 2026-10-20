"""Весь сценарий раздела 4 одним тестом — то же, что проходится руками в Swagger:

супер-админ → приглашение → письмо → invite/accept → логин → /me → истёк access → refresh →
повтор старого refresh (кража) → отзыв сессии → забыл пароль → письмо → сброс → старые
токены мертвы, вход с новым паролем. Письма читаются через API Mailpit, сроки — freezegun.
"""

import uuid
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import Any

from freezegun import freeze_time
from httpx import AsyncClient

from app.services.users import create_superadmin
from app.uow import UnitOfWork
from tests.integration.conftest import Mailpit, bearer, login, read_emails, token_from_email

ADMIN_PASSWORD = "admin password 1"
USER_PASSWORD = "first password 1"
NEW_PASSWORD = "second password 2"


def later(**delta: float) -> AbstractContextManager[Any]:
    # real_asyncio: «замороженное» время не трогает часы event loop — иначе встали бы таймауты.
    return freeze_time(datetime.now(UTC) + timedelta(**delta), real_asyncio=True)


async def test_whole_auth_scenario(client: AsyncClient, uow: UnitOfWork, mailpit: Mailpit) -> None:
    # 1. Супер-админ из команды create-superadmin → Authorize.
    admin_email = f"root-{uuid.uuid4().hex[:8]}@example.com"
    await create_superadmin(uow, admin_email, ADMIN_PASSWORD)
    admin = await login(client, admin_email, ADMIN_PASSWORD)

    # 2. Приглашение → письмо в Mailpit.
    email = f"olena-{uuid.uuid4().hex[:8]}@example.com"
    invited = await client.post(
        "/api/v1/admin/invites", json={"email": email}, headers=bearer(admin)
    )
    assert invited.status_code == 202
    [invite_letter] = await read_emails(mailpit, email)

    # 3. invite/accept → вход новым пользователем → /me.
    accepted = await client.post(
        "/api/v1/auth/invite/accept",
        json={"token": token_from_email(invite_letter), "password": USER_PASSWORD},
    )
    assert accepted.status_code == 201
    first = await login(client, email, USER_PASSWORD)
    me = await client.get("/api/v1/auth/me", headers=bearer(first))
    assert me.json()["email"] == email

    # 4. Через час access истёк — клиент видит auth.token_expired и делает refresh.
    with later(hours=1, seconds=1):
        expired = await client.get("/api/v1/auth/me", headers=bearer(first))
        assert expired.status_code == 401
        assert expired.json()["code"] == "auth.token_expired"

        refreshed = await client.post(
            "/api/v1/auth/refresh", json={"refresh_token": first["refresh_token"]}
        )
        assert refreshed.status_code == 200
        second = refreshed.json()
        assert (await client.get("/api/v1/auth/me", headers=bearer(second))).status_code == 200

        # 5. Старый refresh пришёл ещё раз — его украли: закрывается вся сессия.
        reused = await client.post(
            "/api/v1/auth/refresh", json={"refresh_token": first["refresh_token"]}
        )
        assert reused.status_code == 401
        killed = await client.post(
            "/api/v1/auth/refresh", json={"refresh_token": second["refresh_token"]}
        )
        assert killed.status_code == 401

    # 6. Забыл пароль → письмо → сброс → старые токены не работают.
    current = await login(client, email, USER_PASSWORD)
    forgot = await client.post("/api/v1/auth/password/forgot", json={"email": email})
    assert forgot.status_code == 202
    reset_letter = next(
        e for e in await read_emails(mailpit, email) if e["Subject"] == "Зміна пароля в SalesHub"
    )
    reset = await client.post(
        "/api/v1/auth/password/reset",
        json={"token": token_from_email(reset_letter), "new_password": NEW_PASSWORD},
    )
    assert reset.status_code == 204
    assert (await client.get("/api/v1/auth/me", headers=bearer(current))).status_code == 401

    # 7. Вход с новым паролем.
    final = await login(client, email, NEW_PASSWORD)
    assert (await client.get("/api/v1/auth/me", headers=bearer(final))).status_code == 200


async def test_session_cannot_outlive_30_days(client: AsyncClient, uow: UnitOfWork) -> None:
    email = f"long-{uuid.uuid4().hex[:8]}@example.com"
    await create_superadmin(uow, email, ADMIN_PASSWORD)
    tokens = await login(client, email, ADMIN_PASSWORD)

    # Обновлялся каждые 29 дней — но сессия всё равно кончается через 30 дней после входа.
    with later(days=29):
        resp = await client.post(
            "/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
        )
        assert resp.status_code == 200
        tokens = resp.json()
    with later(days=30, seconds=1):
        resp = await client.post(
            "/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
        )
        assert resp.status_code == 401


async def test_invite_expires_after_7_days(
    client: AsyncClient, uow: UnitOfWork, mailpit: Mailpit
) -> None:
    admin_email = f"root-{uuid.uuid4().hex[:8]}@example.com"
    await create_superadmin(uow, admin_email, ADMIN_PASSWORD)
    admin = await login(client, admin_email, ADMIN_PASSWORD)
    email = f"late-{uuid.uuid4().hex[:8]}@example.com"
    await client.post("/api/v1/admin/invites", json={"email": email}, headers=bearer(admin))
    token = token_from_email((await read_emails(mailpit, email))[0])

    with later(days=7, seconds=1):
        resp = await client.post(
            "/api/v1/auth/invite/accept", json={"token": token, "password": USER_PASSWORD}
        )

    assert resp.status_code == 422
    assert resp.json()["code"] == "email_token.expired"
