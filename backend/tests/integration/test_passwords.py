"""Пароль: забыл → письмо → сброс; смена пароля залогиненным (раздел 4)."""

import uuid
from typing import Any

from httpx import AsyncClient, Response

from tests.integration.conftest import (
    PASSWORD,
    Mailpit,
    MakeUser,
    bearer,
    login,
    read_emails,
    token_from_email,
)

NEW_PASSWORD = "brand new password"


def unique_email() -> str:
    return f"reset-{uuid.uuid4().hex[:8]}@example.com"


async def forgot(client: AsyncClient, email: str) -> Response:
    return await client.post("/api/v1/auth/password/forgot", json={"email": email})


async def reset(client: AsyncClient, token: str, password: str = NEW_PASSWORD) -> Response:
    return await client.post(
        "/api/v1/auth/password/reset", json={"token": token, "new_password": password}
    )


async def refresh_status(client: AsyncClient, tokens: dict[str, Any]) -> int:
    resp = await client.post(
        "/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
    )
    return resp.status_code


async def test_forgot_and_reset_kills_old_tokens(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    # email — в переменной: ответ 401 откатывает сессию теста, и объект user «протухает».
    email = unique_email()
    await make_user(email)
    old = await login(client, email)

    assert (await forgot(client, email)).status_code == 202
    [letter] = await read_emails(mailpit, email)
    assert letter["Subject"] == "Зміна пароля в SalesHub"
    resp = await reset(client, token_from_email(letter))

    assert resp.status_code == 204
    assert (await client.get("/api/v1/auth/me", headers=bearer(old))).status_code == 401
    assert await refresh_status(client, old) == 401
    failed = await client.post("/api/v1/auth/login", data={"username": email, "password": PASSWORD})
    assert failed.status_code == 401
    await login(client, email, NEW_PASSWORD)


async def test_forgot_does_not_reveal_unknown_email(client: AsyncClient, mailpit: Mailpit) -> None:
    email = unique_email()

    resp = await forgot(client, email)

    assert resp.status_code == 202
    assert await read_emails(mailpit, email) == []


async def test_reset_token_works_once(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    user = await make_user(unique_email())
    await forgot(client, user.email)
    token = token_from_email((await read_emails(mailpit, user.email))[0])

    assert (await reset(client, token)).status_code == 204
    again = await reset(client, token, "another password 2")

    assert again.status_code == 422
    assert again.json()["code"] == "email_token.invalid"


async def test_invite_token_cannot_reset_password(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    admin = await make_user(unique_email(), is_superadmin=True)
    admin_tokens = await login(client, admin.email)
    invited = unique_email()
    await client.post(
        "/api/v1/admin/invites", json={"email": invited}, headers=bearer(admin_tokens)
    )
    invite_token = token_from_email((await read_emails(mailpit, invited))[0])

    resp = await reset(client, invite_token)

    assert resp.status_code == 422
    assert resp.json()["code"] == "email_token.invalid"


async def test_change_password_keeps_current_session_only(
    client: AsyncClient, make_user: MakeUser
) -> None:
    user = await make_user()
    laptop = await login(client, user.email)
    phone = await login(client, user.email)

    resp = await client.post(
        "/api/v1/auth/password/change",
        json={"old_password": PASSWORD, "new_password": NEW_PASSWORD},
        headers=bearer(laptop),
    )

    assert resp.status_code == 200
    fresh = resp.json()
    # Телефон вылетел сразу: и refresh, и ещё живой access.
    assert await refresh_status(client, phone) == 401
    assert (await client.get("/api/v1/auth/me", headers=bearer(phone))).status_code == 401
    # Старые токены ноутбука тоже не работают, новые — работают.
    assert (await client.get("/api/v1/auth/me", headers=bearer(laptop))).status_code == 401
    assert (await client.get("/api/v1/auth/me", headers=bearer(fresh))).status_code == 200
    sessions = (await client.get("/api/v1/auth/sessions", headers=bearer(fresh))).json()
    assert [s["current"] for s in sessions] == [True]
    assert await refresh_status(client, fresh) == 200


async def test_change_password_requires_old_password(
    client: AsyncClient, make_user: MakeUser
) -> None:
    user = await make_user()
    tokens = await login(client, user.email)

    resp = await client.post(
        "/api/v1/auth/password/change",
        json={"old_password": "wrong password", "new_password": NEW_PASSWORD},
        headers=bearer(tokens),
    )

    assert resp.status_code == 422
    assert resp.json()["code"] == "auth.wrong_password"
    # Ничего не поменялось: токены живы.
    assert (await client.get("/api/v1/auth/me", headers=bearer(tokens))).status_code == 200
