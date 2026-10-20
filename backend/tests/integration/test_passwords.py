"""Пароль: забыл → письмо → сброс; смена пароля залогиненным (раздел 4)."""

import uuid
from typing import Any

from httpx import AsyncClient, Response

from app.uow import UnitOfWork
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
    email = unique_email()  # в переменной: ответы 401 откатывают сессию теста
    await make_user(email)
    laptop = await login(client, email)
    phone = await login(client, email)

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
    # Пароль действительно сменился.
    old_login = await client.post(
        "/api/v1/auth/login", data={"username": email, "password": PASSWORD}
    )
    assert old_login.status_code == 401
    await login(client, email, NEW_PASSWORD)


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


async def test_change_password_is_rate_limited(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user(unique_email())
    tokens = await login(client, user.email)

    statuses = [
        (
            await client.post(
                "/api/v1/auth/password/change",
                json={"old_password": "wrong password", "new_password": NEW_PASSWORD},
                headers=bearer(tokens),
            )
        ).status_code
        for _ in range(6)
    ]

    assert statuses == [422] * 5 + [429]


async def test_forgot_for_blocked_user_sends_nothing(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    email = unique_email()
    await make_user(email, is_active=False)

    assert (await forgot(client, email)).status_code == 202
    assert await read_emails(mailpit, email) == []


async def test_reset_is_refused_if_user_was_blocked_after_forgot(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit, uow: UnitOfWork
) -> None:
    email = unique_email()
    user = await make_user(email)
    await forgot(client, email)
    user.is_active = False
    await uow.commit()

    resp = await reset(client, token_from_email((await read_emails(mailpit, email))[0]))

    assert resp.status_code == 403
    assert resp.json()["code"] == "auth.user_inactive"


async def test_weak_reset_password_does_not_burn_token(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    email = unique_email()
    await make_user(email)
    await forgot(client, email)
    token = token_from_email((await read_emails(mailpit, email))[0])

    weak = await reset(client, token, email)  # пароль = email

    assert weak.status_code == 422
    assert weak.json()["code"] == "password.same_as_email"
    assert (await reset(client, token)).status_code == 204
