"""Приглашения этапа 2: супер-админ приглашает → письмо → invite/accept → вход."""

import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient, Response
from sqlalchemy import update

from app.models import EmailToken
from app.uow import UnitOfWork
from tests.integration.conftest import (
    Mailpit,
    MakeUser,
    bearer,
    login,
    read_emails,
    token_from_email,
)

NEW_PASSWORD = "brand new password"


def new_email() -> str:
    # Mailpit общий на весь запуск — уникальный адрес, чтобы не видеть письма других тестов.
    return f"new-{uuid.uuid4().hex[:8]}@example.com"


async def invite(client: AsyncClient, admin_tokens: dict[str, str], email: str) -> Response:
    return await client.post(
        "/api/v1/admin/invites", json={"email": email}, headers=bearer(admin_tokens)
    )


async def accept(client: AsyncClient, token: str, password: str = NEW_PASSWORD) -> Response:
    return await client.post(
        "/api/v1/auth/invite/accept", json={"token": token, "password": password}
    )


async def admin_login(client: AsyncClient, make_user: MakeUser) -> dict[str, str]:
    admin = await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    return await login(client, admin.email)


async def test_full_invite_flow(client: AsyncClient, make_user: MakeUser, mailpit: Mailpit) -> None:
    admin = await admin_login(client, make_user)
    email = new_email()

    resp = await invite(client, admin, email)

    assert resp.status_code == 202
    assert resp.json()["email"] == email
    [letter] = await read_emails(mailpit, email)
    assert letter["Subject"] == "Запрошення до SalesHub"

    accepted = await accept(client, token_from_email(letter))
    assert accepted.status_code == 201
    assert accepted.json()["email"] == email
    assert accepted.json()["is_superadmin"] is False
    assert accepted.json()["email_verified_at"] is not None

    tokens = await login(client, email, NEW_PASSWORD)
    me = await client.get("/api/v1/auth/me", headers=bearer(tokens))
    assert me.json()["email"] == email


async def test_only_superadmin_can_invite(client: AsyncClient, make_user: MakeUser) -> None:
    user = await make_user("ivan@example.com")
    tokens = await login(client, user.email)

    resp = await invite(client, tokens, new_email())

    assert resp.status_code == 403
    assert resp.json()["code"] == "forbidden"
    assert (await client.post("/api/v1/admin/invites", json={"email": "a@b.cd"})).status_code == 401


async def test_cannot_invite_existing_email(client: AsyncClient, make_user: MakeUser) -> None:
    admin = await admin_login(client, make_user)
    await make_user("ivan@example.com")

    resp = await invite(client, admin, "IVAN@example.com")

    assert resp.status_code == 409
    assert resp.json()["code"] == "invite.email_taken"


async def test_token_works_once(client: AsyncClient, make_user: MakeUser, mailpit: Mailpit) -> None:
    admin = await admin_login(client, make_user)
    email = new_email()
    await invite(client, admin, email)
    token = token_from_email((await read_emails(mailpit, email))[0])

    assert (await accept(client, token)).status_code == 201
    again = await accept(client, token)

    assert again.status_code == 422
    assert again.json()["code"] == "email_token.invalid"


async def test_new_invite_cancels_previous(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    admin = await admin_login(client, make_user)
    email = new_email()
    await invite(client, admin, email)
    await invite(client, admin, email)
    newest, oldest = await read_emails(mailpit, email)

    assert (await accept(client, token_from_email(oldest))).json()["code"] == "email_token.invalid"
    assert (await accept(client, token_from_email(newest))).status_code == 201


async def test_expired_invite(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit, uow: UnitOfWork
) -> None:
    admin = await admin_login(client, make_user)
    email = new_email()
    await invite(client, admin, email)
    await uow.session.execute(
        update(EmailToken)
        .where(EmailToken.email == email)
        .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )

    resp = await accept(client, token_from_email((await read_emails(mailpit, email))[0]))

    assert resp.status_code == 422
    assert resp.json()["code"] == "email_token.expired"


async def test_accept_checks_password_rules(
    client: AsyncClient, make_user: MakeUser, mailpit: Mailpit
) -> None:
    admin = await admin_login(client, make_user)
    email = new_email()
    await invite(client, admin, email)
    token = token_from_email((await read_emails(mailpit, email))[0])

    short = await accept(client, token, "s3cr-pw")
    same_as_email = await accept(client, token, email)

    assert short.status_code == 422
    assert short.json()["code"] == "validation"
    assert "s3cr-pw" not in short.text  # пароль не возвращается в ответе
    assert same_as_email.json()["code"] == "password.same_as_email"
    # Неудачная попытка не сжигает токен: ошибка откатила транзакцию.
    assert (await accept(client, token)).status_code == 201


async def test_invalid_email_is_rejected(client: AsyncClient, make_user: MakeUser) -> None:
    admin = await admin_login(client, make_user)

    resp = await invite(client, admin, "not-an-email")

    assert resp.status_code == 422
