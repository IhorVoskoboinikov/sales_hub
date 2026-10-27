"""Приглашения (разделы 4, 5): супер-админ → первый админ организации → админ приглашает
людей с ролью и отделом → письмо → invite/accept → вход в своей роли."""

import uuid
from datetime import UTC, datetime, timedelta

from httpx import AsyncClient, Response
from sqlalchemy import select, update

from app.models import AuditEntry, EmailToken
from app.uow import UnitOfWork
from tests.integration.conftest import (
    Inbox,
    MakeOrg,
    MakeUser,
    OrgFixture,
    bearer,
    login,
    member,
    token_from_email,
    tokens_for,
)

NEW_PASSWORD = "brand new password"


def new_email() -> str:
    # Mailpit общий на весь запуск — уникальный адрес, чтобы не видеть письма других тестов.
    return f"new-{uuid.uuid4().hex[:8]}@example.com"


async def invite(
    client: AsyncClient,
    admin: dict[str, str],
    email: str,
    role: str = "sales_rep",
    team_id: object = None,
) -> Response:
    return await client.post(
        "/api/v1/invites",
        json={"email": email, "role": role, "team_id": str(team_id) if team_id else None},
        headers=admin,
    )


async def accept(client: AsyncClient, token: str, password: str = NEW_PASSWORD) -> Response:
    return await client.post(
        "/api/v1/auth/invite/accept", json={"token": token, "password": password}
    )


async def org_admin(client: AsyncClient, make_user: MakeUser, org: OrgFixture) -> dict[str, str]:
    return await tokens_for(client, await member(make_user, org, "org_admin"))


async def invite_token(inbox: Inbox, email: str) -> str:
    return token_from_email((await inbox(email))[0])


async def test_superadmin_invites_first_org_admin(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    org = await make_org()
    org_id = org.id
    root = await tokens_for(
        client, await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    )
    email = new_email()

    resp = await client.post(
        f"/api/v1/admin/orgs/{org_id}/invites", json={"email": email}, headers=root
    )

    assert resp.status_code == 202
    [letter] = await inbox(email)
    assert "Адміністратор організації" in letter["Text"]
    accepted = await accept(client, token_from_email(letter))
    assert accepted.status_code == 201
    me = await client.get(
        "/api/v1/auth/me", headers=bearer(await login(client, email, NEW_PASSWORD))
    )
    assert me.json()["role"] == "org_admin"
    assert me.json()["org_id"] == str(org_id)
    assert me.json()["team_id"] is None


async def test_invite_to_unknown_org_is_404(client: AsyncClient, make_user: MakeUser) -> None:
    root = await tokens_for(
        client, await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    )

    resp = await client.post(
        f"/api/v1/admin/orgs/{uuid.uuid4()}/invites", json={"email": new_email()}, headers=root
    )

    assert resp.status_code == 404


async def test_org_admin_invites_sales_rep_into_team(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    org = await make_org()
    team_id = org.team_ids[1]
    admin = await org_admin(client, make_user, org)
    email = new_email()

    resp = await invite(client, admin, email, "sales_rep", team_id)

    assert resp.status_code == 202
    assert (await accept(client, await invite_token(inbox, email))).status_code == 201
    me = await client.get(
        "/api/v1/auth/me", headers=bearer(await login(client, email, NEW_PASSWORD))
    )
    assert (me.json()["role"], me.json()["team_id"]) == ("sales_rep", str(team_id))


async def test_team_rules(client: AsyncClient, make_user: MakeUser, make_org: MakeOrg) -> None:
    org, other = await make_org(), await make_org()
    admin = await org_admin(client, make_user, org)

    no_team = await invite(client, admin, new_email(), "team_lead")
    extra_team = await invite(client, admin, new_email(), "viewer", org.team_ids[0])
    foreign_team = await invite(client, admin, new_email(), "sales_rep", other.team_ids[0])

    assert no_team.json()["code"] == "member.team_required"
    assert extra_team.json()["code"] == "member.team_not_allowed"
    assert foreign_team.json()["code"] == "member.team_not_found"
    for resp in (no_team, extra_team, foreign_team):
        assert resp.status_code == 422


async def test_superadmin_role_cannot_be_invited(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    admin = await org_admin(client, make_user, await make_org())

    resp = await invite(client, admin, new_email(), "super_admin")

    assert resp.status_code == 422


async def test_only_org_admin_invites(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    lead = await tokens_for(client, await member(make_user, org, "team_lead"))

    resp = await invite(client, lead, new_email(), "sales_rep", org.team_ids[0])

    assert resp.status_code == 403
    assert resp.json()["details"] == {"action": "member:manage"}


async def test_taken_email_looks_like_success_and_warns_the_person(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    inbox: Inbox,
    uow: UnitOfWork,
) -> None:
    org = await make_org()
    admin = await org_admin(client, make_user, org)
    taken = new_email()
    await make_user(taken)
    fresh = new_email()

    for_taken = await invite(client, admin, taken, "viewer")
    for_fresh = await invite(client, admin, fresh, "viewer")

    assert for_taken.status_code == for_fresh.status_code == 202
    assert for_taken.json().keys() == for_fresh.json().keys()
    [letter] = await inbox(taken)
    assert "уже є обліковий запис" in letter["Text"]
    assert "Токен" not in letter["Text"]
    # И журнал организации не выдаёт разницы: для обоих — invite.created.
    logged = await uow.session.scalars(
        select(AuditEntry.action).where(AuditEntry.entity_id.in_([taken, fresh]))
    )
    assert sorted(logged) == ["invite.created", "invite.created"]


async def test_token_works_once(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    admin = await org_admin(client, make_user, await make_org())
    email = new_email()
    await invite(client, admin, email, "viewer")
    token = await invite_token(inbox, email)

    assert (await accept(client, token)).status_code == 201
    again = await accept(client, token)

    assert again.status_code == 422
    assert again.json()["code"] == "email_token.invalid"


async def test_new_invite_cancels_previous(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    admin = await org_admin(client, make_user, await make_org())
    email = new_email()
    await invite(client, admin, email, "viewer")
    await invite(client, admin, email, "viewer")
    newest, oldest = await inbox(email)

    assert (await accept(client, token_from_email(oldest))).json()["code"] == "email_token.invalid"
    assert (await accept(client, token_from_email(newest))).status_code == 201


async def test_expired_invite(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    inbox: Inbox,
    uow: UnitOfWork,
) -> None:
    admin = await org_admin(client, make_user, await make_org())
    email = new_email()
    await invite(client, admin, email, "viewer")
    await uow.session.execute(
        update(EmailToken)
        .where(EmailToken.email == email)
        .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )

    resp = await accept(client, await invite_token(inbox, email))

    assert resp.status_code == 422
    assert resp.json()["code"] == "email_token.expired"


async def test_accept_checks_password_rules(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    admin = await org_admin(client, make_user, await make_org())
    email = new_email()
    await invite(client, admin, email, "viewer")
    token = await invite_token(inbox, email)

    short = await accept(client, token, "s3cr-pw")
    same_as_email = await accept(client, token, email)

    assert short.status_code == 422
    assert short.json()["code"] == "validation"
    assert "s3cr-pw" not in short.text  # пароль не возвращается в ответе
    assert same_as_email.json()["code"] == "password.same_as_email"
    # Неудачная попытка не сжигает токен: ошибка откатила транзакцию.
    assert (await accept(client, token)).status_code == 201


async def test_accept_fails_if_email_registered_meanwhile(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    admin = await org_admin(client, make_user, await make_org())
    email = new_email()
    await invite(client, admin, email, "viewer")
    await make_user(email)  # например, приняли другое приглашение

    resp = await accept(client, await invite_token(inbox, email))

    assert resp.status_code == 409
    assert resp.json()["code"] == "invite.stale"


async def test_accept_fails_if_team_was_deleted(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    org = await make_org()
    team_id = org.team_ids[0]
    admin = await org_admin(client, make_user, org)
    email = new_email()
    await invite(client, admin, email, "sales_rep", team_id)
    assert (await client.delete(f"/api/v1/teams/{team_id}", headers=admin)).status_code == 204

    resp = await accept(client, await invite_token(inbox, email))

    assert resp.status_code == 409
    assert resp.json()["code"] == "invite.stale"


async def test_invalid_email_is_rejected(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    admin = await org_admin(client, make_user, await make_org())

    resp = await invite(client, admin, "not-an-email", "viewer")

    assert resp.status_code == 422


async def test_other_org_invite_does_not_cancel_mine(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, inbox: Inbox
) -> None:
    mine = await org_admin(client, make_user, await make_org())
    rival = await org_admin(client, make_user, await make_org())
    email = new_email()
    await invite(client, mine, email, "viewer")
    await invite(client, rival, email, "viewer")  # чужая организация приглашает тот же email
    rivals_letter, my_letter = await inbox(email)

    # Моё приглашение живо: приглашение другой организации его не погасило.
    assert (await accept(client, token_from_email(my_letter))).status_code == 201
    # Человек уже в моей организации — второе приглашение устарело.
    assert (await accept(client, token_from_email(rivals_letter))).json()["code"] == "invite.stale"
