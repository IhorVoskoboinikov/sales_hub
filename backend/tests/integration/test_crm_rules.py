"""Правила этапа 4.1, которые живут не в CRM-эндпоинтах, а рядом (раздел 5):

- отдел с живыми записями не удалить — 409 team.has_records;
- человека с живыми записями не перевести в другой отдел и не сделать org_admin / viewer,
  пока нет bulk job move_team (этап 5), — 409 member.has_records; отключить можно;
- версия записи растёт при каждом UPDATE, устаревшая версия — 409 version.conflict.
"""

from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError

from app.models import Contact
from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, member, tokens_for


async def test_member_with_records_cannot_change_team_or_role(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=1)
    await make_contact(rep)
    url = f"/api/v1/users/{rep.id}"

    moved = await client.patch(url, json={"team_id": str(org.team_ids[0])}, headers=admin)
    demoted = await client.patch(url, json={"role": "viewer"}, headers=admin)
    promoted = await client.patch(url, json={"role": "team_lead"}, headers=admin)  # тот же отдел
    disabled = await client.patch(url, json={"is_active": False}, headers=admin)

    assert (moved.status_code, moved.json()["code"]) == (409, "member.has_records")
    assert (demoted.status_code, demoted.json()["code"]) == (409, "member.has_records")
    assert promoted.status_code == 200
    # Отключить можно: записи остаются, их передадут другому (bulk, этап 5).
    assert disabled.status_code == 200


async def test_records_in_trash_do_not_block(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=1)
    contact = await make_contact(rep)
    contact.deleted_at = datetime.now(UTC)
    await db_session.commit()

    moved = await client.patch(f"/api/v1/users/{rep.id}", json={"role": "viewer"}, headers=admin)

    assert moved.status_code == 200


async def test_empty_team_with_records_cannot_be_deleted(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_contact: MakeRecord,
    db_session: AsyncSession,
) -> None:
    org = await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    rep = await member(make_user, org, "sales_rep", team=1)
    await make_contact(rep)
    # Человек ушёл из отдела в обход правил (например, данными до этапа 4) — записи остались.
    await db_session.execute(
        update(type(rep)).where(type(rep).id == rep.id).values(role="viewer", team_id=None)
    )
    await db_session.commit()

    resp = await client.delete(f"/api/v1/teams/{org.team_ids[1]}", headers=admin)

    assert (resp.status_code, resp.json()["code"]) == (409, "team.has_records")


async def test_version_grows_on_every_update(
    make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord, db_session: AsyncSession
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    contact: Contact = await make_contact(rep)
    assert contact.version == 1

    contact.first_name = "Петро"
    await db_session.commit()

    assert contact.version == 2


async def test_stale_update_is_rejected_by_database(
    make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord, db_session: AsyncSession
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    contact: Contact = await make_contact(rep)
    # Кто-то другой успел изменить строку: версия в базе уже 2, а в нашем объекте — 1.
    await db_session.execute(
        update(Contact)
        .where(Contact.id == contact.id)
        .values(version=Contact.version + 1)
        .execution_options(synchronize_session=False)  # наш объект об этом не знает
    )

    contact.first_name = "Петро"
    # UPDATE … WHERE id = :id AND version = 1 не найдёт строку. В API это 409 version.conflict.
    with pytest.raises(StaleDataError):
        await db_session.flush()
