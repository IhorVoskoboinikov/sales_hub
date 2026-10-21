"""Компании и контакты — шаг 4.2: ответственный и отдел, дубль email, версия, списки."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AuditEntry
from tests.integration.conftest import (
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
    tokens_for,
)


async def people(client: AsyncClient, make_user: MakeUser, org: OrgFixture) -> dict[str, Any]:
    """Люди организации: лица с токенами и id."""
    users = {
        "admin": await member(make_user, org, "org_admin"),
        "viewer": await member(make_user, org, "viewer"),
        "lead": await member(make_user, org, "team_lead", team=0),
        "rep": await member(make_user, org, "sales_rep", team=0),
        "peer": await member(make_user, org, "sales_rep", team=0),
        "rep2": await member(make_user, org, "sales_rep", team=1),
    }
    result: dict[str, Any] = {name: {"id": str(u.id)} for name, u in users.items()}
    for name, user in users.items():
        result[name]["auth"] = await tokens_for(client, user)
    return result


def email() -> str:
    return f"client-{uuid.uuid4().hex[:8]}@example.com"


async def test_sales_rep_creates_contact_for_himself(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    p = await people(client, make_user, org)

    resp = await client.post(
        "/api/v1/contacts",
        json={"first_name": "Іван", "last_name": "Петренко", "email": email(), "tags": ["vip"]},
        headers=p["rep"]["auth"],
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["owner_id"] == p["rep"]["id"]
    assert body["team_id"] == str(org.team_ids[0])  # отдел ответственного, не из тела
    assert (body["status"], body["version"], body["tags"]) == ("lead", 1, ["vip"])


async def test_owner_rules(client: AsyncClient, make_user: MakeUser, make_org: MakeOrg) -> None:
    org = await make_org()
    p = await people(client, make_user, org)

    async def create(who: str, **body: Any) -> Any:
        resp = await client.post(
            "/api/v1/contacts", json={"first_name": "X", **body}, headers=p[who]["auth"]
        )
        return resp.status_code, resp.json().get("code")

    # org_admin сам записей не ведёт — нужно указать ответственного.
    assert await create("admin") == (422, "owner.required")
    assert await create("admin", owner_id=p["viewer"]["id"]) == (422, "owner.invalid")
    assert await create("admin", owner_id=str(uuid.uuid4())) == (422, "owner.invalid")
    assert (await create("admin", owner_id=p["rep2"]["id"]))[0] == 201
    # Руководитель — только в свой отдел, менеджер — только себе.
    assert (await create("lead", owner_id=p["rep"]["id"]))[0] == 201
    assert await create("lead", owner_id=p["rep2"]["id"]) == (403, "forbidden")
    assert await create("rep", owner_id=p["peer"]["id"]) == (403, "forbidden")


async def test_team_lead_reassigns_inside_team_and_team_follows_owner(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    p = await people(client, make_user, org)
    created = await client.post(
        "/api/v1/contacts", json={"first_name": "Іван"}, headers=p["rep"]["auth"]
    )
    url = f"/api/v1/contacts/{created.json()['id']}"

    moved = await client.patch(
        url, json={"version": 1, "owner_id": p["peer"]["id"]}, headers=p["lead"]["auth"]
    )
    away = await client.patch(
        url, json={"version": 2, "owner_id": p["rep2"]["id"]}, headers=p["lead"]["auth"]
    )
    by_admin = await client.patch(
        url, json={"version": 2, "owner_id": p["rep2"]["id"]}, headers=p["admin"]["auth"]
    )

    assert (moved.status_code, moved.json()["owner_id"]) == (200, p["peer"]["id"])
    assert (away.status_code, away.json()["details"]) == (403, {"reason": "out_of_scope"})
    assert by_admin.json()["team_id"] == str(org.team_ids[1])  # отдел нового ответственного
    # Прежний менеджер свою бывшую запись больше не видит.
    gone = await client.get(url, headers=p["rep"]["auth"])
    assert gone.status_code == 404


async def test_version_conflict(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    p = await people(client, make_user, org)
    created = await client.post(
        "/api/v1/contacts", json={"first_name": "А"}, headers=p["rep"]["auth"]
    )
    url = f"/api/v1/contacts/{created.json()['id']}"

    first = await client.patch(url, json={"version": 1, "phone": "1"}, headers=p["rep"]["auth"])
    second = await client.patch(url, json={"version": 1, "phone": "2"}, headers=p["lead"]["auth"])

    assert (first.status_code, first.json()["version"]) == (200, 2)
    assert second.status_code == 409
    assert second.json()["code"] == "version.conflict"
    assert second.json()["details"] == {"current_version": 2}


async def test_patch_without_changes_keeps_version_and_null_is_rejected(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    p = await people(client, make_user, org)
    created = await client.post(
        "/api/v1/contacts", json={"first_name": "А", "phone": "1"}, headers=p["rep"]["auth"]
    )
    url = f"/api/v1/contacts/{created.json()['id']}"

    same = await client.patch(url, json={"version": 1, "phone": "1"}, headers=p["rep"]["auth"])
    cleared = await client.patch(url, json={"version": 1, "phone": None}, headers=p["rep"]["auth"])
    bad = await client.patch(url, json={"version": 2, "first_name": None}, headers=p["rep"]["auth"])

    assert same.json()["version"] == 1
    assert (cleared.json()["phone"], cleared.json()["version"]) == (None, 2)
    assert bad.status_code == 422


async def test_duplicate_email_reveals_only_the_owner(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    p = await people(client, make_user, org)
    taken = email()
    created = await client.post(
        "/api/v1/contacts", json={"first_name": "А", "email": taken}, headers=p["rep2"]["auth"]
    )
    rep2_email = (await client.get("/api/v1/users", headers=p["admin"]["auth"])).json()
    owner = next(u["email"] for u in rep2_email if u["id"] == p["rep2"]["id"])

    hidden = await client.post(
        "/api/v1/contacts",
        json={"first_name": "Б", "email": taken.upper()},  # citext: регистр не важен
        headers=p["rep"]["auth"],
    )
    visible = await client.post(
        "/api/v1/contacts",
        json={"first_name": "Б", "email": taken, "owner_id": p["rep"]["id"]},
        headers=p["admin"]["auth"],
    )

    assert (hidden.status_code, hidden.json()["code"]) == (409, "contact.duplicate_email")
    assert hidden.json()["details"] == {"owner": owner}  # без id и данных контакта
    assert visible.json()["details"] == {"contact_id": created.json()["id"], "owner": owner}


async def test_reference_to_invisible_company_is_422(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_company: MakeRecord
) -> None:
    org, other = await make_org(), await make_org()
    p = await people(client, make_user, org)
    foreign = await make_company(await member(make_user, other, "sales_rep"))
    colleague = await make_company(await member(make_user, org, "sales_rep", team=1))
    foreign_id, colleague_id = foreign.id, colleague.id

    bad = await client.post(
        "/api/v1/contacts",
        json={"first_name": "А", "company_id": foreign_id},
        headers=p["rep"]["auth"],
    )
    # Компании — общий справочник: привязать контакт к компании коллеги можно.
    good = await client.post(
        "/api/v1/contacts",
        json={"first_name": "А", "company_id": colleague_id},
        headers=p["rep"]["auth"],
    )

    assert (bad.status_code, bad.json()["details"]) == (422, {"field": "company_id"})
    assert good.status_code == 201


async def test_list_filters_and_pages(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_contact: MakeRecord
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    other_rep = await member(make_user, org, "sales_rep")
    base = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    for i in range(5):
        # Свои created_at (в одной транзакции now() у всех одинаковый), у двух — равные:
        # проверяются и время, и id как второй ключ сортировки.
        created = base + timedelta(microseconds=1500 * min(i, 3))
        tags = ["vip"] if i % 2 else []
        await make_contact(rep, first_name=f"Олена{i}", tags=tags, created_at=created)
    await make_contact(other_rep, first_name="Чужа")
    auth = await tokens_for(client, rep)

    first = await client.get("/api/v1/contacts", params={"limit": 2}, headers=auth)
    second = await client.get(
        "/api/v1/contacts",
        params={"limit": 2, "cursor": first.json()["next_cursor"]},
        headers=auth,
    )
    vip = await client.get("/api/v1/contacts", params={"tag": "vip"}, headers=auth)
    found = await client.get("/api/v1/contacts", params={"q": "ЛЕНА3"}, headers=auth)
    short = await client.get("/api/v1/contacts", params={"q": "ол"}, headers=auth)
    no_trigrams = await client.get("/api/v1/contacts", params={"q": "- -"}, headers=auth)
    nul = await client.get("/api/v1/contacts", params={"q": "ол\x00ена"}, headers=auth)

    names = [c["first_name"] for c in first.json()["items"] + second.json()["items"]]
    assert names == ["Олена4", "Олена3", "Олена2", "Олена1"]  # новые сначала, без повторов
    assert {c["first_name"] for c in vip.json()["items"]} == {"Олена1", "Олена3"}
    assert [c["first_name"] for c in found.json()["items"]] == ["Олена3"]
    assert (short.status_code, no_trigrams.status_code, nul.status_code) == (422, 422, 422)
    broken = await client.get("/api/v1/contacts", params={"cursor": "abc"}, headers=auth)
    assert (broken.status_code, broken.json()["code"]) == (422, "pagination.invalid_cursor")


async def test_companies_are_shared_but_edited_by_owner(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, db_session: AsyncSession
) -> None:
    org = await make_org()
    p = await people(client, make_user, org)
    created = await client.post(
        "/api/v1/companies", json={"name": "Рога і копита"}, headers=p["rep"]["auth"]
    )
    company_id = created.json()["id"]
    url = f"/api/v1/companies/{company_id}"

    seen = await client.get(url, headers=p["rep2"]["auth"])
    listed = await client.get(
        "/api/v1/companies", params={"q": "копит"}, headers=p["viewer"]["auth"]
    )
    foreign_edit = await client.patch(
        url, json={"version": 1, "size": 5}, headers=p["rep2"]["auth"]
    )
    own_edit = await client.patch(url, json={"version": 1, "size": 5}, headers=p["rep"]["auth"])

    assert seen.status_code == 200
    assert [c["id"] for c in listed.json()["items"]] == [company_id]
    assert foreign_edit.status_code == 403  # видна, но не своя
    assert own_edit.json()["size"] == 5
    actions = await db_session.scalars(
        select(AuditEntry.action)
        .where(AuditEntry.entity_id == str(company_id))
        .order_by(AuditEntry.id)
    )
    assert list(actions) == ["company.created", "company.updated"]


async def test_out_of_range_and_nul_are_422_not_500(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    auth = await tokens_for(client, rep)

    responses = [
        await client.get("/api/v1/contacts/99999999999999999999", headers=auth),
        await client.get("/api/v1/contacts", params={"company_id": 10**20}, headers=auth),
        await client.post("/api/v1/companies", json={"name": "A", "size": 3 * 10**9}, headers=auth),
        # NUL Postgres в тексте не хранит — страховочный обработчик отвечает 422.
        await client.post("/api/v1/contacts", json={"first_name": "A\u0000"}, headers=auth),
    ]

    assert [r.status_code for r in responses] == [422, 422, 422, 422]
