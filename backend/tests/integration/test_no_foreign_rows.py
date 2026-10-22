"""«Список не возвращает чужого» (раздел 10) — по всем спискам этапа 4.

Две организации с одинаковым набором данных. Каждый пользователь организации A видит в
каждом списке ровно записи своего уровня: ни одной строки организации B, а менеджер — ни
одной записи коллег. Матрица прав проверяет то же по одной записи на цель; здесь — списки
целиком, с фильтрами и в корзине.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from httpx import AsyncClient

from app.uow import UnitOfWork
from tests.integration.conftest import (
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
    tokens_for,
)


@dataclass
class Records:
    """id записей организации по владельцу: rep — менеджер отдела 1, peer — его коллега,
    rep2 — менеджер отдела 2."""

    contacts: dict[str, int]
    deleted: dict[str, int]
    deals: dict[str, int]
    tasks: dict[str, int]
    pipeline_id: int


async def fill(
    org: OrgFixture,
    make_user: MakeUser,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
) -> tuple[dict[str, Any], Records]:
    people = {
        "rep": await member(make_user, org, "sales_rep", team=0),
        "peer": await member(make_user, org, "sales_rep", team=0),
        "rep2": await member(make_user, org, "sales_rep", team=1),
        "lead": await member(make_user, org, "team_lead", team=0),
        "admin": await member(make_user, org, "org_admin"),
        "viewer": await member(make_user, org, "viewer"),
    }
    owners = ("rep", "peer", "rep2")
    pipeline = await make_pipeline(org.id)
    gone = datetime.now(UTC)
    records = Records(
        contacts={o: (await make_contact(people[o], tags=["vip"])).id for o in owners},
        deleted={o: (await make_contact(people[o], deleted_at=gone)).id for o in owners},
        deals={o: (await make_deal(people[o], pipeline.stages[0])).id for o in owners},
        tasks={o: (await make_activity(people[o])).id for o in owners},
        pipeline_id=pipeline.id,
    )
    return people, records


VISIBLE = {  # кто из владельцев виден по уровню роли
    "rep": {"rep"},
    "lead": {"rep", "peer"},
    "admin": {"rep", "peer", "rep2"},
    "viewer": {"rep", "peer", "rep2"},
}


async def test_lists_never_show_foreign_rows(
    client: AsyncClient,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
    uow: UnitOfWork,
) -> None:
    org_a, org_b = await make_org(), await make_org()
    people, mine = await fill(
        org_a, make_user, make_contact, make_pipeline, make_deal, make_activity
    )
    _, foreign = await fill(org_b, make_user, make_contact, make_pipeline, make_deal, make_activity)
    await uow.reports.refresh()
    # По таблицам: id у контактов, сделок и активностей — свои последовательности, и одно и то
    # же число в разных таблицах — не «чужая строка».
    all_foreign = {
        "contacts": {*foreign.contacts.values(), *foreign.deleted.values()},
        "deals": set(foreign.deals.values()),
        "tasks": set(foreign.tasks.values()),
    }

    async def ids(auth: dict[str, str], url: str, **params: Any) -> set[Any]:
        resp = await client.get(url, params=params, headers=auth)
        assert resp.status_code == 200, (url, resp.text)
        return {item["id"] for item in resp.json()["items"]}

    for role, visible in VISIBLE.items():
        auth = await tokens_for(client, people[role])
        expect = {
            "contacts": {mine.contacts[o] for o in visible},
            "deals": {mine.deals[o] for o in visible},
            "tasks": {mine.tasks[o] for o in visible},
        }
        lists = {
            "contacts": await ids(auth, "/api/v1/contacts"),
            "contacts?tag": await ids(auth, "/api/v1/contacts", tag="vip"),
            "deals": await ids(auth, "/api/v1/deals"),
            "tasks": await ids(auth, "/api/v1/activities"),
        }

        assert lists["contacts"] == expect["contacts"], role
        assert lists["contacts?tag"] == expect["contacts"], role
        assert lists["deals"] == expect["deals"], role
        assert lists["tasks"] == expect["tasks"], role
        for name, found in lists.items():
            assert not found & all_foreign[name.split("?")[0]], (role, name)

        if role in ("lead", "admin"):  # корзина — у тех, кто может удалять
            trash = await ids(auth, "/api/v1/contacts/trash")
            assert trash == {mine.deleted[o] for o in visible}, role

        report = await client.get("/api/v1/reports/managers", headers=auth)
        owners = {m["owner_id"] for m in report.json()["managers"]}
        assert owners == {str(people[o].id) for o in visible}, role
