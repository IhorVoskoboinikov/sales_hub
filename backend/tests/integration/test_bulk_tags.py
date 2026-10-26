"""POST /bulk/add-tags и /bulk/remove-tags (раздел 6, «Остальные массовые операции и выгрузка»).

Запуск — через API (TaskQueue — фейк), plan и чанки — фикстурой run_job в транзакции теста
(чанки по 2 строки). Сегмент сравнивается с ответом GET /contacts с теми же фильтрами: у
руководителя и админа уровни contact:read и bulk:run совпадают, поэтому множества обязаны
совпасть. Права ролей на маршруты — в test_access_matrix.py.
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select

from app.models import AuditEntry, BulkJob, Contact, User
from app.uow import UnitOfWork
from tests.integration.conftest import (
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    RunJob,
    member,
    tokens_for,
)

ADD = "/api/v1/bulk/add-tags"
REMOVE = "/api/v1/bulk/remove-tags"


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


async def post(
    client: AsyncClient, url: str, author: User | dict[str, str], body: dict[str, Any]
) -> Response:
    headers = author if isinstance(author, dict) else await tokens_for(client, author)
    return await client.post(url, json=body, headers=headers)


async def tags_of(uow: UnitOfWork, ids: list[int]) -> dict[int, tuple[list[str], int]]:
    rows = await uow.session.execute(
        select(Contact.id, Contact.tags, Contact.version)
        .where(Contact.id.in_(ids))
        .execution_options(populate_existing=True)
    )
    return {row.id: (list(row.tags), row.version) for row in rows}


async def job_of(uow: UnitOfWork, job_id: str) -> BulkJob:
    job = await uow.bulk_jobs.get(uuid.UUID(job_id))
    assert job is not None
    return job


async def run(
    client: AsyncClient,
    run_job: RunJob,
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
) -> str:
    resp = await post(client, url, headers, body)
    assert resp.status_code == 202, resp.text
    assert resp.headers["location"] == f"/api/v1/bulk-jobs/{resp.json()['id']}"
    await run_job(resp.json()["id"])
    return str(resp.json()["id"])


async def test_add_tag_is_idempotent_and_skips_tagged(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    plain = (await make_contact(rep, tags=["kyiv"])).id
    tagged = (await make_contact(rep, tags=["vip"])).id
    deleted = (await make_contact(rep, deleted_at=func.now())).id
    headers = await tokens_for(client, admin)

    job_id = await run(client, run_job, ADD, headers, {"tag": "vip", "filter": {}})
    await run_job(job_id)  # повтор plan и чанков ничего не делает

    tags = await tags_of(uow, [plain, tagged, deleted])
    assert tags[plain] == (["kyiv", "vip"], 2)
    assert tags[tagged] == (["vip"], 1)  # тег уже был — строка не тронута
    assert tags[deleted] == ([], 1)  # корзина не в сегменте
    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.failed_rows) == ("done", 1, 1, 0)

    again = await run(client, run_job, ADD, headers, {"tag": "vip", "filter": {}})
    assert (await job_of(uow, again)).processed == 0
    assert (await tags_of(uow, [plain]))[plain] == (["kyiv", "vip"], 2)


async def test_remove_tag_touches_only_contacts_with_it(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    rep = await member(make_user, org, "sales_rep", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)
    with_tag = (await make_contact(rep, tags=["vip", "kyiv"])).id
    without = (await make_contact(rep, tags=["kyiv"])).id
    foreign = (await make_contact(other_team, tags=["vip"])).id
    headers = await tokens_for(client, lead)

    job_id = await run(client, run_job, REMOVE, headers, {"tag": "vip", "filter": {}})
    again = await run(client, run_job, REMOVE, headers, {"tag": "vip", "filter": {}})

    tags = await tags_of(uow, [with_tag, without, foreign])
    assert tags[with_tag] == (["kyiv"], 2)
    assert tags[without] == (["kyiv"], 1)
    assert tags[foreign] == (["vip"], 1)  # другой отдел — вне уровня руководителя
    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.result) == ("done", 1, {"rows": {"contacts": 1}})
    assert (await job_of(uow, again)).processed == 0
    [entry] = list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == job_id
            )
        )
    )
    assert entry.action == "bulk.remove_tags"
    assert entry.changes == {
        "tag": "vip",
        "filter": {},
        "status": "done",
        "rows": {"contacts": 1},
    }


async def test_contacts_with_20_tags_are_failed_rows(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    full = [f"t{i}" for i in range(20)]
    capped = [(await make_contact(rep, tags=full)).id for _ in range(3)]
    almost = (await make_contact(rep, tags=full[:19])).id
    has_it = (await make_contact(rep, tags=[*full[:19], "vip"])).id  # 20 тегов, но тег уже есть
    plain = (await make_contact(rep)).id

    job_id = await run(
        client, run_job, ADD, await tokens_for(client, admin), {"tag": "vip", "filter": {}}
    )

    tags = await tags_of(uow, [*capped, almost, has_it, plain])
    for contact_id in capped:
        assert tags[contact_id] == (full, 1)
    assert tags[almost] == ([*full[:19], "vip"], 2)
    assert tags[plain] == (["vip"], 2)
    assert tags[has_it][1] == 1
    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.failed_rows) == ("partial_failed", 5, 2, 3)
    assert job.result == {"rows": {"contacts": 2, "failed": 3}}
    [entry] = list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == job_id
            )
        )
    )
    assert entry.action == "bulk.add_tags"
    assert entry.changes["rows"] == {"contacts": 2, "failed": 3}
    assert entry.changes["status"] == "partial_failed"


@pytest.mark.parametrize(
    "case",
    ["q", "status", "tag", "owner", "company", "all"],
)
async def test_filter_selects_same_contacts_as_list(
    case: str,
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
    make_contact: MakeRecord,
    make_company: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    rep = await member(make_user, org, "sales_rep", team=0)
    colleague = await member(make_user, org, "sales_rep", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)
    stranger = await member(make_user, await make_org(), "sales_rep")
    company = await make_company(rep)
    ids: list[int] = []
    for owner in (rep, colleague, other_team, stranger):
        for status, tags, name in (
            ("lead", ["kyiv"], "Петренко"),
            ("customer", ["lviv"], "Шевчук"),
            ("churned", [], "Петренко-Мельник"),
        ):
            contact = await make_contact(
                owner,
                status=status,
                tags=tags,
                last_name=name,
                company_id=company.id if owner is rep and status != "lead" else None,
            )
            ids.append(contact.id)
    filters = {
        "q": {"q": "петренко"},
        "status": {"status": "customer"},
        "tag": {"tag": "kyiv"},
        "owner": {"owner_id": str(colleague.id)},
        "company": {"company_id": company.id},
        "all": {"q": "петренко", "status": "lead", "tag": "kyiv", "owner_id": str(rep.id)},
    }[case]
    headers = await tokens_for(client, lead)
    listed = await client.get(
        "/api/v1/contacts",
        params={"limit": 100, **{k: str(v) for k, v in filters.items()}},
        headers=headers,
    )
    assert listed.status_code == 200, listed.text
    expected = {item["id"] for item in listed.json()["items"]}
    assert expected  # сегмент не пустой — иначе сравнение ничего не проверяет

    await run(client, run_job, ADD, headers, {"tag": "segment", "filter": filters})

    tags = await tags_of(uow, ids)
    tagged = {contact_id for contact_id, (t, _) in tags.items() if "segment" in t}
    assert tagged == expected


async def test_empty_filter_is_whole_level_and_other_org_untouched(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
    make_contact: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    reps = [await member(make_user, org, "sales_rep", team=t) for t in (0, 1)]
    stranger = await member(make_user, await make_org(), "sales_rep")
    mine = [(await make_contact(rep)).id for rep in reps for _ in range(2)]
    foreign = (await make_contact(stranger)).id

    await run(client, run_job, ADD, await tokens_for(client, admin), {"tag": "all", "filter": {}})

    tags = await tags_of(uow, [*mine, foreign])
    assert all(tags[c] == (["all"], 2) for c in mine)
    assert tags[foreign] == ([], 1)


@pytest.mark.parametrize(
    "body",
    [
        {"tag": "", "filter": {}},
        {"tag": "x" * 65, "filter": {}},
        {"tag": "vip"},  # сегмент обязателен: весь уровень — только явным {}
        {"tag": "vip", "filter": {"tags": "vip"}},  # опечатка не превращается во весь уровень
        {"tag": "vip", "filter": {}, "segment": {"status": "lead"}},  # лишнее поле тела
        {"tag": "vip", "filter": {"q": "ab"}},  # как у GET /contacts: от 3 букв подряд
        {"tag": "vip", "filter": {"status": "unknown"}},
        {"tag": "vip", "filter": {"company_id": 0}},
    ],
)
async def test_invalid_body_is_422(
    body: dict[str, Any], client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    resp = await post(client, ADD, admin, body)
    assert resp.status_code == 422, resp.text


async def test_same_filter_written_differently_is_one_request(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    headers = {**await tokens_for(client, admin), "Idempotency-Key": "tags-1"}

    first = await client.post(ADD, json={"tag": "vip", "filter": {}}, headers=headers)
    again = await client.post(
        ADD, json={"tag": "vip", "filter": {"q": None, "tag": ""}}, headers=headers
    )
    other = await client.post(REMOVE, json={"tag": "vip", "filter": {}}, headers=headers)

    assert first.status_code == again.status_code == 202, again.text
    assert again.json()["id"] == first.json()["id"]
    assert other.status_code == 422  # тот же ключ, другой вид job
    assert other.json()["code"] == "bulk.idempotency_key_reused"


async def test_add_and_remove_run_in_parallel_but_not_twice(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    headers = await tokens_for(client, admin)

    added = await post(client, ADD, headers, {"tag": "vip", "filter": {}})
    removed = await post(client, REMOVE, headers, {"tag": "old", "filter": {}})
    second = await post(client, ADD, headers, {"tag": "new", "filter": {}})

    assert (added.status_code, removed.status_code) == (202, 202)
    assert second.status_code == 409
    assert second.json()["code"] == "bulk.already_running"
