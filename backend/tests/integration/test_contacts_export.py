"""GET /contacts/export (раздел 6, «Остальные массовые операции и выгрузка», шаг 5.7): формат
файла, экранирование формул, те же фильтры и порядок, что у GET /contacts, уровень, лимиты и
обратная загрузка файла импортом. Права ролей на маршрут — в test_access_matrix.py.
"""

import csv
import io
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient, Response
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.locks import AdvisoryLock
from app.models import Contact, User
from app.repositories import contacts as contacts_repository
from app.services.bulk import BulkService
from app.services.exports import EXPORT_COLUMNS, ExportSlots
from app.services.imports import ImportService
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeFileStorage,
    FakeTaskQueue,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    import_form,
    member,
    multipart_body,
    tokens_for,
)

URL = "/api/v1/contacts/export"
BOM = "\ufeff"


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


async def export(client: AsyncClient, author: User | dict[str, str], **params: Any) -> Response:
    headers = author if isinstance(author, dict) else await tokens_for(client, author)
    return await client.get(URL, params=params, headers=headers)


def rows_of(resp: Response) -> list[dict[str, str]]:
    assert resp.status_code == 200, resp.text
    text = resp.content.decode("utf-8")
    assert text.startswith(BOM)
    reader = csv.DictReader(io.StringIO(text.removeprefix(BOM), newline=""))
    assert tuple(reader.fieldnames or ()) == EXPORT_COLUMNS
    return list(reader)


def ids_of(resp: Response) -> list[int]:
    return [int(row["id"]) for row in rows_of(resp)]


async def test_file_format_columns_and_escaping(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
    make_company: MakeRecord,
) -> None:
    lead = await member(make_user, org, "team_lead")
    rep = await member(make_user, org, "sales_rep")
    company = await make_company(rep)
    contact = await make_contact(
        rep,
        email="olena@example.com",
        first_name='=HYPERLINK("http://x")',
        last_name="-2+3",
        phone="+380671234567",
        status="customer",
        source="@import",
        tags=["vip", "kyiv"],
        company_id=company.id,
    )
    tabbed = await make_contact(rep, first_name="\tTab", last_name="\rCr", source="веб-сайт")
    plain = await make_contact(rep, first_name="Іван", email=None)
    expected_created = contact.created_at.isoformat()
    rep_email, company_id = rep.email, company.id

    resp = await export(client, lead)

    assert resp.headers["content-type"] == "text/csv; charset=utf-8"
    disposition = resp.headers["content-disposition"]
    assert disposition.startswith('attachment; filename="contacts-')
    assert disposition.endswith('.csv"')
    rows = {int(row["id"]): row for row in rows_of(resp)}
    assert rows[contact.id] == {
        "id": str(contact.id),
        "email": "olena@example.com",
        "first_name": '\'=HYPERLINK("http://x")',
        "last_name": "'-2+3",
        "phone": "'+380671234567",
        "status": "customer",
        "source": "'@import",
        "tags": "vip,kyiv",
        "owner_email": rep_email,
        "company_id": str(company_id),
        "created_at": expected_created,
        "updated_at": rows[contact.id]["updated_at"],
    }
    assert rows[tabbed.id]["first_name"] == "'\tTab"
    assert rows[tabbed.id]["last_name"] == "'\rCr"
    assert rows[tabbed.id]["source"] == "веб-сайт"  # «-» не в начале — без апострофа
    assert (rows[plain.id]["email"], rows[plain.id]["tags"], rows[plain.id]["company_id"]) == (
        "",
        "",
        "",
    )


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"q": "петренко"},
        {"status": "customer"},
        {"tag": "kyiv"},
        {"owner": True},
        {"company": True},
        {"q": "петренко", "status": "lead", "tag": "kyiv"},
    ],
)
async def test_same_filters_and_order_as_list(
    params: dict[str, Any],
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
    make_contact: MakeRecord,
    make_company: MakeRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Пачки курсора по 2 строки: файл склеивается из нескольких пачек.
    monkeypatch.setattr(contacts_repository, "EXPORT_BATCH", 2)
    lead = await member(make_user, org, "team_lead", team=0)
    rep = await member(make_user, org, "sales_rep", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)
    stranger = await member(make_user, await make_org(), "sales_rep")
    company = await make_company(rep)
    for owner in (rep, lead, other_team, stranger):
        for status, tags, name in (
            ("lead", ["kyiv"], "Петренко"),
            ("customer", ["lviv", "kyiv"], "Шевчук"),
            ("churned", [], "Петренко"),
        ):
            await make_contact(
                owner,
                status=status,
                tags=tags,
                last_name=name,
                company_id=company.id if owner is rep else None,
            )
    await make_contact(rep, deleted_at=func.now())  # корзина не выгружается
    if params.pop("owner", False):
        params["owner_id"] = str(rep.id)
    if params.pop("company", False):
        params["company_id"] = company.id
    headers = await tokens_for(client, lead)
    listed = await client.get("/api/v1/contacts", params={**params, "limit": 100}, headers=headers)

    resp = await export(client, headers, **params)

    expected = [item["id"] for item in listed.json()["items"]]
    assert expected
    assert ids_of(resp) == expected  # те же строки и тот же порядок: новые сначала


async def test_export_is_scoped_to_level(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
    make_contact: MakeRecord,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    viewer = await member(make_user, org, "viewer")
    rep = await member(make_user, org, "sales_rep", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)
    stranger = await member(make_user, await make_org(), "sales_rep")
    mine = (await make_contact(rep)).id
    theirs = (await make_contact(other_team)).id
    foreign = (await make_contact(stranger)).id

    assert set(ids_of(await export(client, lead))) == {mine}
    assert set(ids_of(await export(client, viewer))) == {mine, theirs}
    assert foreign not in ids_of(await export(client, viewer))
    resp = await export(client, rep)
    assert resp.status_code == 403  # у менеджера нет export:run


async def test_more_than_max_rows_is_422(
    client: AsyncClient,
    app: FastAPI,
    settings: Settings,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    for _ in range(3):
        await make_contact(rep)
    headers = await tokens_for(client, admin)
    app.state.settings = settings.model_copy(update={"export_max_rows": 2})

    resp = await export(client, headers)
    narrowed = await export(client, headers, owner_id=str(uuid.uuid4()))

    assert resp.status_code == 422, resp.text
    assert (resp.json()["code"], resp.json()["details"]) == ("export.too_large", {"max_rows": 2})
    assert narrowed.status_code == 200  # сузили фильтр — выгрузка проходит
    app.state.settings = settings.model_copy(update={"export_max_rows": 3})
    assert len(ids_of(await export(client, headers))) == 3  # ровно предел — можно


async def test_rate_limit_per_user_is_429(
    client: AsyncClient,
    app: FastAPI,
    settings: Settings,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    admin = await member(make_user, org, "org_admin")
    viewer = await member(make_user, org, "viewer")
    headers = await tokens_for(client, admin)
    other = await tokens_for(client, viewer)
    app.state.settings = settings.model_copy(update={"export_rate_per_hour": 2})

    statuses = [(await export(client, headers)).status_code for _ in range(3)]
    blocked = await export(client, headers)

    assert statuses == [200, 200, 429]
    assert blocked.status_code == 429
    assert blocked.json()["code"] == "rate_limited"
    assert 0 < int(blocked.headers["retry-after"]) <= 3600
    assert (await export(client, other)).status_code == 200  # счётчик — на пользователя


async def test_too_many_concurrent_exports_is_429_without_using_hourly_limit(
    client: AsyncClient,
    app: FastAPI,
    settings: Settings,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    admin = await member(make_user, org, "org_admin")
    headers = await tokens_for(client, admin)
    app.state.settings = settings.model_copy(
        update={"export_max_concurrent": 1, "export_rate_per_hour": 1}
    )
    slots: ExportSlots = app.state.export_slots

    with slots.hold(1):  # процесс уже отдаёт одну выгрузку
        busy = await export(client, headers)
    done = await export(client, headers)

    assert busy.status_code == 429, busy.text
    assert busy.json()["code"] == "export.busy"
    assert int(busy.headers["retry-after"]) > 0
    assert done.status_code == 200  # отказ export.busy не засчитан в час
    assert slots.busy == 0  # слот отдан после ответа


@pytest.fixture
async def lock(postgres_url: str) -> AsyncIterator[AdvisoryLock]:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    yield AdvisoryLock(engine)
    await engine.dispose()


async def test_exported_file_is_accepted_by_import(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_contact: MakeRecord,
    make_company: MakeRecord,
    uow: UnitOfWork,
    file_storage: FakeFileStorage,
    lock: AdvisoryLock,
) -> None:
    lead = await member(make_user, org, "team_lead")
    rep = await member(make_user, org, "sales_rep")
    company = await make_company(rep)
    originals = [
        await make_contact(
            rep,
            first_name="=SUM(A1)",
            last_name="Коваль",
            phone="+380501112233",
            status="customer",
            source="-реклама",
            tags=["vip", "@kyiv"],
            company_id=company.id,
        ),
        await make_contact(lead, first_name="Ольга", last_name="'=1", status="churned"),
        await make_contact(rep, first_name="Іван", last_name="", tags=[]),
    ]
    fields = ("email", "first_name", "last_name", "phone", "status", "source", "tags", "owner_id")
    before = {c.email: tuple(getattr(c, f) for f in fields) for c in originals}
    headers = await tokens_for(client, lead)
    exported = await export(client, headers)
    assert exported.status_code == 200
    # Контакты — в корзину: импорт создаст их заново из файла (email из корзины не конфликтует).
    await uow.session.execute(
        update(Contact).where(Contact.org_id == org.id).values(deleted_at=func.now())
    )
    await uow.commit()

    body, form_headers = multipart_body(import_form(exported.content))
    resp = await client.post(
        "/api/v1/contacts/import", content=body, headers={**headers, **form_headers}
    )
    assert resp.status_code == 202, resp.text
    info = resp.json()["import_info"]
    assert info["ignored_columns"] == ["id", "company_id", "created_at", "updated_at"]
    assert info["columns"] == [
        "email",
        "first_name",
        "last_name",
        "phone",
        "status",
        "source",
        "tags",
        "owner_email",
    ]
    job_id = uuid.UUID(resp.json()["id"])
    bulk = BulkService(uow, RoleCatalog.load(POLICIES_DIR), FakeTaskQueue(), chunk_size=2)
    await ImportService(bulk, file_storage, max_rows=1000).run(job_id, lock)

    job = await uow.bulk_jobs.get(job_id)
    assert job is not None
    assert (job.status, job.result) == ("done", {"created": 3, "updated": 0})
    created = await uow.session.scalars(
        select(Contact)
        .where(Contact.org_id == org.id, Contact.deleted_at.is_(None))
        .execution_options(populate_existing=True)
    )
    after = {c.email: tuple(getattr(c, f) for f in fields) for c in created}
    assert after == before  # апострофы экспорта снял импорт, теги и ответственный те же
