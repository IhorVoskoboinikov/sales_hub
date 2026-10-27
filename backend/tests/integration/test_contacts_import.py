"""POST /contacts/import и GET /bulk-jobs/{job_id}/errors (раздел 6, «Поток: импорт контактов из
CSV», шаг 1 и 4): порядок проверок, коды ошибок, повтор по Idempotency-Key, файл в хранилище.
Права ролей на эти маршруты — в test_access_matrix.py.

Хранилище и TaskQueue — фейки (conftest): файл остаётся в памяти теста, задача импорта никуда не
уходит, поэтому job остаётся pending. Тело формы собирается «руками» (multipart_body), чтобы
отправить и то, что httpx собрать не даст.
"""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient, Response
from sqlalchemy import insert

from app.core.config import Settings
from app.models import ImportRowError, User
from app.repositories.bulk import BulkJobRepository
from app.uow import UnitOfWork
from tests.integration.conftest import (
    FakeFileStorage,
    FakeTaskQueue,
    FormPart,
    MakeJob,
    MakeOrg,
    MakeUser,
    OrgFixture,
    import_form,
    member,
    multipart_body,
    tokens_for,
)

URL = "/api/v1/contacts/import"
CSV = "id,email,first_name,last_name,tags\n7,ivan@example.com,Іван,,vip\n".encode()


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


async def post(
    client: AsyncClient,
    author: User | dict[str, str],
    parts: list[FormPart],
    *,
    key: str | None = None,
) -> Response:
    """author — пользователь или уже готовые заголовки: после ответа 4xx сессия теста
    откатывается, и ORM-объекты «протухают» — повторный запрос берёт заголовки из переменной."""
    headers = dict(author if isinstance(author, dict) else await tokens_for(client, author))
    body, form_headers = multipart_body(parts)
    headers |= form_headers
    if key is not None:
        headers["Idempotency-Key"] = key
    return await client.post(URL, content=body, headers=headers)


def error(resp: Response, status: int) -> tuple[str, dict[str, Any]]:
    assert resp.status_code == status, resp.text
    return resp.json()["code"], resp.json()["details"]


async def test_import_returns_202_stores_file_and_enqueues_task(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    task_queue: FakeTaskQueue,
    file_storage: FakeFileStorage,
    db_session: Any,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    admin_id, rep_id = admin.id, rep.id

    resp = await post(client, admin, import_form(CSV, rep.id))

    assert resp.status_code == 202, resp.text
    job = resp.json()
    assert resp.headers["location"] == f"/api/v1/bulk-jobs/{job['id']}"
    assert (job["kind"], job["status"], job["total"]) == ("import_contacts", "pending", 0)
    assert job["import_info"] == {
        "delimiter": ",",
        "columns": ["email", "first_name", "last_name", "tags"],
        "ignored_columns": ["id"],
    }
    assert task_queue.enqueued == [("imports.import_contacts", (job["id"],))]
    stored = await BulkJobRepository(db_session).get(uuid.UUID(job["id"]))
    assert stored is not None and stored.owner_id == admin_id
    params = stored.params
    assert params["file_key"].startswith(f"imports/{org.id}/")
    assert params["file_key"].endswith(".csv")
    assert params["file_name"] == "contacts.csv"
    assert params["columns"] == ["id", "email", "first_name", "last_name", "tags"]
    assert params["default_owner_id"] == str(rep_id)
    assert params["access"]["action"] == "bulk:run"
    assert file_storage.files == {params["file_key"]: (CSV, "text/csv")}


async def test_team_lead_imports_for_himself_by_default(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, db_session: Any
) -> None:
    lead = await member(make_user, org, "team_lead")
    lead_id = lead.id

    resp = await post(client, lead, import_form(b"Email;First_Name\na@b.c;I\n"))

    assert resp.status_code == 202, resp.text
    assert resp.json()["import_info"]["delimiter"] == ";"
    stored = await BulkJobRepository(db_session).get(uuid.UUID(resp.json()["id"]))
    assert stored is not None and stored.params["default_owner_id"] == str(lead_id)


async def test_job_snapshot_also_has_import_info(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead")
    headers = await tokens_for(client, lead)
    job = (await post(client, headers, import_form(CSV))).json()

    resp = await client.get(f"/api/v1/bulk-jobs/{job['id']}", headers=headers)

    assert resp.status_code == 200
    assert resp.json()["import_info"] == job["import_info"]


# --- до приёма файла: длина тела, повтор, уже идущий импорт


async def test_chunked_body_without_content_length_is_411(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, file_storage: FakeFileStorage
) -> None:
    lead = await member(make_user, org, "team_lead")
    headers = await tokens_for(client, lead)
    body, form_headers = multipart_body(import_form(CSV))

    async def chunks() -> AsyncIterator[bytes]:  # httpx шлёт Transfer-Encoding: chunked
        yield body

    resp = await client.post(URL, content=chunks(), headers=headers | form_headers)

    assert "content-length" not in resp.request.headers
    assert error(resp, 411) == ("import.length_required", {})
    assert file_storage.files == {}


async def test_content_length_over_limit_is_413_before_reading(
    client: AsyncClient, app: FastAPI, settings: Settings, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead")
    headers = await tokens_for(client, lead)
    app.state.settings = settings.model_copy(update={"import_max_bytes": 10})
    body, form_headers = multipart_body(import_form(CSV))
    # Заголовок обещает больше лимита: ответ сразу, тело не читается.
    headers |= form_headers | {"Content-Length": str(10 + 64 * 1024 + 1)}

    resp = await client.post(URL, content=body + b"x" * (64 * 1024), headers=headers)

    assert error(resp, 413) == ("import.file_too_large", {"max_bytes": 10})


async def test_file_over_limit_is_413_after_parsing(
    client: AsyncClient, app: FastAPI, settings: Settings, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead")
    app.state.settings = settings.model_copy(update={"import_max_bytes": len(CSV) - 1})

    resp = await post(client, lead, import_form(CSV))  # тело в пределах запаса на форму

    assert error(resp, 413) == ("import.file_too_large", {"max_bytes": len(CSV) - 1})


async def test_same_key_returns_same_job_without_reading_file(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    task_queue: FakeTaskQueue,
    file_storage: FakeFileStorage,
) -> None:
    lead = await member(make_user, org, "team_lead")
    headers = await tokens_for(client, lead)
    first = await post(client, headers, import_form(CSV), key="import-1")

    # Ключ проверяется до формы: даже битая форма возвращает ту же job.
    again = await post(client, headers, [("junk", b"x", None)], key="import-1")

    assert first.status_code == again.status_code == 202, again.text
    assert again.json()["id"] == first.json()["id"]
    assert again.headers["location"] == first.headers["location"]
    assert len(task_queue.enqueued) == 1
    assert len(file_storage.files) == 1


async def test_key_of_other_operation_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    lead = await member(make_user, org, "team_lead")
    await make_job(lead, idempotency_key="k-1", request_hash="0" * 64)

    resp = await post(client, lead, import_form(CSV), key="k-1")

    assert error(resp, 422)[0] == "bulk.idempotency_key_reused"


async def test_running_import_in_org_is_409(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_job: MakeJob,
    file_storage: FakeFileStorage,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    other_lead = await member(make_user, org, "team_lead", team=1)
    admin = await member(make_user, org, "org_admin")
    running = await make_job(lead, kind="import_contacts", status="running")
    running_id = str(running.id)
    admin_id = admin.id
    lead_headers = await tokens_for(client, lead)
    other_headers = await tokens_for(client, other_lead)
    admin_headers = await tokens_for(client, admin)

    mine = await post(client, lead_headers, import_form(CSV))
    others = await post(client, other_headers, import_form(CSV))  # job чужого отдела не видна
    by_admin = await post(client, admin_headers, import_form(CSV, admin_id))

    assert error(mine, 409) == ("bulk.already_running", {"job_id": running_id})
    assert error(others, 409) == ("bulk.already_running", {"job_id": None})
    assert error(by_admin, 409) == ("bulk.already_running", {"job_id": running_id})
    assert file_storage.files == {}


async def test_race_on_active_index_is_409_and_file_is_discarded(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_job: MakeJob,
    file_storage: FakeFileStorage,
    task_queue: FakeTaskQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lead = await member(make_user, org, "team_lead")
    running = await make_job(lead, kind="import_contacts", status="pending")
    running_id = str(running.id)

    async def no_active(self: BulkJobRepository, org_id: uuid.UUID, kind: str) -> bool:
        return False  # второй запрос прошёл проверку раньше, чем первый вставил job

    monkeypatch.setattr(BulkJobRepository, "has_active", no_active)

    resp = await post(client, lead, import_form(CSV))

    assert error(resp, 409) == ("bulk.already_running", {"job_id": running_id})
    assert file_storage.files == {}
    assert len(file_storage.deleted) == 1 and file_storage.deleted[0].startswith("imports/")
    assert task_queue.enqueued == []


# --- форма и заголовок CSV


@pytest.mark.parametrize(
    "parts",
    [
        pytest.param([("owner_id", str(uuid.uuid4()).encode(), None)], id="no-file"),
        pytest.param([("file", CSV, None)], id="file-as-text-field"),
        pytest.param([("upload", CSV, "a.csv")], id="other-file-name"),
        pytest.param([("file", CSV, "a.csv"), ("file", CSV, "b.csv")], id="two-files"),
        pytest.param(
            [("file", CSV, "a.csv"), ("owner_id", b"", None), ("note", b"x", None)],
            id="two-fields",
        ),
        pytest.param([("file", CSV, "a.csv"), ("note", b"x", None)], id="unknown-field"),
        pytest.param([("file", CSV, "a.csv"), ("owner_id", CSV, "o.csv")], id="owner-as-file"),
    ],
)
async def test_wrong_form_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, parts: list[FormPart]
) -> None:
    lead = await member(make_user, org, "team_lead")

    resp = await post(client, lead, parts)

    assert error(resp, 422)[0] == "import.invalid_form"


async def test_json_body_is_invalid_form(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead")

    resp = await client.post(URL, json={"file": "x"}, headers=await tokens_for(client, lead))

    assert error(resp, 422)[0] == "import.invalid_form"


@pytest.mark.parametrize(
    ("csv", "code"),
    [
        (b"", "import.empty_file"),
        (b"\xef\xbb\xbf\r\n", "import.empty_file"),
        ("email,first_name,прізвище\n".encode("cp1251"), "import.invalid_encoding"),
        (b"email,last_name\na@b.c,X\n", "import.invalid_header"),
        (b"email,first_name,Email\n", "import.invalid_header"),
        (b"email,first_name" + b",x" * 40_000, "import.invalid_header"),  # длиннее 64 КиБ
    ],
)
async def test_bad_file_is_422_without_job(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    file_storage: FakeFileStorage,
    task_queue: FakeTaskQueue,
    csv: bytes,
    code: str,
) -> None:
    lead = await member(make_user, org, "team_lead")

    resp = await post(client, lead, import_form(csv))

    assert error(resp, 422)[0] == code
    assert file_storage.files == {} and task_queue.enqueued == []


async def test_missing_columns_are_in_details(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead")

    resp = await post(client, lead, import_form(b"EMAIL,phone,email\n"))

    assert error(resp, 422) == (
        "import.invalid_header",
        {"missing": ["first_name"], "duplicates": ["email"]},
    )


# --- ответственный по умолчанию


async def test_org_admin_must_give_owner(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")

    resp = await post(client, admin, import_form(CSV))

    assert error(resp, 422) == ("owner.required", {"field": "owner_id"})


@pytest.mark.parametrize("owner", ["viewer", "inactive", "other_org", "not_uuid"])
async def test_owner_who_cannot_own_records_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_org: MakeOrg, owner: str
) -> None:
    admin = await member(make_user, org, "org_admin")
    match owner:
        case "viewer":
            owner_id: Any = (await member(make_user, org, "viewer")).id
        case "inactive":
            owner_id = (await member(make_user, org, "sales_rep", is_active=False)).id
        case "other_org":
            owner_id = (await member(make_user, await make_org(), "sales_rep")).id
        case _:
            owner_id = "not-a-uuid"

    resp = await post(client, admin, import_form(CSV, owner_id))

    code, details = error(resp, 422)
    assert code == ("validation" if owner == "not_uuid" else "owner.invalid")
    assert details == {"field": "owner_id"}


async def test_owner_outside_lead_team_is_403(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, file_storage: FakeFileStorage
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    stranger = await member(make_user, org, "sales_rep", team=1)

    resp = await post(client, lead, import_form(CSV, stranger.id))

    assert error(resp, 403) == ("forbidden", {"reason": "out_of_scope"})
    assert file_storage.files == {}


async def test_storage_down_is_503_without_job(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    file_storage: FakeFileStorage,
    task_queue: FakeTaskQueue,
    db_session: Any,
) -> None:
    lead = await member(make_user, org, "team_lead")
    headers = await tokens_for(client, lead)
    file_storage.down = True

    resp = await post(client, headers, import_form(CSV), key="down-1")

    assert error(resp, 503) == ("unavailable", {"service": "storage"})
    assert task_queue.enqueued == []
    file_storage.down = False  # повтор с тем же ключом создаёт job: первой попытки нет
    assert (await post(client, headers, import_form(CSV), key="down-1")).status_code == 202


def test_swagger_describes_multipart_form(app: FastAPI) -> None:
    operation = app.openapi()["paths"][URL]["post"]

    schema = operation["requestBody"]["content"]["multipart/form-data"]["schema"]
    assert schema["required"] == ["file"]
    assert set(schema["properties"]) == {"file", "owner_id"}
    assert {"202", "411", "413", "409", "422"} <= set(operation["responses"])


# --- GET /bulk-jobs/{job_id}/errors


async def add_errors(uow: UnitOfWork, job_id: uuid.UUID, rows: list[int]) -> None:
    await uow.session.execute(
        insert(ImportRowError),
        [
            {
                "job_id": job_id,
                "row_no": no,
                "code": "validation",
                "field": "email",
                "raw": {"email": f"bad-{no}", "first_name": "Іван"},
            }
            for no in rows
        ],
    )
    await uow.commit()


async def test_errors_are_paged_by_row_no(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob, uow: UnitOfWork
) -> None:
    lead = await member(make_user, org, "team_lead")
    job = await make_job(lead, kind="import_contacts", params={"columns": [], "delimiter": ","})
    job_id = job.id
    await add_errors(uow, job_id, [9, 2, 5])
    headers = await tokens_for(client, lead)
    url = f"/api/v1/bulk-jobs/{job_id}/errors"

    first = await client.get(url, params={"limit": 2}, headers=headers)
    cursor = first.json()["next_cursor"]
    second = await client.get(url, params={"limit": 2, "cursor": cursor}, headers=headers)

    assert first.status_code == second.status_code == 200, second.text
    assert [e["row_no"] for e in first.json()["items"]] == [2, 5]
    assert first.json()["items"][0] == {
        "row_no": 2,
        "code": "validation",
        "field": "email",
        "raw": {"email": "bad-2", "first_name": "Іван"},
    }
    assert [e["row_no"] for e in second.json()["items"]] == [9]
    assert second.json()["next_cursor"] is None


async def test_errors_of_invisible_job_are_404(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_org: MakeOrg,
    make_job: MakeJob,
    uow: UnitOfWork,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    other_team = await member(make_user, org, "team_lead", team=1)
    stranger = await member(make_user, await make_org(), "team_lead")
    job = await make_job(other_team, kind="import_contacts", params={})
    job_id = job.id
    await add_errors(uow, job_id, [1])
    url = f"/api/v1/bulk-jobs/{job_id}/errors"
    headers = [await tokens_for(client, user) for user in (lead, stranger)]

    for user_headers in headers:
        resp = await client.get(url, headers=user_headers)
        assert resp.status_code == 404, resp.text


async def test_errors_of_other_job_kind_are_empty_and_bad_cursor_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    lead = await member(make_user, org, "team_lead")
    job = await make_job(lead)  # reassign_owner — ошибок строк у неё нет
    headers = await tokens_for(client, lead)
    url = f"/api/v1/bulk-jobs/{job.id}/errors"

    empty = await client.get(url, headers=headers)
    broken = await client.get(url, params={"cursor": "not-a-cursor"}, headers=headers)

    assert empty.json() == {"items": [], "next_cursor": None}
    assert error(broken, 422)[0] == "pagination.invalid_cursor"
