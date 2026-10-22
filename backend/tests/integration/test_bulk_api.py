"""POST /bulk/reassign-owner и чтение job (разделы 6 и 15): коды ошибок, повтор по
Idempotency-Key, SSE. Права ролей на эти маршруты — в test_access_matrix.py.

TaskQueue — фейк (conftest): тест видит, что API поставил bulk.plan после коммита, но задача
никуда не уходит, поэтому job остаётся pending.
"""

import json
import uuid
from typing import Any

import pytest
from httpx import AsyncClient, Response

from app.models import BulkJob, User
from app.repositories.bulk import BulkJobRepository
from app.services.bulk import REASSIGN_INCLUDE, request_hash
from tests.integration.conftest import (
    FakeTaskQueue,
    MakeJob,
    MakeOrg,
    MakeUser,
    OrgFixture,
    member,
    tokens_for,
)

URL = "/api/v1/bulk/reassign-owner"


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


async def post(
    client: AsyncClient,
    author: User | dict[str, str],
    body: dict[str, Any],
    key: str | bytes | None = None,
) -> Response:
    """author — пользователь или уже готовые заголовки: после ответа 4xx сессия теста
    откатывается, и ORM-объекты «протухают» — второй запрос берёт заголовки из переменной."""
    headers: dict[str, Any] = dict(
        author if isinstance(author, dict) else await tokens_for(client, author)
    )
    if key is not None:
        headers["Idempotency-Key"] = key
    return await client.post(URL, json=body, headers=headers)


def body(from_user: User, to_user: User, **extra: Any) -> dict[str, Any]:
    return {"from_user_id": str(from_user.id), "to_user_id": str(to_user.id), **extra}


async def test_start_returns_202_with_location_and_enqueues_plan(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    task_queue: FakeTaskQueue,
    db_session: Any,
) -> None:
    admin = await member(make_user, org, "org_admin")
    leaving = await member(make_user, org, "sales_rep")
    heir = await member(make_user, org, "sales_rep", team=1)
    admin_id = admin.id

    resp = await post(client, admin, body(leaving, heir))

    assert resp.status_code == 202, resp.text
    job = resp.json()
    assert resp.headers["location"] == f"/api/v1/bulk-jobs/{job['id']}"
    assert (job["kind"], job["status"], job["total"], job["processed"]) == (
        "reassign_owner",
        "pending",
        0,
        0,
    )
    assert task_queue.enqueued == [("bulk.plan", (job["id"],))]
    stored = await BulkJobRepository(db_session).get(uuid.UUID(job["id"]))
    assert stored is not None
    assert stored.owner_id == admin_id and stored.team_id is None
    assert stored.params["include"] == list(REASSIGN_INCLUDE)  # по умолчанию — всё
    assert stored.params["access"]["action"] == "bulk:run"
    assert stored.params["access"]["scope"] == "org"


async def test_from_user_outside_member_read_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_org: MakeOrg
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)  # руководитель его не видит
    heir = await member(make_user, org, "sales_rep", team=0)
    stranger = await member(make_user, await make_org(), "sales_rep")
    headers = await tokens_for(client, lead)
    bodies = [body(source, heir) for source in (other_team, stranger)]

    for request in bodies:
        resp = await post(client, headers, request)
        assert resp.status_code == 422, resp.text
        assert resp.json()["code"] == "validation"
        assert resp.json()["details"] == {"field": "from_user_id"}


async def test_same_owner_is_422(client: AsyncClient, make_user: MakeUser, org: OrgFixture) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")

    resp = await post(client, admin, body(rep, rep))

    assert resp.status_code == 422
    assert resp.json()["code"] == "bulk.same_owner"


@pytest.mark.parametrize("target", ["viewer", "inactive", "other_org"])
async def test_target_who_cannot_own_records_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_org: MakeOrg, target: str
) -> None:
    admin = await member(make_user, org, "org_admin")
    leaving = await member(make_user, org, "sales_rep")
    match target:
        case "viewer":  # viewer и org_admin записей не ведут
            to_user = await member(make_user, org, "viewer")
        case "inactive":
            to_user = await member(make_user, org, "sales_rep", is_active=False)
        case _:
            to_user = await member(make_user, await make_org(), "sales_rep")

    resp = await post(client, admin, body(leaving, to_user))

    assert resp.status_code == 422, resp.text
    assert resp.json()["code"] == "owner.invalid"
    assert resp.json()["details"] == {"field": "to_user_id"}


async def test_target_outside_author_team_is_403(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    leaving = await member(make_user, org, "sales_rep", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)

    resp = await post(client, lead, body(leaving, other_team))

    assert resp.status_code == 403
    assert resp.json()["details"] == {"reason": "out_of_scope"}


async def test_second_active_job_in_org_is_409_with_job_id(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, task_queue: FakeTaskQueue
) -> None:
    admin = await member(make_user, org, "org_admin")
    lead = await member(make_user, org, "team_lead", team=0)
    a, b, c = [await member(make_user, org, "sales_rep") for _ in range(3)]
    first = await post(client, lead, body(a, b))
    assert first.status_code == 202

    # Другой автор, другие люди — но вид тот же, а первая job ещё pending.
    resp = await post(client, admin, body(c, b))

    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "bulk.already_running"
    assert resp.json()["details"] == {"job_id": first.json()["id"]}  # админ её видит
    assert len(task_queue.enqueued) == 1


async def test_409_hides_id_of_job_outside_bulk_job_read(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    lead = await member(make_user, org, "team_lead", team=0)
    a, b, c = [await member(make_user, org, "sales_rep") for _ in range(3)]
    assert (await post(client, admin, body(a, b))).status_code == 202

    resp = await post(client, lead, body(c, b))  # job админа (team_id NULL) руководитель не видит

    assert (resp.status_code, resp.json()["code"]) == (409, "bulk.already_running")
    assert resp.json()["details"] == {"job_id": None}  # не ссылка на 404


async def test_finished_job_does_not_block_next(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")
    await make_job(admin, status="partial_failed")

    assert (await post(client, admin, body(a, b))).status_code == 202


async def test_same_key_and_body_replays_job(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, task_queue: FakeTaskQueue
) -> None:
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")

    first = await post(client, admin, body(a, b), key="key-1")
    # include по умолчанию и явно все четыре — тот же запрос.
    again = await post(client, admin, body(a, b, include=list(reversed(REASSIGN_INCLUDE))), "key-1")

    assert (first.status_code, again.status_code) == (202, 202)
    assert again.json()["id"] == first.json()["id"]
    assert again.headers["location"] == first.headers["location"]
    assert len(task_queue.enqueued) == 1  # повтор задачу не ставит


async def test_same_key_with_other_body_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")
    assert (await post(client, admin, body(a, b), key="key-2")).status_code == 202

    resp = await post(client, admin, body(a, b, include=["contacts"]), key="key-2")

    assert resp.status_code == 422
    assert resp.json()["code"] == "bulk.idempotency_key_reused"


async def test_key_is_per_author(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_org: MakeOrg
) -> None:
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")
    other_org = await make_org()
    other_admin = await member(make_user, other_org, "org_admin")
    c = await member(make_user, other_org, "sales_rep")
    d = await member(make_user, other_org, "sales_rep")

    first = await post(client, admin, body(a, b), key="shared")
    second = await post(client, other_admin, body(c, d), key="shared")

    assert (first.status_code, second.status_code) == (202, 202)
    assert first.json()["id"] != second.json()["id"]


async def test_key_race_returns_job_of_first_request(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_job: MakeJob,
    task_queue: FakeTaskQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Два одинаковых запроса одновременно: оба не нашли job по ключу, второй INSERT ловит
    уникальный индекс — откат, перечитать по ключу и вернуть job первого."""
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")
    params = {"from_user_id": str(a.id), "to_user_id": str(b.id), "include": list(REASSIGN_INCLUDE)}
    winner = await make_job(
        admin,
        status="running",  # первая job уже идёт: слот вида занят ею же
        idempotency_key="race",
        request_hash=request_hash("reassign_owner", params),
        params=params,
    )
    winner_id = str(winner.id)
    calls: list[str] = []
    real_find = BulkJobRepository.find_by_key

    async def find_too_early(
        self: BulkJobRepository, owner_id: uuid.UUID, key: str
    ) -> BulkJob | None:
        calls.append(key)
        if len(calls) == 1:
            return None  # первая проверка — до того, как первый запрос закоммитил
        return await real_find(self, owner_id, key)

    monkeypatch.setattr(BulkJobRepository, "find_by_key", find_too_early)

    resp = await post(client, admin, body(a, b), key="race")

    assert resp.status_code == 202, resp.text
    assert resp.json()["id"] == winner_id
    assert calls == ["race", "race"]
    assert task_queue.enqueued == []


@pytest.mark.parametrize(
    "key", ["", "x" * 65, "ключ".encode(), b"tab\there"], ids=["empty", "long", "utf8", "tab"]
)
async def test_bad_idempotency_key_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, key: str | bytes
) -> None:
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")

    resp = await post(client, admin, body(a, b), key=key)

    assert resp.status_code == 422, resp.text
    assert resp.json()["details"]["errors"][0]["loc"] == ["header", "Idempotency-Key"]


async def test_empty_include_is_422(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    a, b = await member(make_user, org, "sales_rep"), await member(make_user, org, "sales_rep")

    headers = await tokens_for(client, admin)
    bodies = [body(a, b, include=include) for include in ([], ["deals"])]  # пусто, неизвестное

    for request in bodies:
        resp = await post(client, headers, request)
        assert resp.status_code == 422, resp.text


async def test_get_job_returns_progress(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    job = await make_job(lead, status="running", total=10, processed=4, chunks_total=2, result=None)
    job_id = str(job.id)

    resp = await client.get(f"/api/v1/bulk-jobs/{job_id}", headers=await tokens_for(client, lead))

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == job_id
    assert {k: data[k] for k in ("status", "total", "processed", "chunks_total", "error")} == {
        "status": "running",
        "total": 10,
        "processed": 4,
        "chunks_total": 2,
        "error": None,
    }


async def test_author_sees_own_job_after_moving_to_other_team(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    lead = await member(make_user, org, "team_lead", team=0)
    lead_headers = await tokens_for(client, lead)
    job_id = str((await make_job(lead, status="failed", error="bulk.author_changed")).id)
    moved = await client.patch(
        f"/api/v1/users/{lead.id}", json={"team_id": str(org.team_ids[1])}, headers=admin
    )
    assert moved.status_code == 200, moved.text

    # team_id job — отдел автора при запуске (0), а сам он уже в отделе 1: своя job видна всё
    # равно, иначе причину провала (bulk.author_changed) автор бы не увидел.
    resp = await client.get(f"/api/v1/bulk-jobs/{job_id}", headers=lead_headers)

    assert resp.status_code == 200, resp.text
    assert resp.json()["error"] == "bulk.author_changed"


def parse_sse(text: str) -> list[dict[str, str]]:
    events = []
    for block in text.strip().split("\n\n"):
        fields: dict[str, str] = {}
        for line in block.splitlines():
            name, _, value = line.partition(": ")
            fields[name] = value
        events.append(fields)
    return events


async def test_events_stream_snapshot_and_end_for_finished_job(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    admin = await member(make_user, org, "org_admin")
    job = await make_job(admin, status="done", total=3, processed=3, result={"rows": {"x": 3}})
    job_id = str(job.id)

    resp = await client.get(
        f"/api/v1/bulk-jobs/{job_id}/events", headers=await tokens_for(client, admin)
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    progress, end = parse_sse(resp.text)
    assert progress["event"] == "progress"
    assert progress["retry"] == "3000"  # клиент переподключится через 3 с
    assert json.loads(progress["data"])["processed"] == 3
    assert end["event"] == "end"
    assert json.loads(end["data"])["status"] == "done"


async def test_events_of_invisible_job_is_404_before_stream(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, make_job: MakeJob
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    other_lead = await member(make_user, org, "team_lead", team=1)
    job_id = str((await make_job(other_lead)).id)

    resp = await client.get(
        f"/api/v1/bulk-jobs/{job_id}/events", headers=await tokens_for(client, lead)
    )

    assert resp.status_code == 404
    assert resp.json()["details"] == {"entity": "bulk_job", "id": job_id}
