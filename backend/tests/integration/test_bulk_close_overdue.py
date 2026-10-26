"""POST /bulk/close-overdue-deals (раздел 6, «Остальные массовые операции и выгрузка», шаг 5.7).

Запуск — через API (TaskQueue — фейк), plan и чанки — фикстурой run_job в транзакции теста
(чанки по 2 строки). Права ролей на маршрут — в test_access_matrix.py.

Часы сервера заморожены (frozen_clock): «сегодня» теста и сервера считаются от одного момента,
иначе тест, попавший на полночь по Киеву, видел бы два разных «сегодня».
"""

import uuid
from datetime import UTC, date, datetime, timedelta, tzinfo
from typing import Any, Self
from zoneinfo import ZoneInfo

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select, update

from app.models import AuditEntry, BulkJob, Deal, DealStatus, Organization, OutboxEvent, User
from app.services import fx
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

URL = "/api/v1/bulk/close-overdue-deals"
TZ = "Europe/Kyiv"  # часовой пояс организаций по умолчанию
# 12:00 UTC: в Киеве то же число, в Pacific/Kiritimati (UTC+14) — уже следующее.
NOW = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz: tzinfo | None = None) -> Self:
        return cls.fromtimestamp(NOW.timestamp(), tz)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """org_today сервера (app.services.fx) читает часы отсюда."""
    monkeypatch.setattr(fx, "datetime", FrozenDatetime)


def org_today(tz: str = TZ) -> date:
    return NOW.astimezone(ZoneInfo(tz)).date()


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


async def post(
    client: AsyncClient,
    author: User | dict[str, str],
    body: dict[str, Any],
    key: str | None = None,
) -> Response:
    headers = dict(author if isinstance(author, dict) else await tokens_for(client, author))
    if key is not None:
        headers["Idempotency-Key"] = key
    return await client.post(URL, json=body, headers=headers)


async def deal_row(uow: UnitOfWork, deal_id: int) -> Any:
    return (
        await uow.session.execute(
            select(Deal.status, Deal.closed_at, Deal.version, Deal.stage_id).where(
                Deal.id == deal_id
            )
        )
    ).one()


async def job_of(uow: UnitOfWork, job_id: str) -> BulkJob:
    job = await uow.bulk_jobs.get(uuid.UUID(job_id))
    assert job is not None
    return job


async def test_closes_only_overdue_open_alive_deals_in_author_level(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
    make_pipeline: Any,
    make_deal: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    rep = await member(make_user, org, "sales_rep", team=0)
    other_team = await member(make_user, org, "sales_rep", team=1)
    stranger_org = await make_org()
    stranger = await member(make_user, stranger_org, "sales_rep")
    pipeline = await make_pipeline(org.id)
    stage = pipeline.stages[0]
    past = org_today() - timedelta(days=3)
    overdue = [
        (await make_deal(rep, stage, expected_close_date=past)).id,
        (await make_deal(lead, stage, expected_close_date=past)).id,
        (await make_deal(rep, stage, expected_close_date=past - timedelta(days=30))).id,
    ]
    untouched = [
        (await make_deal(other_team, stage, expected_close_date=past)).id,  # чужой отдел
        (
            await make_deal(rep, stage, expected_close_date=org_today())
        ).id,  # сегодня — не просрочена
        (await make_deal(rep, stage)).id,  # без даты закрытия
        (
            await make_deal(
                rep, stage, expected_close_date=past, status="won", closed_at=func.now()
            )
        ).id,
        (
            await make_deal(
                rep, stage, expected_close_date=past, status="lost", closed_at=func.now()
            )
        ).id,
        (await make_deal(rep, stage, expected_close_date=past, deleted_at=func.now())).id,
        (
            await make_deal(
                stranger, (await make_pipeline(stranger_org.id)).stages[0], expected_close_date=past
            )
        ).id,
    ]
    before = {d: await deal_row(uow, d) for d in untouched}
    stage_id = stage.id

    resp = await post(client, lead, {})
    assert resp.status_code == 202, resp.text
    job = resp.json()
    assert resp.headers["location"] == f"/api/v1/bulk-jobs/{job['id']}"
    assert job["kind"] == "close_overdue_deals"
    await run_job(job["id"])

    for deal_id in overdue:
        row = await deal_row(uow, deal_id)
        assert row.status == DealStatus.LOST
        assert row.closed_at is not None
        assert row.version == 2
        assert row.stage_id == stage_id  # этап не меняется
    for deal_id, row in before.items():
        assert await deal_row(uow, deal_id) == row
    stored = await job_of(uow, job["id"])
    assert (stored.status, stored.total, stored.processed, stored.failed_rows) == ("done", 3, 3, 0)
    assert stored.result == {"rows": {"deals": 3}}
    assert stored.chunks_total == 2  # чанки по 2 строки

    # Одна строка журнала на job; событий deal.closed по каждой сделке нет.
    [entry] = list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == job["id"]
            )
        )
    )
    assert entry.action == "bulk.close_overdue_deals"
    assert entry.changes == {
        "before": org_today().isoformat(),
        "pipeline_id": None,
        "status": "done",
        "rows": {"deals": 3},
    }
    closed_events = await uow.session.scalar(
        select(func.count()).select_from(OutboxEvent).where(OutboxEvent.event_type == "deal.closed")
    )
    assert closed_events == 0


async def test_rerun_finds_nothing_and_keeps_versions(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_pipeline: Any,
    make_deal: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    stage = (await make_pipeline(org.id)).stages[0]
    deal_id = (await make_deal(rep, stage, expected_close_date=date(2020, 1, 1))).id
    headers = await tokens_for(client, admin)

    first = (await post(client, headers, {})).json()
    await run_job(first["id"])
    await run_job(first["id"])  # повтор plan и чанков ничего не делает
    second = await post(client, headers, {})
    assert second.status_code == 202, second.text
    await run_job(second.json()["id"])

    assert (await deal_row(uow, deal_id)).version == 2  # закрыта один раз
    assert (await job_of(uow, first["id"])).processed == 1
    repeated = await job_of(uow, second.json()["id"])
    assert (repeated.status, repeated.total, repeated.processed) == ("done", 0, 0)


async def test_before_defaults_to_org_today_in_its_timezone(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    uow: UnitOfWork,
) -> None:
    # UTC+14: «сегодня» организации почти всегда не то же, что по UTC.
    tz = "Pacific/Kiritimati"
    await uow.session.execute(
        update(Organization).where(Organization.id == org.id).values(timezone=tz)
    )
    await uow.commit()
    admin = await member(make_user, org, "org_admin")

    resp = await post(client, admin, {})

    assert resp.status_code == 202, resp.text
    stored = await job_of(uow, resp.json()["id"])
    assert stored.params["before"] == org_today(tz).isoformat() == "2026-03-11"  # не UTC-дата
    assert stored.params["pipeline_id"] is None


async def test_before_after_today_is_422_and_today_is_allowed(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    headers = await tokens_for(client, admin)
    tomorrow = org_today() + timedelta(days=1)

    resp = await post(client, headers, {"before": tomorrow.isoformat()})
    assert resp.status_code == 422, resp.text
    assert resp.json()["code"] == "validation"
    assert resp.json()["details"]["field"] == "before"

    resp = await post(client, headers, {"before": "not-a-date"})
    assert resp.status_code == 422

    resp = await post(client, headers, {"before": org_today().isoformat()})
    assert resp.status_code == 202, resp.text


@pytest.mark.parametrize("body", [{"pipelineId": 3}, {"pipeline": 3}, {"befor": "2024-01-01"}])
async def test_unknown_field_is_422(
    body: dict[str, Any], client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    # Опечатка не расширяет операцию до умолчаний: все воронки и before = сегодня.
    admin = await member(make_user, org, "org_admin")
    resp = await post(client, admin, body)
    assert resp.status_code == 422, resp.text
    assert resp.json()["code"] == "validation"


async def test_pipeline_filter_and_foreign_pipeline_is_422(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    org: OrgFixture,
    make_pipeline: Any,
    make_deal: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    first, second = await make_pipeline(org.id), await make_pipeline(org.id)
    foreign = await make_pipeline((await make_org()).id)
    past = date(2024, 5, 1)
    in_first = (await make_deal(rep, first.stages[1], expected_close_date=past)).id
    in_second = (await make_deal(rep, second.stages[0], expected_close_date=past)).id
    first_id, foreign_id = first.id, foreign.id
    headers = await tokens_for(client, admin)

    resp = await post(client, headers, {"pipeline_id": foreign_id})
    assert resp.status_code == 422, resp.text
    assert resp.json()["details"] == {"field": "pipeline_id"}

    resp = await post(client, headers, {"pipeline_id": first_id, "before": "2024-06-01"})
    assert resp.status_code == 202, resp.text
    await run_job(resp.json()["id"])

    assert (await deal_row(uow, in_first)).status == DealStatus.LOST
    assert (await deal_row(uow, in_second)).status == DealStatus.OPEN
    stored = await job_of(uow, resp.json()["id"])
    assert stored.params["before"] == "2024-06-01"
    assert stored.params["pipeline_id"] == first_id


async def test_before_is_fixed_in_params_for_all_chunks(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_pipeline: Any,
    make_deal: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    admin = await member(make_user, org, "org_admin")
    rep = await member(make_user, org, "sales_rep")
    stage = (await make_pipeline(org.id)).stages[0]
    older = (await make_deal(rep, stage, expected_close_date=date(2024, 1, 1))).id
    newer = (await make_deal(rep, stage, expected_close_date=date(2024, 3, 1))).id

    resp = await post(client, admin, {"before": "2024-02-01"})
    await run_job(resp.json()["id"])

    assert (await deal_row(uow, older)).status == DealStatus.LOST
    assert (await deal_row(uow, newer)).status == DealStatus.OPEN


async def test_same_key_replays_job_even_on_another_day(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture, uow: UnitOfWork
) -> None:
    admin = await member(make_user, org, "org_admin")
    headers = await tokens_for(client, admin)

    first = await post(client, headers, {}, key="close-1")
    # Хэш — от тела как пришло: before не передан, а дата в params — первого запуска.
    await uow.session.execute(
        update(BulkJob)
        .where(BulkJob.id == uuid.UUID(first.json()["id"]))
        .values(params=BulkJob.params.op("||")(func.jsonb_build_object("before", "2000-01-01")))
    )
    await uow.commit()
    again = await post(client, headers, {}, key="close-1")
    other = await post(client, headers, {"before": "2024-01-01"}, key="close-1")

    assert again.status_code == 202, again.text
    assert again.json()["id"] == first.json()["id"]
    assert other.status_code == 422, other.text
    assert other.json()["code"] == "bulk.idempotency_key_reused"


async def test_second_active_job_is_409(
    client: AsyncClient, make_user: MakeUser, org: OrgFixture
) -> None:
    admin = await member(make_user, org, "org_admin")
    lead = await member(make_user, org, "team_lead")
    first = await post(client, admin, {})

    resp = await post(client, lead, {})

    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == "bulk.already_running"
    assert first.status_code == 202


async def test_author_changed_skips_chunks(
    client: AsyncClient,
    make_user: MakeUser,
    org: OrgFixture,
    make_pipeline: Any,
    make_deal: MakeRecord,
    run_job: RunJob,
    uow: UnitOfWork,
) -> None:
    lead = await member(make_user, org, "team_lead", team=0)
    rep = await member(make_user, org, "sales_rep", team=0)
    stage = (await make_pipeline(org.id)).stages[0]
    deal_id = (await make_deal(rep, stage, expected_close_date=date(2024, 1, 1))).id
    lead_id = lead.id
    resp = await post(client, lead, {})
    await uow.session.execute(update(User).where(User.id == lead_id).values(is_active=False))
    await uow.commit()

    await run_job(resp.json()["id"])

    assert (await deal_row(uow, deal_id)).status == DealStatus.OPEN
    stored = await job_of(uow, resp.json()["id"])
    assert (stored.status, stored.error) == ("failed", "bulk.author_changed")
