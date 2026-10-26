"""Отчёты из materialized view deal_stats — шаг 4.7; обновление под advisory lock (задача beat
reports.refresh и make refresh-reports) — шаг 5.8."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.cli import refresh_reports_command
from app.core.config import Settings
from app.services.reports import refresh_reports
from app.uow import UnitOfWork
from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, member, tokens_for


async def test_reports_show_only_my_level_after_refresh(
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    uow: UnitOfWork,
) -> None:
    org, other = await make_org(), await make_org()
    pipeline = await make_pipeline(org.id)
    new, offer = pipeline.stages
    rep = await member(make_user, org, "sales_rep", team=0)
    rep2 = await member(make_user, org, "sales_rep", team=1)
    stranger = await member(make_user, other, "sales_rep")
    this_month = datetime.now(UTC)
    await make_deal(rep, new, amount_usd=Decimal(100))
    await make_deal(rep, offer, amount_usd=Decimal(50))
    await make_deal(rep, new, amount_usd=Decimal(70), status="won", closed_at=this_month)
    await make_deal(rep2, new, amount_usd=Decimal(1000))
    await make_deal(rep2, new, amount_usd=Decimal(9), deleted_at=this_month)  # корзина — нет
    await make_deal(stranger, (await make_pipeline(other.id)).stages[0])
    await uow.reports.refresh()  # до обновления отчёт не видит новых сделок
    rep_id, rep2_id, pipeline_id = str(rep.id), str(rep2.id), pipeline.id
    as_rep = await tokens_for(client, rep)
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    url = f"/api/v1/reports/pipeline?pipeline_id={pipeline_id}&currency=USD"

    rep_pipeline = (await client.get(url, headers=as_rep)).json()["stages"]
    org_pipeline = (await client.get(url, headers=admin)).json()["stages"]
    rep_managers = (await client.get("/api/v1/reports/managers", headers=as_rep)).json()
    org_managers = (await client.get("/api/v1/reports/managers?currency=USD", headers=admin)).json()

    assert [(s["deals"], Decimal(s["amount_usd"])) for s in rep_pipeline] == [
        (1, Decimal(100)),
        (1, Decimal(50)),
    ]
    assert [s["deals"] for s in org_pipeline] == [2, 1]
    assert [m["owner_id"] for m in rep_managers["managers"]] == [rep_id]
    by_owner = {m["owner_id"]: m for m in org_managers["managers"]}
    assert set(by_owner) == {rep_id, rep2_id}  # чужая организация не видна
    mine = by_owner[rep_id]
    assert (mine["open_deals"], Decimal(mine["open_amount"])) == (2, Decimal(150))
    assert (mine["won_deals"], Decimal(mine["won_amount"])) == (1, Decimal(70))
    assert by_owner[rep2_id]["open_deals"] == 1


async def test_managers_period_and_foreign_pipeline(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_pipeline: MakeRecord
) -> None:
    org, other = await make_org(), await make_org()
    admin = await tokens_for(client, await member(make_user, org, "org_admin"))
    foreign_id = (await make_pipeline(other.id)).id  # до ответов 4xx: они откатывают сессию

    bad_period = await client.get(
        "/api/v1/reports/managers", params={"from": "2026-05", "to": "2026-01"}, headers=admin
    )
    bad_month = await client.get(
        "/api/v1/reports/managers", params={"from": "2026-13"}, headers=admin
    )
    year_zero = await client.get(
        "/api/v1/reports/managers", params={"to": "0000-01"}, headers=admin
    )
    hidden = await client.get(f"/api/v1/reports/pipeline?pipeline_id={foreign_id}", headers=admin)

    assert (bad_period.status_code, bad_month.status_code, hidden.status_code) == (422, 422, 404)
    assert year_zero.status_code == 422  # года 0 в календаре Python нет — не 500


LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended('reports.refresh', 0))")


async def test_refresh_is_skipped_while_another_session_refreshes(
    uow: UnitOfWork,
    postgres_url: str,
    make_user: MakeUser,
    make_org: MakeOrg,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    settings: Settings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    await make_deal(rep, (await make_pipeline(org.id)).stages[0])
    stats = text("SELECT count(*) FROM deal_stats WHERE org_id = :org")
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    try:
        async with engine.connect() as other:
            await other.execute(LOCK)  # другая сессия сейчас обновляет отчёты
            skipped = await refresh_reports(uow)
            assert await refresh_reports_command(settings) == 0  # CLI — тот же путь
            assert await uow.session.scalar(stats, {"org": org.id}) == 0  # REFRESH не было
            await other.rollback()  # обновление закончилось — блокировка отпущена
        refreshed = await refresh_reports(uow)
    finally:
        await engine.dispose()

    assert (skipped, refreshed) == (False, True)
    assert await uow.session.scalar(stats, {"org": org.id}) == 1
    assert "отчёты уже обновляются — пропуск" in capsys.readouterr().out
