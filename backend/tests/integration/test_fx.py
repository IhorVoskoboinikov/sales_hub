"""Конвертер валют по курсам НБУ (раздел 3, «Деньги») — шаг 4.4.

НБУ подменён фикстурой nbu (respx): USD = 41.488 грн, EUR = 46.5 грн.
"""

from datetime import date
from decimal import Decimal

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis

from app.errors import ServiceUnavailable, ValidationFailed
from app.services.fx import FxService, NbuClient
from app.uow import UnitOfWork
from tests.integration.conftest import (
    NBU_URL,
    FakeNbu,
    MakeOrg,
    MakeRecord,
    MakeUser,
    member,
    tokens_for,
)

SATURDAY = date(2026, 10, 17)
FRIDAY = date(2026, 10, 16)


@pytest.fixture
async def fx(uow: UnitOfWork, redis_url: str) -> FxService:
    import httpx

    return FxService(uow, Redis.from_url(redis_url), NbuClient(httpx.AsyncClient(), NBU_URL))


async def test_rate_is_fetched_once_then_read_from_db_and_cache(
    fx: FxService, nbu: FakeNbu
) -> None:
    first = await fx.get_rate("USD", FRIDAY)
    again = await fx.get_rate("EUR", FRIDAY)  # все валюты дня загружены одним запросом

    assert (first.rate_uah, first.stale) == (Decimal("41.488"), False)
    assert again.rate_uah == Decimal("46.5")
    assert nbu.dates == ["20261016"]


async def test_uah_to_usd_keeps_original_and_rate(fx: FxService) -> None:
    converted = await fx.to_usd(Decimal("100000"), "UAH", FRIDAY)

    assert converted.fx_rate == Decimal("41.488000")  # гривен за 1 USD
    assert converted.amount_usd == Decimal("2410.34")  # 100000 / 41.488 = 2410.3355…
    # Обратный пересчёт не даёт ровно 100 000 — поэтому исходную сумму и храним (раздел 3).
    back = converted.amount_usd * converted.fx_rate
    assert abs(back - Decimal("100000")) <= converted.fx_rate * Decimal("0.005")


async def test_cross_rate_through_hryvnia(fx: FxService) -> None:
    converted = await fx.to_usd(Decimal("1000"), "EUR", FRIDAY)

    assert converted.fx_rate == Decimal("0.892215")  # 41.488 / 46.5 евро за 1 USD
    assert converted.amount_usd == Decimal("1120.81")


async def test_weekend_rate_comes_from_nbu_for_that_day(fx: FxService, nbu: FakeNbu) -> None:
    # НБУ отдаёт курс и на выходные — тот, что действует в этот день (установлен в пятницу).
    rate = await fx.get_rate("USD", SATURDAY)

    assert (rate.date, rate.stale) == (SATURDAY, False)
    assert nbu.dates == ["20261017"]


async def test_nbu_down_falls_back_to_last_known_rate(fx: FxService, nbu: FakeNbu) -> None:
    await fx.get_rate("USD", FRIDAY)  # пятничный курс есть в базе
    nbu.down = True

    rate = await fx.get_rate("USD", SATURDAY)
    again = await fx.get_rate("USD", SATURDAY)

    assert (rate.rate_uah, rate.date, rate.stale) == (Decimal("41.488"), FRIDAY, True)
    assert again.stale  # устаревший курс не кэшируется: следующий запрос снова спросит НБУ
    assert nbu.dates.count("20261017") == 2


async def test_holiday_or_future_date_without_nbu_rate_uses_last_known(
    fx: FxService, nbu: FakeNbu
) -> None:
    await fx.get_rate("USD", FRIDAY)
    nbu.rates = {}  # на будущую дату НБУ отвечает пустым списком

    rate = await fx.get_rate("USD", date(2026, 10, 20))

    assert (rate.date, rate.stale) == (FRIDAY, True)


async def test_no_rates_at_all_is_503_and_unknown_currency_is_422(
    fx: FxService, nbu: FakeNbu
) -> None:
    nbu.down = True
    with pytest.raises(ServiceUnavailable) as down:
        await fx.get_rate("USD", FRIDAY)
    nbu.down = False
    await fx.get_rate("USD", FRIDAY)
    with pytest.raises(ValidationFailed) as unknown:
        await fx.get_rate("XYZ", FRIDAY)

    assert down.value.code == "fx.unavailable"
    assert unknown.value.code == "fx.unknown_currency"


async def test_deal_in_hryvnia_is_stored_in_usd_and_shown_in_any_currency(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_pipeline: MakeRecord
) -> None:
    org = await make_org()
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))
    stage_id = (await make_pipeline(org.id)).stages[0].id

    created = await client.post(
        "/api/v1/deals",
        json={"title": "Склад", "amount": "100000", "currency": "UAH", "stage_id": stage_id},
        headers=rep,
    )
    in_eur = await client.get("/api/v1/deals", params={"currency": "EUR"}, headers=rep)
    default = await client.get("/api/v1/deals", headers=rep)  # валюта организации — UAH
    renamed = await client.patch(
        f"/api/v1/deals/{created.json()['id']}",
        json={"version": 1, "title": "Склад 2"},
        headers=rep,
    )

    deal = created.json()
    assert (deal["amount_original"], deal["currency_original"]) == ("100000.00", "UAH")
    assert (deal["amount_usd"], deal["fx_rate"]) == ("2410.34", "41.488000")
    eur = in_eur.json()
    assert (eur["fx"]["currency"], eur["fx"]["rate"], eur["fx"]["stale"]) == (
        "EUR",
        "0.892215",
        False,
    )
    assert eur["items"][0]["amount"] == "2150.54"  # 2410.34 × 0.892215 = 2150.5355
    assert default.json()["items"][0]["amount"] == "100000.19"  # не ровно 100 000: округление
    assert renamed.json()["fx_rate"] == "41.488000"  # сумма не менялась — курс не пересчитан


async def test_unknown_currency_in_deal_is_422(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, make_pipeline: MakeRecord
) -> None:
    org = await make_org()
    rep = await tokens_for(client, await member(make_user, org, "sales_rep"))
    stage_id = (await make_pipeline(org.id)).stages[0].id

    resp = await client.post(
        "/api/v1/deals",
        json={"title": "X", "amount": "1", "currency": "XYZ", "stage_id": stage_id},
        headers=rep,
    )

    assert (resp.status_code, resp.json()["code"]) == (422, "fx.unknown_currency")
