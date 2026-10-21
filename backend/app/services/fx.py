"""Курсы НБУ и конвертер валют (раздел 3, «Деньги»).

Все суммы хранятся в USD. Пользователь вводит и видит любую валюту; пересчёт — по
официальному курсу НБУ на дату. НБУ публикует курс гривни к каждой валюте (rate_uah —
гривен за 1 единицу), поэтому любая пара считается через гривну.

get_rate(валюта, дата):
1. Redis (1 час) — курс прошлого дня не меняется, сегодняшний — до вечера тоже;
2. строка fx_rates на эту дату;
3. запрос к НБУ за все валюты на эту дату и upsert — так курсы подтягиваются по первому
   запросу дня (с этапа 5 их заранее загружает задача beat); НБУ ответил, а валюты в ответе
   нет — 422 fx.unknown_currency;
4. НБУ недоступен или ответил пусто — последняя известная строка и stale=true (не кэшируется);
5. строк нет совсем — 503 fx.unavailable.
"""

import contextlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal

import httpx
import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.errors import ServiceUnavailable, ValidationFailed
from app.uow import UnitOfWork

logger = structlog.get_logger("fx")

NBU_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"
NBU_TIMEOUT = 3.0
CACHE_TTL = 3600
CENT = Decimal("0.01")
RATE_PLACES = Decimal("0.000001")


@dataclass(frozen=True)
class Rate:
    currency: str
    rate_uah: Decimal  # гривен за 1 единицу валюты
    date: date  # на какую дату курс
    stale: bool = False  # НБУ недоступен — взят последний известный


@dataclass(frozen=True)
class Display:
    """Курс, по которому суммы из USD показываются в валюте ответа (?currency=)."""

    currency: str
    rate: Decimal  # единиц валюты за 1 USD
    date: date
    stale: bool

    def amount(self, amount_usd: Decimal) -> Decimal:
        # Агрегаты считаются в USD в SQL, а пересчитываются один раз — здесь (раздел 3).
        return (amount_usd * self.rate).quantize(CENT, ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Converted:
    """Сумма в USD и то, по какому курсу посчитано — для пяти полей сделки."""

    amount_usd: Decimal
    fx_rate: Decimal  # единиц исходной валюты за 1 USD
    fx_date: date
    stale: bool


def cross_rate(currency: Rate, usd: Rate) -> Decimal:
    """Сколько единиц валюты стоит 1 USD: rate_uah(USD) / rate_uah(валюта), 6 знаков."""
    return (usd.rate_uah / currency.rate_uah).quantize(RATE_PLACES, ROUND_HALF_EVEN)


def rate_date(*rates: Rate) -> date:
    """Дата, на которую взят курс: при резервном (stale) курсе — его дата, а не сегодняшняя.
    Если две валюты взяты за разные дни — более старая (раздел 3: fx_date — дата курса)."""
    return min(rate.date for rate in rates)


class NbuClient:
    def __init__(self, http: httpx.AsyncClient, url: str = NBU_URL) -> None:
        self.http = http
        self.url = url

    async def rates(self, on: date) -> dict[str, Decimal]:
        """Все курсы на дату: {"USD": Decimal("41.488"), ...}. Пусто — НБУ курса не дал."""
        resp = await self.http.get(
            self.url, params={"date": on.strftime("%Y%m%d"), "json": ""}, timeout=NBU_TIMEOUT
        )
        resp.raise_for_status()
        return {row["cc"]: Decimal(str(row["rate"])) for row in resp.json()}


class FxService:
    def __init__(self, uow: UnitOfWork, redis: Redis, nbu: NbuClient) -> None:
        self.uow = uow
        self.redis = redis
        self.nbu = nbu

    async def get_rate(self, currency: str, on: date) -> Rate:
        if currency == "UAH":
            return Rate("UAH", Decimal(1), on)
        key = f"fx:{currency}:{on.isoformat()}"
        cached = await self._cache_get(key)
        if cached is not None:
            return Rate(currency, Decimal(cached), on)
        row = await self.uow.fx_rates.get(currency, on)
        if row is None:
            fetched = await self._fetch(on)
            if fetched and currency not in fetched:
                raise ValidationFailed("fx.unknown_currency", field="currency")
            if fetched:
                row = await self.uow.fx_rates.get(currency, on)
        if row is not None:
            await self._cache_set(key, str(row.rate_uah))
            return Rate(currency, row.rate_uah, on)
        fallback = await self.uow.fx_rates.latest(currency, on)
        if fallback is None:
            raise ServiceUnavailable("fx.unavailable", failed=["nbu"])
        logger.warning(
            "fx_rate_stale", currency=currency, on=on.isoformat(), used=str(fallback.rate_date)
        )
        return Rate(currency, fallback.rate_uah, fallback.rate_date, stale=True)

    async def display(self, currency: str, on: date) -> Display:
        if currency == "USD":
            return Display("USD", Decimal(1), on, False)
        rate, usd = await self.get_rate(currency, on), await self.get_rate("USD", on)
        return Display(
            currency, cross_rate(rate, usd), rate_date(rate, usd), rate.stale or usd.stale
        )

    async def to_usd(self, amount: Decimal, currency: str, on: date) -> Converted:
        if currency == "USD":
            return Converted(amount, Decimal(1), on, False)
        rate, usd = await self.get_rate(currency, on), await self.get_rate("USD", on)
        fx_rate = cross_rate(rate, usd)
        amount_usd = (amount / fx_rate).quantize(CENT, ROUND_HALF_EVEN)
        return Converted(amount_usd, fx_rate, rate_date(rate, usd), rate.stale or usd.stale)

    async def _fetch(self, on: date) -> dict[str, Decimal]:
        """Курсы НБУ на дату → fx_rates. Пусто — НБУ недоступен или курса на дату ещё нет.

        Коммитит транзакцию запроса: курс сохраняется, даже если сам запрос потом упадёт.
        Поэтому сервисы получают курс до любых своих изменений и блокировок."""
        try:
            rates = await self.nbu.rates(on)
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            logger.warning("nbu_unavailable", error=repr(exc))
            return {}
        if rates:
            await self.uow.fx_rates.upsert(on, rates, datetime.now(UTC))
            await self.uow.commit()
        return rates

    async def _cache_get(self, key: str) -> str | None:
        try:
            value = await self.redis.get(key)
        except RedisError:
            return None  # кэш — только ускорение: без Redis идём в базу
        return value.decode() if isinstance(value, bytes) else value

    async def _cache_set(self, key: str, value: str) -> None:
        with contextlib.suppress(RedisError):  # не записали в кэш — в следующий раз из базы
            await self.redis.set(key, value, ex=CACHE_TTL)
