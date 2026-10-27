"""AgendaService (раздел 16): повестка дня для /today и утреннего дайджеста, сводка /deals.

Граница дней — полночь по часовому поясу пользователя (coalesce(users.timezone,
organizations.timezone)); now передаётся явно, как дайджест (шаг 5.14) передаст время тика.
"""

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from redis.asyncio import Redis

from app.authz.roles import RoleCatalog
from app.errors import Forbidden, ServiceUnavailable
from app.services.agenda import (
    DEALS_LIMIT,
    OVERDUE_LIMIT,
    SUBJECT_MAX,
    TODAY_LIMIT,
    Agenda,
    AgendaItem,
    AgendaService,
    DealLine,
    DealsSummary,
    render_agenda,
    render_deals,
    utf16_len,
)
from app.services.fx import FxService, NbuClient, kyiv_today
from app.texts import uk
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    NBU_URL,
    FakeNbu,
    MakeOrg,
    MakeRecord,
    MakeUser,
    member,
    roles_without,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
# 27.10.2026 22:30 UTC — уже 28.10 00:30 в Киеве (UTC+2 после перехода на зимнее время 25.10),
# но ещё 27.10 18:30 в Нью-Йорке (UTC−4 до 1.11).
NOW = datetime(2026, 10, 27, 22, 30, tzinfo=UTC)
KYIV = ZoneInfo("Europe/Kyiv")


def utc(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC)


@pytest.fixture
async def make_agenda(uow: UnitOfWork, redis_url: str) -> AsyncIterator[Any]:
    async with httpx.AsyncClient() as http:
        redis = Redis.from_url(redis_url)

        def make(roles: RoleCatalog = CATALOG) -> AgendaService:
            return AgendaService(uow, roles, FxService(uow, redis, NbuClient(http, NBU_URL)))

        yield make
        await redis.aclose()


@pytest.fixture
def agenda(make_agenda: Any) -> AgendaService:
    return make_agenda()  # type: ignore[no-any-return]


def subjects(items: list[AgendaItem]) -> list[str]:
    return [item.subject for item in items]


async def test_day_splits_by_midnight_in_org_time_zone(
    agenda: AgendaService, make_org: MakeOrg, make_user: MakeUser, make_activity: MakeRecord
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    await make_activity(rep, subject="вчора 23:59", due_at=utc(27, 21, 59))
    await make_activity(rep, subject="сьогодні 00:00", due_at=utc(27, 22, 0))
    await make_activity(rep, subject="сьогодні 23:59", due_at=utc(28, 21, 59))
    await make_activity(rep, subject="завтра 00:00", due_at=utc(28, 22, 0))
    await make_activity(rep, subject="минулого тижня", due_at=utc(20, 7, 0))

    day = await agenda.day(rep, NOW)

    assert day.date == date(2026, 10, 28)
    assert subjects(day.today) == ["сьогодні 00:00", "сьогодні 23:59"]
    # Просроченные — недавние первыми (в API ?due=overdue — наоборот, по сроку).
    assert subjects(day.overdue) == ["вчора 23:59", "минулого тижня"]
    assert [i.due_at.strftime("%d.%m %H:%M") for i in day.today] == ["28.10 00:00", "28.10 23:59"]
    assert (day.more_today, day.more_overdue) == (False, False)


async def test_day_uses_user_time_zone_over_org(
    agenda: AgendaService, make_org: MakeOrg, make_user: MakeUser, make_activity: MakeRecord
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep", timezone="America/New_York")
    await make_activity(rep, subject="київська ніч", due_at=utc(27, 21, 59))  # 17:59 в NY
    await make_activity(rep, subject="NY вчора", due_at=utc(27, 3, 59))  # 26.10 23:59 в NY

    day = await agenda.day(rep, NOW)

    assert day.date == date(2026, 10, 27)
    assert [(i.subject, i.due_at.strftime("%H:%M")) for i in day.today] == [
        ("київська ніч", "17:59")
    ]
    assert subjects(day.overdue) == ["NY вчора"]


async def test_task_earlier_today_is_today_not_overdue(
    agenda: AgendaService, make_org: MakeOrg, make_user: MakeUser, make_activity: MakeRecord
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    await make_activity(rep, subject="зранку", due_at=utc(27, 7, 0))  # 09:00 Київ, вже минуло

    day = await agenda.day(rep, utc(27, 10, 0))

    assert subjects(day.today) == ["зранку"]
    assert day.overdue == []


async def test_day_limits(
    agenda: AgendaService, make_org: MakeOrg, make_user: MakeUser, make_activity: MakeRecord
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    start = utc(27, 22, 0)  # 28.10 00:00 Київ
    for i in range(TODAY_LIMIT + 1):
        await make_activity(rep, subject=f"t{i:02}", due_at=start + timedelta(minutes=30 * i))
    for i in range(OVERDUE_LIMIT + 1):
        await make_activity(rep, subject=f"o{i:02}", due_at=start - timedelta(days=1 + i))

    day = await agenda.day(rep, NOW)

    assert subjects(day.today) == [f"t{i:02}" for i in range(TODAY_LIMIT)]
    # Самая давняя (o10) не влезла: вчерашние важнее забытых.
    assert subjects(day.overdue) == [f"o{i:02}" for i in range(OVERDUE_LIMIT)]
    assert (day.more_today, day.more_overdue) == (True, True)


async def test_day_of_dst_change_ends_at_next_local_midnight(
    agenda: AgendaService, make_org: MakeOrg, make_user: MakeUser, make_activity: MakeRecord
) -> None:
    # 25.10.2026 в Киеве — 25 часов: полночь в UTC+3, следующая полночь — уже в UTC+2.
    rep = await member(make_user, await make_org(), "sales_rep")
    await make_activity(
        rep, subject="24.10 23:59", due_at=datetime(2026, 10, 24, 20, 59, tzinfo=UTC)
    )
    await make_activity(rep, subject="00:30", due_at=datetime(2026, 10, 24, 21, 30, tzinfo=UTC))
    await make_activity(rep, subject="23:30", due_at=utc(25, 21, 30))  # полночь + 24 ч + 30 мин
    await make_activity(rep, subject="26.10 00:00", due_at=utc(25, 22, 0))

    day = await agenda.day(rep, utc(25, 7, 0))  # 09:00 по Киеву

    assert day.date == date(2026, 10, 25)
    assert [(i.subject, i.due_at.strftime("%H:%M")) for i in day.today] == [
        ("00:30", "00:30"),
        ("23:30", "23:30"),
    ]
    assert subjects(day.overdue) == ["24.10 23:59"]


async def test_day_shows_only_my_open_tasks(
    agenda: AgendaService, make_org: MakeOrg, make_user: MakeUser, make_activity: MakeRecord
) -> None:
    org = await make_org()
    lead = await member(make_user, org, "team_lead")
    rep = await member(make_user, org, "sales_rep")  # тот же отдел: руководитель видит его в API
    stranger = await member(make_user, await make_org(), "sales_rep")
    await make_activity(lead, subject="моя", due_at=utc(28, 8, 0))
    await make_activity(lead, subject="виконана", due_at=utc(28, 9, 0), done_at=utc(27, 20, 0))
    await make_activity(rep, subject="менеджера", due_at=utc(28, 8, 0))
    await make_activity(stranger, subject="чужа", due_at=utc(28, 8, 0))

    day = await agenda.day(lead, NOW)

    assert subjects(day.today) == ["моя"]
    assert day.overdue == []


async def test_day_without_action_is_forbidden(
    make_agenda: Any, make_org: MakeOrg, make_user: MakeUser
) -> None:
    rep = await member(make_user, await make_org(), "sales_rep")
    with pytest.raises(Forbidden) as error:
        await make_agenda(roles_without("activity:read")).day(rep, NOW)
    assert error.value.details == {"action": "activity:read"}


async def test_deals_only_mine_open_with_total_in_org_currency(
    agenda: AgendaService,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_pipeline: Any,
    make_deal: MakeRecord,
) -> None:
    org = await make_org()
    lead = await member(make_user, org, "team_lead")
    rep = await member(make_user, org, "sales_rep")
    stage = (await make_pipeline(org.id)).stages[0]
    for i in range(12):
        await make_deal(lead, stage, title=f"d{i:02}", amount_usd=Decimal("1000.00"))
    await make_deal(lead, stage, title="виграна", status="won", closed_at=NOW)
    await make_deal(lead, stage, title="в кошику", deleted_at=NOW)
    await make_deal(rep, stage, title="менеджера")

    summary = await agenda.deals(lead)

    assert summary is not None
    assert [d.title for d in summary.deals] == [f"d{i:02}" for i in range(11, 1, -1)]  # новые
    assert summary.more
    # Организация показывает суммы в UAH: 1000 USD × 41.488 (курс НБУ в тестах).
    assert summary.deals[0].amount == Decimal("41488.00")
    assert (summary.count, summary.total, summary.currency) == (12, Decimal("497856.00"), "UAH")
    assert (summary.stale, summary.rate_date) == (False, kyiv_today())


async def test_deals_with_stale_rate(
    agenda: AgendaService,
    uow: UnitOfWork,
    nbu: FakeNbu,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_pipeline: Any,
    make_deal: MakeRecord,
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    await make_deal(rep, (await make_pipeline(org.id)).stages[0])
    known = kyiv_today() - timedelta(days=3)
    await uow.fx_rates.upsert(known, {"USD": Decimal("40")}, datetime.now(UTC))
    await uow.commit()
    nbu.down = True

    summary = await agenda.deals(rep)

    assert summary is not None
    assert (summary.count, summary.total) == (1, Decimal("40000.00"))
    assert (summary.stale, summary.rate_date) == (True, known)


async def test_deals_without_rates_at_all_is_unavailable(
    agenda: AgendaService,
    nbu: FakeNbu,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_pipeline: Any,
    make_deal: MakeRecord,
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    await make_deal(rep, (await make_pipeline(org.id)).stages[0])
    nbu.down = True  # и в fx_rates пусто

    with pytest.raises(ServiceUnavailable) as error:
        await agenda.deals(rep)
    assert error.value.code == "fx.unavailable"


async def test_no_open_deals_does_not_need_rate(
    agenda: AgendaService,
    nbu: FakeNbu,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_pipeline: Any,
    make_deal: MakeRecord,
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    admin = await member(make_user, org, "org_admin")  # ответственным не бывает
    peer = await member(make_user, org, "sales_rep")
    stage = (await make_pipeline(org.id)).stages[0]
    await make_deal(rep, stage, status="won", closed_at=NOW)
    await make_deal(peer, stage)  # открытая, но чужая
    nbu.down = True  # курсов нет совсем — но и пересчитывать нечего

    assert await agenda.deals(rep) is None
    assert await agenda.deals(admin) is None
    assert nbu.dates == []  # в НБУ не ходили


async def test_deals_without_action_is_forbidden(
    make_agenda: Any, make_org: MakeOrg, make_user: MakeUser
) -> None:
    viewer = await member(make_user, await make_org(), "viewer")
    with pytest.raises(Forbidden):
        await make_agenda(roles_without("deal:read")).deals(viewer)


def test_render_agenda() -> None:
    agenda = Agenda(
        date=date(2026, 10, 28),
        today=[AgendaItem("call", "Петренко", datetime(2026, 10, 28, 9, 30, tzinfo=KYIV))],
        overdue=[
            AgendaItem("task", "x" * (SUBJECT_MAX + 5), datetime(2026, 10, 24, 16, 0, tzinfo=KYIV))
        ],
        more_today=False,
        more_overdue=True,
    )

    text = render_agenda(agenda, "План:")

    assert text.splitlines() == [
        "План:",
        "09:30 — дзвінок: Петренко",
        "",
        uk.AGENDA["overdue"],
        f"24.10 16:00 — завдання: {'x' * (SUBJECT_MAX - 1)}…",
        uk.AGENDA["more"],
    ]


def test_render_agenda_with_only_overdue() -> None:
    agenda = Agenda(
        date=date(2026, 10, 28),
        today=[],
        overdue=[AgendaItem("meeting", "Демо", datetime(2026, 10, 27, 15, 0, tzinfo=KYIV))],
        more_today=False,
        more_overdue=False,
    )

    assert render_agenda(agenda, "План:").splitlines() == [
        "План:",
        uk.AGENDA["today_none"],
        "",
        uk.AGENDA["overdue"],
        "27.10 15:00 — зустріч: Демо",
    ]


@pytest.mark.parametrize(("stale", "more"), [(False, False), (True, True)])
def test_render_deals(stale: bool, more: bool) -> None:
    summary = DealsSummary(
        deals=[DealLine("Поставка", Decimal("41488.00"))],
        more=more,
        count=3,
        total=Decimal("124464.00"),
        currency="UAH",
        rate_date=date(2026, 10, 24),
        stale=stale,
    )

    total = "Ваші відкриті угоди: 3 на суму 124464.00 UAH"
    if stale:
        total += " (курс на 24.10.2026)"
    expected = [total, "", "• Поставка — 41488.00 UAH"] + ([uk.DEALS["more"]] if more else [])
    assert render_deals(summary).splitlines() == expected


def test_subject_is_clipped_in_utf16_units() -> None:
    # Telegram считает длину в UTF-16: эмодзи — две единицы.
    item = AgendaItem("task", "😀" * 100, datetime(2026, 10, 28, 9, 30, tzinfo=KYIV))
    agenda = Agenda(date(2026, 10, 28), [item], [], more_today=False, more_overdue=False)

    line = render_agenda(agenda, "План:").splitlines()[1]

    assert line == f"09:30 — завдання: {'😀' * ((SUBJECT_MAX - 1) // 2)}…"


def test_longest_messages_fit_telegram_limit() -> None:
    """Худший случай: все строки с самой длинной темой из символов вне BMP — сообщение всё равно
    не длиннее 4096 единиц UTF-16 (иначе sendMessage — 400 и ответа нет)."""
    due = datetime(2026, 10, 28, 9, 30, tzinfo=KYIV)
    agenda = Agenda(
        date=due.date(),
        today=[AgendaItem("meeting", "😀" * 300, due)] * TODAY_LIMIT,
        overdue=[AgendaItem("meeting", "😀" * 300, due)] * OVERDUE_LIMIT,
        more_today=True,
        more_overdue=True,
    )
    deals = DealsSummary(
        deals=[DealLine("😀" * 200, Decimal("9" * 15 + ".99"))] * DEALS_LIMIT,
        more=True,
        count=10**6,
        total=Decimal("9" * 18 + ".99"),
        currency="UAH",
        rate_date=date(2026, 10, 24),
        stale=True,
    )

    assert utf16_len(render_agenda(agenda, "Доброго ранку! План на 28.10.2026")) <= 4096
    assert utf16_len(render_deals(deals)) <= 4096
