"""Повестка пользователя для бота (раздел 16): день — /today и утренний дайджест (шаг 5.14,
тот же AgendaService.day), открытые сделки — /deals.

Только свои записи: Access.for_user(user, действие, roles).own() — руководитель и админ тоже
видят в боте свои задачи и сделки, а не отдела. У роли нет действия — Forbidden (бот отвечает
«недоступно»). Запросы — те же методы репозиториев, что у API (списки задач и сделок), по тем же
частичным индексам: ix_activities_owner_id_due_at (WHERE done_at IS NULL) и
ix_deals_owner_id_created_at.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from app.authz.access import Access, Member
from app.authz.roles import RoleCatalog
from app.errors import Forbidden
from app.models import Activity, DealStatus
from app.repositories.activities import TASKS_RECENT_ORDER, TaskWindow
from app.repositories.deals import DealFilters
from app.repositories.pagination import Page
from app.services.activities import user_zone
from app.services.fx import FxService
from app.texts import uk
from app.uow import UnitOfWork

TODAY_LIMIT = 20
OVERDUE_LIMIT = 10
DEALS_LIMIT = 10
# Тема задачи — до 300 символов, название сделки — до 200; 30 строк повестки по 300 не влезли бы
# в сообщение Telegram (4096). Длина — в единицах UTF-16, как считает Telegram: эмодзи и другие
# символы вне BMP занимают две.
SUBJECT_MAX = 80


@dataclass(frozen=True)
class AgendaItem:
    kind: str  # call / meeting / task / email
    subject: str
    due_at: datetime  # в часовом поясе пользователя


@dataclass(frozen=True)
class Agenda:
    date: date  # «сегодня» пользователя
    today: list[AgendaItem]  # срок сегодня, по времени
    overdue: list[AgendaItem]  # срок в прошлые дни, недавние первыми
    more_today: bool  # задач на сегодня больше TODAY_LIMIT
    more_overdue: bool

    @property
    def empty(self) -> bool:
        return not self.today and not self.overdue


@dataclass(frozen=True)
class DealLine:
    title: str
    amount: Decimal  # в валюте организации


@dataclass(frozen=True)
class DealsSummary:
    deals: list[DealLine]  # до DEALS_LIMIT новых
    more: bool
    count: int  # итог по всем открытым
    total: Decimal
    currency: str
    rate_date: date
    stale: bool  # НБУ недоступен — взят последний известный курс


class AgendaService:
    def __init__(self, uow: UnitOfWork, roles: RoleCatalog, fx: FxService) -> None:
        self.uow = uow
        self.roles = roles
        self.fx = fx

    async def day(self, user: Member, now: datetime | None = None) -> Agenda:
        """Мои открытые задачи на сегодня (до 20) и просроченные с прошлых дней (до 10). Граница
        дней — полночь по моему часовому поясу (coalesce(users.timezone,
        organizations.timezone)), поэтому списки не пересекаются: задача на сегодняшнее утро,
        время которой уже прошло, — в «сегодня», а не в просроченных. now — для дайджеста и
        тестов."""
        access = self._own(user, "activity:read")
        zone = await user_zone(self.uow, access.user_id, access.org_id)
        local = (now or datetime.now(UTC)).astimezone(zone)
        start = datetime.combine(local.date(), time(), zone)
        # Сложение по местному времени: следующая полночь и в день перехода на летнее время.
        end = start + timedelta(days=1)
        today = await self.uow.activities.tasks(
            access, TaskWindow(start, end), Page(limit=TODAY_LIMIT)
        )
        # Просроченные — недавние первыми: при десятке давно забытых задач вчерашний пропущенный
        # звонок иначе не попал бы ни в /today, ни в дайджест.
        overdue = await self.uow.activities.tasks(
            access, TaskWindow(due_to=start), Page(limit=OVERDUE_LIMIT), TASKS_RECENT_ORDER
        )

        def item(activity: Activity) -> AgendaItem:
            assert activity.due_at is not None  # открытая задача всегда со сроком (CHECK)
            return AgendaItem(activity.type, activity.subject, activity.due_at.astimezone(zone))

        return Agenda(
            date=local.date(),
            today=[item(a) for a in today.items],
            overdue=[item(a) for a in overdue.items],
            more_today=today.next_cursor is not None,
            more_overdue=overdue.next_cursor is not None,
        )

    async def deals(self, user: Member) -> DealsSummary | None:
        """Мои открытые сделки (10 новых) и итог по всем открытым: число и сумма в валюте
        организации по курсу на её сегодня. None — открытых сделок нет."""
        access = self._own(user, "deal:read")
        count, total_usd = await self.uow.deals.open_totals(access)
        if count == 0:
            return None
        # Курс — только когда есть что пересчитывать: без сделок ответ не зависит от НБУ и
        # fx_rates. Первая за день загрузка из НБУ коммитит транзакцию — для чтения безвредно.
        display = await self.fx.display_for(access.org_id, None)
        page = await self.uow.deals.list(
            access, DealFilters(status=DealStatus.OPEN), Page(limit=DEALS_LIMIT)
        )
        return DealsSummary(
            deals=[DealLine(d.title, display.amount(d.amount_usd)) for d in page.items],
            more=page.next_cursor is not None,
            count=count,
            total=display.amount(total_usd),
            currency=display.currency,
            rate_date=display.date,
            stale=display.stale,
        )

    def _own(self, user: Member, action: str) -> Access:
        access = Access.for_user(user, action, self.roles)
        if access is None:
            raise Forbidden(action=action)
        return access.own()


def render_agenda(agenda: Agenda, title: str) -> str:
    """Текст повестки: заголовок у /today и дайджеста свой, остальное общее. Время — HH:MM по
    часовому поясу пользователя, у просроченных — ещё и дата."""
    lines = [title]
    lines += [_line(item, "%H:%M") for item in agenda.today] or [uk.AGENDA["today_none"]]
    if agenda.more_today:
        lines.append(uk.AGENDA["more"])
    if agenda.overdue:
        lines += ["", uk.AGENDA["overdue"]]
        lines += [_line(item, "%d.%m %H:%M") for item in agenda.overdue]
        if agenda.more_overdue:
            lines.append(uk.AGENDA["more"])
    return "\n".join(lines)


def render_deals(summary: DealsSummary) -> str:
    """Итог по всем открытым сделкам и список новых; «(курс на <дата>)» — при fx.stale."""
    total = uk.DEALS["total"].format(
        count=summary.count, amount=_money(summary.total, summary.currency)
    )
    if summary.stale:
        total += uk.DEALS["stale"].format(date=summary.rate_date.strftime("%d.%m.%Y"))
    lines = [total, ""]
    lines += [
        f"• {_clip(deal.title)} — {_money(deal.amount, summary.currency)}" for deal in summary.deals
    ]
    if summary.more:
        lines.append(uk.DEALS["more"])
    return "\n".join(lines)


def _line(item: AgendaItem, time_format: str) -> str:
    kind = uk.AGENDA_KINDS.get(item.kind, item.kind)
    return f"{item.due_at.strftime(time_format)} — {kind}: {_clip(item.subject)}"


def utf16_len(text: str) -> int:
    """Длина так, как её считает Telegram (лимит 4096): символ вне BMP — две единицы."""
    return len(text.encode("utf-16-le")) // 2


def _clip(text: str) -> str:
    """Обрезка до SUBJECT_MAX единиц UTF-16 с «…» на конце."""
    if utf16_len(text) <= SUBJECT_MAX:
        return text
    clipped = text[: SUBJECT_MAX - 1]  # одна единица — на «…»
    while utf16_len(clipped) > SUBJECT_MAX - 1:
        clipped = clipped[:-1]
    return clipped + "…"


def _money(amount: Decimal, currency: str) -> str:
    return f"{amount} {currency}"  # как в уведомлениях: 41488.00 UAH
