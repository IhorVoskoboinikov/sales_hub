"""Задачи beat уведомлений (раздел 16, «Напоминания и дайджест»): напоминание за 15 минут до
звонка или встречи и утренний дайджест.

Обе пишут команды telegram.send в outbox — дальше тот же путь, что у notifier: relay →
notifications.send_telegram → дедуп по sent_notifications. Уже отправленное (ключ в
sent_notifications) вычитается прямо в запросе, поэтому повтор тика после отправки ничего не
пишет. Команду, которую отправитель ещё не успел выполнить, повтор тика напишет ещё раз с тем же
ключом — второе сообщение не уйдёт. У каждой команды срок (not_after): застрявшую в очереди или в
ретраях отправитель после него пропустит, и уйдёт свежая — с верным «через N хв». Только
Telegram и только привязанным: других каналов у этих событий нет (catalog.DEFAULTS).
"""

import math
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

import structlog

from app.authz.roles import RoleCatalog
from app.errors import Forbidden
from app.notifications import catalog
from app.notifications.catalog import NotificationEvent
from app.notifications.keys import digest_key, reminder_key
from app.repositories.telegram_links import DueReminder
from app.services.agenda import AgendaService, render_agenda
from app.services.fx import FxService
from app.texts import uk
from app.uow import UnitOfWork

logger = structlog.get_logger("notifications")

# Окно напоминаний — (now, now + 15 минут]: «до начала события», а не «ровно за 15 минут». Так
# догоняются пропущенные тики beat и активности, созданные меньше чем за 15 минут до срока.
REMINDER_AHEAD = timedelta(minutes=15)
REMINDER_LIMIT = 20  # на пользователя за тик; остальные — следующим тиком
# Срок команды напоминания: две минуты после тика, но не позже начала события. Тик — каждую
# минуту, и пока ключ свободен, каждый пишет свежую команду: устаревшая (застряла в очереди, в
# ретраях Telegram) не уйдёт, «через N хв» в отправленном врёт не больше чем на пару минут, а
# после начала встречи напоминания нет вовсе.
REMINDER_FRESH = timedelta(minutes=2)
# Утро по местному времени пользователя: [09:00, 12:00). После 12:00 дайджест за этот день уже не
# уходит — «утренний» не придёт вечером, даже если beat лежал всё утро. Это и срок команды:
# поставленную в 11:45 отправитель после 12:00 пропустит.
DIGEST_START = time(9)
DIGEST_END = time(12)
# Получателей дайджеста в 09:00 — почти все (пояс обычно один на организацию), а на каждого —
# несколько запросов AgendaService. Коммит — пачками: падение теряет только текущую пачку. Бюджет
# тика меньше task_soft_time_limit (240 с), как у trash.purge: кончился — остальных возьмёт
# следующий тик, пока их ключа нет.
DIGEST_BATCH = 100
DIGEST_BUDGET_SECONDS = 200.0


def reminder_text(reminder: DueReminder, now: datetime) -> str:
    """«Через N хв (о HH:MM) — дзвінок: тема». N округляется вверх: срок в (now, now + 15 хв],
    поэтому от 1 до 15, «через 0 хв» не бывает. Время — по поясу пользователя."""
    minutes = math.ceil((reminder.due_at - now).total_seconds() / 60)
    local = reminder.due_at.astimezone(ZoneInfo(reminder.zone))
    return uk.REMINDER.format(
        minutes=minutes,
        time=local.strftime("%H:%M"),
        kind=uk.AGENDA_KINDS.get(reminder.kind, reminder.kind),
        subject=reminder.subject,
    )


def digest_not_after(day: date, zone: str) -> datetime:
    """Срок команды дайджеста — местный конец окна (12:00) того дня, за который он собран."""
    return datetime.combine(day, DIGEST_END, tzinfo=ZoneInfo(zone)).astimezone(UTC)


class ReminderService:
    def __init__(
        self,
        uow: UnitOfWork,
        roles: RoleCatalog,
        fx: FxService,
        *,
        budget_seconds: float = DIGEST_BUDGET_SECONDS,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        """roles и fx — для AgendaService: дайджест собирает ту же повестку, что /today.
        budget_seconds и clock — бюджет тика дайджеста (тест подменяет часы)."""
        self.uow = uow
        self.agenda = AgendaService(uow, roles, fx)
        self.budget_seconds = budget_seconds
        self.clock = clock

    async def send_reminders(self, now: datetime | None = None) -> int:
        """Тик напоминаний (каждую минуту): одна выборка — команда telegram.send на строку,
        один коммит. Возвращает число команд. now — для тестов."""
        now = now or datetime.now(UTC)
        default = catalog.DEFAULTS[(NotificationEvent.ACTIVITY_REMINDER, catalog.TELEGRAM)]
        try:
            due = await self.uow.telegram_links.reminders(
                now, now + REMINDER_AHEAD, enabled_default=default, limit=REMINDER_LIMIT
            )
            for reminder in due:
                self.uow.outbox.add_telegram(
                    reminder.user_id,
                    reminder_text(reminder, now),
                    reminder_key(reminder.activity_id, reminder.user_id, reminder.due_at),
                    org_id=reminder.org_id,
                    not_after=min(reminder.due_at, now + REMINDER_FRESH),
                )
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        if due:  # пустой тик — каждую минуту, в лог его не пишем
            logger.info("reminders_queued", commands=len(due))
        return len(due)

    async def morning_digest(self, now: datetime | None = None) -> int:
        """Тик дайджеста (каждые 15 минут): тем, у кого сейчас местное утро и дайджеста за
        сегодня ещё не было, — повестка AgendaService.day на момент тика с заголовком «Доброго
        ранку! План на <дата>:». Пустая повестка — ничего (следующий тик проверит снова).
        Коммит — каждые DIGEST_BATCH получателей и в конце; бюджет кончился — выход, остальных
        возьмёт следующий тик. Возвращает число команд."""
        now = now or datetime.now(UTC)
        deadline = self.clock() + self.budget_seconds
        default = catalog.DEFAULTS[(NotificationEvent.DIGEST_MORNING, catalog.TELEGRAM)]
        written = 0
        try:
            recipients = await self.uow.telegram_links.digest_recipients(
                now, enabled_default=default, start=DIGEST_START, end=DIGEST_END
            )
            for done, (user, zone) in enumerate(recipients):
                if self.clock() >= deadline:
                    logger.warning(
                        "digest_budget_exhausted", commands=written, left=len(recipients) - done
                    )
                    break
                if done and done % DIGEST_BATCH == 0:
                    await self.uow.commit()
                try:
                    day = await self.agenda.day(user, now)
                except Forbidden:
                    continue  # у роли нет activity:read — как «недоступно» в /today
                if day.empty:
                    continue
                title = uk.DIGEST_TITLE.format(date=day.date.strftime("%d.%m.%Y"))
                # day.date — местная дата по тому же правилу поясов, что в запросе: ключ тот же.
                self.uow.outbox.add_telegram(
                    user.id,
                    render_agenda(day, title),
                    digest_key(user.id, day.date),
                    org_id=user.org_id,
                    not_after=digest_not_after(day.date, zone),
                )
                written += 1
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
        if written:
            logger.info("digest_queued", commands=written)
        return written
