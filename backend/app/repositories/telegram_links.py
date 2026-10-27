import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any

from sqlalchemy import (
    ColumnElement,
    DateTime,
    Select,
    Time,
    and_,
    delete,
    exists,
    func,
    literal,
    literal_column,
    select,
    true,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Activity,
    ActivityType,
    NotificationSetting,
    Organization,
    Role,
    SentNotification,
    TelegramLink,
    User,
)
from app.models.activities import OPEN
from app.notifications.keys import TELEGRAM, digest_key_sql, reminder_key_sql

# UNIQUE (chat_id): в него упирается гонка двух /start из одного чата (раздел 16).
CHAT_CONSTRAINT = "uq_telegram_links_chat_id"

# Напоминания — только о звонках и встречах: у задачи нет момента начала (раздел 16).
REMINDER_TYPES = (ActivityType.CALL.value, ActivityType.MEETING.value)
# Дайджест — тем, кто ведёт записи; org_admin и viewer ответственными не бывают.
DIGEST_ROLES = (Role.TEAM_LEAD.value, Role.SALES_REP.value)


@dataclass(frozen=True)
class DueReminder:
    """Строка выборки напоминаний: кому, в каком поясе показать время и о чём."""

    user_id: uuid.UUID
    org_id: uuid.UUID
    zone: str  # coalesce(users.timezone, organizations.timezone)
    activity_id: int
    kind: str  # call / meeting
    subject: str
    due_at: datetime


def _zone() -> ColumnElement[str]:
    return func.coalesce(User.timezone, Organization.timezone)


def _enabled(event_type: str, default: bool) -> tuple[Any, ColumnElement[bool]]:
    """Условие join строки настроек пары (event_type, telegram) и итог «включено»: строка
    пользователя, иначе умолчание — параметром из catalog.DEFAULTS, второго источника правды в
    SQL нет."""
    on = and_(
        NotificationSetting.user_id == User.id,
        NotificationSetting.event_type == event_type,
        NotificationSetting.channel == TELEGRAM,
    )
    return on, func.coalesce(NotificationSetting.enabled, default)


class TelegramLinkRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # populate_existing: строки меняет upsert мимо ORM — объект из сессии мог устареть.
    async def get(self, user_id: uuid.UUID) -> TelegramLink | None:
        return await self.session.get(TelegramLink, user_id, populate_existing=True)

    async def get_by_chat(self, chat_id: int) -> TelegramLink | None:
        return await self.session.scalar(
            select(TelegramLink)
            .where(TelegramLink.chat_id == chat_id)
            .execution_options(populate_existing=True)
        )

    async def linked(self, user_ids: Collection[uuid.UUID]) -> set[uuid.UUID]:
        """Кто из этих пользователей привязал Telegram — notifier пишет telegram.send только им."""
        if not user_ids:
            return set()
        return set(
            await self.session.scalars(
                select(TelegramLink.user_id).where(TelegramLink.user_id.in_(list(user_ids)))
            )
        )

    async def upsert(
        self, user_id: uuid.UUID, chat_id: int, username: str | None, now: datetime
    ) -> None:
        """Привязать чат. У пользователя уже есть привязка — новый чат заменяет старый.
        Чат занят другим пользователем, которого TelegramService.link ещё не видел
        (параллельная транзакция), — IntegrityError по CHAT_CONSTRAINT."""
        query = insert(TelegramLink).values(
            user_id=user_id, chat_id=chat_id, username=username, linked_at=now
        )
        await self.session.execute(
            query.on_conflict_do_update(
                index_elements=[TelegramLink.user_id],
                set_={
                    "chat_id": query.excluded.chat_id,
                    "username": query.excluded.username,
                    "linked_at": query.excluded.linked_at,
                },
            )
        )

    async def delete_for_user(self, user_id: uuid.UUID) -> None:
        await self.session.execute(delete(TelegramLink).where(TelegramLink.user_id == user_id))

    async def delete_by_chat(self, chat_id: int) -> uuid.UUID | None:
        """/stop: отвязать этот чат, чей бы он ни был. Возвращает, чью привязку удалили
        (None — чат не был привязан)."""
        return await self.session.scalar(
            delete(TelegramLink)
            .where(TelegramLink.chat_id == chat_id)
            .returning(TelegramLink.user_id)
        )

    async def delete(self, user_id: uuid.UUID, chat_id: int, linked_at: datetime) -> None:
        """Удалить именно ту привязку, которую прочитали: пару (user_id, chat_id) и её
        linked_at. Свежую перепривязку это не задевает — ни к другому чату, ни к тому же: upsert
        тот же chat_id оставляет, а linked_at меняет."""
        await self.session.execute(
            delete(TelegramLink).where(
                TelegramLink.user_id == user_id,
                TelegramLink.chat_id == chat_id,
                TelegramLink.linked_at == linked_at,
            )
        )

    # --- задачи beat (раздел 16, «Напоминания и дайджест»): одним запросом от привязанных

    @staticmethod
    def reminders_query(
        now: datetime, until: datetime, *, enabled_default: bool, limit: int
    ) -> Select[Any]:
        """Напоминания тика: telegram_links ⨝ users ⨝ LATERAL (открытые звонки и встречи этого
        ответственного со сроком в (now, until], ещё не отправленные, до limit по сроку).

        Ключ дедупа вычитается внутри LATERAL, до limit: отправленные не занимают места, и 21-е
        напоминание уйдёт следующим тиком. Индекс — ix_activities_owner_id_due_at (owner_id,
        due_at, id) WHERE done_at IS NULL в каждой партиции; предикат — литералом, как в
        индексе. Активность — только своей организации: owner_id без FK копирует сервис."""
        due = (
            select(Activity.id, Activity.type, Activity.subject, Activity.due_at)
            .where(
                Activity.owner_id == User.id,
                Activity.org_id == User.org_id,
                literal_column(OPEN),
                Activity.type.in_(REMINDER_TYPES),
                Activity.due_at > now,
                Activity.due_at <= until,
                ~exists().where(
                    SentNotification.dedup_key
                    == reminder_key_sql(Activity.id, Activity.owner_id, Activity.due_at)
                ),
            )
            .order_by(Activity.due_at, Activity.id)
            .limit(limit)
            .lateral("due")
        )
        on, enabled = _enabled("activity.reminder", enabled_default)
        return (
            select(
                User.id,
                User.org_id,
                _zone(),
                due.c.id,
                due.c.type,
                due.c.subject,
                due.c.due_at,
            )
            .select_from(TelegramLink)
            .join(User, User.id == TelegramLink.user_id)
            .join(Organization, Organization.id == User.org_id)
            .outerjoin(NotificationSetting, on)
            .join(due, true())
            .where(User.is_active, enabled)
            .order_by(User.id, due.c.due_at, due.c.id)
        )

    async def reminders(
        self, now: datetime, until: datetime, *, enabled_default: bool, limit: int
    ) -> list[DueReminder]:
        query = self.reminders_query(now, until, enabled_default=enabled_default, limit=limit)
        return [DueReminder(*row) for row in await self.session.execute(query)]

    @staticmethod
    def digest_recipients_query(
        now: datetime, *, enabled_default: bool, start: time, end: time
    ) -> Select[User, str]:
        """Кому сейчас утренний дайджест: активные team_lead и sales_rep с привязанным Telegram,
        у которых местное время (coalesce(users.timezone, organizations.timezone)) в [start,
        end), настройка включена, а ключа за местную дату ещё нет. Строка — (пользователь, его
        пояс): по поясу сервис ставит команде срок — местный конец окна."""
        local = func.timezone(_zone(), literal(now, DateTime(timezone=True)))
        on, enabled = _enabled("digest.morning", enabled_default)
        return (
            select(User, _zone())
            .select_from(TelegramLink)
            .join(User, User.id == TelegramLink.user_id)
            .join(Organization, Organization.id == User.org_id)
            .outerjoin(NotificationSetting, on)
            .where(
                User.is_active,
                User.role.in_(DIGEST_ROLES),
                enabled,
                local.cast(Time) >= start,
                local.cast(Time) < end,
                ~exists().where(SentNotification.dedup_key == digest_key_sql(User.id, local)),
            )
            .order_by(User.id)
        )

    async def digest_recipients(
        self, now: datetime, *, enabled_default: bool, start: time, end: time
    ) -> list[tuple[User, str]]:
        query = self.digest_recipients_query(
            now, enabled_default=enabled_default, start=start, end=end
        )
        return [(user, zone) for user, zone in await self.session.execute(query)]
