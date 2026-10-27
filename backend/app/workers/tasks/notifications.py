"""Уведомления (разделы 15 и 16): notifier, письма и сообщения в Telegram из outbox,
напоминания и утренний дайджест.

notify, send_email и send_telegram ставит relay, в аргументах только id строки outbox_events.
Очередь — по имени в TASK_ROUTES: письма — notifications (общий rate limit), notify и Telegram —
default. send_reminders и morning_digest ставит beat (шаг 5.14, очередь default): они только
пишут команды telegram.send в outbox, отправляет их send_telegram.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog

from app.services.fx import FxService
from app.services.notifications import NotificationService
from app.services.reminders import ReminderService
from app.uow import UnitOfWork
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from app.workers.runtime import resources, run_async

logger = structlog.get_logger("notifications")


# rate_limit действует на экземпляр воркера: очередь notifications слушает ровно один воркер,
# поэтому лимит 50 писем в минуту — общий (раздел 15, «Процессы»).
@celery_app.task(base=BaseTask, name="notifications.send_email", rate_limit="50/m")
def send_email(event_id: int) -> None:
    """Письмо команды email.send из outbox_events (ставит relay, в аргументах только id)."""
    run_async(_send_email(event_id))


async def _send_email(event_id: int) -> None:
    res = resources()
    if res.email_sender is None:  # Resources собраны не для воркера — ошибка в коде
        raise RuntimeError("email_sender is not configured: Resources.create(for_worker=True)")
    async with res.session_factory() as session:  # своя сессия и UoW на задачу
        await NotificationService(UnitOfWork(session), res.email_sender).send_email(event_id)


@celery_app.task(base=BaseTask, name="notifications.notify")
def notify(event_id: int) -> None:
    """Доменное событие → команды email.send / telegram.send (раздел 16, notifier)."""
    run_async(_notify(event_id))


async def _notify(event_id: int) -> None:
    async with resources().session_factory() as session:
        await NotificationService(UnitOfWork(session)).notify(event_id)


# Без rate limit: Telegram ограничивает ~30 сообщений в секунду на бота, а при превышении
# отвечает 429 с retry_after — его повторяет BaseTask.
@celery_app.task(base=BaseTask, name="notifications.send_telegram")
def send_telegram(event_id: int) -> None:
    """Сообщение команды telegram.send. Бот не настроен (нет токена) — пропуск."""
    run_async(_send_telegram(event_id))


async def _send_telegram(event_id: int) -> None:
    res = resources()
    async with res.session_factory() as session:
        await NotificationService(UnitOfWork(session), telegram=res.telegram).send_telegram(
            event_id
        )


@celery_app.task(base=BaseTask, name="notifications.send_reminders")
def send_reminders() -> None:
    """Каждую минуту: напоминания за 15 минут до звонков и встреч (раздел 16)."""
    run_async(_send_reminders())


async def _send_reminders() -> None:
    async with _reminders() as service:
        if service is not None:
            await service.send_reminders()


@celery_app.task(base=BaseTask, name="notifications.morning_digest")
def morning_digest() -> None:
    """Каждые 15 минут: утренний дайджест тем, у кого по их поясу 09:00–12:00 (раздел 16)."""
    run_async(_morning_digest())


async def _morning_digest() -> None:
    async with _reminders() as service:
        if service is not None:
            await service.morning_digest()


@asynccontextmanager
async def _reminders() -> AsyncIterator[ReminderService | None]:
    """ReminderService на своей сессии. Бот не настроен (нет токена) — None: send_telegram такие
    команды всё равно пропустил бы, а тик каждую минуту копил бы их в outbox зря."""
    res = resources()
    if res.telegram is None:
        logger.info("reminders_skipped", reason="bot_not_configured")
        yield None
        return
    async with res.session_factory() as session:
        uow = UnitOfWork(session)
        yield ReminderService(uow, res.roles, FxService(uow, res.redis, res.nbu))
