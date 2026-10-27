"""Ключи дедупа для sent_notifications (раздел 15, «Идемпотентность и защита от дублей»).

Ключ — один факт «это сообщение этому человеку уже ушло». Повторная доставка задачи или
повторная публикация события дают тот же ключ, и отправитель второй раз не отправляет.

Ключи напоминаний и дайджеста (шаг 5.14) нужны в двух местах: задача beat вычитает уже
отправленные прямо в SQL-запросе, а команде ключ ставит Python. Поэтому у них по паре функций —
строка и то же выражение SQL; integration-тест сверяет, что они совпадают.
"""

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, ColumnElement, ColumnExpressionArgument, Date, func

# Канал — часть ключа: напоминание и дайджест — только Telegram (раздел 16).
TELEGRAM = "telegram"


def email_key(event_id: int) -> str:
    """Письмо сервиса (приглашение, сброс пароля), если команда не задала свой ключ: одна
    строка outbox — одно письмо. Новое приглашение — новая строка и новое письмо."""
    return f"email:{event_id}"


def notify_key(event_type: str, subject: str, user_id: uuid.UUID, channel: str) -> str:
    """Сообщение notifier: {event_type}:{subject}:{user_id}:{channel}. Повтор notify того же
    события пишет команды с теми же ключами — отправитель их пропустит."""
    return f"{event_type}:{subject}:{user_id}:{channel}"


def event_subject(event_id: int) -> str:
    """record.assigned и deal.closed: каждое изменение — отдельный факт, ключ — id события."""
    return str(event_id)


def job_subject(job_id: uuid.UUID | str) -> str:
    """bulk_job.completed: одно сообщение на job, сколько бы раз ни прошёл finalize."""
    return f"job:{job_id}"


def reminder_key(activity_id: int, user_id: uuid.UUID, due_at: datetime) -> str:
    """Напоминание о звонке или встрече: activity.reminder:{id}:{user_id}:{unix due_at}:telegram.
    Срок — часть ключа: перенесли встречу — новый ключ и новое напоминание. Получатель — тоже:
    звонок передали другому после напоминания прежнему — новый ответственный своё получит."""
    return f"activity.reminder:{activity_id}:{user_id}:{int(due_at.timestamp())}:{TELEGRAM}"


def reminder_key_sql(
    activity_id: ColumnExpressionArgument[Any],
    user_id: ColumnExpressionArgument[Any],
    due_at: ColumnExpressionArgument[Any],
) -> ColumnElement[str]:
    """reminder_key в SQL. floor — как int() в Python: у срока бывают доли секунды, а ::bigint
    округлил бы их до ближайшего."""
    epoch = func.floor(func.extract("epoch", due_at)).cast(BigInteger)
    return func.concat("activity.reminder:", activity_id, ":", user_id, ":", epoch, f":{TELEGRAM}")


def digest_key(user_id: uuid.UUID, day: date) -> str:
    """Утренний дайджест: digest.morning:{user_id}:{YYYY-MM-DD}:telegram — один в местный день."""
    return f"digest.morning:{user_id}:{day.isoformat()}:{TELEGRAM}"


def digest_key_sql(
    user_id: ColumnExpressionArgument[Any], local: ColumnElement[Any]
) -> ColumnElement[str]:
    """digest_key в SQL; local — местное время пользователя (timestamp без пояса)."""
    day = func.to_char(local.cast(Date), "YYYY-MM-DD")
    return func.concat("digest.morning:", user_id, ":", day, f":{TELEGRAM}")
