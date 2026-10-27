"""Ключи дедупа для sent_notifications (раздел 15, «Идемпотентность и защита от дублей»).

Ключ — один факт «это сообщение этому человеку уже ушло». Повторная доставка задачи или
повторная публикация события дают тот же ключ, и отправитель второй раз не отправляет.
Ключи напоминаний и дайджеста появятся в шаге 5.14.
"""

import uuid


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
