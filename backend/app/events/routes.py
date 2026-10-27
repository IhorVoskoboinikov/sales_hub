"""Маршруты событий: тип в outbox → имя задачи Celery (раздел 15, «Маршруты событий»).

Задача получает только id события (args=[event_id]) и сама читает payload из базы. Очередь
задачи — по её имени в TASK_ROUTES (app/workers/celery_app.py), не здесь. Строки добавляются
вместе с типами EventType: в шаге 5.3 — email.send → notifications.send_email, в 5.12 —
telegram.send и доменные события (их разбирает notifier).

Ключ — строка, а не EventType: relay ищет маршрут по значению колонки event_type.
"""

NOTIFY_TASK = "notifications.notify"

ROUTES: dict[str, str] = {
    "email.send": "notifications.send_email",
    "telegram.send": "notifications.send_telegram",
    "record.assigned": NOTIFY_TASK,
    "deal.closed": NOTIFY_TASK,
    "bulk_job.completed": NOTIFY_TASK,
}
