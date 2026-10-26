"""Расписание beat (раздел 15). Время в crontab — по Киеву (timezone в celery_app.py).

У каждой записи expires = интервал между запусками: если воркер лежал, пропущенные запуски не
копятся в очереди. Все задачи — в очереди default (в TASK_ROUTES их нет). Каждый шаг этапа 5
добавляет сюда свои задачи. Правку применяет make restart-workers.

Переход на летнее и зимнее время: час 03:00–04:00 в октябре повторяется, в марте пропадает —
задачи идемпотентны, лишний или пропущенный запуск безопасен.
"""

from typing import Any

from celery.schedules import crontab

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR

BEAT_SCHEDULE: dict[str, dict[str, Any]] = {
    # Жив ли beat и очередь default: ключ старше 3 минут — алерт CeleryBeatDown (этап 8).
    "workers.heartbeat": {
        "task": "workers.heartbeat",
        "schedule": crontab(),  # каждую минуту
        "options": {"expires": MINUTE},
    },
    # Job с потерянными задачами: plan, finalize, чанки, импорт (раздел 15).
    "bulk.reap_stuck": {
        "task": "bulk.reap_stuck",
        "schedule": crontab(),
        "options": {"expires": MINUTE},
    },
    # Отчёты: REFRESH deal_stats; идёт прошлое обновление — пропуск (advisory lock).
    "reports.refresh": {
        "task": "reports.refresh",
        "schedule": crontab(minute="*/5"),
        "options": {"expires": 5 * MINUTE},
    },
    # Курсы НБУ на сегодня и завтра: НБУ публикует курс до 18:00, 19:30 — повтор.
    "fx.fetch_nbu_rates": {
        "task": "fx.fetch_nbu_rates",
        "schedule": crontab(minute=30, hour="18,19"),
        "options": {"expires": HOUR},
    },
    # Партиции activities и audit_log на 2 месяца вперёд — ежедневно, а не 1-го числа.
    "maintenance.create_partitions": {
        "task": "maintenance.create_partitions",
        "schedule": crontab(minute=0, hour=1),
        "options": {"expires": DAY},
    },
    # Корзина старше TRASH_RETENTION_DAYS — насовсем (раздел 3).
    "trash.purge": {
        "task": "trash.purge",
        "schedule": crontab(minute=0, hour=3),
        "options": {"expires": DAY},
    },
    # Токены, outbox, ключи дедупа, файлы импорта, старые bulk job.
    "maintenance.cleanup": {
        "task": "maintenance.cleanup",
        "schedule": crontab(minute=0, hour=4),
        "options": {"expires": DAY},
    },
}
