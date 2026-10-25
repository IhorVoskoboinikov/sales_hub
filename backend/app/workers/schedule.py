"""Расписание beat (раздел 15). Время в crontab — по Киеву (timezone в celery_app.py).

У каждой записи expires = интервал: если воркер лежал, пропущенные запуски не копятся в очереди.
Каждый шаг этапа 5 добавляет сюда свои задачи. Правку применяет make restart-workers.
"""

from typing import Any

from celery.schedules import crontab

MINUTE = 60

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
}
