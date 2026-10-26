"""Приложение Celery: брокер, надёжность, маршруты очередей (раздел 15).

Запуск воркера: celery -A app.workers.celery_app worker -Q default,notifications -n worker@%h
Запуск beat:    celery -A app.workers.celery_app beat -s /tmp/celerybeat-schedule
"""

from celery import Celery

from app.core.config import get_settings
from app.workers import base, context, runtime  # noqa: F401 — подключают сигналы Celery
from app.workers.schedule import BEAT_SCHEDULE

# Очередь — не в декораторе задачи, а здесь, по полному имени. Шаблон bulk.* отправил бы
# короткие bulk.plan и bulk.reap_stuck в очередь за тысячей чужих чанков. Всё, чего нет в
# таблице, — в default (и bulk.plan, bulk.reap_stuck).
TASK_ROUTES = {
    "bulk.chunk": {"queue": "bulk"},
    "imports.import_contacts": {"queue": "imports"},
    "notifications.send_email": {"queue": "notifications"},
}

settings = get_settings()

celery_app = Celery(
    "saleshub",
    include=[
        "app.workers.tasks.bulk",
        "app.workers.tasks.fx",
        "app.workers.tasks.imports",
        "app.workers.tasks.maintenance",
        "app.workers.tasks.notifications",
    ],
)
celery_app.conf.update(
    broker_url=settings.celery_broker_url,  # db 1; кэш и rate limit — в db 0
    broker_connection_retry_on_startup=True,  # брокер ещё стартует — ждать, а не падать
    broker_transport_options={
        # Одинаковый у всех процессов: больше самой долгой задачи, иначе Redis выдаст её повторно.
        "visibility_timeout": settings.celery_visibility_timeout,
        # Без них (у kombu None) зависший Redis — пауза контейнера, потерянные пакеты без RST —
        # держит send_task бесконечно: relay стоит в открытой транзакции батча. С ними — ошибка
        # через 5 с, как у отказа в соединении. BRPOP воркера ждёт 1 с, таймаут его не задевает.
        "socket_timeout": 5,
        "socket_connect_timeout": 5,
    },
    # Аргументы — только id и простые значения.
    task_serializer="json",
    accept_content=["json"],
    # Результаты никто не читает: завершение bulk job видно по базе. Result backend не нужен.
    task_ignore_result=True,
    task_default_queue="default",
    task_routes=TASK_ROUTES,
    # Подтверждать после выполнения: упал воркер (и даже kill -9 процесса пула) — задачу
    # возьмёт другой. Поэтому каждая задача идемпотентна.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # не забирать лишние длинные задачи
    # Потерян брокер — задача отменяется, а не выполняется параллельно с повторной доставкой.
    worker_cancel_long_running_tasks_on_connection_loss=True,
    # Лимиты по умолчанию; у чанков и импорта — свои, в декораторе.
    task_soft_time_limit=240,
    task_time_limit=300,
    # Flower видит задачи по событиям.
    worker_send_task_events=True,
    task_send_sent_event=True,
    # Расписание beat — по Киеву, данные — в UTC.
    timezone="Europe/Kyiv",
    enable_utc=True,
    beat_schedule=BEAT_SCHEDULE,
)
