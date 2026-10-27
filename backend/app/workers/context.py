"""Контекст задачи (раздел 15, «Контекст и логи»): request_id и user_id через заголовки.

Публикация (API, relay, сам воркер) кладёт request_id и user_id текущего контекста в заголовки
задачи, воркер перед задачей восстанавливает их в request_id_var и в поля structlog. Так у
HTTP-запроса и порождённых им задач в логах один request_id. Организацию не переносим — она
есть в аргументах задачи или в job.
"""

from typing import Any

import structlog
from celery import Task
from celery.signals import before_task_publish, setup_logging, task_postrun, task_prerun

from app.core.context import request_id_var
from app.core.logging import configure_logging
from app.workers import runtime

REQUEST_ID_HEADER = "saleshub_request_id"
USER_ID_HEADER = "saleshub_user_id"


@before_task_publish.connect
def put_context_headers(headers: dict[str, Any] | None = None, **_: Any) -> None:
    if headers is None:
        return
    request_id = request_id_var.get()
    user_id = structlog.contextvars.get_contextvars().get("user_id")
    if request_id is not None:
        headers[REQUEST_ID_HEADER] = request_id
    if user_id is not None:
        headers[USER_ID_HEADER] = user_id


@task_prerun.connect
def restore_context(task_id: str, task: "Task[Any, Any]", **_: Any) -> None:
    # Заголовки сообщения Celery кладёт прямо в task.request. У задач beat запроса нет —
    # request_id = task_id, чтобы строки одной задачи всё равно искались вместе.
    request_id = task.request.get(REQUEST_ID_HEADER) or task_id
    request_id_var.set(request_id)  # его читает AuditRepository.record
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        user_id=task.request.get(USER_ID_HEADER),
        task_id=task_id,
        task_name=task.name,
    )


@task_postrun.connect
def clear_context(**_: Any) -> None:
    # Процесс выполняет много задач подряд: поля одной не должны попасть в логи следующей.
    request_id_var.set(None)
    structlog.contextvars.clear_contextvars()


@setup_logging.connect
def setup_worker_logging(**_: Any) -> None:
    # Есть обработчик этого сигнала — Celery не настраивает логи сам, и формат строк
    # у воркеров тот же, что у API.
    settings = runtime.current_settings()
    configure_logging(settings.log_level, json=settings.log_json)
