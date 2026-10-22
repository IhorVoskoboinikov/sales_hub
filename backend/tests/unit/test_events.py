"""Типы событий и маршруты relay без базы и брокера (раздел 15, «Маршруты событий»)."""

import pytest

from app.events.relay import UnknownEventType, listen_dsn, route
from app.events.routes import ROUTES
from app.events.types import EventType
from app.workers.celery_app import TASK_ROUTES, celery_app


def test_every_event_type_has_route() -> None:
    # Тип без маршрута relay не опубликует; маршрут без типа никто не запишет в outbox.
    assert {str(event_type) for event_type in EventType} == set(ROUTES)


def test_every_route_is_a_registered_task() -> None:
    # Задача из маршрута есть в celery_app (модуль в include): иначе воркер отверг бы сообщение
    # как неизвестное. Очередь письма — notifications, где rate_limit общий на один воркер.
    celery_app.loader.import_default_modules()
    assert set(ROUTES.values()) <= set(celery_app.tasks)
    assert TASK_ROUTES[ROUTES["email.send"]] == {"queue": "notifications"}
    assert celery_app.tasks["notifications.send_email"].rate_limit == "50/m"


def test_unknown_event_type_is_an_error() -> None:
    with pytest.raises(UnknownEventType, match=r"no\.such_event"):
        route("no.such_event")


def test_listen_dsn_is_plain_postgres_url() -> None:
    # asyncpg.connect не понимает схему postgresql+asyncpg; пароль должен остаться в адресе.
    dsn = listen_dsn("postgresql+asyncpg://saleshub:secret@postgres:5432/saleshub")

    assert dsn == "postgresql://saleshub:secret@postgres:5432/saleshub"
