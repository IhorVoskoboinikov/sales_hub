"""Типы событий и маршруты relay без базы и брокера (раздел 15, «Маршруты событий»)."""

import pytest

from app.events.relay import UnknownEventType, listen_dsn, route
from app.events.routes import ROUTES
from app.events.types import EventType


def test_every_event_type_has_route() -> None:
    # Тип без маршрута relay не опубликует; маршрут без типа никто не запишет в outbox.
    assert {str(event_type) for event_type in EventType} == set(ROUTES)


def test_unknown_event_type_is_an_error() -> None:
    with pytest.raises(UnknownEventType, match=r"no\.such_event"):
        route("no.such_event")


def test_listen_dsn_is_plain_postgres_url() -> None:
    # asyncpg.connect не понимает схему postgresql+asyncpg; пароль должен остаться в адресе.
    dsn = listen_dsn("postgresql+asyncpg://saleshub:secret@postgres:5432/saleshub")

    assert dsn == "postgresql://saleshub:secret@postgres:5432/saleshub"
