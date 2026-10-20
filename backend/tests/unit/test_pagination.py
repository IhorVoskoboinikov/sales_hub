"""Keyset-пагинация: курсор непрозрачен для клиента, битый курсор — 422, а не 500."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from app.errors import ValidationFailed
from app.models import Company, Contact
from app.repositories.pagination import (
    Key,
    Page,
    after,
    decode_cursor,
    encode_cursor,
    next_cursor,
    paginate,
)

NEWEST_FIRST = (Key(Contact.created_at, desc=True), Key(Contact.id, desc=True))


def sql(statement: object) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))  # type: ignore[attr-defined]


def test_cursor_round_trip_keeps_types() -> None:
    created = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)

    cursor = encode_cursor([created, 42])

    assert decode_cursor(cursor, NEWEST_FIRST) == [created, 42]


@pytest.mark.parametrize(
    "cursor",
    [
        "not-base64!",
        encode_cursor([1]),
        encode_cursor(["yesterday", 1]),
        encode_cursor([None, "x"]),
    ],
    ids=["garbage", "wrong-length", "bad-date", "bad-id"],
)
def test_broken_cursor_is_422(cursor: str) -> None:
    with pytest.raises(ValidationFailed) as exc:
        decode_cursor(cursor, NEWEST_FIRST)

    assert exc.value.code == "pagination.invalid_cursor"


def test_same_direction_uses_row_comparison() -> None:
    # (created_at, id) < (:c, :i) — Postgres видит в этом диапазон по составному индексу.
    condition = after(NEWEST_FIRST, [datetime(2026, 1, 1, tzinfo=UTC), 7])

    assert "(contacts.created_at, contacts.id) < (" in sql(condition)


def test_mixed_directions_expand_to_or() -> None:
    keys = (Key(Company.name), Key(Company.id, desc=True))

    text = sql(after(keys, ["Acme", 7]))

    assert "companies.name > " in text
    assert "companies.name = " in text and "companies.id < " in text


def test_paginate_asks_one_extra_row_and_next_cursor_trims_it() -> None:
    page = Page(limit=2)
    query = paginate(select(Contact), NEWEST_FIRST, page)
    rows = [Contact(id=i, created_at=datetime(2026, 1, i, tzinfo=UTC)) for i in (3, 2, 1)]

    cursor = next_cursor(rows, NEWEST_FIRST, page)

    assert "LIMIT" in sql(query)
    assert [r.id for r in rows] == [3, 2]
    assert cursor is not None
    assert decode_cursor(cursor, NEWEST_FIRST)[1] == 2


def test_last_page_has_no_cursor() -> None:
    rows = [Contact(id=1, created_at=datetime(2026, 1, 1, tzinfo=UTC))]

    assert next_cursor(rows, NEWEST_FIRST, Page(limit=2)) is None
