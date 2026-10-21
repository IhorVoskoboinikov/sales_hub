"""Keyset-пагинация (раздел 6): «следующая страница — строки после последней показанной».

OFFSET 100000 заставляет базу прочитать и выбросить сто тысяч строк. Keyset помнит ключ
сортировки последней строки (курсор) и начинает с него: WHERE (created_at, id) < (:c, :i) —
это один переход по индексу, на какой бы странице мы ни были.

Курсор для клиента непрозрачен: base64url от JSON со значениями ключа последней строки.
"""

import base64
import binascii
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import ColumnElement, Select, and_, literal, or_, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.errors import ValidationFailed

DEFAULT_LIMIT = 50
MAX_LIMIT = 100


@dataclass(frozen=True)
class Page:
    """Что просит клиент: сколько строк и после какой."""

    limit: int = DEFAULT_LIMIT
    cursor: str | None = None


@dataclass(frozen=True)
class Key:
    """Колонка сортировки: desc — по убыванию (новые сначала)."""

    column: Any
    desc: bool = False


def encode_cursor(values: Sequence[Any]) -> str:
    plain = [v.isoformat() if isinstance(v, date | datetime) else v for v in values]
    return base64.urlsafe_b64encode(json.dumps(plain).encode()).decode().rstrip("=")


def decode_cursor(cursor: str, keys: Sequence[Key]) -> list[Any]:
    """Значения курсора в типах колонок. Битый или подделанный курсор — 422, а не 500."""
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        if not isinstance(raw, list) or len(raw) != len(keys):
            raise ValueError("wrong length")
        return [_parse(value, key) for value, key in zip(raw, keys, strict=True)]
    except (
        ValueError,
        TypeError,
        OverflowError,
        RecursionError,
        binascii.Error,
        UnicodeDecodeError,
    ):
        raise ValidationFailed("pagination.invalid_cursor", field="cursor") from None


def _parse(value: Any, key: Key) -> Any:
    """Ключи сортировки не бывают NULL (списки фильтруют такие строки), а bool в JSON
    формально int — оба признак подделки."""
    python_type = key.column.type.python_type
    if value is None or isinstance(value, bool):
        raise ValueError("bad value")
    if python_type is datetime:
        moment = datetime.fromisoformat(value)
        if moment.tzinfo is None:
            raise ValueError("naive datetime")
        return moment.astimezone(UTC)  # OverflowError на краях диапазона — тоже 422
    if python_type is date:
        return date.fromisoformat(value)
    if not isinstance(value, python_type):
        raise ValueError(f"not a {python_type.__name__}")
    if python_type is int and not -(2**63) <= value < 2**63:
        raise ValueError("out of bigint range")
    return value


def after(keys: Sequence[Key], values: Sequence[Any]) -> ColumnElement[bool]:
    """Строки строго после курсора в порядке сортировки.

    Все ключи в одном направлении — сравнение строк (row comparison):
    (created_at, id) < (:c, :i). Postgres понимает его как диапазон по составному индексу.
    Разные направления — развёрнутое условие: a > :a OR (a = :a AND b < :b)."""
    if all(k.desc == keys[0].desc for k in keys):
        left = tuple_(*(k.column for k in keys))
        # Параметры с типами колонок: Postgres сравнивает timestamptz с timestamptz.
        right = tuple_(*(literal(v, k.column.type) for k, v in zip(keys, values, strict=True)))
        return left < right if keys[0].desc else left > right
    conditions = []
    for i, key in enumerate(keys):
        equal = [keys[j].column == values[j] for j in range(i)]
        step = key.column < values[i] if key.desc else key.column > values[i]
        conditions.append(and_(*equal, step))
    return or_(*conditions)


def paginate(query: Select[Any], keys: Sequence[Key], page: Page) -> Select[Any]:
    """Сортировка по ключу, условие «после курсора» и LIMIT на одну строку больше:
    лишняя строка говорит, что есть следующая страница."""
    if page.cursor:
        query = query.where(after(keys, decode_cursor(page.cursor, keys)))
    order = [k.column.desc() if k.desc else k.column.asc() for k in keys]
    return query.order_by(*order).limit(page.limit + 1)


def next_cursor(rows: list[Any], keys: Sequence[Key], page: Page) -> str | None:
    """Обрезает лишнюю строку и возвращает курсор следующей страницы (или None)."""
    if len(rows) <= page.limit:
        return None
    del rows[page.limit :]
    last = rows[-1]
    return encode_cursor([getattr(last, k.column.key) for k in keys])


@dataclass
class PageResult[T]:
    items: list[T]
    next_cursor: str | None


async def fetch_page[T](
    session: AsyncSession, query: Select[Any], keys: Sequence[Key], page: Page
) -> PageResult[T]:
    """Выполнить запрос из paginate() и разрезать на страницу и курсор следующей."""
    rows: list[T] = list(await session.scalars(query))
    cursor = next_cursor(rows, keys, page)
    return PageResult(rows, cursor)
