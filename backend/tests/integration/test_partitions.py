"""Партиции заранее (раздел 9): ensure_partitions — одна функция у migrate и задачи beat
maintenance.create_partitions (шаг 5.8)."""

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.cli import ensure_partitions_command
from app.core.config import Settings
from app.db.partitions import add_months, ensure_partitions

# Далёкое будущее: таких партиций ещё нет, а всё созданное откатится вместе с тестом (DDL в
# Postgres транзакционен).
FUTURE = date(2031, 11, 20)


def name(table: str, month: date) -> str:
    return f"{table}_p{month:%Y_%m}"


async def partitions(session: AsyncSession, table: str, like: str = "p203%") -> list[str]:
    rows = await session.scalars(
        text(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
            " WHERE i.inhparent = CAST(:table AS regclass) AND c.relname LIKE :like"
            " ORDER BY c.relname"
        ),
        {"table": table, "like": f"{table}_{like}"},
    )
    return list(rows)


async def test_creates_two_months_ahead_and_is_idempotent(db_session: AsyncSession) -> None:
    first = await ensure_partitions(db_session, FUTURE)
    again = await ensure_partitions(db_session, FUTURE)

    assert first == {"activities": 3, "audit_log": 3}  # ноябрь, декабрь, январь
    assert again == {"activities": 0, "audit_log": 0}
    assert await partitions(db_session, "activities") == [
        "activities_p2031_11",
        "activities_p2031_12",
        "activities_p2032_01",
    ]
    assert len(await partitions(db_session, "audit_log")) == 3


async def test_today_partitions_exist_after_the_call(db_session: AsyncSession) -> None:
    # Обычно партиции уже создала миграция, но тесты могли перейти через 1-е число (UTC) —
    # поэтому первый вызов без проверки числа, а второй уже ничего не создаёт.
    today = datetime.now(UTC).date()
    await ensure_partitions(db_session)

    assert await ensure_partitions(db_session) == {"activities": 0, "audit_log": 0}
    for table in ("activities", "audit_log"):
        existing = set(await partitions(db_session, table, like="p%"))
        assert {name(table, add_months(today, n)) for n in range(3)} <= existing


@pytest.mark.parametrize(
    ("day", "months", "expected"),
    [
        (date(2026, 10, 26), 2, date(2026, 12, 1)),
        (date(2026, 11, 30), 2, date(2027, 1, 1)),
        (date(2026, 12, 31), 1, date(2027, 1, 1)),
        (date(2026, 1, 15), 0, date(2026, 1, 1)),
    ],
)
def test_add_months(day: date, months: int, expected: date) -> None:
    assert add_months(day, months) == expected


async def test_cli_command_uses_the_same_function(
    settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    # Первый запуск может создать партиции (тесты перешли через 1-е число UTC), второй — нет.
    assert await ensure_partitions_command(settings) == 0
    first = capsys.readouterr().out
    assert await ensure_partitions_command(settings) == 0

    out = capsys.readouterr().out
    assert "activities: создано партиций — " in first
    assert "activities: создано партиций — 0" in out
    assert "audit_log: создано партиций — 0" in out
