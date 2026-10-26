"""Месячные партиции activities и audit_log заранее (раздел 9).

Одна функция на двоих: её вызывают сервис migrate после alembic upgrade (python -m app.cli
ensure-partitions) и задача beat maintenance.create_partitions каждый день в 01:00 — ежедневно, а
не 1-го числа: если beat в тот день лежал, партиции всё равно появятся.

Саму работу делает SQL-функция ensure_monthly_partitions из миграции: существующие партиции
она пропускает. Но «проверить, потом создать» у неё не атомарно — два одновременных вызова
(migrate и beat) могли бы оба решить, что партиции нет, и второй упал бы на CREATE TABLE.
Поэтому вызовы идут под транзакционным advisory lock: второй ждёт первого и уже ничего не
создаёт.
"""

from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Партиционированные таблицы и на сколько месяцев вперёд готовить партиции.
PARTITIONED = ("activities", "audit_log")
MONTHS_AHEAD = 2
LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended('ensure_partitions', 0))")
ENSURE = text("SELECT ensure_monthly_partitions(CAST(:table AS regclass), :from_day, :to_day)")


def add_months(day: date, months: int) -> date:
    """Первое число месяца через months месяцев: функции хватает месяца, день она отбрасывает."""
    index = day.year * 12 + day.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


async def ensure_partitions(session: AsyncSession, today: date | None = None) -> dict[str, int]:
    """Партиции с месяца today по today + MONTHS_AHEAD для каждой таблицы и коммит. Возвращает,
    сколько партиций создано: {"activities": 0, "audit_log": 1}. today — дата по UTC (границы
    партиций — полночь UTC); по умолчанию — сегодня."""
    today = today or datetime.now(UTC).date()
    await session.execute(LOCK)
    created = {}
    for table in PARTITIONED:
        created[table] = int(
            await session.scalar(
                ENSURE,
                {"table": table, "from_day": today, "to_day": add_months(today, MONTHS_AHEAD)},
            )
            or 0
        )
    await session.commit()
    return created
