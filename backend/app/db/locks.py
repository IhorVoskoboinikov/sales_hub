"""Сессионный advisory lock Postgres на отдельном соединении пула.

Задача импорта (раздел 6, «Поток: импорт контактов из CSV», шаг 3) держит одну блокировку на job
весь запуск, а её шаги коммитят свои транзакции. Транзакционная блокировка
(pg_advisory_xact_lock) отпускалась бы на первом же коммите, а сессионная на соединении сессии
SQLAlchemy «уехала» бы вместе с ним в пул после коммита. Поэтому — своё соединение: блокировка
берётся и сразу коммитится (соединение не стоит «idle in transaction» и не держит горизонт
VACUUM), а в конце снимается pg_advisory_unlock. Не снялась (соединение оборвалось) — соединение
закрывается, а не возвращается в пул: с закрытием сессии Postgres снимает блокировку сам.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = structlog.get_logger("db")

# Ключ блокировки — 64-битный хэш текста (job_id::text): тот же, что в разделе 6.
TRY_LOCK = text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))")
UNLOCK = text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))")


class AdvisoryLock:
    """hold(key) — взять блокировку без ожидания: True — взята и держится до выхода из блока,
    False — её держит другая сессия (другая копия задачи)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    @asynccontextmanager
    async def hold(self, key: str) -> AsyncIterator[bool]:
        async with self.engine.connect() as connection:
            taken = bool(await connection.scalar(TRY_LOCK, {"key": key}))
            await connection.commit()  # блокировка сессионная — транзакцию не держим
            try:
                yield taken
            finally:
                if taken:
                    try:
                        await connection.execute(UNLOCK, {"key": key})
                        await connection.commit()
                    except BaseException:
                        # Соединение сломано или задачу отменили посреди снятия: закрыть его
                        # насовсем — вместе с сессией Postgres снимет и блокировку.
                        logger.warning("advisory_unlock_failed", key=key)
                        await connection.invalidate()
                        raise
