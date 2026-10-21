"""Подключение к Postgres: один engine (пул соединений) на процесс."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(database_url: str, *, pool_size: int = 5, max_overflow: int = 10) -> AsyncEngine:
    # pool_pre_ping: перед выдачей соединения из пула проверить, что оно живо
    # (после рестарта Postgres старые соединения в пуле мертвы).
    # Размер пула: у API 5 + 10 сверху, у процесса воркера — 2 (раздел 15, бюджет соединений).
    return create_async_engine(
        database_url, pool_pre_ping=True, pool_size=pool_size, max_overflow=max_overflow
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: после commit объекты можно читать без нового запроса в БД.
    return async_sessionmaker(engine, expire_on_commit=False)
