"""Health-эндпоинты для liveness и readiness проб. Они вне /api/v1 и без авторизации."""

import asyncio

import structlog
from fastapi import APIRouter, Depends
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import get_engine, get_redis
from app.errors import ServiceUnavailable
from app.schemas.errors import ErrorOut

logger = structlog.get_logger("health")

router = APIRouter(prefix="/health", tags=["health"])

CHECK_TIMEOUT_SECONDS = 2


@router.get("/live")
async def live() -> dict[str, str]:
    """Процесс жив. Ничего внешнего не проверяет: иначе при падении БД Kubernetes
    перезапускал бы все поды, хотя они ни в чём не виноваты."""
    return {"status": "ok"}


@router.get("/ready", responses={503: {"model": ErrorOut}})
async def ready(
    engine: AsyncEngine = Depends(get_engine),
    redis: Redis = Depends(get_redis),
) -> dict[str, str]:
    """Под готов принимать трафик: отвечают Postgres и Redis. Иначе 503 и список упавших."""
    failed = [
        name
        for name, ok in [
            ("postgres", await _check_postgres(engine)),
            ("redis", await _check_redis(redis)),
        ]
        if not ok
    ]
    if failed:
        raise ServiceUnavailable(failed=failed)
    return {"status": "ok"}


async def _check_postgres(engine: AsyncEngine) -> bool:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SECONDS), engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("health_check_failed", dependency="postgres", error=repr(exc))
        return False
    return True


async def _check_redis(redis: Redis) -> bool:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_SECONDS):
            await redis.ping()
    except Exception as exc:
        logger.warning("health_check_failed", dependency="redis", error=repr(exc))
        return False
    return True
