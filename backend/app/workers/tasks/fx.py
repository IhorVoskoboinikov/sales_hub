"""Курсы НБУ заранее: fx.fetch_nbu_rates (раздел 3, «Деньги»; раздел 15, «Расписание beat»)."""

from app.services.fx import FxService
from app.uow import UnitOfWork
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from app.workers.runtime import resources, run_async


@celery_app.task(base=BaseTask, name="fx.fetch_nbu_rates")
def fetch_nbu_rates() -> None:
    """18:30 и 19:30 по Киеву: курсы на сегодня и завтра → fx_rates. НБУ недоступен —
    ServiceUnavailable fx.unavailable, и BaseTask повторяет задачу; курса на завтра ещё нет —
    не ошибка. Курс по-прежнему подтягивается и по первому запросу дня."""
    run_async(_fetch_nbu_rates())


async def _fetch_nbu_rates() -> None:
    async with resources().session_factory() as session:
        await FxService(UnitOfWork(session), resources().redis, resources().nbu).load_rates()
