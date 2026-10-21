"""Гонки на настоящих транзакциях (мимо общей транзакции теста).

Админ удаляет этап воронки, а менеджер в это же время ставит на него сделку.

Без блокировок проверка «на этапе нет сделок» проходила, а коммит одной из сторон падал на
внешнем ключе — 500. С FOR UPDATE на удаляемом этапе и FOR KEY SHARE при выборе этапа сделкой
стороны идут по очереди: сделка, успевшая первой, даёт удалению 409.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.exc import StaleDataError

from app.authz.access import Access
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.errors import Conflict
from app.models import Contact, Deal, Organization, Pipeline, PipelineStage, Role, Team, User
from app.services.deals import PipelineService, StageIn
from app.uow import UnitOfWork


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


async def test_stage_removal_waits_for_deal_being_placed_on_it(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    org_id, team_id, rep_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Race", slug=f"race-{org_id.hex[:8]}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Відділ"))
        await s.flush()
        s.add(
            User(
                id=rep_id,
                email=f"rep-{org_id.hex[:8]}@example.com",
                password_hash="x",
                org_id=org_id,
                role=Role.SALES_REP,
                team_id=team_id,
            )
        )
        pipeline = Pipeline(
            org_id=org_id,
            name="Продажі",
            stages=[PipelineStage(name="A", position=0), PipelineStage(name="B", position=1)],
        )
        s.add(pipeline)
        await s.commit()
        keep, doomed = pipeline.stages
        pipeline_id, keep_id, doomed_id = pipeline.id, keep.id, doomed.id
    admin = Access("org:manage", "org", uuid.uuid4(), org_id, None, "org_admin")
    try:
        async with sessions() as placing, sessions() as removing:
            uow = UnitOfWork(placing)
            stage = await uow.pipelines.get_stage(doomed_id, org_id)  # FOR KEY SHARE
            assert stage is not None
            removal = asyncio.create_task(
                PipelineService(UnitOfWork(removing)).update(
                    admin, pipeline_id, None, [StageIn("A", 0, keep_id)]
                )
            )
            await asyncio.sleep(0.5)
            assert not removal.done()  # удаление ждёт, пока сделка встанет на этап

            uow.deals.add(
                Deal(
                    org_id=org_id,
                    team_id=team_id,
                    owner_id=rep_id,
                    pipeline_id=pipeline_id,
                    stage_id=doomed_id,
                    title="Угода",
                    amount_usd=Decimal(1),
                    amount_original=Decimal(1),
                    currency_original="USD",
                    fx_rate=Decimal(1),
                    fx_date=date.today(),
                )
            )
            await uow.commit()

            with pytest.raises(Conflict) as exc:
                await removal
            assert exc.value.code == "pipeline.stage_has_deals"
    finally:
        async with sessions() as s:
            await s.execute(delete(Deal).where(Deal.org_id == org_id))
            await s.execute(delete(PipelineStage).where(PipelineStage.pipeline_id == pipeline_id))
            await s.execute(delete(Pipeline).where(Pipeline.org_id == org_id))
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()


async def test_two_editors_of_one_version_second_gets_conflict(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    """Оптимистическая блокировка на настоящих транзакциях: оба прочитали версию 1, первый
    сохранил — у второго UPDATE … WHERE version = 1 ничего не находит (в API — 409)."""
    org_id, team_id, rep_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Race", slug=f"race-{org_id.hex[:8]}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Відділ"))
        await s.flush()
        s.add(
            User(
                id=rep_id,
                email=f"rep-{org_id.hex[:8]}@example.com",
                password_hash="x",
                org_id=org_id,
                role=Role.SALES_REP,
                team_id=team_id,
            )
        )
        await s.flush()
        contact = Contact(org_id=org_id, team_id=team_id, owner_id=rep_id, first_name="Іван")
        s.add(contact)
        await s.commit()
        contact_id = contact.id
    try:
        async with sessions() as first, sessions() as second:
            a = await first.get(Contact, contact_id)
            b = await second.get(Contact, contact_id)
            assert a is not None and b is not None and a.version == b.version == 1
            a.phone = "111"
            await first.commit()

            b.phone = "222"
            with pytest.raises(StaleDataError):
                await second.commit()
        async with sessions() as s:
            saved = await s.get(Contact, contact_id)
            assert saved is not None and (saved.phone, saved.version) == ("111", 2)
    finally:
        async with sessions() as s:
            await s.execute(delete(Contact).where(Contact.org_id == org_id))
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()
