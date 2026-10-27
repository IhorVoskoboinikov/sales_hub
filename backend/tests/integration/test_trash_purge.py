"""trash.purge — окончательное удаление корзины (раздел 3), шаг 5.8: сервис в транзакции теста.

Каждый чанк коммитит — в тесте это SAVEPOINT. Чанк берёт строки FOR UPDATE SKIP LOCKED;
пропуск строки, которую держит восстановление, проверяет committing-тест в
test_maintenance_worker.py: в одной транзакции блокировка себя не задерживает.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select

from app.models import Activity, AuditEntry, Company, Contact, Deal, Note
from app.repositories.trash import TrashRepository
from app.services.trash import TrashService
from app.uow import UnitOfWork
from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, member

NOW = datetime.now(UTC)
OLD = NOW - timedelta(days=31)  # старше TRASH_RETENTION_DAYS (30)
RECENT = NOW - timedelta(days=29)


def service(uow: UnitOfWork, **options: Any) -> TrashService:
    return TrashService(uow, retention_days=30, **options)


async def add_note(uow: UnitOfWork, author_id: uuid.UUID, entity: str, entity_id: int) -> int:
    author = await uow.users.get(author_id)
    assert author is not None and author.org_id is not None
    note = Note(
        org_id=author.org_id,
        entity_type=entity,
        entity_id=entity_id,
        author_id=author_id,
        body="Нотатка",
    )
    uow.notes.add(note)
    await uow.commit()
    return note.id


async def existing(uow: UnitOfWork, column: Any, ids: list[int]) -> set[int]:
    return set(await uow.session.scalars(select(column).where(column.in_(ids))))


async def test_purges_old_trash_with_notes_and_contact_activities(
    uow: UnitOfWork,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_company: MakeRecord,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
) -> None:
    org, other = await make_org(), await make_org()
    rep = await member(make_user, org, "sales_rep")
    stranger = await member(make_user, other, "sales_rep")
    stage = (await make_pipeline(org.id)).stages[0]
    rep_id = rep.id

    old_company = await make_company(rep, deleted_at=OLD)
    old_contact = await make_contact(rep, deleted_at=OLD)
    old_deal = await make_deal(rep, stage, deleted_at=OLD)
    recent_contact = await make_contact(rep, deleted_at=RECENT)  # 29 дней — ещё в корзине
    # Живые записи, которые ссылаются на удаляемые: их ссылки обнулит FK, версия — та же.
    live_contact = await make_contact(rep, company_id=old_company.id)
    live_deal = await make_deal(rep, stage, contact_id=old_contact.id, company_id=old_company.id)
    alone = await make_activity(rep, contact_id=old_contact.id)  # без сделки — удаляется
    of_live_deal = await make_activity(rep, contact_id=old_contact.id, deal_id=live_deal.id)
    of_old_deal = await make_activity(rep, deal_id=old_deal.id)  # CASCADE со сделкой
    notes = {
        "gone": [
            await add_note(uow, rep_id, "company", old_company.id),
            await add_note(uow, rep_id, "contact", old_contact.id),
            await add_note(uow, rep_id, "deal", old_deal.id),
        ],
        "kept": [
            await add_note(uow, rep_id, "contact", live_contact.id),
            await add_note(uow, rep_id, "contact", recent_contact.id),
        ],
    }
    foreign_old = await make_contact(stranger, deleted_at=OLD)  # другая организация — тоже
    foreign_recent = await make_contact(stranger, deleted_at=RECENT)
    ids = {
        "old_company": old_company.id,
        "old_contact": old_contact.id,
        "old_deal": old_deal.id,
        "recent_contact": recent_contact.id,
        "live_contact": live_contact.id,
        "live_deal": live_deal.id,
        "foreign_old": foreign_old.id,
        "foreign_recent": foreign_recent.id,
    }
    activity_ids = [alone.id, of_live_deal.id, of_old_deal.id]
    kept_activity = of_live_deal.id
    audit = select(func.count()).select_from(AuditEntry).where(AuditEntry.org_id == org.id)
    audit_before = await uow.session.scalar(audit)

    result = await service(uow).purge(NOW)

    assert result.complete
    assert result.deleted["deals"] >= 1
    assert result.deleted["contacts"] >= 2
    assert result.deleted["companies"] >= 1
    contacts = [ids[k] for k in ("old_contact", "recent_contact", "live_contact")]
    contacts += [ids["foreign_old"], ids["foreign_recent"]]
    assert await existing(uow, Contact.id, contacts) == {
        ids["recent_contact"],
        ids["live_contact"],
        ids["foreign_recent"],
    }
    assert await existing(uow, Company.id, [ids["old_company"]]) == set()
    assert await existing(uow, Deal.id, [ids["old_deal"], ids["live_deal"]]) == {ids["live_deal"]}
    assert await existing(uow, Note.id, notes["gone"] + notes["kept"]) == set(notes["kept"])
    # Активность контакта без сделки — удалена, сделки из корзины — каскадом, живой сделки —
    # осталась, но без контакта.
    left = await uow.session.execute(
        select(Activity.id, Activity.contact_id, Activity.deal_id).where(
            Activity.id.in_(activity_ids)
        )
    )
    assert [tuple(row) for row in left] == [(kept_activity, None, ids["live_deal"])]
    deal_row = (
        await uow.session.execute(
            select(Deal.contact_id, Deal.company_id, Deal.version).where(
                Deal.id == ids["live_deal"]
            )
        )
    ).one()
    contact_row = (
        await uow.session.execute(
            select(Contact.company_id, Contact.version).where(Contact.id == ids["live_contact"])
        )
    ).one()
    assert tuple(deal_row) == (None, None, 1)  # SET NULL от FK — без version + 1
    assert tuple(contact_row) == (None, 1)
    assert await uow.session.scalar(audit) == audit_before  # в журнал purge не пишет


async def test_order_and_chunks(
    uow: UnitOfWork,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_company: MakeRecord,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    stage = (await make_pipeline(org.id)).stages[0]
    mine: set[int] = set()
    for _ in range(3):
        mine.add((await make_deal(rep, stage, deleted_at=OLD)).id)
        mine.add((await make_contact(rep, deleted_at=OLD)).id)
    for _ in range(2):
        mine.add((await make_company(rep, deleted_at=OLD)).id)
    chunks: list[tuple[str, int]] = []
    commits = 0
    delete_with_children = TrashRepository.delete_with_children
    commit = uow.commit

    async def recording(self: TrashRepository, model: Any, entity: str, ids: list[int]) -> None:
        if set(ids) & mine:
            chunks.append((entity, len(ids)))
        await delete_with_children(self, model, entity, ids)

    async def counting() -> None:
        nonlocal commits
        commits += 1
        await commit()

    monkeypatch.setattr(TrashRepository, "delete_with_children", recording)
    monkeypatch.setattr(uow, "commit", counting)

    await service(uow, chunk_size=2).purge(NOW)

    # От детей к родителям, по 2 строки в чанке; каждый чанк — свой коммит.
    assert chunks == [("deal", 2), ("deal", 1), ("contact", 2), ("contact", 1), ("company", 2)]
    assert commits >= len(chunks)


async def test_stops_when_time_budget_is_spent(
    uow: UnitOfWork,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_contact: MakeRecord,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    ids = [(await make_contact(rep, deleted_at=OLD)).id for _ in range(5)]
    # Часы идут только на удалённых чанках: каждый чанк «длится» секунду.
    done = 0
    delete_with_children = TrashRepository.delete_with_children

    async def counting(self: TrashRepository, model: Any, entity: str, chunk: list[int]) -> None:
        nonlocal done
        done += 1
        await delete_with_children(self, model, entity, chunk)

    monkeypatch.setattr(TrashRepository, "delete_with_children", counting)

    first = await service(uow, chunk_size=1, budget_seconds=2, clock=lambda: done).purge(NOW)
    left_after_first = await existing(uow, Contact.id, ids)
    second = await service(uow, chunk_size=1).purge(NOW)  # остаток удалит следующий запуск

    assert (first.complete, first.deleted["contacts"]) == (False, 2)
    assert len(left_after_first) == 3
    assert second.complete
    assert await existing(uow, Contact.id, ids) == set()


async def test_nothing_to_purge_is_noop(uow: UnitOfWork) -> None:
    result = await service(uow).purge(NOW - timedelta(days=3650))  # корзина «из будущего»

    assert result.complete
    assert result.deleted == {"deals": 0, "contacts": 0, "companies": 0}
