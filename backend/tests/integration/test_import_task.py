"""Задача импорта CSV (раздел 6, «Поток: импорт контактов из CSV», шаг 3): parse → stage →
merge → финал в транзакции теста.

Job создаёт ImportService.start (как POST /contacts/import), файл лежит в FakeFileStorage, а
задачу выполняет ImportService.run прямо в сессии теста — чанки merge по 2 строки, разбор по 2
строки на COPY, поэтому даже у пары строк их несколько. Блокировка — настоящий AdvisoryLock на
своём соединении: сессионный advisory lock не зависит от транзакции теста.

ORM-объекты после run «протухают» (сервис откатывает свои транзакции), поэтому id и Access
запоминаются заранее (People).
"""

import io
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.db.locks import AdvisoryLock
from app.models import AuditEntry, BulkJob, Contact, ContactStaging, ImportRowError, User
from app.services.bulk import BulkService
from app.services.imports import ImportService, ImportUpload, read_header
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeFileStorage,
    FakeTaskQueue,
    MakeOrg,
    MakeRecord,
    MakeUser,
    OrgFixture,
    member,
    tokens_for,
)

CATALOG = RoleCatalog.load(POLICIES_DIR)
HEADER = "email,first_name,last_name,phone,status,source,tags,owner_email"


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    email: str
    team_id: uuid.UUID | None
    access: Access  # bulk:run — с ним запускают импорт


@dataclass(frozen=True)
class People:
    org_id: uuid.UUID
    admin: Person
    lead: Person  # руководитель отдела 1
    rep: Person  # менеджер отдела 1
    other_rep: Person  # менеджер отдела 2


async def person(make_user: MakeUser, org: OrgFixture, role: str, team: int = 0) -> Person:
    user = await member(make_user, org, role, team)
    # У менеджера bulk:run нет — его Access в тестах не используется.
    scope = CATALOG.scope_or_none(user.role, "bulk:run") or "own"
    access = Access("bulk:run", scope, user.id, org.id, user.team_id, user.role)
    return Person(user.id, user.email, user.team_id, access)


@pytest.fixture
async def org(make_org: MakeOrg) -> OrgFixture:
    return await make_org()


@pytest.fixture
async def people(make_user: MakeUser, org: OrgFixture) -> People:
    return People(
        org.id,
        admin=await person(make_user, org, "org_admin"),
        lead=await person(make_user, org, "team_lead"),
        rep=await person(make_user, org, "sales_rep"),
        other_rep=await person(make_user, org, "sales_rep", team=1),
    )


@pytest.fixture
async def lock(postgres_url: str) -> AsyncIterator[AdvisoryLock]:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    yield AdvisoryLock(engine)
    await engine.dispose()


def service(uow: UnitOfWork, storage: FakeFileStorage, **options: Any) -> ImportService:
    options.setdefault("max_rows", 1000)
    options.setdefault("merge_chunk", 2)
    options.setdefault("parse_batch", 2)
    bulk = BulkService(uow, CATALOG, FakeTaskQueue(), chunk_size=2)
    return ImportService(bulk, storage, **options)


@pytest.fixture
def imports(uow: UnitOfWork, file_storage: FakeFileStorage) -> ImportService:
    return service(uow, file_storage)


def csv_of(*rows: str, header: str = HEADER) -> bytes:
    return ("\n".join([header, *rows]) + "\n").encode()


async def start(
    imports: ImportService, author: Person, content: bytes, owner_id: uuid.UUID | None = None
) -> uuid.UUID:
    """Job импорта так же, как её создаёт POST /contacts/import: файл — в хранилище."""
    upload = ImportUpload(
        io.BytesIO(content),
        len(content),
        "contacts.csv",
        read_header(content[:65536], whole_file=True),
        owner_id,
    )
    return (await imports.start(author.access, upload, None)).id


async def job_of(uow: UnitOfWork, job_id: uuid.UUID) -> BulkJob:
    job = await uow.bulk_jobs.get(job_id)
    assert job is not None
    return job


async def contacts_of(uow: UnitOfWork, org_id: uuid.UUID) -> dict[str, Contact]:
    """Живые контакты организации по email."""
    rows = await uow.session.scalars(
        select(Contact)
        .where(Contact.org_id == org_id, Contact.deleted_at.is_(None))
        .execution_options(populate_existing=True)
    )
    return {str(c.email): c for c in rows}


async def errors_of(uow: UnitOfWork, job_id: uuid.UUID) -> list[tuple[int, str, str | None]]:
    rows = await uow.session.execute(
        select(ImportRowError.row_no, ImportRowError.code, ImportRowError.field)
        .where(ImportRowError.job_id == job_id)
        .order_by(ImportRowError.row_no)
    )
    return [tuple(r) for r in rows.all()]  # type: ignore[misc]


async def raw_of(uow: UnitOfWork, job_id: uuid.UUID, row_no: int) -> Any:
    return await uow.session.scalar(
        select(ImportRowError.raw).where(
            ImportRowError.job_id == job_id, ImportRowError.row_no == row_no
        )
    )


async def staged(uow: UnitOfWork, job_id: uuid.UUID) -> int:
    count = await uow.session.scalar(
        select(func.count()).select_from(ContactStaging).where(ContactStaging.job_id == job_id)
    )
    return int(count or 0)


async def audit_of(uow: UnitOfWork, job_id: uuid.UUID) -> list[AuditEntry]:
    return list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "bulk_job", AuditEntry.entity_id == str(job_id)
            )
        )
    )


async def test_new_contacts_are_created_and_job_is_done(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    content = csv_of(
        'ivan@example.com,Іван,Петренко,+380671234567,,ads,"vip,b2b",',
        "olha@example.com,Ольга,,,customer,,,",
        f"taras@example.com,Тарас,,,,,,{people.rep.email.upper()}",
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.stage, job.error) == ("done", "merge", None)
    assert (job.total, job.processed, job.failed_rows) == (3, 3, 0)
    assert job.result == {"created": 3, "updated": 0}
    assert job.started_at is not None and job.finished_at is not None
    found = await contacts_of(uow, people.org_id)
    assert set(found) == {"ivan@example.com", "olha@example.com", "taras@example.com"}
    ivan = found["ivan@example.com"]
    assert (ivan.first_name, ivan.last_name, ivan.phone, ivan.source) == (
        "Іван",
        "Петренко",
        "+380671234567",
        "ads",
    )
    assert (ivan.status, ivan.tags, ivan.version) == ("lead", ["vip", "b2b"], 1)
    assert (ivan.owner_id, ivan.team_id) == (people.lead.id, people.lead.team_id)
    assert (found["olha@example.com"].status, found["olha@example.com"].last_name) == (
        "customer",
        "",
    )
    assert found["taras@example.com"].owner_id == people.rep.id  # owner_email без учёта регистра
    assert await errors_of(uow, job_id) == []
    assert await staged(uow, job_id) == 0
    [entry] = await audit_of(uow, job_id)
    assert entry.action == "bulk.import_contacts"
    assert entry.actor_id == people.lead.id
    assert entry.changes["status"] == "done"
    assert entry.changes["rows"] == {"created": 3, "updated": 0, "failed": 0}
    assert "access" not in entry.changes


async def test_existing_contact_is_updated_by_non_empty_cells(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
    make_user: MakeUser,
) -> None:
    rep = await uow.users.get(people.rep.id)
    assert rep is not None
    await make_contact(
        rep,
        email="Ivan@Example.com",
        first_name="Старе",
        last_name="Петренко",
        phone="+380000000000",
        source="web",
        status="customer",
        tags=["a", "b"],
    )
    await make_contact(rep, email="full@example.com", tags=[f"t{i}" for i in range(19)])
    await make_contact(rep, email="churn@example.com", status="customer")
    content = csv_of(
        # Пустые last_name, status, source — не трогать; phone — перезаписать; теги — union.
        'ivan@example.com,Іван,,+380671234567,,,"b,c",',
        'full@example.com,Повний,,,,,"t0,x1,x2,x3",',  # 19 + 3 новых → только 20
        "churn@example.com,Пішов,,,churned,,,",
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.result) == ("done", 3, {"created": 0, "updated": 3})
    found = await contacts_of(uow, people.org_id)
    ivan = found["Ivan@Example.com"]  # email контакта не меняется, сравнение — без регистра
    assert (ivan.first_name, ivan.last_name, ivan.phone, ivan.source, ivan.status) == (
        "Іван",
        "Петренко",
        "+380671234567",
        "web",
        "customer",
    )
    assert ivan.tags == ["a", "b", "c"]
    assert (ivan.owner_id, ivan.team_id, ivan.version) == (people.rep.id, people.rep.team_id, 2)
    assert found["full@example.com"].tags == [*(f"t{i}" for i in range(19)), "x1"]
    assert found["churn@example.com"].status == "churned"
    assert await errors_of(uow, job_id) == []


async def test_duplicates_in_file_first_row_wins(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    content = csv_of(
        "a@example.com,Перша,,,,,,",
        "b@example.com,Б,,,,,,",
        "A@EXAMPLE.COM,Друга,,,,,,",
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.failed_rows) == ("partial_failed", 3, 2, 1)
    assert await errors_of(uow, job_id) == [(4, "duplicate_in_file", "email")]
    assert (await contacts_of(uow, people.org_id))["a@example.com"].first_name == "Перша"
    assert await raw_of(uow, job_id, 4) == {"email": "A@example.com", "first_name": "Друга"}
    assert await staged(uow, job_id) == 0


async def test_owner_email_is_resolved_within_author_scope(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    make_user: MakeUser,
    org: OrgFixture,
) -> None:
    viewer = await member(make_user, org, "viewer")
    gone = await member(make_user, org, "sales_rep", is_active=False)
    viewer_email, gone_email = viewer.email, gone.email
    content = csv_of(
        f"ok@example.com,Свій,,,,,,{people.rep.email}",
        "nobody@example.com,Ніхто,,,,,,nobody@example.com",
        f"viewer@example.com,Глядач,,,,,,{viewer_email}",
        f"gone@example.com,Звільнений,,,,,,{gone_email}",
        f"other@example.com,Чужий,,,,,,{people.other_rep.email}",
        "default@example.com,За замовчуванням,,,,,,",
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    assert await errors_of(uow, job_id) == [
        (3, "owner_invalid", "owner_email"),
        (4, "owner_invalid", "owner_email"),
        (5, "owner_invalid", "owner_email"),
        (6, "owner_out_of_scope", "owner_email"),
    ]
    found = await contacts_of(uow, people.org_id)
    assert set(found) == {"ok@example.com", "default@example.com"}
    assert found["ok@example.com"].owner_id == people.rep.id
    assert found["default@example.com"].owner_id == people.lead.id  # owner_id формы — я
    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.failed_rows) == ("partial_failed", 2, 4)


async def test_org_admin_assigns_any_team_and_default_owner(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    content = csv_of(
        f"one@example.com,Один,,,,,,{people.other_rep.email}",
        "two@example.com,Два,,,,,,",
    )
    job_id = await start(imports, people.admin, content, owner_id=people.rep.id)

    await imports.run(job_id, lock)

    found = await contacts_of(uow, people.org_id)
    one, two = found["one@example.com"], found["two@example.com"]
    assert (one.owner_id, one.team_id) == (people.other_rep.id, people.other_rep.team_id)
    assert (two.owner_id, two.team_id) == (people.rep.id, people.rep.team_id)
    assert (await job_of(uow, job_id)).status == "done"


async def test_contact_outside_scope_is_not_touched_and_not_leaked(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
) -> None:
    other = await uow.users.get(people.other_rep.id)
    assert other is not None
    await make_contact(other, email="secret@example.com", first_name="Таємний", phone="+1")
    content = csv_of("SECRET@example.com,Чужий,,+380671234567,,,,", "mine@example.com,Мій,,,,,,")
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    assert await errors_of(uow, job_id) == [(2, "duplicate_outside_scope", "email")]
    # raw — только то, что было в файле: ни имени, ни телефона чужого контакта.
    assert await raw_of(uow, job_id, 2) == {
        "email": "SECRET@example.com",
        "first_name": "Чужий",
        "phone": "+380671234567",
    }
    secret = (await contacts_of(uow, people.org_id))["secret@example.com"]
    assert (secret.first_name, secret.phone, secret.version) == ("Таємний", "+1", 1)
    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.failed_rows) == ("partial_failed", 1, 1)
    assert job.result == {"created": 1, "updated": 0}


async def test_email_only_in_trash_creates_new_contact(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    make_contact: MakeRecord,
) -> None:
    rep = await uow.users.get(people.rep.id)
    assert rep is not None
    old = await make_contact(rep, email="back@example.com", deleted_at=datetime.now(UTC))
    old_id = old.id
    job_id = await start(imports, people.lead, csv_of("back@example.com,Повернувся,,,,,,"))

    await imports.run(job_id, lock)

    alive = (await contacts_of(uow, people.org_id))["back@example.com"]
    assert alive.id != old_id
    assert (await job_of(uow, job_id)).result == {"created": 1, "updated": 0}


async def test_invalid_rows_are_errors_and_valid_rows_go_on(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    client: AsyncClient,
) -> None:
    content = csv_of(
        "ok@example.com,Добре,,,,,,",
        ",Без пошти,,,,,,",
        "bad-email,Погана,,,,,,",
        "short@example.com,Мало",
        "status@example.com,Статус,,,vip,,,",
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.failed_rows) == ("partial_failed", 5, 1, 4)
    expected = [
        {"row_no": 3, "code": "email_required", "field": "email"},
        {"row_no": 4, "code": "validation", "field": "email"},
        {"row_no": 5, "code": "invalid_columns", "field": None},
        {"row_no": 6, "code": "validation", "field": "status"},
    ]
    # Ошибки строк — GET /bulk-jobs/{job_id}/errors, по порядку файла, страницами.
    lead = await uow.users.get(people.lead.id)
    assert lead is not None
    headers = await tokens_for(client, lead)
    first = await client.get(f"/api/v1/bulk-jobs/{job_id}/errors?limit=3", headers=headers)
    assert first.status_code == 200, first.text
    cursor = first.json()["next_cursor"]
    rest = await client.get(
        f"/api/v1/bulk-jobs/{job_id}/errors", params={"cursor": cursor}, headers=headers
    )
    items = first.json()["items"] + rest.json()["items"]
    assert [{k: e[k] for k in ("row_no", "code", "field")} for e in items] == expected
    assert items[2]["raw"] == {"email": "short@example.com", "first_name": "Мало"}


async def test_bytes_not_in_utf8_and_nul_are_invalid_encoding(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    # cp1251 вместо UTF-8 портит только свою строку; NUL Postgres не хранит ни в text, ни в
    # jsonb — без проверки COPY провалил бы весь шаг parse.
    content = csv_of("ok@example.com,Добре,,,,,,") + (
        b"cp@example.com,\xc4\xe5\xed,,,,,,\n"
        b"nul@example.com,A\x00B,,,,,,\n"
        b"nul-extra@example.com,A,\x00,,,\n"
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.failed_rows) == ("partial_failed", 4, 1, 3)
    assert await errors_of(uow, job_id) == [
        (3, "invalid_encoding", "first_name"),
        (4, "invalid_encoding", "first_name"),
        (5, "invalid_columns", None),
    ]
    assert (await raw_of(uow, job_id, 4))["first_name"] == "A�B"
    assert (await raw_of(uow, job_id, 5))["last_name"] == "�"
    assert set(await contacts_of(uow, people.org_id)) == {"ok@example.com"}


async def test_too_many_rows_fails_job_without_contacts(
    uow: UnitOfWork, file_storage: FakeFileStorage, lock: AdvisoryLock, people: People
) -> None:
    imports = service(uow, file_storage, max_rows=2)
    content = csv_of(*(f"u{i}@example.com,U,,,,,," for i in range(3)))
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.total) == ("failed", "import.too_many_rows", 0)
    assert job.result == {"created": 0, "updated": 0}
    assert await contacts_of(uow, people.org_id) == {}
    assert await staged(uow, job_id) == 0
    [entry] = await audit_of(uow, job_id)
    assert entry.changes["status"] == "failed"


async def test_malformed_csv_fails_with_row_number(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    content = csv_of("a@example.com,A,,,,,,", "b@example.com,B,,,,,,", 'c@example.com,"C,,,,,,')
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.error) == ("failed", "import.malformed_csv")
    assert await errors_of(uow, job_id) == [(4, "malformed_csv", None)]
    assert await contacts_of(uow, people.org_id) == {}  # parse — одна транзакция: откат
    assert await staged(uow, job_id) == 0


async def test_resume_after_crash_between_steps(
    uow: UnitOfWork,
    file_storage: FakeFileStorage,
    lock: AdvisoryLock,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = service(uow, file_storage)
    content = csv_of(*(f"u{i}@example.com,U{i},,,,,," for i in range(5)), ",Без пошти,,,,,,")
    job_id = await start(imports, people.lead, content)

    async def crash(*_: Any) -> None:
        raise ConnectionResetError("воркер упал после parse")

    with monkeypatch.context() as patch:
        patch.setattr(ImportService, "_stage", crash)
        with pytest.raises(ConnectionResetError):
            await imports.run(job_id, lock)
    job = await job_of(uow, job_id)
    assert (job.status, job.stage, job.total, job.failed_rows) == ("running", "parse", 6, 1)
    assert await staged(uow, job_id) == 5

    await service(uow, file_storage).run(job_id, lock)  # повтор задачи: parse не повторяется

    job = await job_of(uow, job_id)
    assert (job.status, job.total, job.processed, job.failed_rows) == ("partial_failed", 6, 5, 1)
    assert len(await contacts_of(uow, people.org_id)) == 5
    assert await errors_of(uow, job_id) == [(7, "email_required", "email")]


async def test_resume_after_crash_inside_merge_counts_once(
    uow: UnitOfWork,
    file_storage: FakeFileStorage,
    lock: AdvisoryLock,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = service(uow, file_storage)
    content = csv_of(*(f"u{i}@example.com,U{i},,,,,," for i in range(5)))
    job_id = await start(imports, people.lead, content)
    original = uow.imports.mark_merged
    calls = 0

    async def crash_on_second_chunk(*args: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:  # INSERT второго чанка уже выполнен, но транзакция не закоммичена
            raise ConnectionResetError("воркер упал посреди чанка")
        await original(*args)

    with monkeypatch.context() as patch:
        patch.setattr(uow.imports, "mark_merged", crash_on_second_chunk)
        with pytest.raises(ConnectionResetError):
            await imports.run(job_id, lock)
    job = await job_of(uow, job_id)
    assert (job.stage, job.processed, job.result) == ("stage", 2, {"created": 2, "updated": 0})
    assert len(await contacts_of(uow, people.org_id)) == 2  # второй чанк откатился целиком

    await service(uow, file_storage).run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.result) == ("done", 5, {"created": 5, "updated": 0})
    assert len(await contacts_of(uow, people.org_id)) == 5


async def test_second_copy_exits_on_advisory_lock(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    job_id = await start(imports, people.lead, csv_of("a@example.com,A,,,,,,"))

    async with lock.hold(str(job_id)) as taken:  # первая копия задачи ещё работает
        assert taken
        await imports.run(job_id, lock)
        job = await job_of(uow, job_id)
        assert (job.status, job.stage) == ("pending", None)

    await imports.run(job_id, lock)  # блокировку отпустили — следующая копия работает
    assert (await job_of(uow, job_id)).status == "done"


async def test_cancel_mid_import_stops_before_next_step(
    uow: UnitOfWork,
    file_storage: FakeFileStorage,
    lock: AdvisoryLock,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = service(uow, file_storage)
    job_id = await start(imports, people.lead, csv_of("a@example.com,A,,,,,,"))

    async def crash(*_: Any) -> None:
        raise ConnectionResetError

    with monkeypatch.context() as patch:
        patch.setattr(ImportService, "_stage", crash)
        with pytest.raises(ConnectionResetError):
            await imports.run(job_id, lock)
    cancelled = await imports.bulk.cancel(people.lead.access, job_id)
    assert cancelled.status == "cancelling"

    await service(uow, file_storage).run(job_id, lock)  # reaper переотправил задачу

    job = await job_of(uow, job_id)
    assert (job.status, job.result) == ("cancelled", {"created": 0, "updated": 0})
    assert await staged(uow, job_id) == 0
    assert await contacts_of(uow, people.org_id) == {}
    [entry] = await audit_of(uow, job_id)
    assert entry.changes["status"] == "cancelled"


async def test_cancelled_pending_import_has_empty_result(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    job_id = await start(imports, people.lead, csv_of("a@example.com,A,,,,,,"))
    await imports.bulk.cancel(people.lead.access, job_id)

    await imports.run(job_id, lock)  # задача пришла уже после отмены — ничего не делает

    job = await job_of(uow, job_id)
    assert (job.status, job.started_at) == ("cancelled", None)
    assert job.result == {"created": 0, "updated": 0}
    assert await contacts_of(uow, people.org_id) == {}
    assert len(await audit_of(uow, job_id)) == 1


async def test_cancel_between_read_and_start_does_not_parse(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # _checkpoint прочитал pending, и тут отмена закоммитилась раньше, чем pending → running:
    # UPDATE не прошёл — задача перечитывает статус и не начинает parse у отменённой job.
    job_id = await start(imports, people.lead, csv_of("a@example.com,A,,,,,,"))
    original = uow.bulk_jobs.start_import

    async def cancel_first(job: uuid.UUID) -> bool:
        await imports.bulk.cancel(people.lead.access, job)
        return await original(job)

    monkeypatch.setattr(uow.bulk_jobs, "start_import", cancel_first)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.stage, job.total, job.started_at) == ("cancelled", None, 0, None)
    assert await staged(uow, job_id) == 0
    assert await errors_of(uow, job_id) == []
    assert len(await audit_of(uow, job_id)) == 1


async def test_step_after_reaper_gave_up_is_rolled_back(
    imports: ImportService,
    lock: AdvisoryLock,
    uow: UnitOfWork,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Пока шёл parse, reaper сдался (failed, bulk.stuck) и удалил staging. Записи шага
    # условные: шаг откатывается целиком, после финала ничего не остаётся.
    content = csv_of("a@example.com,A,,,,,,", "b@example.com,B,,,,,,", ",Без пошти,,,,,,")
    job_id = await start(imports, people.lead, content)
    original = uow.imports.copy_staging
    reaped = False

    async def reaper_first(records: Any) -> None:
        nonlocal reaped
        if not reaped:
            reaped = True
            finished = await uow.bulk_jobs.fail_stuck(job_id)
            assert finished is not None
            await imports.bulk._record_final(job_id, finished)
            await uow.commit()
        await original(records)

    monkeypatch.setattr(uow.imports, "copy_staging", reaper_first)

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.stage) == ("failed", "bulk.stuck", None)
    assert (job.total, job.failed_rows) == (0, 0)
    assert await staged(uow, job_id) == 0
    assert await errors_of(uow, job_id) == []
    [entry] = await audit_of(uow, job_id)
    assert entry.changes["status"] == "failed"


async def test_author_changed_fails_import(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    job_id = await start(imports, people.lead, csv_of("a@example.com,A,,,,,,"))
    await uow.session.execute(update(User).where(User.id == people.lead.id).values(is_active=False))
    await uow.commit()

    await imports.run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.error) == ("failed", "bulk.author_changed")
    assert await contacts_of(uow, people.org_id) == {}


async def test_author_changed_after_parse_fails_and_cleans_staging(
    uow: UnitOfWork,
    file_storage: FakeFileStorage,
    lock: AdvisoryLock,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = service(uow, file_storage)
    job_id = await start(imports, people.lead, csv_of("a@example.com,A,,,,,,", ",B,,,,,,"))

    async def crash(*_: Any) -> None:
        raise ConnectionResetError("воркер упал после parse")

    with monkeypatch.context() as patch:
        patch.setattr(ImportService, "_stage", crash)
        with pytest.raises(ConnectionResetError):
            await imports.run(job_id, lock)
    assert await staged(uow, job_id) == 1
    # Руководителя перевели в менеджеры: bulk:run у него больше нет.
    await uow.session.execute(
        update(User).where(User.id == people.lead.id).values(role="sales_rep")
    )
    await uow.commit()

    await service(uow, file_storage).run(job_id, lock)

    job = await job_of(uow, job_id)
    assert (job.status, job.error, job.failed_rows) == ("failed", "bulk.author_changed", 1)
    assert job.result == {"created": 0, "updated": 0}
    assert await staged(uow, job_id) == 0  # staging — в транзакции финального перехода
    assert await errors_of(uow, job_id) == [(3, "email_required", "email")]
    [entry] = await audit_of(uow, job_id)
    assert entry.changes["rows"] == {"created": 0, "updated": 0, "failed": 1}


async def test_owner_deactivated_before_merge_makes_rows_owner_invalid(
    uow: UnitOfWork,
    file_storage: FakeFileStorage,
    lock: AdvisoryLock,
    people: People,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = service(uow, file_storage)
    content = csv_of("a@example.com,A,,,,,,", f"b@example.com,B,,,,,,{people.other_rep.email}")
    job_id = await start(imports, people.admin, content, owner_id=people.rep.id)
    original = ImportService._merge_chunk

    async def deactivate_then_merge(self: ImportService, *args: Any) -> int | None:
        # После stage, перед первым чанком: ответственного по умолчанию отключили.
        await self.uow.session.execute(
            update(User).where(User.id == people.rep.id).values(is_active=False)
        )
        await self.uow.commit()
        monkeypatch.setattr(ImportService, "_merge_chunk", original)
        return await original(self, *args)

    monkeypatch.setattr(ImportService, "_merge_chunk", deactivate_then_merge)

    await imports.run(job_id, lock)

    assert await errors_of(uow, job_id) == [(2, "owner_invalid", None)]
    assert set(await contacts_of(uow, people.org_id)) == {"b@example.com"}
    job = await job_of(uow, job_id)
    assert (job.status, job.processed, job.failed_rows) == ("partial_failed", 1, 1)
    assert await staged(uow, job_id) == 0


async def test_semicolon_file_with_unknown_columns(
    imports: ImportService, lock: AdvisoryLock, uow: UnitOfWork, people: People
) -> None:
    # Наш экспорт загружается обратно: id и created_at игнорируются; Excel — «;».
    content = csv_of(
        "17;a@example.com;'=Формула;2026-01-01",
        header="id;Email;First_Name;created_at",
    )
    job_id = await start(imports, people.lead, content)

    await imports.run(job_id, lock)

    found = await contacts_of(uow, people.org_id)
    assert found["a@example.com"].first_name == "=Формула"
    assert (await job_of(uow, job_id)).status == "done"
