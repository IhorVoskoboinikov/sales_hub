"""Импорт CSV конец в конец (маркеры worker и committing): POST /contacts/import настоящего
приложения → файл в RustFS → imports.import_contacts в Redis → настоящий воркер скачивает файл,
проходит parse → stage → merge → job завершена, контакты в базе. И parse на настоящих
транзакциях: у общей транзакции теста каждый COPY шёл бы в SAVEPOINT и откатывался бы с ней,
даже если бы шаг parse транзакцию не открыл.

Данные коммитятся (воркер не видит транзакцию теста), после теста фикстура удаляет организацию.
"""

import asyncio
import io
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from celery.worker import WorkController
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.locks import AdvisoryLock
from app.db.session import create_engine, create_session_factory
from app.main import create_app
from app.models import (
    AuditEntry,
    BulkJob,
    Contact,
    ContactStaging,
    ImportRowError,
    Organization,
    Role,
    Team,
    User,
)
from app.repositories.imports import ImportRepository
from app.services.bulk import BulkService
from app.services.imports import ImportService, ImportUpload, read_header
from app.storage import S3FileStorage
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import (
    FakeFileStorage,
    FakeTaskQueue,
    RustFs,
    bearer,
    import_form,
    login,
    multipart_body,
    password_hash,
    s3_settings,
)

WAIT_SECONDS = 30
CATALOG = RoleCatalog.load(POLICIES_DIR)


@dataclass(frozen=True)
class CommittedLead:
    org_id: uuid.UUID
    team_id: uuid.UUID
    lead_id: uuid.UUID
    email: str


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def committed(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[CommittedLead]:
    """Организация с руководителем отдела — закоммиченные; после теста всё удаляется."""
    org_id, team_id, lead_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    email = f"lead-{org_id.hex[:8]}@example.com"
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Import", slug=f"import-{org_id.hex[:8]}"))
        await s.flush()
        s.add(Team(id=team_id, org_id=org_id, name="Продажі"))
        await s.flush()
        s.add(
            User(
                id=lead_id,
                email=email,
                password_hash=await password_hash(),
                email_verified_at=datetime.now(UTC),
                org_id=org_id,
                role=Role.TEAM_LEAD,
                team_id=team_id,
            )
        )
        await s.commit()
    try:
        yield CommittedLead(org_id, team_id, lead_id, email)
    finally:
        async with sessions() as s:
            await s.execute(delete(AuditEntry).where(AuditEntry.org_id == org_id))
            # staging и ошибки строк — каскадом от job
            await s.execute(delete(BulkJob).where(BulkJob.org_id == org_id))
            await s.execute(delete(Contact).where(Contact.org_id == org_id))
            await s.execute(delete(User).where(User.org_id == org_id))
            await s.execute(delete(Team).where(Team.org_id == org_id))
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()


@pytest.fixture
async def real_client(
    settings: Settings, rustfs: RustFs, storage: S3FileStorage
) -> AsyncIterator[AsyncClient]:
    """Приложение без подмен: настоящие сессии (коммиты), TaskQueue в Redis и RustFS (бакет
    уже создан фикстурой storage)."""
    app: FastAPI = create_app(settings.model_copy(update=s3_settings(rustfs)))
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.mark.worker
@pytest.mark.committing
async def test_import_end_to_end_on_worker(
    celery_worker: WorkController,
    real_client: AsyncClient,
    sessions: async_sessionmaker[AsyncSession],
    committed: CommittedLead,
) -> None:
    content = (
        "email;first_name;tags\n"
        + "".join(f"client{i}@example.com;Клієнт {i};vip\n" for i in range(30))
        + "CLIENT0@example.com;Дубль;\n"  # дубль в файле — ошибка строки
        + ";Без пошти;\n"
    ).encode()
    headers = bearer(await login(real_client, committed.email))
    body, form_headers = multipart_body(import_form(content))

    resp = await real_client.post(
        "/api/v1/contacts/import", content=body, headers=headers | form_headers
    )

    assert resp.status_code == 202, resp.text
    job_id = uuid.UUID(resp.json()["id"])
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while True:
        async with sessions() as s:
            job = await s.get(BulkJob, job_id)
            assert job is not None
            if job.finished_at is not None:
                break
        assert asyncio.get_running_loop().time() < deadline, f"job не завершилась: {job.status}"
        await asyncio.sleep(0.2)

    assert (job.status, job.stage, job.error) == ("partial_failed", "merge", None)
    assert (job.total, job.processed, job.failed_rows) == (32, 30, 2)
    assert job.result == {"created": 30, "updated": 0}
    async with sessions() as s:
        contacts = list(await s.scalars(select(Contact).where(Contact.org_id == committed.org_id)))
        codes = list(
            await s.scalars(
                select(ImportRowError.code)
                .where(ImportRowError.job_id == job_id)
                .order_by(ImportRowError.row_no)
            )
        )
    assert len(contacts) == 30
    assert {c.owner_id for c in contacts} == {committed.lead_id}
    assert {tuple(c.tags) for c in contacts} == {("vip",)}
    assert codes == ["duplicate_in_file", "email_required"]
    snapshot = await real_client.get(f"/api/v1/bulk-jobs/{job_id}", headers=headers)
    assert snapshot.json()["status"] == "partial_failed"


async def leftovers(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> int:
    """Строк staging и ошибок строк job — из новой сессии, то есть только закоммиченные."""
    async with sessions() as s:
        staged = await s.scalar(
            select(func.count()).select_from(ContactStaging).where(ContactStaging.job_id == job_id)
        )
        errors = await s.scalar(
            select(func.count()).select_from(ImportRowError).where(ImportRowError.job_id == job_id)
        )
    return int(staged or 0) + int(errors or 0)


@pytest.mark.committing
async def test_parse_crash_commits_nothing_and_retry_parses_again(
    settings: Settings,
    sessions: async_sessionmaker[AsyncSession],
    committed: CommittedLead,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Воркер падает после первого COPY (батчи по 2 строки): шаг parse — одна транзакция,
    # поэтому в базе не остаётся ни staging, ни ошибок строк, а повтор задачи не упирается
    # в PK (job_id, row_no).
    storage = FakeFileStorage()
    scope = CATALOG.scope_or_none("team_lead", "bulk:run")
    assert scope is not None
    access = Access(
        "bulk:run", scope, committed.lead_id, committed.org_id, committed.team_id, "team_lead"
    )
    content = (
        "email,first_name\n,Без пошти\n" + "".join(f"u{i}@example.com,U{i}\n" for i in range(5))
    ).encode()
    engine = create_engine(settings.database_url, pool_size=2, max_overflow=0)
    lock = AdvisoryLock(engine)
    factory = create_session_factory(engine)

    def service(session: AsyncSession) -> ImportService:
        bulk = BulkService(UnitOfWork(session), CATALOG, FakeTaskQueue(), chunk_size=2)
        return ImportService(bulk, storage, merge_chunk=2, parse_batch=2)

    original = ImportRepository.copy_staging
    calls = 0

    async def crash_on_second_batch(self: ImportRepository, records: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:  # первый батч (ошибка строки и u0) уже ушёл COPY
            raise ConnectionResetError("воркер упал посреди parse")
        await original(self, records)

    try:
        async with factory() as session:
            imports = service(session)
            header = read_header(content, whole_file=True)
            upload = ImportUpload(io.BytesIO(content), len(content), "c.csv", header, None)
            job_id = (await imports.start(access, upload, None)).id
            with monkeypatch.context() as patch:
                patch.setattr(ImportRepository, "copy_staging", crash_on_second_batch)
                with pytest.raises(ConnectionResetError):
                    await imports.run(job_id, lock)
        assert calls == 2
        async with sessions() as s:
            job = await s.get(BulkJob, job_id)
            assert job is not None
            assert (job.status, job.stage, job.total) == ("running", None, 0)
        assert await leftovers(sessions, job_id) == 0

        async with factory() as session:
            await service(session).run(job_id, lock)  # reaper переотправил задачу

        async with sessions() as s:
            job = await s.get(BulkJob, job_id)
            assert job is not None
            assert (job.status, job.total, job.processed, job.failed_rows) == (
                "partial_failed",
                6,
                5,
                1,
            )
            codes = list(
                await s.scalars(select(ImportRowError.code).where(ImportRowError.job_id == job_id))
            )
        assert codes == ["email_required"]
        assert await leftovers(sessions, job_id) == 1  # staging удалён финалом
    finally:
        await engine.dispose()
