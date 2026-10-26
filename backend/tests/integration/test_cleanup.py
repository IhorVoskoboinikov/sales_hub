"""maintenance.cleanup — очистка служебных данных (раздел 15, «Расписание beat»), шаг 5.8.

Сервис — в транзакции теста (коммиты чанков — SAVEPOINT-ы). Хранилище — фейк в памяти, у
одного теста — настоящий RustFS. Очистка глобальная, поэтому тесты проверяют свои строки по
id, а не итоговые числа.
"""

import io
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, update

from app.errors import NotFound
from app.models import (
    BulkJob,
    BulkJobChunk,
    EmailToken,
    ImportRowError,
    OutboxEvent,
    RefreshToken,
    SentNotification,
    User,
)
from app.repositories.maintenance import CleanupRepository
from app.services.maintenance import CleanupService
from app.storage import FileStorage, S3FileStorage
from app.uow import UnitOfWork
from tests.integration.conftest import (
    FakeFileStorage,
    MakeJob,
    MakeOrg,
    MakeUser,
    member,
)

NOW = datetime.now(UTC)
DAY = timedelta(days=1)


def import_params(key: str) -> dict[str, Any]:
    return {"file_key": key, "file_name": "c.csv", "delimiter": ",", "columns": ["email"]}


async def put(storage: FileStorage, key: str) -> None:
    await storage.put(key, io.BytesIO(b"email\n"), 6, "text/csv")


async def ids_of(uow: UnitOfWork, column: Any, ids: list[Any]) -> set[Any]:
    return set(await uow.session.scalars(select(column).where(column.in_(ids))))


@pytest.fixture
async def author(make_org: MakeOrg, make_user: MakeUser) -> User:
    return await member(make_user, await make_org(), "team_lead")


async def test_tokens_outbox_and_sent_notifications(uow: UnitOfWork, author: User) -> None:
    user_id = author.id
    refresh = {
        "expired": RefreshToken(
            id=uuid.uuid4(),
            user_id=user_id,
            family_id=uuid.uuid4(),
            token_hash=uuid.uuid4().hex,
            expires_at=NOW - DAY,
        ),
        "alive": RefreshToken(
            id=uuid.uuid4(),
            user_id=user_id,
            family_id=uuid.uuid4(),
            token_hash=uuid.uuid4().hex,
            expires_at=NOW + DAY,
            revoked_at=NOW - DAY,  # отозван, но не истёк — нужен для поимки повтора
        ),
    }
    email = {
        name: EmailToken(
            id=uuid.uuid4(),
            email=f"{name}-{uuid.uuid4().hex[:6]}@example.com",
            purpose="invite",
            token_hash=uuid.uuid4().hex,
            expires_at=expires,
        )
        for name, expires in (("expired", NOW - DAY), ("alive", NOW + DAY))
    }
    outbox = {
        "old_published": OutboxEvent(
            event_type="email.send", payload={}, published_at=NOW - 8 * DAY
        ),
        "recent_published": OutboxEvent(
            event_type="email.send", payload={}, published_at=NOW - 6 * DAY
        ),
        # Застрявшее событие месячной давности не удаляется никогда: его ещё доставит relay.
        "old_unpublished": OutboxEvent(
            event_type="email.send", payload={}, created_at=NOW - 30 * DAY
        ),
    }
    sent = {
        name: SentNotification(dedup_key=f"test:{uuid.uuid4()}", channel="email", sent_at=at)
        for name, at in (("old", NOW - 31 * DAY), ("recent", NOW - 29 * DAY))
    }
    uow.session.add_all([*refresh.values(), *email.values(), *outbox.values(), *sent.values()])
    await uow.commit()
    refresh_ids = {name: token.id for name, token in refresh.items()}
    email_ids = {name: token.id for name, token in email.items()}
    outbox_ids = {name: event.id for name, event in outbox.items()}
    sent_keys = {name: row.dedup_key for name, row in sent.items()}

    result = await CleanupService(uow, FakeFileStorage(), chunk_size=1).run(NOW)

    assert None not in result.values()
    assert result["refresh_tokens"] >= 1  # type: ignore[operator]
    assert await ids_of(uow, RefreshToken.id, list(refresh_ids.values())) == {refresh_ids["alive"]}
    assert await ids_of(uow, EmailToken.id, list(email_ids.values())) == {email_ids["alive"]}
    assert await ids_of(uow, OutboxEvent.id, list(outbox_ids.values())) == {
        outbox_ids["recent_published"],
        outbox_ids["old_unpublished"],
    }
    assert await ids_of(uow, SentNotification.dedup_key, list(sent_keys.values())) == {
        sent_keys["recent"]
    }


async def test_import_files_and_orphans(uow: UnitOfWork, author: User, make_job: MakeJob) -> None:
    storage = FakeFileStorage()
    prefix = f"imports/{author.org_id}"
    keys = {
        name: f"{prefix}/{name}.csv"
        for name in ("finished_old", "finished_recent", "running", "orphan_old", "orphan_new")
    }
    for key in keys.values():
        await put(storage, key)
    for name in ("finished_old", "finished_recent", "running", "orphan_old"):
        storage.modified[keys[name]] = NOW - 10 * DAY
    storage.modified[keys["orphan_new"]] = NOW - DAY / 2  # job, может быть, ещё коммитится
    finished_old = await make_job(
        author,
        kind="import_contacts",
        params=import_params(keys["finished_old"]),
        finished_at=NOW - 8 * DAY,
    )
    await make_job(
        author,
        kind="import_contacts",
        status="failed",
        params=import_params(keys["finished_recent"]),
        finished_at=NOW - 6 * DAY,
    )
    await make_job(
        author, kind="import_contacts", status="running", params=import_params(keys["running"])
    )
    job_id = finished_old.id

    result = await CleanupService(uow, storage).run(NOW)
    again = await CleanupService(uow, storage).run(NOW)

    assert (result["import_files"], result["orphan_files"]) == (1, 1)
    assert (again["import_files"], again["orphan_files"]) == (0, 0)  # повтор ничего не делает
    assert set(storage.files) == {keys["finished_recent"], keys["running"], keys["orphan_new"]}
    params = await uow.session.scalar(select(BulkJob.params).where(BulkJob.id == job_id))
    assert params is not None
    assert params["file_key"] == keys["finished_old"]  # ключ остаётся — для истории
    assert "file_deleted_at" in params


async def test_old_finished_jobs_are_deleted_with_chunks(
    uow: UnitOfWork, author: User, make_job: MakeJob, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Строки ошибок — чанками по 2: у job импорта их 5, значит, три чанка до удаления job.
    monkeypatch.setattr("app.services.maintenance.JOB_ROWS_CHUNK", 2)
    old_done = await make_job(author)
    old_import = await make_job(
        author, kind="import_contacts", status="failed", params=import_params("imports/x.csv")
    )
    old_running = await make_job(author, kind="add_tags", status="running")
    recent_done = await make_job(author, kind="remove_tags")
    uow.session.add(BulkJobChunk(job_id=old_done.id, no=0, entity="contacts", lo=1, hi=2))
    uow.session.add_all(
        ImportRowError(job_id=old_import.id, row_no=n, code="email_required") for n in range(5)
    )
    await uow.commit()
    ids = {
        "old_done": old_done.id,
        "old_import": old_import.id,
        "old_running": old_running.id,
        "recent": recent_done.id,
    }
    for name, days in (("old_done", 91), ("old_import", 92), ("old_running", 91), ("recent", 89)):
        await uow.session.execute(
            update(BulkJob).where(BulkJob.id == ids[name]).values(created_at=NOW - days * DAY)
        )
    await uow.commit()

    result = await CleanupService(uow, FakeFileStorage()).run(NOW)

    assert result["bulk_jobs"] >= 2  # type: ignore[operator]
    assert await ids_of(uow, BulkJob.id, list(ids.values())) == {
        ids["old_running"],  # незавершённую не трогаем, сколько бы ей ни было
        ids["recent"],
    }
    assert await ids_of(uow, BulkJobChunk.job_id, [ids["old_done"]]) == set()  # каскадом
    assert await ids_of(uow, ImportRowError.job_id, [ids["old_import"]]) == set()


async def test_failing_step_does_not_stop_the_others(
    uow: UnitOfWork, author: User, make_job: MakeJob, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = FakeFileStorage()
    key = f"imports/{author.org_id}/down.csv"
    await put(storage, key)
    await make_job(
        author, kind="import_contacts", params=import_params(key), finished_at=NOW - 8 * DAY
    )
    expired = EmailToken(
        id=uuid.uuid4(),
        email="expired@example.com",
        purpose="reset_password",
        token_hash=uuid.uuid4().hex,
        expires_at=NOW - DAY,
    )
    uow.session.add(expired)
    old = await make_job(author, kind="add_tags")
    await uow.commit()
    expired_id, old_id = expired.id, old.id
    await uow.session.execute(
        update(BulkJob).where(BulkJob.id == old_id).values(created_at=NOW - 91 * DAY)
    )
    await uow.commit()
    storage.down = True  # S3 недоступен: оба шага с файлами падают

    async def broken(self: CleanupRepository, now: datetime, limit: int) -> int:
        raise ConnectionResetError("postgres")

    monkeypatch.setattr(CleanupRepository, "delete_expired_refresh_tokens", broken)

    result = await CleanupService(uow, storage).run(NOW)

    assert result["refresh_tokens"] is None
    assert (result["import_files"], result["orphan_files"]) == (None, None)
    assert result["email_tokens"] is not None and result["bulk_jobs"] is not None
    assert await ids_of(uow, EmailToken.id, [expired_id]) == set()
    assert await ids_of(uow, BulkJob.id, [old_id]) == set()
    assert key in storage.files  # файл остался: удалит следующий запуск


async def test_files_are_deleted_from_real_s3(
    uow: UnitOfWork,
    author: User,
    make_job: MakeJob,
    storage: S3FileStorage,
    tmp_path: Path,
) -> None:
    """RustFS контейнера: удаление и список файлов через S3 API. Время файлов S3 не сдвинуть
    назад, поэтому «сейчас» очистки — через 8 дней."""
    later = NOW + 8 * DAY
    prefix = f"imports/{uuid.uuid4()}"
    finished, orphan, running = (f"{prefix}/{n}.csv" for n in ("finished", "orphan", "running"))
    for key in (finished, orphan, running):
        await put(storage, key)
    await make_job(author, kind="import_contacts", params=import_params(finished), finished_at=NOW)
    await make_job(author, kind="import_contacts", status="running", params=import_params(running))

    result = await CleanupService(uow, storage).run(later)

    assert result["import_files"] is not None and result["orphan_files"] is not None
    listed = {f.key async for page in storage.list_files(prefix) for f in page}
    assert listed == {running}
    for gone in (finished, orphan):
        with pytest.raises(NotFound):
            await storage.download_to(gone, tmp_path / "file.csv")
