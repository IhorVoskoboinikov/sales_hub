"""Задачи, упавшие после всех ретраев (раздел 15): строка failed_tasks из сигнала task_failure на
настоящем воркере и эндпоинты супер-админа — список, карточка, перезапуск.

Воркер (маркеры worker и committing) пишет строки своей сессией и коммитит — после теста они
удаляются по task_id. Эндпоинты работают в транзакции теста, TaskSender — фейк (conftest):
публикация запоминается, а не уходит в Redis сессионного воркера.
"""

import queue
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog
from celery.worker import WorkController
from httpx import AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.context import request_id_var
from app.db.session import create_engine, create_session_factory
from app.models import AuditEntry, FailedTask
from app.uow import UnitOfWork
from app.workers import base
from app.workers.base import BaseTask
from app.workers.celery_app import celery_app
from tests.integration.conftest import (
    FakeTaskSender,
    MakeFailedTask,
    MakeUser,
    tokens_for,
)

WAIT_SECONDS = 15
URL = "/api/v1/admin/failed-tasks"

ran: queue.Queue[str] = queue.Queue()


# Тестовые задачи регистрируются при импорте модуля — до старта сессионного воркера.
@celery_app.task(base=BaseTask, name="tests.always_broken")
def always_broken(label: str, *, extra: int = 0) -> None:
    ran.put(label)
    raise ValueError(f"broken: {label}")  # не повторяется — провал сразу


@celery_app.task(base=BaseTask, name="tests.broker_flaky")
def broker_flaky(label: str) -> None:
    ran.put(label)
    raise ConnectionRefusedError("redis:6379")  # повторяется, пока не кончатся ретраи


@celery_app.task(base=BaseTask, name="tests.marker")
def marker(label: str) -> None:
    # Воркер solo берёт задачи по порядку: маркер выполнился — у предыдущих отработал и
    # обработчик task_failure.
    ran.put(label)


def wait_ran(*labels: str) -> None:
    got = [ran.get(timeout=WAIT_SECONDS) for _ in labels]
    assert got == list(labels)


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def task_ids(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[list[str]]:
    """id задач теста: после него их строки failed_tasks удаляются (таблица общая)."""
    ids: list[str] = []
    yield ids
    async with sessions() as s:
        await s.execute(delete(FailedTask).where(FailedTask.task_id.in_(ids)))
        await s.commit()


async def rows_of(sessions: async_sessionmaker[AsyncSession], task_id: str) -> list[FailedTask]:
    async with sessions() as s:
        return list(await s.scalars(select(FailedTask).where(FailedTask.task_id == task_id)))


@pytest.mark.worker
@pytest.mark.committing
async def test_failed_task_is_recorded_with_context(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    task_ids: list[str],
) -> None:
    user_id = uuid.uuid4()
    token = request_id_var.set("req-failed")
    try:
        with structlog.contextvars.bound_contextvars(user_id=str(user_id)):
            result = celery_app.send_task("tests.always_broken", args=["a"], kwargs={"extra": 7})
    finally:
        request_id_var.reset(token)
    task_ids.append(result.id)
    marker_id = celery_app.send_task("tests.marker", args=["after-a"]).id
    task_ids.append(marker_id)

    wait_ran("a", "after-a")

    [row] = await rows_of(sessions, result.id)
    assert (row.task_name, row.queue, row.args, row.kwargs) == (
        "tests.always_broken",
        "default",
        ["a"],
        {"extra": 7},
    )
    assert (row.request_id, row.user_id) == ("req-failed", user_id)
    assert (row.exc_type, row.exc_message, row.retries) == ("builtins.ValueError", "broken: a", 0)
    assert row.traceback is not None and "ValueError: broken: a" in row.traceback
    assert row.replayed_at is None
    assert await rows_of(sessions, marker_id) == []  # успешная задача строки не даёт


@pytest.mark.worker
@pytest.mark.committing
async def test_redelivered_failure_is_recorded_once(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    task_ids: list[str],
) -> None:
    # Повторная доставка — то же сообщение с тем же task_id (воркер упал до подтверждения).
    task_id = f"redelivered-{uuid.uuid4()}"
    task_ids.append(task_id)
    for _ in range(2):
        celery_app.send_task("tests.always_broken", args=["dup"], task_id=task_id)
    celery_app.send_task("tests.marker", args=["after-dup"])

    wait_ran("dup", "dup", "after-dup")

    assert len(await rows_of(sessions, task_id)) == 1  # ON CONFLICT (task_id) DO NOTHING


@pytest.mark.worker
@pytest.mark.committing
async def test_failure_is_recorded_after_all_retries(
    celery_worker: WorkController,
    sessions: async_sessionmaker[AsyncSession],
    task_ids: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(base, "backoff", lambda retries: 0.1)
    result = celery_app.send_task("tests.broker_flaky", args=["retry"])
    task_ids.append(result.id)

    wait_ran(*["retry"] * (BaseTask.max_retries + 1))  # первая попытка и все повторы
    celery_app.send_task("tests.marker", args=["after-retry"])
    wait_ran("after-retry")

    # На Retry сигнала нет: строка одна — после последней попытки.
    [row] = await rows_of(sessions, result.id)
    assert (row.exc_type, row.retries) == ("builtins.ConnectionRefusedError", BaseTask.max_retries)


# --- эндпоинты супер-админа


@pytest.fixture
async def root(client: AsyncClient, make_user: MakeUser) -> dict[str, str]:
    user = await make_user(f"root-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    return await tokens_for(client, user)


async def root_id(client: AsyncClient, headers: dict[str, str]) -> uuid.UUID:
    return uuid.UUID((await client.get("/api/v1/auth/me", headers=headers)).json()["id"])


async def test_list_newest_first_without_traceback(
    client: AsyncClient, root: dict[str, str], make_failed_task: MakeFailedTask
) -> None:
    name = f"tests.list_{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC)
    old = await make_failed_task(task_name=name, failed_at=now - timedelta(hours=2))
    replayed = await make_failed_task(
        task_name=name, failed_at=now - timedelta(hours=1), replayed_at=now
    )
    new = await make_failed_task(task_name=name, failed_at=now)
    await make_failed_task()  # чужое имя — фильтр его не покажет
    ids = (old.id, replayed.id, new.id)

    resp = await client.get(URL, params={"task_name": name}, headers=root)

    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert [i["id"] for i in items] == [ids[2], ids[1], ids[0]]
    assert "traceback" not in items[0]
    assert items[0]["exc_type"] == "builtins.ConnectionRefusedError"

    pending = await client.get(URL, params={"task_name": name, "replayed": False}, headers=root)
    done = await client.get(URL, params={"task_name": name, "replayed": True}, headers=root)
    assert [i["id"] for i in pending.json()["items"]] == [ids[2], ids[0]]
    assert [i["id"] for i in done.json()["items"]] == [ids[1]]


async def test_list_pages_by_cursor(
    client: AsyncClient, root: dict[str, str], make_failed_task: MakeFailedTask
) -> None:
    name = f"tests.page_{uuid.uuid4().hex[:6]}"
    ids = [(await make_failed_task(task_name=name)).id for _ in range(3)]  # один failed_at

    first = (await client.get(URL, params={"task_name": name, "limit": 2}, headers=root)).json()
    second = (
        await client.get(
            URL,
            params={"task_name": name, "limit": 2, "cursor": first["next_cursor"]},
            headers=root,
        )
    ).json()

    assert [i["id"] for i in first["items"]] == [ids[2], ids[1]]  # при равном времени — по id
    assert [i["id"] for i in second["items"]] == [ids[0]]
    assert second["next_cursor"] is None


async def test_card_has_all_fields(
    client: AsyncClient, root: dict[str, str], make_failed_task: MakeFailedTask
) -> None:
    task = await make_failed_task(
        task_name="notifications.send_email", args=[17], traceback="Traceback: smtp"
    )

    resp = await client.get(f"{URL}/{task.id}", headers=root)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert (data["task_name"], data["args"], data["traceback"]) == (
        "notifications.send_email",
        [17],
        "Traceback: smtp",
    )
    assert data["replayed_at"] is None


async def test_missing_card_is_404(client: AsyncClient, root: dict[str, str]) -> None:
    resp = await client.get(f"{URL}/{2**62}", headers=root)

    assert resp.status_code == 404
    assert resp.json()["details"] == {"entity": "failed_task", "id": 2**62}


async def test_replay_sends_task_by_name_and_marks_row(
    client: AsyncClient,
    root: dict[str, str],
    make_failed_task: MakeFailedTask,
    task_sender: FakeTaskSender,
    uow: UnitOfWork,
) -> None:
    task = await make_failed_task(
        task_name="notifications.send_email", args=[42], kwargs={"force": True}
    )
    task_id = task.id
    admin_id = await root_id(client, root)

    resp = await client.post(f"{URL}/{task_id}/replay", headers=root)

    assert resp.status_code == 202, resp.text
    new_task_id = resp.json()["task_id"]
    assert task_sender.sent == [("notifications.send_email", [42], {"force": True})]
    row = await uow.failed_tasks.get_or_404(task_id)
    assert (row.replayed_by, row.replay_task_id) == (admin_id, new_task_id)
    assert row.replayed_at is not None
    [entry] = list(
        await uow.session.scalars(
            select(AuditEntry).where(
                AuditEntry.entity_type == "failed_task", AuditEntry.entity_id == str(task_id)
            )
        )
    )
    assert (entry.action, entry.org_id, entry.actor_id) == (
        "failed_task.replayed",
        None,
        admin_id,
    )
    assert entry.changes == {
        "task_name": "notifications.send_email",
        "replay_task_id": new_task_id,
    }

    again = await client.post(f"{URL}/{task_id}/replay", headers=root)
    assert again.status_code == 409
    assert again.json()["code"] == "failed_task.already_replayed"
    assert len(task_sender.sent) == 1


@pytest.mark.parametrize(
    ("task_name", "code"),
    [
        ("tests.renamed_long_ago", "failed_task.unknown_task"),
        ("bulk.chunk", "failed_task.not_replayable"),
        ("bulk.plan", "failed_task.not_replayable"),
    ],
)
async def test_replay_conflicts(
    client: AsyncClient,
    root: dict[str, str],
    make_failed_task: MakeFailedTask,
    task_sender: FakeTaskSender,
    uow: UnitOfWork,
    task_name: str,
    code: str,
) -> None:
    task_id = (await make_failed_task(task_name=task_name, args=[str(uuid.uuid4()), 0])).id

    resp = await client.post(f"{URL}/{task_id}/replay", headers=root)

    assert resp.status_code == 409, resp.text
    assert resp.json()["code"] == code
    assert resp.json()["details"] == {"task_name": task_name}
    assert task_sender.sent == []
    assert (await uow.failed_tasks.get_or_404(task_id)).replayed_at is None


async def test_replay_with_broker_down_is_503_and_changes_nothing(
    client: AsyncClient,
    root: dict[str, str],
    make_failed_task: MakeFailedTask,
    task_sender: FakeTaskSender,
    uow: UnitOfWork,
) -> None:
    task_id = (await make_failed_task()).id
    task_sender.broker_down = True

    resp = await client.post(f"{URL}/{task_id}/replay", headers=root)

    assert resp.status_code == 503, resp.text
    assert resp.json()["code"] == "unavailable"
    assert (await uow.failed_tasks.get_or_404(task_id)).replayed_at is None
    audit = await uow.session.scalar(
        select(func.count())
        .select_from(AuditEntry)
        .where(AuditEntry.entity_type == "failed_task", AuditEntry.entity_id == str(task_id))
    )
    assert audit == 0

    task_sender.broker_down = False  # брокер вернулся — перезапуск проходит
    assert (await client.post(f"{URL}/{task_id}/replay", headers=root)).status_code == 202


async def test_org_admin_cannot_see_failed_tasks(
    client: AsyncClient, make_user: MakeUser, make_org: Any, make_failed_task: MakeFailedTask
) -> None:
    org = await make_org()
    admin = await make_user(
        f"admin-{uuid.uuid4().hex[:8]}@example.com", org_id=org.id, role="org_admin"
    )
    headers = await tokens_for(client, admin)
    task_id = (await make_failed_task()).id

    for method, path in (("GET", URL), ("POST", f"{URL}/{task_id}/replay")):
        resp = await client.request(method, path, headers=headers)
        assert resp.status_code == 403, resp.text
