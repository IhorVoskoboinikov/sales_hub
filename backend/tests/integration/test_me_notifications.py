"""GET / PUT /api/v1/me/notifications (раздел 16, «Настройки»): все разрешённые пары, только
отличия от умолчаний в notification_settings, полная замена, 422 на неизвестную,
неразрешённую и повторную пару, 401 / 403 без организации. Без OPA и без журнала.
Одновременные замены — на настоящих транзакциях, мимо общей транзакции теста."""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.models import AuditEntry, NotificationSetting, Organization, Role, TelegramLink, User
from app.uow import UnitOfWork
from tests.integration.conftest import MakeOrg, MakeUser, member, tokens_for

URL = "/api/v1/me/notifications"

DEFAULT_CHANNELS = [
    {"event_type": "record.assigned", "channel": "telegram", "enabled": True},
    {"event_type": "record.assigned", "channel": "email", "enabled": True},
    {"event_type": "deal.closed", "channel": "telegram", "enabled": True},
    {"event_type": "deal.closed", "channel": "email", "enabled": False},
    {"event_type": "activity.reminder", "channel": "telegram", "enabled": True},
    {"event_type": "digest.morning", "channel": "telegram", "enabled": True},
    {"event_type": "bulk_job.completed", "channel": "telegram", "enabled": True},
    {"event_type": "bulk_job.completed", "channel": "email", "enabled": True},
]


def with_defaults(**changed: bool) -> list[dict[str, Any]]:
    """Ответ GET: умолчания, а изменённые пары (ключ — event_type/channel) — с is_default."""
    result = []
    for item in DEFAULT_CHANNELS:
        key = f"{item['event_type']}/{item['channel']}"
        enabled = changed.get(key, item["enabled"])
        result.append({**item, "enabled": enabled, "is_default": enabled == item["enabled"]})
    return result


async def rows(uow: UnitOfWork, user_id: uuid.UUID) -> set[tuple[str, str, bool]]:
    found = await uow.session.execute(
        select(
            NotificationSetting.event_type, NotificationSetting.channel, NotificationSetting.enabled
        ).where(NotificationSetting.user_id == user_id)
    )
    return {tuple(row) for row in found}  # type: ignore[misc]


async def test_defaults_and_telegram_linked(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, uow: UnitOfWork
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    headers = await tokens_for(client, rep)

    before = await client.get(URL, headers=headers)
    uow.session.add(TelegramLink(user_id=rep_id, chat_id=3_000_000 + uuid.uuid4().int % 10**9))
    await uow.commit()
    after = await client.get(URL, headers=headers)

    assert before.status_code == 200
    assert before.json() == {"telegram_linked": False, "channels": with_defaults()}
    assert after.json()["telegram_linked"] is True


async def test_put_stores_only_differences_and_replaces_everything(
    client: AsyncClient, make_user: MakeUser, make_org: MakeOrg, uow: UnitOfWork
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    headers = await tokens_for(client, rep)

    first = await client.put(
        URL,
        json={
            "channels": [
                {"event_type": "deal.closed", "channel": "email", "enabled": True},
                {"event_type": "digest.morning", "channel": "telegram", "enabled": False},
                # Значение по умолчанию — строки не даёт.
                {"event_type": "record.assigned", "channel": "email", "enabled": True},
            ]
        },
        headers=headers,
    )

    assert first.status_code == 200, first.text
    expected = with_defaults(**{"deal.closed/email": True, "digest.morning/telegram": False})
    assert first.json() == {"telegram_linked": False, "channels": expected}
    assert (await client.get(URL, headers=headers)).json()["channels"] == expected
    assert await rows(uow, rep_id) == {
        ("deal.closed", "email", True),
        ("digest.morning", "telegram", False),
    }

    # Полная замена: чего нет в списке — снова по умолчанию.
    second = await client.put(
        URL,
        json={
            "channels": [{"event_type": "bulk_job.completed", "channel": "email", "enabled": False}]
        },
        headers=headers,
    )
    assert second.json()["channels"] == with_defaults(**{"bulk_job.completed/email": False})
    assert await rows(uow, rep_id) == {("bulk_job.completed", "email", False)}

    cleared = await client.put(URL, json={"channels": []}, headers=headers)
    assert cleared.json()["channels"] == with_defaults()
    assert await rows(uow, rep_id) == set()
    # Личные настройки в журнал не пишутся.
    audit = await uow.session.scalar(
        select(func.count()).select_from(AuditEntry).where(AuditEntry.actor_id == rep_id)
    )
    assert audit == 0


@pytest.mark.parametrize(
    ("channels", "reason"),
    [
        ([{"event_type": "activity.reminder", "channel": "email", "enabled": True}], "not_allowed"),
        ([{"event_type": "digest.morning", "channel": "email", "enabled": False}], "not_allowed"),
        (
            [
                {"event_type": "deal.closed", "channel": "email", "enabled": True},
                {"event_type": "deal.closed", "channel": "email", "enabled": False},
            ],
            "duplicate",
        ),
    ],
)
async def test_disallowed_or_repeated_pair_is_422(
    channels: list[dict[str, Any]],
    reason: str,
    client: AsyncClient,
    make_user: MakeUser,
    make_org: MakeOrg,
    uow: UnitOfWork,
) -> None:
    org = await make_org()
    rep = await member(make_user, org, "sales_rep")
    rep_id = rep.id
    headers = await tokens_for(client, rep)

    resp = await client.put(URL, json={"channels": channels}, headers=headers)

    assert resp.status_code == 422
    body = resp.json()
    assert body["code"] == "validation"
    assert body["details"] == {"field": "channels", "index": len(channels) - 1, "reason": reason}
    assert await rows(uow, rep_id) == set()


@pytest.mark.parametrize(
    "channels",
    [
        [{"event_type": "deal.opened", "channel": "telegram", "enabled": True}],
        [{"event_type": "deal.closed", "channel": "sms", "enabled": True}],
        [{"event_type": "deal.closed", "channel": "email"}],
    ],
)
async def test_unknown_pair_is_422(
    channels: list[dict[str, Any]], client: AsyncClient, make_user: MakeUser, make_org: MakeOrg
) -> None:
    org = await make_org()
    headers = await tokens_for(client, await member(make_user, org, "viewer"))

    resp = await client.put(URL, json={"channels": channels}, headers=headers)

    assert (resp.status_code, resp.json()["code"]) == (422, "validation")


@pytest.mark.parametrize("method", ["GET", "PUT"])
async def test_requires_member(method: str, client: AsyncClient, make_user: MakeUser) -> None:
    body: dict[str, Any] = {"channels": []}
    resp = await client.request(method, URL, json=body)
    assert (resp.status_code, resp.json()["code"]) == (401, "auth.invalid_token")

    # Супер-админ без организации: личных настроек CRM у него нет.
    admin = await make_user(f"admin-{uuid.uuid4().hex[:8]}@example.com", is_superadmin=True)
    resp = await client.request(method, URL, json=body, headers=await tokens_for(client, admin))
    assert (resp.status_code, resp.json()["code"]) == (403, "forbidden")


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_engine(settings.database_url)
    yield create_session_factory(engine)
    await engine.dispose()


SettingRows = list[tuple[str, str, bool]]


@pytest.mark.parametrize(
    "second",
    [
        [],  # «сбросить всё»
        [("digest.morning", "telegram", True), ("deal.closed", "email", False)],
    ],
    ids=["reset", "reversed_pairs"],
)
async def test_concurrent_replaces_go_in_turn_and_last_wins(
    second: SettingRows, sessions: async_sessionmaker[AsyncSession]
) -> None:
    """Два PUT одного пользователя одновременно: второй ждёт advisory lock первого, и его
    DELETE видит вставленные строки — остаётся ровно набор второго. Без очереди «сброс»
    оставил бы строки первого, а пары в обратном порядке — deadlock или конфликт ключа (500)."""
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    async with sessions() as s:
        s.add(Organization(id=org_id, name="Race", slug=f"race-{org_id.hex[:8]}"))
        await s.flush()
        s.add(
            User(
                id=user_id,
                email=f"n-{org_id.hex[:8]}@example.com",
                password_hash="x",
                org_id=org_id,
                role=Role.VIEWER,
            )
        )
        await s.commit()
    first: SettingRows = [("deal.closed", "email", True), ("digest.morning", "telegram", False)]
    try:
        async with sessions() as one, sessions() as two:
            # Первый PUT заменил строки и ещё не закоммитил…
            await UnitOfWork(one).notification_settings.replace(user_id, first)

            async def replace_second() -> None:
                await UnitOfWork(two).notification_settings.replace(user_id, second)
                await two.commit()

            later = asyncio.create_task(replace_second())
            await asyncio.sleep(0.5)
            assert not later.done()  # …второй ждёт его, а не удаляет по старому снимку

            await one.commit()
            await later

        async with sessions() as s:
            stored = await UnitOfWork(s).notification_settings.for_user(user_id)
        assert stored == {(event, channel): enabled for event, channel, enabled in second}
    finally:
        async with sessions() as s:
            await s.execute(delete(User).where(User.id == user_id))  # строки — CASCADE
            await s.execute(delete(Organization).where(Organization.id == org_id))
            await s.commit()
