"""Две формы одного правила должны отвечать одинаково (раздел 5): matches() для объекта
в памяти и sql_filter() для WHERE. Иначе «в списке не видно, а по прямой ссылке открывается».

Проверяем полным перебором: все сочетания «кто спрашивает» × все сочетания «чья запись».
"""

import itertools
import uuid
from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, Table, Uuid, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.access import Access

ORGS = [uuid.uuid4(), uuid.uuid4()]
TEAMS = [uuid.uuid4(), uuid.uuid4(), None]
USERS = [uuid.uuid4(), uuid.uuid4(), None]

rows_table = Table(
    "access_rows",
    MetaData(),
    Column("id", Integer, primary_key=True),
    Column("org_id", Uuid),
    Column("team_id", Uuid),
    Column("owner_id", Uuid),
    prefixes=["TEMPORARY"],  # живёт до конца соединения теста
)


async def test_matches_and_sql_filter_agree(db_session: AsyncSession) -> None:
    rows = [
        SimpleNamespace(id=i, org_id=org, team_id=team, owner_id=owner)
        for i, (org, team, owner) in enumerate(itertools.product(ORGS, TEAMS, USERS))
    ]
    connection = await db_session.connection()
    await connection.run_sync(rows_table.create)
    await connection.execute(rows_table.insert(), [vars(r) for r in rows])

    checked = 0
    for scope, user_id, org_id, team_id in itertools.product(
        ["own", "team", "org"], USERS[:2], ORGS, TEAMS
    ):
        access = Access("x:read", scope, user_id, org_id, team_id)  # type: ignore[arg-type]
        in_sql = set(
            (
                await connection.execute(
                    select(rows_table.c.id).where(access.sql_filter(rows_table.c))
                )
            )
            .scalars()
            .all()
        )
        in_memory = {r.id for r in rows if access.matches(r)}  # type: ignore[arg-type]

        assert in_sql == in_memory, access
        checked += 1

    assert checked == 36


def test_team_scope_without_team_sees_nothing() -> None:
    access = Access("x:read", "team", USERS[0], ORGS[0], None)  # type: ignore[arg-type]
    orphan = SimpleNamespace(org_id=ORGS[0], team_id=None, owner_id=USERS[1])

    assert not access.matches(orphan)  # type: ignore[arg-type]
