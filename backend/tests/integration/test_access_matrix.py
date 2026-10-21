"""Матрица прав «роль × эндпоинт × чей объект» (разделы 5 и 10).

Ожидаемый ответ не пишется руками, а считается из тех же источников, что и права:
- маршрут → действие — у живого OPA (routes.rego);
- роль → действие и уровень — из policies/saleshub/data.yaml.

Правило: действия нет у роли → 403; объект вне уровня чтения → 404 (не раскрываем, что он
есть); объект виден, но вне уровня действия (чужая компания у sales_rep) → 403; иначе 2xx.
Супер-админ проходит только admin:*. Новый маршрут в routes.rego без описания здесь роняет
test_every_route_has_a_case.

Цели (чей объект подставляется в путь):
- own — запись sales_rep-а из «мира» (он же вызывающий с ролью sales_rep, отдел 1);
- same_team — запись его коллеги по отделу 1 (team_lead «мира» — руководитель этого отдела);
- same_org — запись менеджера отдела 2 той же организации;
- other_org — запись чужой организации.
У объектов этапа 3 (отделы, люди) отделов-владельцев нет: для них цели только same_org и
other_org.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from httpx import AsyncClient

from app.authz.opa import ROUTES_PATH
from app.authz.roles import RoleCatalog, Scope
from app.core.config import Settings
from app.core.jwt import KeySet
from app.models import Team, User
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR
from tests.integration.conftest import MakeOrg, MakeRecord, MakeUser, OrgFixture, member

ROLES = ["org_admin", "team_lead", "sales_rep", "viewer", "super_admin"]
CATALOG = RoleCatalog.load(POLICIES_DIR)
CRM_TARGETS = ("own", "same_team", "same_org", "other_org")
ORG_TARGETS = ("same_org", "other_org")


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@dataclass
class World:
    """Две организации; в моей — люди всех ролей и записи разных владельцев."""

    mine: OrgFixture
    other: OrgFixture
    users: dict[str, User]  # роль → вызывающий из моей организации
    empty_team: dict[str, uuid.UUID]  # same_org / other_org → отдел без людей и записей
    any_user: dict[str, uuid.UUID]  # same_org / other_org → пользователь (не вызывающий)
    ids: dict[str, dict[str, int]] = field(default_factory=dict)  # сущность → цель → id

    def owner_for(self, role: str) -> dict[str, Any]:
        """Ответственный в теле запроса: менеджер сам, руководитель и админ — менеджер отдела."""
        if role in ("org_admin", "team_lead"):
            return {"owner_id": str(self.users["sales_rep"].id)}
        return {}


Body = Callable[[World, str], dict[str, Any]]


@dataclass(frozen=True)
class Case:
    """Как вызвать эндпоинт и какое действие он должен требовать.

    Действие записано здесь ещё раз намеренно: если в routes.rego его поменяют (например,
    переименование отдела станет org:read), тест упадёт, а не подстроится.
    read — действие чтения сущности: объект вне его уровня — 404, в нём, но вне уровня
    action — 403 (компании читают все, а меняет менеджер только свои)."""

    action: str
    body: Body | None = None
    targets: tuple[str | None, ...] = (None,)
    read: str | None = None
    lists: str | None = None  # сущность списка: проверить, что в нём ровно записи уровня
    query: Body | None = None  # query-параметры (канбану нужен ?pipeline_id=)
    org_object: bool = False  # объект организации (воронка): чужая — 404, своя видна всем


def company_body(world: World, role: str) -> dict[str, Any]:
    return {"name": unique("Acme"), **world.owner_for(role)}


def deal_body(world: World, role: str) -> dict[str, Any]:
    return {
        "title": unique("Угода"),
        "amount": "100.00",
        "stage_id": world.ids["stage"]["same_org"],
        **world.owner_for(role),
    }


def contact_body(world: World, role: str) -> dict[str, Any]:
    return {"first_name": "Іван", "email": f"{unique('c')}@example.com", **world.owner_for(role)}


# Описание каждого маршрута routes.rego: тело запроса и какие объекты подставлять в путь.
CASES: dict[str, Case] = {
    "GET /api/v1/admin/orgs": Case("admin:orgs"),
    "POST /api/v1/admin/orgs": Case(
        "admin:orgs", lambda w, r: {"name": "Acme", "slug": unique("acme")}
    ),
    "POST /api/v1/admin/orgs/{org_id}/invites": Case(
        "admin:orgs", lambda w, r: {"email": f"{unique('boss')}@example.com"}, ORG_TARGETS
    ),
    "GET /api/v1/org": Case("org:read"),
    "PATCH /api/v1/org": Case("org:manage", lambda w, r: {}),
    "GET /api/v1/teams": Case("org:read"),
    "POST /api/v1/teams": Case("org:manage", lambda w, r: {"name": unique("Відділ")}),
    "PATCH /api/v1/teams/{team_id}": Case(
        "org:manage", lambda w, r: {"name": unique("Відділ")}, ORG_TARGETS
    ),
    "DELETE /api/v1/teams/{team_id}": Case("org:manage", None, ORG_TARGETS),
    "GET /api/v1/users": Case("member:read"),
    "PATCH /api/v1/users/{user_id}": Case(
        "member:manage", lambda w, r: {"is_active": True}, ORG_TARGETS
    ),
    "POST /api/v1/invites": Case(
        "member:manage",
        lambda w, r: {"email": f"{unique('rep')}@example.com", "role": "viewer"},
    ),
    # --- этап 4
    "GET /api/v1/companies": Case("company:read", lists="company"),
    "GET /api/v1/companies/{company_id}": Case("company:read", None, CRM_TARGETS),
    "POST /api/v1/companies": Case("company:write", company_body),
    "PATCH /api/v1/companies/{company_id}": Case(
        "company:write", lambda w, r: {"version": 1, "industry": "IT"}, CRM_TARGETS, "company:read"
    ),
    "GET /api/v1/contacts": Case("contact:read", lists="contact"),
    "GET /api/v1/contacts/{contact_id}": Case("contact:read", None, CRM_TARGETS),
    "POST /api/v1/contacts": Case("contact:write", contact_body),
    "PATCH /api/v1/contacts/{contact_id}": Case(
        "contact:write", lambda w, r: {"version": 1, "phone": "+380"}, CRM_TARGETS, "contact:read"
    ),
    "GET /api/v1/pipelines": Case("org:read"),
    "POST /api/v1/pipelines": Case(
        "org:manage", lambda w, r: {"name": unique("Воронка"), "stages": [{"name": "Новий"}]}
    ),
    "PATCH /api/v1/pipelines/{pipeline_id}": Case(
        "org:manage", lambda w, r: {"name": unique("Воронка")}, ORG_TARGETS, org_object=True
    ),
    "GET /api/v1/deals": Case("deal:read", lists="deal"),
    "GET /api/v1/deals/kanban": Case(
        "deal:read",
        targets=ORG_TARGETS,
        query=lambda w, t: {"pipeline_id": w.ids["pipeline"][t]},
        org_object=True,
    ),
    "GET /api/v1/deals/{deal_id}": Case("deal:read", None, CRM_TARGETS),
    "POST /api/v1/deals": Case("deal:write", deal_body),
    "PATCH /api/v1/deals/{deal_id}": Case(
        "deal:write", lambda w, r: {"version": 1, "title": "Нова"}, CRM_TARGETS, "deal:read"
    ),
    "GET /api/v1/activities": Case("activity:read", lists="activity"),
    "POST /api/v1/activities": Case(
        "activity:write",
        lambda w, r: {"type": "call", "subject": "Дзвінок", **w.owner_for(r)},
    ),
    "PATCH /api/v1/activities/{activity_id}": Case(
        "activity:write", lambda w, r: {"subject": "Інша тема"}, CRM_TARGETS, "activity:read"
    ),
    "DELETE /api/v1/activities/{activity_id}": Case(
        "activity:write", None, CRM_TARGETS, "activity:read"
    ),
    "GET /api/v1/contacts/{contact_id}/notes": Case("contact:read", None, CRM_TARGETS),
    "POST /api/v1/contacts/{contact_id}/notes": Case(
        "contact:write",
        lambda w, r: {"body": "Зателефонувати в понеділок"},
        CRM_TARGETS,
        "contact:read",
    ),
    "GET /api/v1/audit": Case("audit:read"),
    # --- корзина: удаление ищет живую запись по уровню чтения, восстановление и список —
    # удалённую по уровню *:delete.
    "DELETE /api/v1/companies/{company_id}": Case(
        "company:delete", None, CRM_TARGETS, "company:read"
    ),
    "POST /api/v1/companies/{company_id}/restore": Case(
        "company:delete", lambda w, r: {}, tuple(f"deleted_{t}" for t in CRM_TARGETS)
    ),
    "GET /api/v1/contacts/trash": Case("contact:delete", lists="deleted_contact"),
    "DELETE /api/v1/contacts/{contact_id}": Case(
        "contact:delete", None, CRM_TARGETS, "contact:read"
    ),
    "POST /api/v1/contacts/{contact_id}/restore": Case(
        "contact:delete", lambda w, r: {}, tuple(f"deleted_{t}" for t in CRM_TARGETS)
    ),
    "DELETE /api/v1/deals/{deal_id}": Case("deal:delete", None, CRM_TARGETS, "deal:read"),
    "POST /api/v1/deals/{deal_id}/restore": Case(
        "deal:delete", lambda w, r: {}, tuple(f"deleted_{t}" for t in CRM_TARGETS)
    ),
    "GET /api/v1/reports/pipeline": Case(
        "report:read",
        targets=ORG_TARGETS,
        query=lambda w, t: {"pipeline_id": w.ids["pipeline"][t]},
        org_object=True,
    ),
    "GET /api/v1/reports/managers": Case("report:read"),
}


@pytest.fixture(scope="module")
def route_actions(opa_url: str) -> dict[str, str]:
    # Синхронно: один запрос на весь модуль, event loop для этого не нужен.
    resp = httpx.post(f"{opa_url}/v1/data/{ROUTES_PATH}", json={})
    return dict(resp.json()["result"])


def all_cases() -> list[tuple[str, str, str | None]]:
    return [
        (role, route, target)
        for route, case in CASES.items()
        for role in ROLES
        for target in case.targets
    ]


async def test_every_route_has_a_case(route_actions: dict[str, str]) -> None:
    assert set(route_actions) == set(CASES), "опишите новый маршрут в CASES"
    changed = {r: a for r, a in route_actions.items() if CASES[r].action != a}
    assert changed == {}, "действие в routes.rego изменилось — проверьте, что так и задумано"


def in_scope(target: str | None, scope: Scope | None) -> bool:
    """Попадает ли объект цели в уровень. Вызывающие team_lead и sales_rep — из отдела 1, как
    владельцы целей own и same_team."""
    if target is None:
        return True
    target = target.removeprefix("deleted_")  # запись в корзине — тот же владелец
    if scope is None or target == "other_org":
        return False
    match scope:
        case "org":
            return True
        case "team":
            return target in ("own", "same_team")
        case "own":
            return target == "own"


def expected_status(role: str, case: Case, target: str | None, success: int) -> int:
    if role == "super_admin":
        return success if case.action.startswith("admin:") else 403
    levels = CATALOG.permissions(role)
    if case.action not in levels:
        return 403
    if case.org_object:
        return 404 if target == "other_org" else success
    if not in_scope(target, levels.get(case.read or case.action)):
        return 404  # вне уровня чтения — как несуществующий
    if not in_scope(target, levels[case.action]):
        return 403  # виден, но менять нельзя
    return success


async def build_world(
    make_org: MakeOrg,
    make_user: MakeUser,
    uow: UnitOfWork,
    make_company: MakeRecord,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
) -> World:
    mine, other = await make_org(), await make_org()
    users = {role: await member(make_user, mine, role) for role in ROLES[:4]}
    users["super_admin"] = await make_user(f"{unique('root')}@example.com", is_superadmin=True)
    empty: dict[str, uuid.UUID] = {}
    for key, org in (("same_org", mine), ("other_org", other)):
        team = Team(id=uuid.uuid4(), org_id=org.id, name=unique("Порожній"))
        uow.teams.add(team)
        empty[key] = team.id
    await uow.commit()
    targets = {
        "same_org": (await member(make_user, mine, "viewer")).id,
        "other_org": (await member(make_user, other, "viewer")).id,
    }
    world = World(mine, other, users, empty, targets)
    owners = {
        "own": users["sales_rep"],
        "same_team": await member(make_user, mine, "sales_rep", team=0),
        "same_org": await member(make_user, mine, "sales_rep", team=1),
        "other_org": await member(make_user, other, "sales_rep"),
    }
    world.ids["company"] = {t: (await make_company(o)).id for t, o in owners.items()}
    world.ids["contact"] = {t: (await make_contact(o)).id for t, o in owners.items()}
    pipelines = {
        "same_org": await make_pipeline(mine.id),
        "other_org": await make_pipeline(other.id),
    }
    world.ids["pipeline"] = {t: p.id for t, p in pipelines.items()}
    world.ids["stage"] = {t: p.stages[0].id for t, p in pipelines.items()}
    world.ids["deal"] = {
        t: (
            await make_deal(o, pipelines["other_org" if t == "other_org" else "same_org"].stages[0])
        ).id
        for t, o in owners.items()
    }
    world.ids["activity"] = {t: (await make_activity(o)).id for t, o in owners.items()}
    # Записи в корзине — для восстановления и списка корзины.
    gone = datetime.now(UTC)
    world.ids["company"].update(
        {f"deleted_{t}": (await make_company(o, deleted_at=gone)).id for t, o in owners.items()}
    )
    world.ids["contact"].update(
        {f"deleted_{t}": (await make_contact(o, deleted_at=gone)).id for t, o in owners.items()}
    )
    world.ids["deleted_contact"] = {t: world.ids["contact"][f"deleted_{t}"] for t in CRM_TARGETS}
    world.ids["deal"].update(
        {
            f"deleted_{t}": (
                await make_deal(
                    o,
                    pipelines["other_org" if t == "other_org" else "same_org"].stages[0],
                    deleted_at=gone,
                )
            ).id
            for t, o in owners.items()
        }
    )
    return world


def url_for(route: str, target: str | None, world: World) -> str:
    path = route.split(" ", 1)[1]
    if target is None:
        return path
    org = world.mine if target == "same_org" else world.other
    replacements = {
        "{team_id}": lambda: str(world.empty_team[target]),
        "{user_id}": lambda: str(world.any_user[target]),
        "{org_id}": lambda: str(org.id),
        "{company_id}": lambda: str(world.ids["company"][target]),
        "{contact_id}": lambda: str(world.ids["contact"][target]),
        "{deal_id}": lambda: str(world.ids["deal"][target]),
        "{activity_id}": lambda: str(world.ids["activity"][target]),
        "{pipeline_id}": lambda: str(world.ids["pipeline"][target]),
    }
    for placeholder, value in replacements.items():
        if placeholder in path:
            path = path.replace(placeholder, value())
    return path


SUCCESS = {"GET": 200, "PATCH": 200, "POST": 201, "DELETE": 204}
RESTORE = 200  # POST …/restore возвращает восстановленную запись, а не создаёт новую
ACCEPTED = {"POST /api/v1/invites", "POST /api/v1/admin/orgs/{org_id}/invites"}


@pytest.mark.parametrize(("role", "route", "target"), all_cases())
async def test_access(
    role: str,
    route: str,
    target: str | None,
    route_actions: dict[str, str],
    client: AsyncClient,
    make_org: MakeOrg,
    make_user: MakeUser,
    make_company: MakeRecord,
    make_contact: MakeRecord,
    make_pipeline: MakeRecord,
    make_deal: MakeRecord,
    make_activity: MakeRecord,
    uow: UnitOfWork,
    settings: Settings,
) -> None:
    world = await build_world(
        make_org,
        make_user,
        uow,
        make_company,
        make_contact,
        make_pipeline,
        make_deal,
        make_activity,
    )
    user = world.users[role]
    # Access-токен выпускаем напрямую: логин с argon2 на сотни кейсов был бы медленным.
    token = KeySet.from_settings(settings).issue_access_token(user.id, uuid.uuid4(), 0)
    method = route.split(" ", 1)[0]
    case = CASES[route]
    success = 202 if route in ACCEPTED else SUCCESS[method]
    if route.endswith("/restore"):
        success = RESTORE

    path = url_for(route, target, world) if case.query is None else route.split(" ", 1)[1]
    resp = await client.request(
        method,
        path,
        params=case.query(world, target) if case.query else None,
        json=case.body(world, role) if case.body else None,
        headers={"Authorization": f"Bearer {token}"},
    )

    expected = expected_status(role, case, target, success)
    assert resp.status_code == expected, resp.text
    if case.lists and resp.status_code == 200:
        # Список — 200 у всех, у кого есть действие, но без строк вне уровня (раздел 10).
        ids = world.ids[case.lists]
        listed = {item["id"] for item in resp.json()["items"]} & set(ids.values())
        scope = CATALOG.permissions(role)[case.action]
        assert listed == {ids[t] for t in CRM_TARGETS if in_scope(t, scope)}
