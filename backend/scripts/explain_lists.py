"""Проверка планов списков и отчётов на seed-данных (этап 4.9, «Готово, когда»).

Запуск: make explain (после make seed-big). Запросы строятся теми же методами *_query()
репозиториев, что выполняет API, — для пользователей каждой роли из самой крупной
seed-организации, включая менеджера с наибольшим и наименьшим числом клиентов. Каждый
запрос проверяется дважды:

- custom — план под конкретные значения (так asyncpg выполняет первые пять запусков);
- generic — общий план для любых значений (plan_cache_mode = force_generic_plan; так
  asyncpg может выполнять запрос дальше). В нём Postgres не видит значений параметров:
  частичный индекс с условием status = $1 он применить не может — поэтому такие условия
  в запросах записаны литералами.

С шага 5.7 — и запросы выгрузки (первая пачка серверного курсора и count с пределом) и
массовых операций: нарезка на чанки (plan) тегов сегмента и закрытия просроченных сделок и сам
чанк add-tags (UPDATE в CTE). Каждый EXPLAIN ANALYZE идёт в транзакции, которая откатывается:
у чанка он действительно выполняет UPDATE.

Ошибка — Seq Scan по большой таблице (companies, contacts, deals, activities и её партиции)
больше SEQ_SCAN_PAGES страниц: код выхода 1. Запрос дольше SLOW_MS — предупреждение: индекс
используется, но данных слишком много (например, частая подстрока в ?q= — см.
docs/experiments.md).
"""

import asyncio
import json
import re
import sys
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
from sqlalchemy import Select, literal
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import asyncpg as pg_asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # чтобы импортировать app

from app.authz.access import Access
from app.authz.roles import RoleCatalog
from app.core.config import get_settings
from app.models import Contact, Deal
from app.repositories.activities import ActivityRepository, AuditFilters, AuditQueries, TaskWindow
from app.repositories.bulk import BulkRepository, close_overdue_filter, tag_filter
from app.repositories.contacts import (
    EXPORT_BATCH,
    CompanyRepository,
    ContactFilters,
    ContactRepository,
)
from app.repositories.deals import DealFilters, DealRepository
from app.repositories.pagination import Page, encode_cursor
from app.repositories.reports import ReportRepository

BIG = re.compile(r"^(companies|contacts|deals|activities)(_p\d{4}_\d{2})?$")
SEQ_SCAN_PAGES = 1000  # маленькие партиции и пустые таблицы честно читаются целиком
SLOW_MS = 300.0
# Запросы воркера (plan и чанк массовых операций), а не API: они и должны прочитать весь
# сегмент, а на сегменте в 20 % организации Seq Scan бывает честным выбором планировщика —
# для них это предупреждение, а не ошибка. Чанк меняет 10 000 строк с GIN-индексом — его время
# секундами тоже ожидаемо.
BACKGROUND = frozenset(
    {
        "plan add-tags ?status=churned",
        "plan remove-tags vip",
        "plan close-overdue-deals",
        "чанк add-tags",
    }
)


@dataclass(frozen=True)
class Person:
    title: str  # кто: «sales_rep (больше всех клиентов)»
    role: str
    user_id: uuid.UUID
    org_id: uuid.UUID
    team_id: uuid.UUID | None


@dataclass
class Result:
    who: str
    query: str
    mode: str
    ms: float
    buffers: int
    scans: str
    problems: list[str]
    slow: bool
    warnings: list[str]  # Seq Scan в фоновом запросе (BACKGROUND)


def render(statement: Select[Any]) -> tuple[str, list[str]]:
    """SQL с $1, $2… (как его готовит asyncpg) и значения параметров литералами SQL."""
    compiled = statement.compile(
        dialect=pg_asyncpg.dialect(), compile_kwargs={"render_postcompile": True}
    )
    values = []
    for name in compiled.positiontup or []:
        bind = compiled.binds[name]
        value = compiled.params[name]
        values.append(
            str(
                literal(value, bind.type).compile(
                    dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
                )
            )
        )
    return str(compiled), values


def walk(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from walk(child)


async def explain(
    conn: asyncpg.Connection, statement: Select[Any], mode: str, pages: dict[str, int]
) -> tuple[float, int, str, list[str]]:
    sql, values = render(statement)
    await conn.execute(f"SET plan_cache_mode = force_{mode}_plan")
    await conn.execute(f"PREPARE q AS {sql}")  # PREPARE не транзакционен — DEALLOCATE в конце
    try:
        args = f"({', '.join(values)})" if values else ""
        # Откат после замера: у чанка add-tags EXPLAIN ANALYZE выполняет UPDATE по-настоящему.
        transaction = conn.transaction()
        await transaction.start()
        try:
            raw = await conn.fetchval(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE q{args}")
        finally:
            await transaction.rollback()
    finally:
        await conn.execute("DEALLOCATE q")
    plan = json.loads(raw)[0]
    nodes = list(walk(plan["Plan"]))
    problems, scans = [], []
    for node in nodes:
        relation = node.get("Relation Name", "")
        if node.get("Index Name"):
            scans.append(f"{node['Node Type']} {node['Index Name']}")
        elif relation:
            scans.append(f"{node['Node Type']} {relation}")
        big = BIG.match(relation) and pages.get(relation, 0) > SEQ_SCAN_PAGES
        if node["Node Type"] == "Seq Scan" and big:
            problems.append(f"Seq Scan {relation} ({pages[relation]} стр.)")
    ms = float(plan["Execution Time"])
    top = plan["Plan"]
    buffers = int(top.get("Shared Hit Blocks", 0)) + int(top.get("Shared Read Blocks", 0))
    unique_scans = list(dict.fromkeys(scans))
    summary = ", ".join(unique_scans[:4]) + (" …" if len(unique_scans) > 4 else "")
    return ms, buffers, summary, problems


async def people(conn: asyncpg.Connection) -> list[Person]:
    """Пользователи для замера — из самой крупной seed-организации."""
    org_id = await conn.fetchval(
        "SELECT o.id FROM organizations o JOIN contacts c ON c.org_id = o.id"
        " WHERE o.slug LIKE 'seed-%' GROUP BY o.id ORDER BY count(*) DESC LIMIT 1"
    )
    if org_id is None:
        sys.exit("нет seed-данных: сначала make seed-big")
    rows = await conn.fetch(
        "SELECT u.id, u.role, u.team_id, count(c.id) AS contacts FROM users u"
        " LEFT JOIN contacts c ON c.owner_id = u.id AND c.deleted_at IS NULL"
        " WHERE u.org_id = $1 GROUP BY u.id ORDER BY contacts DESC",
        org_id,
    )
    by_role = {r: [row for row in rows if row["role"] == r] for r in {row["role"] for row in rows}}
    reps = by_role["sales_rep"]
    picks = [
        ("org_admin", by_role["org_admin"][0]),
        ("viewer", by_role["viewer"][0]),
        ("team_lead (крупнейший отдел)", by_role["team_lead"][0]),
        ("sales_rep (больше всех клиентов)", reps[0]),
        ("sales_rep (меньше всех клиентов)", reps[-1]),
    ]
    return [Person(title, row["role"], row["id"], org_id, row["team_id"]) for title, row in picks]


def access(person: Person, action: str, roles: RoleCatalog) -> Access | None:
    scope = roles.scope_or_none(person.role, action)
    if scope is None:
        return None
    return Access(action, scope, person.user_id, person.org_id, person.team_id, person.role)


async def deep_cursor(conn: asyncpg.Connection, query: Select[Any]) -> str | None:
    """Курсор на 5000-ю строку списка: замер глубокой страницы keyset против OFFSET."""
    sql, values = render(query.limit(None).offset(5000).limit(1))
    await conn.execute(f"PREPARE c AS {sql}")
    try:
        args = f"({', '.join(values)})" if values else ""
        row = await conn.fetchrow(f"EXECUTE c{args}")
    finally:
        await conn.execute("DEALLOCATE c")
    return encode_cursor([row["created_at"], row["id"]]) if row else None


async def first_chunk(conn: asyncpg.Connection, bounds: Select[Any]) -> tuple[int, int] | None:
    """Границы первого чанка — тем же запросом, что строит plan (chunk_bounds_query)."""
    sql, values = render(bounds)
    await conn.execute(f"PREPARE b AS {sql}")
    try:
        args = f"({', '.join(values)})" if values else ""
        rows = await conn.fetch(f"EXECUTE b{args}")
    finally:
        await conn.execute("DEALLOCATE b")
    if not rows:
        return None
    hi = rows[1]["id"] - 1 if len(rows) > 1 else rows[0]["id"]
    return rows[0]["id"], hi


def add_tags_where(a: Access) -> Any:
    """Выборка add-tags «vip» сегменту ?status=churned — как у job (tag_filter)."""
    segment = ContactRepository.filter_conditions(ContactFilters(status="churned"))
    return tag_filter(a, "vip", True, segment)


async def main() -> int:
    settings = get_settings()
    roles = RoleCatalog.load(settings.policies_dir)
    conn = await asyncpg.connect(settings.database_url.replace("postgresql+asyncpg", "postgresql"))
    try:
        pages = {
            r["relname"]: r["relpages"]
            for r in await conn.fetch("SELECT relname, relpages FROM pg_class WHERE relkind = 'r'")
        }
        sample = await conn.fetchrow(
            "SELECT c.company_id, d.id AS deal_id, d.contact_id, d.pipeline_id"
            " FROM deals d JOIN contacts c ON c.id = d.contact_id"
            " WHERE c.company_id IS NOT NULL LIMIT 1"
        )
        contacts, companies = ContactRepository(None), CompanyRepository(None)  # type: ignore[arg-type]
        deals, activities = DealRepository(None), ActivityRepository(None)  # type: ignore[arg-type]
        audit, reports = AuditQueries(None), ReportRepository(None)  # type: ignore[arg-type]
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        page = Page(limit=50)
        size = settings.bulk_chunk_size
        no_filter = ContactRepository.filter_conditions(ContactFilters())

        Builder = Callable[[Access], Any]  # Select с разными колонками
        checks: list[tuple[str, str, Builder]] = [
            ("контакты", "contact:read", lambda a: contacts.list_query(a, ContactFilters(), page)),
            (
                "контакты ?q= частый",
                "contact:read",
                lambda a: contacts.list_query(a, ContactFilters(q="петренко"), page),
            ),
            (
                "контакты ?q= редкий",
                "contact:read",
                lambda a: contacts.list_query(a, ContactFilters(q="c4242@m"), page),
            ),
            (
                "контакты ?tag=vip",
                "contact:read",
                lambda a: contacts.list_query(a, ContactFilters(tag="vip"), page),
            ),
            (
                "контакты ?status=churned",
                "contact:read",
                lambda a: contacts.list_query(a, ContactFilters(status="churned"), page),
            ),
            (
                "контакты ?company_id=",
                "contact:read",
                lambda a: contacts.list_query(
                    a, ContactFilters(company_id=sample["company_id"]), page
                ),
            ),
            ("корзина контактов", "contact:delete", lambda a: contacts.trash_query(a, page)),
            ("компании", "company:read", lambda a: companies.list_query(a, None, page)),
            (
                "компании ?q=",
                "company:read",
                lambda a: companies.list_query(a, "компанія 12", page),
            ),
            ("сделки", "deal:read", lambda a: deals.list_query(a, DealFilters(), page)),
            (
                "сделки ?status=won",
                "deal:read",
                lambda a: deals.list_query(a, DealFilters(status="won"), page),
            ),
            (
                "сделки ?overdue=true",
                "deal:read",
                lambda a: deals.list_query(a, DealFilters(overdue_on=today.date()), page),
            ),
            (
                "сделки ?contact_id=",
                "deal:read",
                lambda a: deals.list_query(a, DealFilters(contact_id=sample["contact_id"]), page),
            ),
            (
                "канбан: итоги",
                "deal:read",
                lambda a: deals.kanban_totals_query(a, sample["pipeline_id"]),
            ),
            (
                "канбан: карточки",
                "deal:read",
                lambda a: deals.kanban_cards_query(a, sample["pipeline_id"], 20),
            ),
            ("задачи", "activity:read", lambda a: activities.tasks_query(a, TaskWindow(), page)),
            (
                "задачи ?due=today",
                "activity:read",
                lambda a: activities.tasks_query(
                    a, TaskWindow(today, today + timedelta(days=1)), page
                ),
            ),
            (
                "задачи ?due=overdue",
                "activity:read",
                lambda a: activities.tasks_query(a, TaskWindow(due_to=datetime.now(UTC)), page),
            ),
            (
                "история сделки",
                "activity:read",
                lambda a: activities.history_query(
                    deal_id=sample["deal_id"], contact_id=None, page=page
                ),
            ),
            (
                "история контакта",
                "activity:read",
                lambda a: activities.history_query(
                    deal_id=None, contact_id=sample["contact_id"], page=page
                ),
            ),
            ("журнал", "audit:read", lambda a: audit.list_query(a.org_id, AuditFilters(), page)),
            (
                "отчёт: воронка",
                "report:read",
                lambda a: reports.pipeline_query(a, sample["pipeline_id"]),
            ),
            (
                "отчёт: менеджеры",
                "report:read",
                lambda a: reports.managers_query(
                    a, today.date().replace(month=1, day=1), today.date()
                ),
            ),
            # --- шаг 5.7: выгрузка — первая пачка серверного курсора и count с пределом.
            (
                "выгрузка: первая пачка",
                "export:run",
                lambda a: contacts.export_query(a, ContactFilters()).limit(EXPORT_BATCH),
            ),
            (
                "выгрузка ?tag=vip: первая пачка",
                "export:run",
                lambda a: contacts.export_query(a, ContactFilters(tag="vip")).limit(EXPORT_BATCH),
            ),
            (
                "выгрузка: count",
                "export:run",
                lambda a: contacts.export_count_query(
                    a, ContactFilters(), settings.export_max_rows + 1
                ),
            ),
            # --- шаг 5.7: plan массовых операций (нарезка на чанки по строкам выборки).
            (
                "plan add-tags ?status=churned",
                "bulk:run",
                lambda a: BulkRepository.chunk_bounds_query(Contact, add_tags_where(a), size),
            ),
            (
                "plan remove-tags vip",
                "bulk:run",
                lambda a: BulkRepository.chunk_bounds_query(
                    Contact, tag_filter(a, "vip", False, no_filter), size
                ),
            ),
            (
                "plan close-overdue-deals",
                "bulk:run",
                lambda a: BulkRepository.chunk_bounds_query(
                    Deal, close_overdue_filter(a, today.date(), None), size
                ),
            ),
        ]

        results: list[Result] = []
        for person in await people(conn):
            for name, action, build in checks:
                acc = access(person, action, roles)
                if acc is None:
                    continue  # у роли нет действия — в API это 403 от OPA
                queries = [(name, build(acc))]
                if name == "контакты":
                    cursor = await deep_cursor(
                        conn, contacts.list_query(acc, ContactFilters(), Page(limit=1))
                    )
                    if cursor:
                        deep = contacts.list_query(
                            acc, ContactFilters(), Page(limit=50, cursor=cursor)
                        )
                        queries.append(("контакты, 101-я страница (keyset)", deep))
                if name == "plan add-tags ?status=churned":
                    # Сам чанк: UPDATE первого диапазона plan в CTE (в транзакции с откатом).
                    bounds = await first_chunk(conn, build(acc))
                    if bounds:
                        chunk = BulkRepository.add_tag_query(*bounds, add_tags_where(acc), "vip")
                        queries.append(("чанк add-tags", chunk))
                for query_name, query in queries:
                    for mode in ("custom", "generic"):
                        ms, buffers, scans, problems = await explain(conn, query, mode, pages)
                        warnings: list[str] = []
                        if query_name in BACKGROUND:
                            problems, warnings = [], problems
                        results.append(
                            Result(
                                person.title,
                                query_name,
                                mode,
                                ms,
                                buffers,
                                scans,
                                problems,
                                ms > SLOW_MS,
                                warnings,
                            )
                        )
    finally:
        await conn.close()

    print("| Кто | Запрос | План | мс | Буферы | Как читает |")
    print("| --- | --- | --- | ---: | ---: | --- |")
    for r in results:
        flag = " ❌" if r.problems else " ⚠" if r.slow or r.warnings else ""
        print(f"| {r.who} | {r.query} | {r.mode}{flag} | {r.ms:.1f} | {r.buffers} | {r.scans} |")
    bad = [r for r in results if r.problems]
    slow = [r for r in results if r.slow]
    print(f"\nПроверено планов: {len(results)}, Seq Scan по большим таблицам: {len(bad)}")
    for r in bad:
        print(f"- ❌ {r.who} / {r.query} / {r.mode}: {'; '.join(r.problems)}")
    for r in slow:
        print(f"- ⚠ дольше {SLOW_MS:.0f} мс: {r.who} / {r.query} / {r.mode}: {r.ms:.0f} мс")
    for r in results:
        if r.warnings:
            print(f"- ⚠ фоновый запрос: {r.who} / {r.query} / {r.mode}: {'; '.join(r.warnings)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
