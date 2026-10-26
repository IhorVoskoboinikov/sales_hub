"""Компании и контакты: списки с фильтрами и keyset-пагинацией, поиск дубля email, выгрузка."""

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import (
    BindParameter,
    ColumnElement,
    Row,
    Select,
    String,
    bindparam,
    func,
    literal_column,
    select,
)
from sqlalchemy.orm import aliased

from app.authz.access import Access
from app.models import Company, Contact, User
from app.models.crm import CONTACT_SEARCH
from app.repositories.crm import SoftDeleteRepository
from app.repositories.pagination import Key, Page, PageResult, fetch_page, paginate

# Сортировка списков: новые сначала. Тот же порядок колонок — в индексах ix_contacts_*_created_at.
CONTACT_ORDER = (Key(Contact.created_at, desc=True), Key(Contact.id, desc=True))
COMPANY_ORDER = (Key(Company.name), Key(Company.id))
# Корзина: удалённые недавно — сначала. Тот же порядок — в индексе ix_contacts_trash.
TRASH_ORDER = (Key(Contact.deleted_at, desc=True), Key(Contact.id, desc=True))
# Выгрузка (раздел 6): строк за одно чтение серверного курсора — память API не зависит от
# размера файла.
EXPORT_BATCH = 2000


# Поиск по подстроке идёт через триграммы pg_trgm: из запроса без трёх букв или цифр подряд
# («---», «a b») триграмм не получится, и Postgres прочитал бы все строки организации.
# Управляющие символы (NUL) запрещены: шаблон подставляется в текст запроса.
SEARCH_PATTERN = r"^[^\x00-\x1f]*\w{3}[^\x00-\x1f]*$"


def like_pattern(q: str) -> BindParameter[str]:
    """LIKE '%q%' — символы % и _ из запроса ищутся буквально.

    literal_execute: шаблон подставляется в текст запроса, а не параметром $1. С параметром
    asyncpg после пяти запусков переходит на общий (generic) план, а в нём планировщик не видит
    шаблона, не может оценить триграммы и выбирает Seq Scan по всей таблице."""
    escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return bindparam("q", f"%{escaped.lower()}%", literal_execute=True)


def contact_search_text() -> ColumnElement[str]:
    """То же выражение, что в индексе ix_contacts_search_trgm. Текстом, а не через func:
    SQLAlchemy передал бы пробелы параметрами ($1), а планировщик сопоставляет с индексом
    только выражение с теми же константами."""
    return literal_column(CONTACT_SEARCH, String)


@dataclass(frozen=True)
class ContactFilters:
    q: str | None = None
    status: str | None = None
    tag: str | None = None
    owner_id: Any = None
    company_id: int | None = None


class ContactRepository(SoftDeleteRepository[Contact]):
    model = Contact
    entity = "contact"

    @staticmethod
    def filter_conditions(filters: ContactFilters) -> list[ColumnElement[bool]]:
        """Условия фильтров GET /contacts — одни для списка, выгрузки и тегов сегмента (раздел 6):
        выгрузка и bulk не шире и не уже того, что человек видит в списке. Тег — типизированным
        массивом (tags @> $1::VARCHAR(64)[]): с text[] Postgres не применил бы GIN-индекс."""
        conditions: list[ColumnElement[bool]] = []
        if filters.q:
            conditions.append(contact_search_text().like(like_pattern(filters.q)))
        if filters.status:
            conditions.append(Contact.status == filters.status)
        if filters.tag:
            conditions.append(Contact.tags.contains([filters.tag]))
        if filters.owner_id:
            conditions.append(Contact.owner_id == filters.owner_id)
        if filters.company_id:
            conditions.append(Contact.company_id == filters.company_id)
        return conditions

    def list_query(self, access: Access, filters: ContactFilters, page: Page) -> Select[Any]:
        query = self.visible_query(access).where(*self.filter_conditions(filters))
        return paginate(query, CONTACT_ORDER, page)

    def export_query(self, access: Access, filters: ContactFilters) -> Select[Any]:
        """Выгрузка (раздел 6): те же строки и тот же порядок, что у списка, но без страниц.
        Порядок по индексу уровня (…, created_at, id) — первые строки уходят клиенту сразу, без
        сортировки миллиона. email ответственного — подзапросом по первичному ключу users: JOIN
        мог бы стать hash join, и тогда планировщику пришлось бы сортировать всё заново."""
        owner_email = select(User.email).where(User.id == Contact.owner_id).scalar_subquery()
        order = [k.column.desc() if k.desc else k.column.asc() for k in CONTACT_ORDER]
        return (
            select(
                Contact.id,
                Contact.email,
                Contact.first_name,
                Contact.last_name,
                Contact.phone,
                Contact.status,
                Contact.source,
                Contact.tags,
                owner_email.label("owner_email"),
                Contact.company_id,
                Contact.created_at,
                Contact.updated_at,
            )
            .where(
                Contact.deleted_at.is_(None),
                access.sql_filter(Contact),
                *self.filter_conditions(filters),
            )
            .order_by(*order)
        )

    def export_count_query(
        self, access: Access, filters: ContactFilters, limit: int
    ) -> Select[Any]:
        """Сколько строк выгрузит export_query, но не больше limit: count по сегменту дальше
        предела не идёт — EXPORT_MAX_ROWS + 1 строк уже значит «слишком много»."""
        rows = (
            select(Contact.id)
            .where(
                Contact.deleted_at.is_(None),
                access.sql_filter(Contact),
                *self.filter_conditions(filters),
            )
            .limit(limit)
            .subquery()
        )
        return select(func.count()).select_from(rows)

    async def export_count(self, access: Access, filters: ContactFilters, limit: int) -> int:
        return int(await self.session.scalar(self.export_count_query(access, filters, limit)) or 0)

    async def export_rows(
        self, access: Access, filters: ContactFilters
    ) -> AsyncIterator[Sequence[Row[Any]]]:
        """Строки выгрузки пачками по EXPORT_BATCH — серверный курсор (session.stream с
        yield_per): курсор живёт в транзакции сессии запроса, пока ответ не отдан целиком."""
        result = await self.session.stream(
            self.export_query(access, filters), execution_options={"yield_per": EXPORT_BATCH}
        )
        async for batch in result.partitions():
            yield batch

    async def list(
        self, access: Access, filters: ContactFilters, page: Page
    ) -> PageResult[Contact]:
        query = self.list_query(access, filters, page)
        return await fetch_page(self.session, query, CONTACT_ORDER, page)

    def trash_query(self, access: Access, page: Page) -> Select[Any]:
        query = select(Contact).where(Contact.deleted_at.is_not(None), access.sql_filter(Contact))
        return paginate(query, TRASH_ORDER, page)

    async def trash(self, access: Access, page: Page) -> PageResult[Contact]:
        return await fetch_page(self.session, self.trash_query(access, page), TRASH_ORDER, page)

    async def find_by_email(self, org_id: Any, email: str) -> tuple[Contact, str] | None:
        """Живой контакт организации с таким email и email его ответственного — без Access:
        дубль ищется по всей организации, а наружу сервис отдаёт только безопасные поля."""
        owner = aliased(User)
        row = (
            await self.session.execute(
                select(Contact, owner.email)
                .join(owner, owner.id == Contact.owner_id)
                .where(
                    Contact.org_id == org_id, Contact.email == email, Contact.deleted_at.is_(None)
                )
            )
        ).first()
        return (row[0], row[1]) if row else None


class CompanyRepository(SoftDeleteRepository[Company]):
    model = Company
    entity = "company"

    def list_query(self, access: Access, q: str | None, page: Page) -> Select[Any]:
        query = self.visible_query(access)
        if q:
            query = query.where(Company.name.ilike(like_pattern(q)))
        return paginate(query, COMPANY_ORDER, page)

    async def list(self, access: Access, q: str | None, page: Page) -> PageResult[Company]:
        query = self.list_query(access, q, page)
        return await fetch_page(self.session, query, COMPANY_ORDER, page)
