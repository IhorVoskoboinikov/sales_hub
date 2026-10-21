"""Компании и контакты: списки с фильтрами и keyset-пагинацией, поиск дубля email."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import (
    BindParameter,
    ColumnElement,
    Select,
    String,
    bindparam,
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

    def list_query(self, access: Access, filters: ContactFilters, page: Page) -> Select[Any]:
        query = self.visible_query(access)
        if filters.q:
            query = query.where(contact_search_text().like(like_pattern(filters.q)))
        if filters.status:
            query = query.where(Contact.status == filters.status)
        if filters.tag:
            query = query.where(Contact.tags.contains([filters.tag]))
        if filters.owner_id:
            query = query.where(Contact.owner_id == filters.owner_id)
        if filters.company_id:
            query = query.where(Contact.company_id == filters.company_id)
        return paginate(query, CONTACT_ORDER, page)

    async def list(
        self, access: Access, filters: ContactFilters, page: Page
    ) -> PageResult[Contact]:
        query = self.list_query(access, filters, page)
        return await fetch_page(self.session, query, CONTACT_ORDER, page)

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
