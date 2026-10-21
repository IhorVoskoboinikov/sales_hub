"""Настоящие Postgres и Redis в Docker (testcontainers): одни контейнеры на весь запуск.

Изоляция (раздел 10): каждый тест работает во внешней транзакции, которая откатывается после
него. Коммиты сервисов внутри теста становятся SAVEPOINT-ами — таблицы чистить не нужно.
"""

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import respx
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

from app.api.deps import get_session
from app.core.config import Settings
from app.core.security import hash_password
from app.models import (
    Company,
    Contact,
    Deal,
    Organization,
    Pipeline,
    PipelineStage,
    Team,
    User,
)
from app.uow import UnitOfWork
from tests.conftest import POLICIES_DIR

BACKEND_DIR = Path(__file__).resolve().parents[2]

# Те же версии, что в deploy/compose/docker-compose.yml.
POSTGRES_IMAGE = "postgres:18.6"
REDIS_IMAGE = "redis:8.10"
MAILPIT_IMAGE = "axllent/mailpit:v1.31.6"
OPA_IMAGE = "openpolicyagent/opa:1.21.3"


def alembic_config(database_url: str) -> Config:
    config = Config(str(BACKEND_DIR / "alembic.ini"))
    config.attributes["database_url"] = database_url
    config.attributes["configure_logger"] = False  # не сбрасывать настройки логов pytest
    return config


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as postgres:
        url = postgres.get_connection_url()
        command.upgrade(alembic_config(url), "head")
        yield url


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer(REDIS_IMAGE) as redis:
        yield f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"


@dataclass(frozen=True)
class Mailpit:
    smtp_host: str
    smtp_port: int
    api_url: str  # REST API Mailpit: тесты читают через него отправленные письма


@pytest.fixture(scope="session")
def mailpit() -> Iterator[Mailpit]:
    container = (
        DockerContainer(MAILPIT_IMAGE)
        .with_exposed_ports(1025, 8025)
        .waiting_for(HttpWaitStrategy(8025, "/livez"))
    )
    with container:
        host = container.get_container_host_ip()
        yield Mailpit(
            smtp_host=host,
            smtp_port=int(container.get_exposed_port(1025)),
            api_url=f"http://{host}:{container.get_exposed_port(8025)}/api/v1",
        )


@pytest.fixture(scope="session")
def opa_url() -> Iterator[str]:
    """Настоящий OPA с политиками из policies/ — как сервис opa в compose."""
    container = (
        DockerContainer(OPA_IMAGE)
        .with_volume_mapping(str(POLICIES_DIR), "/policies", "ro")
        .with_command("run --server --addr=0.0.0.0:8181 --ignore=*_test.rego /policies")
        .with_exposed_ports(8181)
        .waiting_for(HttpWaitStrategy(8181, "/health"))
    )
    with container:
        yield f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8181)}"


@pytest.fixture
def settings(
    settings: Settings, postgres_url: str, redis_url: str, mailpit: Mailpit, opa_url: str
) -> Settings:
    # Те же настройки, что у unit-тестов, но с адресами настоящих контейнеров.
    return settings.model_copy(
        update={
            "database_url": postgres_url,
            "redis_url": redis_url,
            "smtp_host": mailpit.smtp_host,
            "smtp_port": mailpit.smtp_port,
            "opa_url": opa_url,
            "nbu_url": NBU_URL,
        }
    )


@pytest.fixture
async def db_session(postgres_url: str) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    async with engine.connect() as connection:
        transaction = await connection.begin()
        session = AsyncSession(
            bind=connection,
            join_transaction_mode="create_savepoint",  # commit() сервиса → RELEASE SAVEPOINT
            expire_on_commit=False,
        )
        try:
            yield session
        finally:
            await session.close()
            await transaction.rollback()
    await engine.dispose()


@pytest.fixture
def uow(db_session: AsyncSession) -> UnitOfWork:
    return UnitOfWork(db_session)


@pytest.fixture
def app(app: FastAPI, db_session: AsyncSession) -> FastAPI:
    # Запросы API идут в ту же транзакцию теста: после теста всё откатится.
    async def test_session() -> AsyncIterator[AsyncSession]:
        try:
            yield db_session
        except Exception:
            # Как в настоящем запросе: ошибка — изменения запроса не сохраняются.
            # Откатывается только SAVEPOINT запроса, данные теста остаются.
            await db_session.rollback()
            raise

    app.dependency_overrides[get_session] = test_session
    return app


PASSWORD = "correct horse battery"  # пароль всех пользователей из make_user


@dataclass(frozen=True)
class OrgFixture:
    """Простые значения, а не ORM-объекты: ответ 4xx откатывает сессию теста, и объекты
    «протухают» — обращение к ним после этого упало бы."""

    id: uuid.UUID
    name: str
    team_ids: list[uuid.UUID]  # два отдела: «Продажі 1» и «Продажі 2»


MakeOrg = Callable[..., Awaitable[OrgFixture]]


@pytest.fixture
def make_org(uow: UnitOfWork) -> MakeOrg:
    """Организация с двумя отделами."""

    async def make(slug: str | None = None) -> OrgFixture:
        slug = slug or f"org-{uuid.uuid4().hex[:8]}"
        org = Organization(id=uuid.uuid4(), name=slug.title(), slug=slug)
        uow.organizations.add(org)
        await uow.flush()
        teams = [Team(id=uuid.uuid4(), org_id=org.id, name=f"Продажі {i}") for i in (1, 2)]
        for team in teams:
            uow.teams.add(team)
        await uow.commit()
        return OrgFixture(org.id, org.name, [team.id for team in teams])

    return make


MakeUser = Callable[..., Awaitable[User]]

_password_hash: list[str] = []


async def password_hash() -> str:
    """Хэш PASSWORD один на весь запуск: argon2 медленный специально, а пользователей в тестах
    создаются сотни."""
    if not _password_hash:
        _password_hash.append(await hash_password(PASSWORD))
    return _password_hash[0]


@pytest.fixture
def make_user(uow: UnitOfWork) -> MakeUser:
    """Создать пользователя с подтверждённым email и паролем PASSWORD."""

    async def make(email: str = "ivan@example.com", **fields: object) -> User:
        fields.setdefault("email_verified_at", datetime.now(UTC))
        user = User(email=email, password_hash=await password_hash(), **fields)
        uow.users.add(user)
        await uow.commit()
        return user

    return make


async def login(client: AsyncClient, email: str, password: str = PASSWORD) -> dict[str, Any]:
    resp = await client.post("/api/v1/auth/login", data={"username": email, "password": password})
    assert resp.status_code == 200, resp.text
    return cast(dict[str, Any], resp.json())


def bearer(tokens: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def read_emails(mailpit: Mailpit, to: str) -> list[dict[str, Any]]:
    """Письма, которые Mailpit получил для адреса to, от новых к старым — с текстом."""
    async with AsyncClient(base_url=mailpit.api_url) as api:
        found = (await api.get("/search", params={"query": f"to:{to}"})).json()["messages"]
        return [(await api.get(f"/message/{m['ID']}")).json() for m in found]


def token_from_email(email: dict[str, Any]) -> str:
    """Токен в письме стоит отдельной строкой после строки «Токен…:»."""
    lines: list[str] = [line.strip() for line in str(email["Text"]).splitlines()]
    return next(lines[i + 1] for i, line in enumerate(lines) if line.startswith("Токен"))


async def member(
    make_user: MakeUser, org: OrgFixture, role: str, team: int | None = 0, **fields: object
) -> User:
    """Пользователь организации с ролью. team — номер отдела (только для team_lead и sales_rep)."""
    needs_team = team is not None and role in ("team_lead", "sales_rep")
    team_id = org.team_ids[team] if team is not None and needs_team else None
    return await make_user(
        f"{role}-{uuid.uuid4().hex[:8]}@example.com",
        org_id=org.id,
        role=role,
        team_id=team_id,
        **fields,
    )


async def tokens_for(client: AsyncClient, user: User) -> dict[str, str]:
    return bearer(await login(client, user.email))


MakeRecord = Callable[..., Awaitable[Any]]


@pytest.fixture
def make_contact(uow: UnitOfWork) -> MakeRecord:
    """Контакт, который ведёт owner (team_lead или sales_rep): org_id и team_id — его."""

    async def make(owner: User, **fields: Any) -> Contact:
        fields.setdefault("first_name", "Іван")
        fields.setdefault("email", f"client-{uuid.uuid4().hex[:8]}@example.com")
        contact = Contact(org_id=owner.org_id, team_id=owner.team_id, owner_id=owner.id, **fields)
        uow.contacts.add(contact)
        await uow.commit()
        return contact

    return make


@pytest.fixture
def make_company(uow: UnitOfWork) -> MakeRecord:
    async def make(owner: User, **fields: Any) -> Company:
        fields.setdefault("name", f"Acme {uuid.uuid4().hex[:6]}")
        company = Company(org_id=owner.org_id, team_id=owner.team_id, owner_id=owner.id, **fields)
        uow.companies.add(company)
        await uow.commit()
        return company

    return make


@pytest.fixture
def make_pipeline(uow: UnitOfWork) -> Callable[..., Awaitable[Pipeline]]:
    """Воронка организации с этапами «Новий» и «Пропозиція»."""

    async def make(org_id: uuid.UUID, name: str | None = None) -> Pipeline:
        pipeline = Pipeline(
            org_id=org_id,
            name=name or f"Продажі {uuid.uuid4().hex[:6]}",
            stages=[
                PipelineStage(name="Новий", position=0, probability=10),
                PipelineStage(name="Пропозиція", position=1, probability=50),
            ],
        )
        uow.pipelines.add(pipeline)
        await uow.commit()
        return pipeline

    return make


@pytest.fixture
def make_deal(uow: UnitOfWork) -> MakeRecord:
    """Открытая сделка на 1000 USD на первом этапе воронки (stage)."""

    async def make(owner: User, stage: PipelineStage, **fields: Any) -> Deal:
        fields.setdefault("title", f"Угода {uuid.uuid4().hex[:6]}")
        fields.setdefault("amount_usd", Decimal("1000.00"))
        deal = Deal(
            org_id=owner.org_id,
            team_id=owner.team_id,
            owner_id=owner.id,
            pipeline_id=stage.pipeline_id,
            stage_id=stage.id,
            amount_original=fields["amount_usd"],
            currency_original="USD",
            fx_rate=Decimal(1),
            fx_date=date.today(),
            **fields,
        )
        uow.deals.add(deal)
        await uow.commit()
        return deal

    return make


NBU_URL = "http://nbu.test/exchange"
# Курсы НБУ в тестах: гривен за 1 единицу валюты.
NBU_RATES = {"USD": Decimal("41.488"), "EUR": Decimal("46.5"), "PLN": Decimal("10.25")}


@dataclass
class FakeNbu:
    """Подмена API НБУ (respx): тесты не ходят в интернет. rates — курсы на любую дату;
    down — НБУ не отвечает; dates — за какие даты спрашивали."""

    rates: dict[str, Decimal]
    down: bool = False
    dates: list[str] = field(default_factory=list)

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.dates.append(request.url.params["date"])
        if self.down:
            return httpx.Response(503)
        return httpx.Response(
            200, json=[{"cc": cc, "rate": float(rate)} for cc, rate in self.rates.items()]
        )


@pytest.fixture(autouse=True)
async def nbu(redis_url: str) -> AsyncIterator[FakeNbu]:
    fake = FakeNbu(dict(NBU_RATES))
    # Кэш курсов в Redis общий на весь запуск — каждый тест начинает с пустого.
    redis = Redis.from_url(redis_url)
    async for key in redis.scan_iter("fx:*"):
        await redis.delete(key)
    await redis.aclose()
    with respx.mock(assert_all_called=False) as router:
        router.get(NBU_URL).mock(side_effect=fake.respond)
        router.route().pass_through()  # OPA, Mailpit — настоящие
        yield fake
