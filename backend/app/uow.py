"""Unit of Work: одна транзакция на сценарий. Коммитит только сервис, репозитории — никогда."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Company, Contact, Deal
from app.repositories.audit import AuditRepository
from app.repositories.contacts import CompanyRepository, ContactRepository
from app.repositories.crm import LiveRecords
from app.repositories.deals import DealRepository, PipelineRepository
from app.repositories.email_tokens import EmailTokenRepository
from app.repositories.fx import FxRateRepository
from app.repositories.organizations import OrganizationRepository, TeamRepository
from app.repositories.refresh_tokens import RefreshTokenRepository
from app.repositories.users import UserRepository


class UnitOfWork:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.users = UserRepository(session)
        self.refresh_tokens = RefreshTokenRepository(session)
        self.email_tokens = EmailTokenRepository(session)
        self.organizations = OrganizationRepository(session)
        self.teams = TeamRepository(session)
        self.audit = AuditRepository(session)
        self.companies = CompanyRepository(session)
        self.contacts = ContactRepository(session)
        self.pipelines = PipelineRepository(session)
        self.deals = DealRepository(session)
        self.fx_rates = FxRateRepository(session)
        self.live_records = LiveRecords(session, (Company, Contact, Deal))

    async def flush(self) -> None:
        """Отправить изменения в базу без коммита: проверить ограничения, получить значения
        по умолчанию из базы."""
        await self.session.flush()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
