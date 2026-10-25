"""Unit of Work: одна транзакция на сценарий. Коммитит только сервис, репозитории — никогда."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Activity, Company, Contact, Deal
from app.repositories.activities import ActivityRepository, AuditQueries, NoteRepository
from app.repositories.audit import AuditRepository
from app.repositories.bulk import BulkChunkRepository, BulkJobRepository, BulkRepository
from app.repositories.contacts import CompanyRepository, ContactRepository
from app.repositories.crm import LiveRecords
from app.repositories.deals import DealRepository, PipelineRepository
from app.repositories.email_tokens import EmailTokenRepository
from app.repositories.failed_tasks import FailedTaskRepository
from app.repositories.fx import FxRateRepository
from app.repositories.organizations import OrganizationRepository, TeamRepository
from app.repositories.outbox import OutboxRepository
from app.repositories.refresh_tokens import RefreshTokenRepository
from app.repositories.reports import ReportRepository
from app.repositories.sent_notifications import SentNotificationRepository
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
        self.activities = ActivityRepository(session)
        self.notes = NoteRepository(session)
        self.audit_log = AuditQueries(session)
        self.reports = ReportRepository(session)
        self.live_records = LiveRecords(session, (Company, Contact, Deal, Activity))
        self.outbox = OutboxRepository(session)
        self.sent_notifications = SentNotificationRepository(session)
        self.bulk_jobs = BulkJobRepository(session)
        self.bulk_chunks = BulkChunkRepository(session)
        self.bulk = BulkRepository(session)
        self.failed_tasks = FailedTaskRepository(session)

    async def flush(self) -> None:
        """Отправить изменения в базу без коммита: проверить ограничения, получить значения
        по умолчанию из базы."""
        await self.session.flush()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
