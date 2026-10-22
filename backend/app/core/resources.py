"""Ресурсы процесса: пул Postgres, Redis, HTTP-клиент, файл ролей, отправка писем.

Собирает их одна функция — для API (lifespan в main.py) и для воркеров (workers/runtime.py),
поэтому таймауты у всех одинаковые, а отличаются размер пула и то, что SMTP-отправитель есть
только у воркеров (for_worker=True): с шага 5.3 API почту не отправляет (раздел 15).
Соединения открываются лениво — при первом запросе, а не при создании.
"""

from dataclasses import dataclass

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.authz.opa import OpaClient
from app.authz.roles import RoleCatalog
from app.core.config import Settings
from app.db.session import create_engine, create_session_factory
from app.emails.sender import EmailSender, SmtpEmailSender
from app.services.fx import NbuClient

REDIS_TIMEOUT_SECONDS = 2


@dataclass(frozen=True)
class Resources:
    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis
    http: httpx.AsyncClient
    opa: OpaClient
    nbu: NbuClient
    roles: RoleCatalog
    # Только у воркеров: API пишет команду email.send в outbox, письмо отправляет воркер.
    email_sender: EmailSender | None

    @classmethod
    def create(
        cls,
        settings: Settings,
        *,
        roles: RoleCatalog | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        for_worker: bool = False,
    ) -> "Resources":
        """roles — уже прочитанный файл ролей (API читает его при сборке приложения),
        иначе он читается здесь. Нет файла — RuntimeError: процесс не стартует.
        for_worker — собрать и отправителя писем (у API его нет)."""
        # Файл ролей — первым: при ошибке пул и HTTP-клиент ещё не созданы, закрывать нечего.
        if roles is None:
            roles = RoleCatalog.load(settings.policies_dir)
        engine = create_engine(
            settings.database_url, pool_size=pool_size, max_overflow=max_overflow
        )
        http = httpx.AsyncClient()  # один пул соединений на процесс
        return cls(
            settings=settings,
            engine=engine,
            session_factory=create_session_factory(engine),
            redis=Redis.from_url(
                settings.redis_url,
                socket_connect_timeout=REDIS_TIMEOUT_SECONDS,
                socket_timeout=REDIS_TIMEOUT_SECONDS,
            ),
            http=http,
            opa=OpaClient(http, settings.opa_url),
            nbu=NbuClient(http, settings.nbu_url),  # тот же пул: OPA и НБУ — разные хосты
            roles=roles,
            email_sender=smtp_sender(settings) if for_worker else None,
        )

    async def aclose(self) -> None:
        await self.http.aclose()
        await self.redis.aclose()
        await self.engine.dispose()


def smtp_sender(settings: Settings) -> SmtpEmailSender:
    """Отправитель по SMTP из настроек. Соединение открывается на каждое письмо, не здесь."""
    return SmtpEmailSender(
        host=settings.smtp_host,
        port=settings.smtp_port,
        sender=settings.email_from,
        username=settings.smtp_username,
        password=settings.smtp_password.get_secret_value() if settings.smtp_password else None,
        start_tls=settings.smtp_start_tls,
    )
