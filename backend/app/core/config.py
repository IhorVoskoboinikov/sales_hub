"""Настройки приложения: переменные окружения, а при локальном запуске ещё и файл .env."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # env_ignore_empty: пустая переменная (DOCS_ENABLED=) — то же, что не заданная.
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

    # Профиль: dev — локальная разработка, test — pytest, prod — кластер.
    app_env: Literal["dev", "test", "prod"] = "dev"

    database_url: str = "postgresql+asyncpg://saleshub:saleshub@localhost:5432/saleshub"
    redis_url: str = "redis://localhost:6379/0"

    # --- Воркеры (раздел 15): брокер — db 1 того же Redis, чтобы FLUSHDB кэша не стёр задачи.
    celery_broker_url: str = "redis://localhost:6379/1"
    # Секунды. Больше самой долгой задачи (запуск импорта — 12 минут), иначе Redis выдаст
    # её повторно, пока она ещё выполняется.
    celery_visibility_timeout: int = 1200
    # Outbox relay (раздел 15): строк за одну транзакцию и сколько ждать NOTIFY до опроса.
    outbox_batch_size: int = 100
    outbox_poll_seconds: float = 2.0
    # Массовые операции (раздел 6): строк в чанке. Границы чанков считаются по реальным
    # строкам выборки, а не равными диапазонами id. 0 — ошибка конфигурации уже при старте.
    bulk_chunk_size: int = Field(10_000, ge=1)

    # --- Вход по почте (раздел 4)
    # Путь относительно backend/ (в контейнере — /app). В dev файл создаёт make keys.
    jwt_private_key_file: Path = Path("secrets/jwt_private.pem")
    # Ротация: старые публичные ключи, по которым ещё проверяются выданные токены (JSON-список).
    jwt_previous_public_key_files: list[Path] = []
    jwt_issuer: str = "https://saleshub.local"
    jwt_audience: str = "saleshub-api"
    jwt_access_ttl: int = 3600  # секунды
    refresh_ttl_days: int = 30  # абсолютный предел сессии: от входа, ротация его не продлевает

    # Публичный адрес API — для ссылок в письмах.
    public_url: str = "http://localhost:8000"
    invite_ttl_days: int = 7
    reset_password_ttl_minutes: int = 60

    # --- Письма. В dev — Mailpit: SMTP на 1025, веб-интерфейс на localhost:8025.
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_username: str | None = None
    smtp_password: SecretStr | None = None
    smtp_start_tls: bool = False
    email_from: str = "SalesHub <noreply@saleshub.local>"

    # --- Права (раздел 5): OPA и файл ролей policies/saleshub/data.yaml, который читают оба.
    opa_url: str = "http://localhost:8181"
    policies_dir: Path = Path("../policies")  # относительно backend/; в контейнере — /policies
    # Сколько раз при старте спросить у OPA маршруты для Swagger (раз в секунду).
    opa_startup_attempts: int = 5

    # --- Курсы валют (раздел 3): открытый API НБУ.
    nbu_url: str = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"

    # Первый пользователь — для команды create-superadmin (в Kubernetes — из Secret).
    superadmin_email: str | None = None
    superadmin_password: SecretStr | None = None

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    # None — решает профиль: Swagger есть везде, кроме prod.
    docs_enabled: bool | None = None

    @property
    def show_docs(self) -> bool:
        if self.docs_enabled is not None:
            return self.docs_enabled
        return self.app_env != "prod"

    @property
    def log_json(self) -> bool:
        # В dev — цветной вывод для человека, в остальных профилях — JSON для Loki.
        return self.app_env != "dev"


@lru_cache
def get_settings() -> Settings:
    return Settings()
