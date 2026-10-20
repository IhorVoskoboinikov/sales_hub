"""Настройки приложения: переменные окружения, а при локальном запуске ещё и файл .env."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # env_ignore_empty: пустая переменная (DOCS_ENABLED=) — то же, что не заданная.
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

    # Профиль: dev — локальная разработка, test — pytest, prod — кластер.
    app_env: Literal["dev", "test", "prod"] = "dev"

    database_url: str = "postgresql+asyncpg://saleshub:saleshub@localhost:5432/saleshub"
    redis_url: str = "redis://localhost:6379/0"

    # --- Вход по почте (раздел 4)
    # Путь относительно backend/ (в контейнере — /app). В dev файл создаёт make keys.
    jwt_private_key_file: Path = Path("secrets/jwt_private.pem")
    # Ротация: старые публичные ключи, по которым ещё проверяются выданные токены (JSON-список).
    jwt_previous_public_key_files: list[Path] = []
    jwt_issuer: str = "https://saleshub.local"
    jwt_audience: str = "saleshub-api"
    jwt_access_ttl: int = 3600  # секунды
    refresh_ttl_days: int = 30  # абсолютный предел сессии: от входа, ротация его не продлевает

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
