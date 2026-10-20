import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Organization(Base):
    """Тенант: компания, которая пользуется CRM. Создаёт только супер-админ."""

    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200))
    slug: Mapped[str] = mapped_column(String(64), unique=True)  # короткое имя для URL и логов
    plan: Mapped[str] = mapped_column(String(32), default="free", server_default=text("'free'"))
    # Валюта, в которой показываются суммы, если клиент не передал ?currency= (раздел 3).
    display_currency: Mapped[str] = mapped_column(
        String(3), default="UAH", server_default=text("'UAH'")
    )
    timezone: Mapped[str] = mapped_column(
        String(64), default="Europe/Kyiv", server_default=text("'Europe/Kyiv'")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Team(Base):
    """Отдел продаж внутри организации."""

    __tablename__ = "teams"
    __table_args__ = (
        # Для составного FK users (team_id, org_id) → teams (id, org_id):
        # так база сама не даст записать человека в отдел чужой организации.
        UniqueConstraint("id", "org_id"),
        UniqueConstraint("org_id", "name"),  # два «Отдела продаж» в одной организации путают
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # Отдельный индекс не нужен: org_id — первая колонка уникального (org_id, name).
    org_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("organizations.id"))
    name: Mapped[str] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
