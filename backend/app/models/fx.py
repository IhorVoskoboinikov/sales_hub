from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, Numeric, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class FxRate(Base):
    """Официальный курс НБУ: сколько гривен стоит 1 единица валюты на дату (раздел 3).

    Строка на каждую дату, на которую курс запрашивали, включая выходные: НБУ отдаёт и их
    (курс пятницы действует все выходные). Повторная загрузка — upsert по первичному ключу."""

    __tablename__ = "fx_rates"

    rate_date: Mapped[date] = mapped_column(Date, primary_key=True)
    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
    rate_uah: Mapped[Decimal] = mapped_column(Numeric(14, 6))
    source: Mapped[str] = mapped_column(String(16), server_default=text("'NBU'"))
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
