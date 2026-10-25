"""Задачи Celery, упавшие после всех ретраев (раздел 15, «Задачи, упавшие после всех ретраев»).

У Celery с Redis нет dead-letter очереди — это она: сигнал task_failure пишет строку своей
сессией, супер-админ смотрит список и перезапускает задачу (replay). task_id уникален:
повторно доставленная и снова упавшая задача дубля не даёт (ON CONFLICT DO NOTHING).
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Identity, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

EXC_MESSAGE_MAX = 2_000  # символов текста ошибки
TRACEBACK_MAX = 20_000  # символов traceback


class FailedTask(Base):
    __tablename__ = "failed_tasks"
    __table_args__ = (
        # Список супер-админа: новые сначала (keyset по failed_at, id).
        Index("ix_failed_tasks_failed_at_id", "failed_at", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(255), unique=True)  # id задачи Celery
    task_name: Mapped[str] = mapped_column(String(255))  # bulk.chunk, notifications.send_email
    queue: Mapped[str | None] = mapped_column(String(64))  # из какой очереди взята
    # Аргументы — как их сериализовал Celery (json): по ним replay ставит задачу снова.
    args: Mapped[list[Any]] = mapped_column(JSONB)
    kwargs: Mapped[dict[str, Any]] = mapped_column(JSONB)
    request_id: Mapped[str | None] = mapped_column(String(64))  # связь с логами
    user_id: Mapped[uuid.UUID | None]  # кто вызвал (из заголовков задачи), без FK
    exc_type: Mapped[str] = mapped_column(String(255))  # модуль и имя класса исключения
    exc_message: Mapped[str] = mapped_column(String(EXC_MESSAGE_MAX))
    traceback: Mapped[str | None] = mapped_column(Text)  # не длиннее TRACEBACK_MAX
    retries: Mapped[int] = mapped_column(Integer)  # сколько повторов успело пройти
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # Перезапуск руками: когда, кто (супер-админ, без FK) и id новой задачи.
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    replayed_by: Mapped[uuid.UUID | None]
    replay_task_id: Mapped[str | None] = mapped_column(String(255))

    # failed_at из базы — тем же INSERT … RETURNING (ленивая загрузка в async — ошибка).
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012
