"""Отправка уведомлений из воркера (разделы 15 и 16): команды из outbox → SMTP.

Задача notifications.send_email получает только id события: payload с адресом, шаблоном и
контекстом (в нём сырой токен) читается из базы, в Redis его нет. Отправка «как минимум один
раз», дубли гасит sent_notifications: ключ занимается в той же транзакции, что и отправка.
"""

import uuid

import structlog

from app.emails.render import render_email
from app.emails.sender import EmailSender
from app.models import NotificationChannel
from app.notifications.keys import email_key
from app.uow import UnitOfWork

logger = structlog.get_logger("notifications")


class NotificationService:
    def __init__(self, uow: UnitOfWork, email_sender: EmailSender) -> None:
        self.uow = uow
        self.email_sender = email_sender

    async def send_email(self, event_id: int) -> None:
        """Отправить письмо команды email.send одной транзакцией (раздел 15):

        занять ключ дедупа → отрисовать шаблон → SMTP → стереть токен из payload → коммит.
        Ключ уже занят — письмо уже ушло, выход. Ошибка — откат: ключ освобождается, токен
        остаётся в payload, и ретрай (или перезапуск из failed_tasks) отправит письмо снова.
        Дубль возможен, только если процесс упадёт между ответом SMTP и коммитом.

        Таймаут — на каждую команду SMTP (SmtpEmailSender, 10 с), а не asyncio.timeout на всю
        отправку: тот прервал бы и QUIT после принятого DATA — письмо ушло, а откат и ретрай
        отправили бы его снова. Ошибки QUIT aiosmtplib игнорирует сам; от медленного сервера
        воркер страхует task_soft_time_limit."""
        try:
            event = await self.uow.outbox.get(event_id)
            if event is None:
                # Строку удалила очистка (опубликованные старше 7 дней) — отправлять нечего.
                logger.warning("email_event_missing", event_id=event_id)
                return
            payload = event.payload
            key = payload.get("dedup_key") or email_key(event_id)
            user_id = uuid.UUID(payload["user_id"]) if payload.get("user_id") else None
            if not await self.uow.sent_notifications.claim(key, NotificationChannel.EMAIL, user_id):
                logger.info("email_duplicate", event_id=event_id, dedup_key=key)
                await self.uow.rollback()
                return
            email = render_email(payload["template"], payload["to"], **payload["context"])
            await self.email_sender.send(email)
            await self.uow.outbox.scrub_token(event_id)
            await self.uow.commit()
        except BaseException:
            await self.uow.rollback()
            raise
