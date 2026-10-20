import structlog
from fastapi import BackgroundTasks

from app.emails.sender import Email, EmailSender

logger = structlog.get_logger("email")


class BackgroundMailer:
    """Mailer до этапа 5: отправка — после ответа клиенту (BackgroundTasks FastAPI).
    Ответ не ждёт SMTP, а по времени ответа нельзя понять, ушло ли письмо (password/forgot)."""

    def __init__(self, sender: EmailSender, background: BackgroundTasks) -> None:
        self.sender = sender
        self.background = background

    def send_later(self, email: Email) -> None:
        self.background.add_task(send_safely, self.sender, email)


async def send_safely(sender: EmailSender, email: Email) -> None:
    # Ответ уже ушёл клиенту — сообщить об ошибке некому, остаётся лог. Письмо теряется;
    # повторные попытки появятся на этапе 5 вместе с очередью.
    try:
        await sender.send(email)
    except Exception:
        logger.exception("email_send_failed", to=email.to, subject=email.subject)
