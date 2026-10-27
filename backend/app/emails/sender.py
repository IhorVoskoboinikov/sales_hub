from dataclasses import dataclass
from email.message import EmailMessage
from typing import Protocol

import aiosmtplib
import structlog

logger = structlog.get_logger("email")


@dataclass(frozen=True)
class Email:
    to: str
    subject: str
    text: str  # обычный текст — для почтовых клиентов без HTML
    html: str


class EmailSender(Protocol):
    """Отправить письмо прямо сейчас. Есть только у воркеров (Resources, for_worker=True)."""

    async def send(self, email: Email) -> None: ...


class SmtpEmailSender:
    """Отправка по SMTP. В dev — Mailpit (письма видно на localhost:8025), в prod — любой SMTP."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender: str,
        username: str | None = None,
        password: str | None = None,
        start_tls: bool = False,
        timeout: float = 10,
    ) -> None:
        self.host = host
        self.port = port
        self.sender = sender
        self.username = username
        self.password = password
        self.start_tls = start_tls
        self.timeout = timeout

    async def send(self, email: Email) -> None:
        message = EmailMessage()
        message["From"] = self.sender
        message["To"] = email.to
        message["Subject"] = email.subject
        message.set_content(email.text)
        message.add_alternative(email.html, subtype="html")
        await aiosmtplib.send(
            message,
            hostname=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            start_tls=self.start_tls,
            timeout=self.timeout,
        )
        # Адрес и тему пишем, текст — нет: в нём одноразовый токен.
        logger.info("email_sent", to=email.to, subject=email.subject)
