"""Настоящая отправка по SMTP в Mailpit."""

import uuid

from app.emails.render import render_email
from app.emails.sender import SmtpEmailSender
from tests.integration.conftest import Mailpit, read_emails, token_from_email


async def test_smtp_sender_delivers_text_and_html(mailpit: Mailpit) -> None:
    to = f"{uuid.uuid4().hex}@example.com"
    sender = SmtpEmailSender(
        host=mailpit.smtp_host, port=mailpit.smtp_port, sender="SalesHub <noreply@saleshub.local>"
    )

    await sender.send(
        render_email("invite", to, token="tok-42", docs_url="http://x/docs", expires_at="завтра")
    )

    [email] = await read_emails(mailpit, to)
    assert email["Subject"] == "Запрошення до SalesHub"
    assert email["From"]["Address"] == "noreply@saleshub.local"
    assert token_from_email(email) == "tok-42"
    assert "tok-42" in email["HTML"]
