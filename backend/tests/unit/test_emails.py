"""Письма: шаблоны и отправка после ответа (BackgroundTasks)."""

import pytest
from fastapi import APIRouter, BackgroundTasks, FastAPI
from httpx import AsyncClient
from structlog.testing import capture_logs

from app.emails.mailer import BackgroundMailer, send_safely
from app.emails.render import render_email
from app.emails.sender import Email
from tests.conftest import FakeEmailSender

sender = FakeEmailSender()
router = APIRouter(prefix="/test")


@router.post("/invite")
async def invite(background: BackgroundTasks) -> dict[str, str]:
    mailer = BackgroundMailer(sender, background)
    mailer.send_later(Email(to="ivan@example.com", subject="s", text="t", html="h"))
    return {"status": "queued"}


@pytest.fixture
def app(app: FastAPI) -> FastAPI:
    app.include_router(router)
    return app


def test_invite_email_has_token_in_text_and_html() -> None:
    email = render_email(
        "invite",
        "ivan@example.com",
        token="tok-123",
        org_name="Acme",
        role_title="Менеджер з продажів",
        docs_url="http://localhost:8000/docs",
        expires_at="27.10.2026 12:00 UTC",
    )

    assert email.to == "ivan@example.com"
    assert email.subject == "Запрошення до SalesHub"
    for body in (email.text, email.html):
        assert "tok-123" in body
        assert "27.10.2026 12:00 UTC" in body
    assert email.html.startswith("<!doctype html>")


def test_html_escapes_values() -> None:
    email = render_email("reset_password", "a@b.c", token="<script>", docs_url="u", expires_at="e")

    assert "<script>" not in email.html
    assert "&lt;script&gt;" in email.html
    assert "<script>" in email.text  # обычный текст не экранируется


def test_missing_variable_is_an_error() -> None:
    with pytest.raises(Exception, match="undefined"):
        render_email("invite", "a@b.c", token="t")


async def test_email_is_sent_after_response(client: AsyncClient) -> None:
    sender.sent.clear()

    resp = await client.post("/test/invite")

    assert resp.status_code == 200
    assert [e.to for e in sender.sent] == ["ivan@example.com"]


async def test_send_failure_is_logged_not_raised() -> None:
    class BrokenSender:
        async def send(self, email: Email) -> None:
            raise ConnectionRefusedError("smtp down")

    with capture_logs() as logs:
        await send_safely(BrokenSender(), Email(to="a@b.c", subject="s", text="t", html="h"))

    assert any(e["event"] == "email_send_failed" and e["to"] == "a@b.c" for e in logs)
