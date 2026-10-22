"""Письма: шаблоны и ключи дедупа (отправка — воркером из outbox, tests/integration)."""

import pytest

from app.emails.render import render_email
from app.notifications.keys import email_key


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


def test_email_dedup_key_format() -> None:
    # Формат ключа — часть данных: строки sent_notifications живут 30 дней, и смена формата
    # между релизами отправила бы уже отправленные письма ещё раз.
    assert email_key(42) == "email:42"
