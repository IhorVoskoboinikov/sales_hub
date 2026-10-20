"""Письма собираются из Jinja2-шаблонов app/texts/emails/<имя>.txt и <имя>.html."""

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from app.emails.sender import Email
from app.texts import uk

TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "texts" / "emails"

_env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(["html"]),  # в HTML экранируем всё, что подставляем
    undefined=StrictUndefined,  # забытая переменная — ошибка, а не пустое место в письме
    keep_trailing_newline=True,
)


def render_email(template: str, to: str, **context: Any) -> Email:
    return Email(
        to=to,
        subject=uk.EMAIL_SUBJECTS[template],
        text=_env.get_template(f"{template}.txt").render(**context),
        html=_env.get_template(f"{template}.html").render(**context),
    )
