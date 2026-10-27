"""Команды python -m app.cli, которым не нужны ни база, ни другие сервисы."""

import json
from pathlib import Path

from app.cli import export_openapi
from app.core.config import Settings


def test_export_openapi_is_stable_and_needs_no_services(settings: Settings, tmp_path: Path) -> None:
    # Адреса в settings ведут на закрытый порт: схема строится без Postgres, Redis и OPA.
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    export_openapi(settings, first)
    export_openapi(settings, second)

    text = first.read_text(encoding="utf-8")
    assert text == second.read_text(encoding="utf-8")  # CI сравнивает файл побайтно
    schema = json.loads(text)
    assert schema["info"]["title"] == "SalesHub API"
    assert "/api/v1/contacts" in schema["paths"]
    assert "Требуется" not in text  # пометки прав — только у запущенного API (из OPA)
