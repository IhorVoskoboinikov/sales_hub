"""Роли и уровни из policies/saleshub/data.yaml — того же файла, что читает OPA."""

from pathlib import Path
from typing import Literal, cast

import yaml

from app.errors import ServiceUnavailable

Scope = Literal["own", "team", "org"]
SCOPES: frozenset[str] = frozenset({"own", "team", "org"})


class RoleCatalog:
    """Что может каждая роль и на каком уровне: {"sales_rep": {"deal:write": "own", ...}}."""

    def __init__(self, permissions: dict[str, dict[str, Scope]], titles: dict[str, str]) -> None:
        self._permissions = permissions
        self.titles = titles

    @classmethod
    def load(cls, policies_dir: Path) -> "RoleCatalog":
        path = policies_dir / "saleshub" / "data.yaml"
        if not path.is_file():
            raise RuntimeError(f"нет файла ролей: {path}. Задай POLICIES_DIR — папку policies/")
        roles = yaml.safe_load(path.read_text())["roles"]
        permissions: dict[str, dict[str, Scope]] = {}
        for role, body in roles.items():
            levels = body.get("permissions", {})
            bad = {action: level for action, level in levels.items() if level not in SCOPES}
            if bad:  # то же правило, что test_scopes_are_valid в authz_test.rego
                raise RuntimeError(f"{path}: у роли {role} неизвестные уровни {bad}")
            permissions[role] = cast(dict[str, Scope], levels)
        titles = {role: str(body.get("title", role)) for role, body in roles.items()}
        return cls(permissions, titles)

    def permissions(self, role: str | None) -> dict[str, Scope]:
        """Действия роли с уровнями — для /auth/me (фронт по ним прячет кнопки)."""
        return dict(self._permissions.get(role or "", {}))

    def scope_for(self, role: str | None, action: str) -> Scope:
        level = self._permissions.get(role or "", {}).get(action)
        if level is None:
            # OPA разрешил, а у бэкенда в data.yaml такого действия нет — файлы разошлись.
            raise ServiceUnavailable("authz.policy_out_of_sync", action=action)
        return level

    def scope_or_none(self, role: str | None, action: str) -> Scope | None:
        return self._permissions.get(role or "", {}).get(action)

    def levels(self, action: str) -> dict[str, Scope]:
        """У каких ролей есть действие и с каким уровнем — для описаний в Swagger."""
        return {
            role: levels[action] for role, levels in self._permissions.items() if action in levels
        }
