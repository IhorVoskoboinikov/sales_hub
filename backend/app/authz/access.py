"""Access — одно правило «своё / отдел / организация» в двух формах (раздел 5):

1. matches(obj) — объект уже в памяти: можно ли его видеть или записать такую версию;
2. sql_filter(Model) — список и bulk: условие для WHERE.

Обе формы — это три ветки own / team / org; тест test_access_forms проверяет, что они
отвечают одинаково на всех сочетаниях. Третья форма — es_filter() — появится с поиском (этап 9).

Массовая операция (раздел 6) хранит Access автора в bulk_jobs.params (to_params), а воркер
восстанавливает его в каждом чанке (from_params): чанк обновляет только строки в уровне автора.

Вне HTTP (бот, задачи beat — раздел 16) Access строится без OPA: for_user() берёт уровень роли
из data.yaml, а own() сужает его до своих записей.
"""

import uuid
from dataclasses import dataclass, replace
from typing import Any, Protocol, cast

from sqlalchemy import ColumnElement, and_, false

from app.authz.roles import SCOPES, RoleCatalog, Scope
from app.errors import Forbidden


class Scoped(Protocol):
    """Запись, к которой применяется Access: организация, отдел и ответственный."""

    @property
    def org_id(self) -> uuid.UUID | None: ...
    @property
    def team_id(self) -> uuid.UUID | None: ...
    @property
    def owner_id(self) -> uuid.UUID | None: ...


class Member(Protocol):
    """Пользователь организации, от имени которого читаются данные: CachedUser или User."""

    @property
    def id(self) -> uuid.UUID: ...
    @property
    def org_id(self) -> uuid.UUID | None: ...
    @property
    def team_id(self) -> uuid.UUID | None: ...
    @property
    def role(self) -> str | None: ...


@dataclass(frozen=True)
class Access:
    action: str  # что делаем: deal:write
    scope: Scope  # на каком уровне: own / team / org
    user_id: uuid.UUID
    org_id: uuid.UUID
    team_id: uuid.UUID | None  # у org_admin и viewer отдела нет — уровень team им и не выдаётся
    role: str | None = None  # чтобы получить уровень другого действия: for_action()

    @classmethod
    def for_user(cls, user: Member, action: str, roles: RoleCatalog) -> "Access | None":
        """Access без OPA — для бота и задач beat, которые не ходят через HTTP-маршруты:
        уровень действия у роли из data.yaml. None — у роли нет действия или у пользователя
        нет организации (супер-админ)."""
        scope = roles.scope_or_none(user.role, action)
        if scope is None or user.org_id is None:
            return None
        return cls(action, scope, user.id, user.org_id, user.team_id, user.role)

    def own(self) -> "Access":
        """Тот же человек и действие, уровень own — только записи, где я ответственный. Бот
        показывает только свои данные, даже руководителю и админу (раздел 16)."""
        return replace(self, scope="own")

    def for_action(self, action: str, roles: RoleCatalog) -> "Access | None":
        """Тот же человек, другое действие: сделку создают с deal:write, а привязанный к ней
        контакт проверяют на contact:read. None — у роли такого действия нет."""
        scope = roles.scope_or_none(self.role, action)
        if scope is None:
            return None
        return Access(action, scope, self.user_id, self.org_id, self.team_id, self.role)

    def to_params(self) -> dict[str, Any]:
        """Access автора для bulk_jobs.params: только JSON-типы (uuid → строка)."""
        return {
            "action": self.action,
            "scope": self.scope,
            "user_id": str(self.user_id),
            "org_id": str(self.org_id),
            "team_id": str(self.team_id) if self.team_id is not None else None,
            "role": self.role,
        }

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "Access":
        """Обратно из to_params(): воркер восстанавливает Access автора в каждом чанке."""
        scope = params["scope"]
        if scope not in SCOPES:
            raise ValueError(f"неизвестный уровень: {scope}")
        team_id = params.get("team_id")
        return cls(
            action=params["action"],
            scope=cast(Scope, scope),
            user_id=uuid.UUID(params["user_id"]),
            org_id=uuid.UUID(params["org_id"]),
            team_id=uuid.UUID(team_id) if team_id is not None else None,
            role=params.get("role"),
        )

    def matches(self, obj: Scoped) -> bool:
        if obj.org_id != self.org_id:
            return False
        match self.scope:
            case "org":
                return True
            case "team":
                # Без отдела уровень team не даёт ничего: иначе team_id = NULL совпал бы
                # со всеми записями без отдела.
                return self.team_id is not None and obj.team_id == self.team_id
            case "own":
                return obj.owner_id == self.user_id
        raise ValueError(f"неизвестный уровень: {self.scope}")

    def sql_filter(self, model: Any) -> ColumnElement[bool]:
        """Условие WHERE для модели с колонками org_id, team_id, owner_id."""
        same_org: ColumnElement[bool] = model.org_id == self.org_id
        match self.scope:
            case "org":
                return same_org
            case "team":
                if self.team_id is None:
                    return false()
                return and_(same_org, model.team_id == self.team_id)
            case "own":
                return and_(same_org, model.owner_id == self.user_id)
        raise ValueError(f"неизвестный уровень: {self.scope}")

    def ensure_allowed(self, new_obj: Scoped) -> None:
        """Создать или изменить так, что объект выйдет за мой уровень, — 403. Старую версию
        проверяет репозиторий (невидимый объект — 404), новую — этот метод."""
        if not self.matches(new_obj):
            raise Forbidden(reason="out_of_scope")
