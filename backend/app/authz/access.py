"""Access — одно правило «своё / отдел / организация» в двух формах (раздел 5):

1. matches(obj) — объект уже в памяти: можно ли его видеть или записать такую версию;
2. sql_filter(Model) — список и bulk: условие для WHERE.

Обе формы — это три ветки own / team / org; тест test_access_forms проверяет, что они
отвечают одинаково на всех сочетаниях. Третья форма — es_filter() — появится с поиском (этап 9).
"""

import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import ColumnElement, and_, false

from app.authz.roles import Scope
from app.errors import Forbidden


class Scoped(Protocol):
    """Запись, к которой применяется Access: организация, отдел и ответственный."""

    @property
    def org_id(self) -> uuid.UUID | None: ...
    @property
    def team_id(self) -> uuid.UUID | None: ...
    @property
    def owner_id(self) -> uuid.UUID | None: ...


@dataclass(frozen=True)
class Access:
    action: str  # что делаем: deal:write
    scope: Scope  # на каком уровне: own / team / org
    user_id: uuid.UUID
    org_id: uuid.UUID
    team_id: uuid.UUID | None  # у org_admin и viewer отдела нет — уровень team им и не выдаётся

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
