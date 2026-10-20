"""Клиент OPA: один POST на запрос (раздел 5).

OPA — сайдкар на localhost (в compose — сервис opa), ответ за ~1 мс, поэтому таймаут короткий.
OPA не отвечает → 503: без решения о доступе никого не пускаем (fail closed).
"""

import uuid
from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from app.errors import ServiceUnavailable

logger = structlog.get_logger("authz")

DECISION_PATH = "saleshub/authz/decision"  # package saleshub.authz, правило decision
ROUTES_PATH = "saleshub/routes/actions"
TIMEOUT_SECONDS = 0.5


@dataclass(frozen=True)
class Decision:
    allow: bool
    action: str | None  # None — эндпоинта нет в routes.rego


class OpaClient:
    def __init__(self, http: httpx.AsyncClient, url: str) -> None:
        self.http = http
        self.url = url.rstrip("/")

    async def query(self, path: str, input: dict[str, Any] | None = None) -> Any:
        try:
            resp = await self.http.post(
                f"{self.url}/v1/data/{path}",
                json={"input": input} if input is not None else {},
                timeout=TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error("opa_unavailable", error=repr(exc))
            raise ServiceUnavailable(failed=["opa"]) from exc
        return resp.json().get("result")

    async def decide(
        self,
        method: str,
        route: str,
        user_id: uuid.UUID,
        role: str | None,
        is_superadmin: bool,
    ) -> Decision:
        result = await self.query(
            DECISION_PATH,
            {
                "method": method,
                "route": route,  # шаблон пути: /api/v1/users/{user_id}
                # Организация и отдел OPA не нужны: данные он не проверяет. id — для логов.
                "user": {"id": str(user_id), "role": role, "is_superadmin": is_superadmin},
            },
        )
        result = result or {}
        return Decision(allow=bool(result.get("allow")), action=result.get("action"))

    async def route_actions(self) -> dict[str, str]:
        """Все маршруты из routes.rego: {"PATCH /api/v1/users/{user_id}": "member:manage"}."""
        return dict(await self.query(ROUTES_PATH) or {})
