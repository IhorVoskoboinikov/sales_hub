"""Роутеры /api/v1.

- protected_router (/api/v1) — данные организации: OPA (может ли роль вызвать эндпоинт) + Access
  (какие записи видны). Эндпоинты объявляются в модулях org.py, teams.py, users.py, invites.py.
- admin_router (/api/v1/admin) — только супер-админ: OPA разрешает ему действия admin:*, Access нет.
- auth.py — свой роутер /api/v1/auth: вход и личные данные, только current_user, без OPA.
- me.py — свой роутер /api/v1/me: личные настройки (привязка Telegram), только current_member
  (current_user + организация), без OPA и без строк в routes.rego.

Правило: эндпоинты объявляются прямо на роутере с полным префиксом
(@protected_router.get("/teams")), а роутер подключается только в app (main.py) — без
include_router(prefix=...) и без вложения роутера в роутер. Только так scope["route"].path —
полный путь-шаблон /api/v1/teams/{team_id}: его пишет access-лог, и по нему OPA ищет действие
в routes.rego. У вложенного роутера FastAPI 0.151 оставляет в пути только его собственный
префикс (/teams/{team_id}). Каждый маршрут роутеров protected и admin обязан иметь строку
в routes.rego — это проверяет test_routes_match_policy.
"""

from fastapi import APIRouter, Depends

from app.authz.deps import authorize, opa_decision

# Имена с _router: модуль admin.py в этой папке иначе перекрыл бы переменную admin.
protected_router = APIRouter(prefix="/api/v1", dependencies=[Depends(authorize)])
admin_router = APIRouter(
    prefix="/api/v1/admin", tags=["admin"], dependencies=[Depends(opa_decision)]
)
