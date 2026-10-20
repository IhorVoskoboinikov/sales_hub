"""Публичные ключи для проверки access-токенов. Вне /api/v1 и без авторизации."""

from typing import Any

from fastapi import APIRouter, Depends, Response

from app.api.deps import get_key_set
from app.core.jwt import KeySet

router = APIRouter(prefix="/.well-known", tags=["auth"])


@router.get("/jwks.json")
async def jwks(response: Response, keys: KeySet = Depends(get_key_set)) -> dict[str, Any]:
    """Публичные ключи RS256 (JWKS). По kid из заголовка токена выбирается ключ для проверки."""
    response.headers["Cache-Control"] = "public, max-age=300"
    return keys.jwks()
