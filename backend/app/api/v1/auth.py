"""Вход по почте (раздел 4): токены, сессии, пароль, приглашения."""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm

from app.api.deps import current_user, get_auth_service, get_client_info, get_uow
from app.errors import NotFound
from app.schemas.auth import MeOut, RefreshIn, SessionOut, TokenOut
from app.schemas.errors import ErrorOut
from app.services.auth import AuthService, ClientInfo, CurrentUser, TokenPair
from app.uow import UnitOfWork

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

AUTH_ERRORS: dict[int | str, dict[str, Any]] = {401: {"model": ErrorOut}, 403: {"model": ErrorOut}}


def token_out(pair: TokenPair) -> TokenOut:
    return TokenOut(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
    )


@router.post("/login", responses=AUTH_ERRORS)
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    client: ClientInfo = Depends(get_client_info),
    auth: AuthService = Depends(get_auth_service),
) -> TokenOut:
    """Вход по email и паролю. Поле username — это email (так требует OAuth2).

    Неверный email и неверный пароль дают одинаковый ответ 401 `auth.invalid_credentials`:
    по нему нельзя узнать, есть ли такой пользователь."""
    return token_out(await auth.login(form.username.strip(), form.password, client))


@router.post("/refresh", responses=AUTH_ERRORS)
async def refresh(
    body: RefreshIn,
    client: ClientInfo = Depends(get_client_info),
    auth: AuthService = Depends(get_auth_service),
) -> TokenOut:
    """Новая пара токенов по refresh-токену. Старый refresh после этого не работает.

    Если прислать уже использованный refresh, закроется вся сессия: так ведёт себя система,
    когда токен украли и им воспользовались дважды (вор и хозяин)."""
    return token_out(await auth.refresh(body.refresh_token, client))


@router.get("/me", responses=AUTH_ERRORS)
async def me(
    user: CurrentUser = Depends(current_user),
    uow: UnitOfWork = Depends(get_uow),
) -> MeOut:
    """Мой профиль. Роль, отдел и права появятся на этапе 3."""
    row = await uow.users.get(user.id)
    if row is None:  # удалили между проверкой токена и этим запросом
        raise NotFound(entity="user")
    return MeOut.model_validate(row)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT, responses=AUTH_ERRORS)
async def logout(
    user: CurrentUser = Depends(current_user),
    auth: AuthService = Depends(get_auth_service),
) -> None:
    """Выйти на этом устройстве: refresh-токены текущей сессии больше не работают.
    Access-токен доживёт до конца своего часа — он не хранится на сервере."""
    await auth.logout(user)


@router.post("/logout-all", status_code=status.HTTP_204_NO_CONTENT, responses=AUTH_ERRORS)
async def logout_all(
    user: CurrentUser = Depends(current_user),
    auth: AuthService = Depends(get_auth_service),
) -> None:
    """Выйти везде: закрываются все сессии, и все выданные access-токены сразу перестают
    работать (token_version + 1)."""
    await auth.logout_all(user)


@router.get("/sessions", responses=AUTH_ERRORS)
async def sessions(
    user: CurrentUser = Depends(current_user),
    auth: AuthService = Depends(get_auth_service),
) -> list[SessionOut]:
    """Мои активные сессии: устройство, IP, когда вошёл и когда последний раз обновлял токены."""
    return [SessionOut.model_validate(s) for s in await auth.sessions(user)]


@router.delete(
    "/sessions/{sid}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**AUTH_ERRORS, 404: {"model": ErrorOut}},
)
async def revoke_session(
    sid: uuid.UUID,
    user: CurrentUser = Depends(current_user),
    auth: AuthService = Depends(get_auth_service),
) -> None:
    """Выйти на конкретном устройстве (sid — из списка сессий)."""
    await auth.revoke_session(user, sid)
