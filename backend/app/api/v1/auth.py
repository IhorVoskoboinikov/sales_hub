"""Вход по почте (раздел 4): токены, сессии, пароль, приглашения."""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm

from app.api.deps import (
    current_user,
    get_auth_service,
    get_client_info,
    get_invite_service,
    get_password_reset_service,
    get_uow,
)
from app.errors import NotFound
from app.schemas.auth import (
    InviteAcceptIn,
    MeOut,
    PasswordChangeIn,
    PasswordForgotIn,
    PasswordResetIn,
    RefreshIn,
    SessionOut,
    TokenOut,
)
from app.schemas.errors import ErrorOut
from app.services.auth import AuthService, ClientInfo, CurrentUser, TokenPair
from app.services.invites import InviteService
from app.services.password_reset import PasswordResetService
from app.uow import UnitOfWork

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

AUTH_ERRORS: dict[int | str, dict[str, Any]] = {
    401: {"model": ErrorOut},
    403: {"model": ErrorOut},
}
RATE_LIMITED: dict[int | str, dict[str, Any]] = {429: {"model": ErrorOut}}


def token_out(pair: TokenPair) -> TokenOut:
    return TokenOut(
        access_token=pair.access_token,
        refresh_token=pair.refresh_token,
        expires_in=pair.expires_in,
    )


@router.post("/login", responses={**AUTH_ERRORS, **RATE_LIMITED})
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    client: ClientInfo = Depends(get_client_info),
    auth: AuthService = Depends(get_auth_service),
) -> TokenOut:
    """Вход по email и паролю. Поле username — это email (так требует OAuth2).

    Неверный email и неверный пароль дают одинаковый ответ 401 `auth.invalid_credentials`:
    по нему нельзя узнать, есть ли такой пользователь. После 5 неудачных попыток за 15 минут
    с одного IP для этого email — 429 и заголовок Retry-After."""
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


@router.post(
    "/invite/accept",
    status_code=status.HTTP_201_CREATED,
    responses={409: {"model": ErrorOut}, 422: {"model": ErrorOut}},
)
async def accept_invite(
    body: InviteAcceptIn, invites: InviteService = Depends(get_invite_service)
) -> MeOut:
    """Принять приглашение: токен из письма и пароль. Email считается подтверждённым.
    Дальше — обычный вход через POST /auth/login (кнопка Authorize)."""
    return MeOut.model_validate(await invites.accept(body.token, body.password))


@router.post("/password/forgot", status_code=status.HTTP_202_ACCEPTED, responses=RATE_LIMITED)
async def forgot_password(
    body: PasswordForgotIn,
    passwords: PasswordResetService = Depends(get_password_reset_service),
) -> None:
    """Письмо со ссылкой для сброса пароля (действует 1 час). Ответ всегда 202 — даже если
    такого email нет: иначе по ответу можно было бы проверять, кто зарегистрирован."""
    await passwords.forgot(str(body.email))


@router.post(
    "/password/reset",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={403: {"model": ErrorOut}, 422: {"model": ErrorOut}},
)
async def reset_password(
    body: PasswordResetIn,
    passwords: PasswordResetService = Depends(get_password_reset_service),
) -> None:
    """Новый пароль по токену из письма. Все сессии закрываются, старые токены не работают."""
    await passwords.reset(body.token, body.new_password)


@router.post("/password/change", responses={**AUTH_ERRORS, 422: {"model": ErrorOut}})
async def change_password(
    body: PasswordChangeIn,
    user: CurrentUser = Depends(current_user),
    client: ClientInfo = Depends(get_client_info),
    auth: AuthService = Depends(get_auth_service),
) -> TokenOut:
    """Сменить пароль. Остальные сессии закрываются, их access-токены сразу перестают
    работать; эта сессия продолжается с новой парой токенов из ответа."""
    return token_out(await auth.change_password(user, body.old_password, body.new_password, client))
