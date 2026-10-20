"""Пароли (argon2) и случайные токены для refresh и писем.

argon2 специально медленный (~50 мс и 64 МБ памяти на хэш), поэтому считается в отдельном
потоке: иначе на время хэширования встал бы весь event loop и все остальные запросы.
"""

import asyncio
import hashlib
import secrets
from functools import lru_cache

from pwdlib import PasswordHash
from pwdlib.hashers.argon2 import Argon2Hasher

from app.errors import ValidationFailed

MIN_PASSWORD_LENGTH = 10
MAX_PASSWORD_LENGTH = 128  # длиннее не нужно, а хэшировать мегабайтный «пароль» дорого

_hasher = PasswordHash((Argon2Hasher(),))


async def hash_password(password: str) -> str:
    return await asyncio.to_thread(_hasher.hash, password)


async def verify_password(password: str, password_hash: str) -> tuple[bool, str | None]:
    """Проверить пароль. Второй элемент — новый хэш, если параметры argon2 с тех пор
    поменялись (например, выросла стоимость): его стоит сохранить вместо старого."""
    return await asyncio.to_thread(_hasher.verify_and_update, password, password_hash)


@lru_cache
def _dummy_hash() -> str:
    return _hasher.hash(secrets.token_urlsafe())


async def verify_dummy_password(password: str) -> None:
    """Проверка пароля для email, которого нет в базе. Тратит столько же времени, сколько
    настоящая: иначе по скорости ответа логина можно было бы перебирать, кто зарегистрирован."""
    dummy = await asyncio.to_thread(_dummy_hash)
    await asyncio.to_thread(_hasher.verify, password, dummy)


def check_password_rules(password: str, email: str) -> None:
    """Правила пароля (раздел 4). Длину проверяет и схема запроса, но команда
    create-superadmin идёт мимо схем, поэтому здесь — ещё раз."""
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValidationFailed("password.too_short", min_length=MIN_PASSWORD_LENGTH)
    if len(password) > MAX_PASSWORD_LENGTH:
        raise ValidationFailed("password.too_long", max_length=MAX_PASSWORD_LENGTH)
    lowered = password.casefold()
    if lowered in (email.casefold(), email.split("@")[0].casefold()):
        raise ValidationFailed("password.same_as_email")


def new_token() -> str:
    """Случайная строка на 256 бит для refresh-токена и ссылок из писем."""
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    """SHA-256 токена — то, что лежит в базе. Соль не нужна: токен и так случайный и длинный,
    а быстрый хэш позволяет искать по нему индексом."""
    return hashlib.sha256(token.encode()).hexdigest()
