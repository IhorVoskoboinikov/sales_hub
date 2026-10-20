"""Пароли и токены: argon2, правила пароля, хэш токенов."""

import pytest

from app.core.security import (
    check_password_rules,
    hash_password,
    hash_token,
    new_token,
    verify_dummy_password,
    verify_password,
)
from app.errors import ValidationFailed


async def test_password_hash_verifies_only_the_same_password() -> None:
    password_hash = await hash_password("correct horse battery")

    assert password_hash.startswith("$argon2id$")
    assert await verify_password("correct horse battery", password_hash) == (True, None)
    assert (await verify_password("wrong horse battery", password_hash))[0] is False


async def test_same_password_gives_different_hashes() -> None:
    # Соль случайная: по одинаковым хэшам нельзя понять, что пароли совпадают.
    assert await hash_password("correct horse battery") != await hash_password(
        "correct horse battery"
    )


async def test_dummy_check_works_without_user() -> None:
    await verify_dummy_password("anything at all")


@pytest.mark.parametrize(
    ("password", "code"),
    [
        ("short", "password.too_short"),
        ("x" * 129, "password.too_long"),
        ("Ivan.Petrenko@Example.com", "password.same_as_email"),  # регистр не важен
        ("IVAN.PETRENKO", "password.same_as_email"),  # имя до @ — тоже нельзя
    ],
)
def test_password_rules_reject(password: str, code: str) -> None:
    with pytest.raises(ValidationFailed) as exc:
        check_password_rules(password, "ivan.petrenko@example.com")

    assert exc.value.code == code


def test_password_rules_accept_good_password() -> None:
    check_password_rules("correct horse battery", "ivan@example.com")


def test_tokens_are_random_and_hash_is_stable() -> None:
    token = new_token()

    assert len(token) >= 43  # 32 байта в base64url
    assert token != new_token()
    assert hash_token(token) == hash_token(token)
    assert len(hash_token(token)) == 64
