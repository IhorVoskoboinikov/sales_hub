"""Команда create-superadmin: первый пользователь, под которым можно войти."""

import pytest
from pydantic import SecretStr
from sqlalchemy import delete

from app.cli import create_superadmin_command
from app.core.config import Settings
from app.core.security import verify_password
from app.db.session import create_engine, create_session_factory
from app.errors import ValidationFailed
from app.models import User
from app.services.users import create_superadmin
from app.uow import UnitOfWork


async def test_creates_verified_superadmin(uow: UnitOfWork) -> None:
    user, created = await create_superadmin(uow, "root@example.com", "correct horse battery")

    assert created
    assert user.is_superadmin
    assert user.is_active
    assert user.email_verified_at is not None
    assert (await verify_password("correct horse battery", user.password_hash))[0]


async def test_is_idempotent(uow: UnitOfWork) -> None:
    first, _ = await create_superadmin(uow, "root@example.com", "correct horse battery")

    again, created = await create_superadmin(uow, "ROOT@example.com", "another password 1")

    assert not created
    assert again.id == first.id
    assert (await verify_password("correct horse battery", again.password_hash))[0]


async def test_rejects_weak_password(uow: UnitOfWork) -> None:
    with pytest.raises(ValidationFailed):
        await create_superadmin(uow, "root@example.com", "root")


async def test_cli_command(settings: Settings, capsys: pytest.CaptureFixture[str]) -> None:
    settings = settings.model_copy(
        update={
            "superadmin_email": "cli-root@example.com",
            "superadmin_password": SecretStr("cli password 1"),
        }
    )
    try:
        assert await create_superadmin_command(settings) == 0
        assert await create_superadmin_command(settings) == 0
    finally:
        # Команда коммитит по-настоящему (мимо транзакции теста) — убираем за собой.
        engine = create_engine(settings.database_url)
        async with create_session_factory(engine)() as session:
            await session.execute(delete(User).where(User.email == "cli-root@example.com"))
            await session.commit()
        await engine.dispose()

    out = capsys.readouterr().out
    assert "создан супер-админ cli-root@example.com" in out
    assert "уже есть (супер-админ)" in out


async def test_cli_requires_env(settings: Settings, capsys: pytest.CaptureFixture[str]) -> None:
    assert await create_superadmin_command(settings) == 1
    assert "SUPERADMIN_EMAIL" in capsys.readouterr().err
