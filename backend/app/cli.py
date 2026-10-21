"""Команды для администрирования: python -m app.cli <команда>.

generate-jwt-key   создать приватный RS256-ключ для JWT (make keys)
create-superadmin  создать супер-админа из SUPERADMIN_EMAIL и SUPERADMIN_PASSWORD
ensure-partitions  месячные партиции activities и audit_log на 2 месяца вперёд
"""

import argparse
import asyncio
import sys
from pathlib import Path

from pydantic import EmailStr, TypeAdapter, ValidationError
from sqlalchemy import text

from app.core.config import Settings, get_settings
from app.core.jwt import generate_private_key, private_key_to_pem
from app.db.session import create_engine, create_session_factory
from app.errors import AppError
from app.services.users import create_superadmin
from app.uow import UnitOfWork


def generate_jwt_key(path: Path) -> None:
    if path.exists():
        print(f"ключ уже есть: {path}")  # не перезаписываем: иначе все выданные токены умрут
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(private_key_to_pem(generate_private_key()))
    # Ключ для dev: контейнер api работает под другим uid, поэтому файл читаем для всех.
    # В Kubernetes это Secret, права на файл задаёт defaultMode.
    path.chmod(0o644)
    print(f"создан ключ: {path}")


async def create_superadmin_command(settings: Settings) -> int:
    if not settings.superadmin_email or not settings.superadmin_password:
        print("задай SUPERADMIN_EMAIL и SUPERADMIN_PASSWORD", file=sys.stderr)
        return 1
    try:
        email = TypeAdapter(EmailStr).validate_python(settings.superadmin_email)
    except ValidationError:
        print(f"некорректный SUPERADMIN_EMAIL: {settings.superadmin_email}", file=sys.stderr)
        return 1

    engine = create_engine(settings.database_url)
    try:
        async with create_session_factory(engine)() as session:
            user, created = await create_superadmin(
                UnitOfWork(session), email, settings.superadmin_password.get_secret_value()
            )
    except AppError as exc:  # пароль не прошёл правила
        print(f"{exc.title}: {exc.code} {exc.details}", file=sys.stderr)
        return 1
    finally:
        await engine.dispose()

    if not created:
        role = "супер-админ" if user.is_superadmin else "обычный пользователь, не супер-админ"
        print(f"пользователь {user.email} уже есть ({role}) — ничего не меняю")
    else:
        print(f"создан супер-админ {user.email}")
    return 0


# Партиционированные таблицы и на сколько месяцев вперёд готовить партиции (раздел 9).
PARTITIONED = ("activities", "audit_log")
MONTHS_AHEAD = 2


async def ensure_partitions_command(settings: Settings) -> int:
    """Идемпотентно: существующие партиции пропускаются. Запускается после миграций (сервис
    migrate), с этапа 5 — ещё и задачей beat раз в месяц."""
    engine = create_engine(settings.database_url)
    try:
        async with engine.begin() as connection:
            for table in PARTITIONED:
                created = await connection.scalar(
                    text(
                        "SELECT ensure_monthly_partitions(CAST(:table AS regclass), CAST(now() AS"
                        " date), CAST(now() + make_interval(months => :ahead) AS date))"
                    ),
                    {"table": table, "ahead": MONTHS_AHEAD},
                )
                print(f"{table}: создано партиций — {created}")
    finally:
        await engine.dispose()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    commands = parser.add_subparsers(dest="command", required=True)

    keys = commands.add_parser("generate-jwt-key", help="создать приватный ключ для JWT")
    keys.add_argument("--out", type=Path, help="путь (по умолчанию — JWT_PRIVATE_KEY_FILE)")
    commands.add_parser(
        "create-superadmin", help="создать супер-админа из SUPERADMIN_EMAIL и SUPERADMIN_PASSWORD"
    )
    commands.add_parser("ensure-partitions", help="месячные партиции на 2 месяца вперёд")

    args = parser.parse_args(argv)
    settings = get_settings()
    if args.command == "generate-jwt-key":
        generate_jwt_key(args.out or settings.jwt_private_key_file)
        return 0
    if args.command == "ensure-partitions":
        return asyncio.run(ensure_partitions_command(settings))
    return asyncio.run(create_superadmin_command(settings))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
