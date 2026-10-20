"""Команды для администрирования: python -m app.cli <команда>.

generate-jwt-key   создать приватный RS256-ключ для JWT (make keys)
"""

import argparse
import sys
from pathlib import Path

from app.core.config import get_settings
from app.core.jwt import generate_private_key, private_key_to_pem


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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    commands = parser.add_subparsers(dest="command", required=True)

    keys = commands.add_parser("generate-jwt-key", help="создать приватный ключ для JWT")
    keys.add_argument("--out", type=Path, help="путь (по умолчанию — JWT_PRIVATE_KEY_FILE)")

    args = parser.parse_args(argv)
    if args.command == "generate-jwt-key":
        generate_jwt_key(args.out or get_settings().jwt_private_key_file)


if __name__ == "__main__":
    main(sys.argv[1:])
