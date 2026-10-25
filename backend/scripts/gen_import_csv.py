"""Генератор CSV для импорта контактов (раздел 3, «Генерация данных»; шаг 5.6).

Запуск: make import-csv (ROWS=1000 — меньше). По умолчанию — 500 тыс. строк в
backend/tmp/contacts_import.csv: около 5% строк повторяют email одной из прежних строк (часть —
в другом регистре: дубль в файле ищется без учёта регистра), около 1% — битые (пустой или
неверный email, пустое имя, неизвестный статус, длинный телефон, лишняя ячейка). Остальные —
валидные контакты с тегами, статусом и источником. Тот же seed — тот же файл, поэтому замеры
повторяемы.

Колонки — как у импорта (раздел 6), без owner_email: ответственный — тот, кого указали в форме
(owner_id), или автор импорта. Ожидаемый итог импорта в пустую организацию: created ≈ 94%
строк, failed_rows ≈ 6% (дубли + битые), job — partial_failed.
"""

import argparse
import csv
import random
import sys
from collections.abc import Callable
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUT = BACKEND_DIR / "tmp" / "contacts_import.csv"
COLUMNS = ["email", "first_name", "last_name", "phone", "status", "source", "tags"]

FIRST_NAMES = ["Іван", "Ольга", "Тарас", "Марія", "Андрій", "Олена", "Петро", "Наталія", "Богдан"]
LAST_NAMES = ["Шевченко", "Коваленко", "Бондаренко", "Ткаченко", "Мельник", "Кравченко", "Олійник"]
LATIN = ["ivan", "olha", "taras", "maria", "andrii", "olena", "petro", "natalia", "bohdan"]
DOMAINS = ["example.com", "example.org", "mail.example.net", "corp.example.com"]
STATUSES = ["", "", "lead", "customer", "churned"]  # пусто — статус не меняется / lead
SOURCES = ["", "ads", "web", "referral", "conference", "cold_call"]
TAGS = ["vip", "b2b", "b2c", "kyiv", "lviv", "odesa", "newsletter", "partner", "trial"]


def valid_row(rnd: random.Random, no: int) -> list[str]:
    tags = rnd.sample(TAGS, rnd.randint(0, 3))
    phone = f"+38067{rnd.randint(0, 9_999_999):07d}" if rnd.random() < 0.7 else ""
    return [
        f"{rnd.choice(LATIN)}.{no}@{rnd.choice(DOMAINS)}",
        rnd.choice(FIRST_NAMES),
        rnd.choice(LAST_NAMES) if rnd.random() < 0.8 else "",
        phone,
        rnd.choice(STATUSES),
        rnd.choice(SOURCES),
        ",".join(tags),
    ]


def _empty_email(row: list[str]) -> list[str]:
    return ["", *row[1:]]


def _bad_email(row: list[str]) -> list[str]:
    return [row[0].replace("@", " at "), *row[1:]]


def _empty_name(row: list[str]) -> list[str]:
    return [row[0], "", *row[2:]]


def _bad_status(row: list[str]) -> list[str]:
    return [*row[:4], "vip", *row[5:]]


def _long_phone(row: list[str]) -> list[str]:
    return [*row[:3], "+380" + "1" * 40, *row[4:]]


def _extra_cell(row: list[str]) -> list[str]:
    return [*row, "зайва"]


BREAKERS: list[Callable[[list[str]], list[str]]] = [
    _empty_email,
    _bad_email,
    _empty_name,
    _bad_status,
    _long_phone,
    _extra_cell,
]


def generate(path: Path, rows: int, seed: int, duplicates: float, broken: float) -> dict[str, int]:
    """Записать файл и вернуть, сколько каких строк получилось."""
    rnd = random.Random(seed)
    emails: list[str] = []
    stats = {"rows": rows, "duplicates": 0, "broken": 0}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(COLUMNS)
        for no in range(rows):
            row = valid_row(rnd, no)
            chance = rnd.random()
            if chance < duplicates and emails:
                email = rnd.choice(emails)
                row[0] = email.upper() if rnd.random() < 0.3 else email
                stats["duplicates"] += 1
            elif chance < duplicates + broken:
                row = rnd.choice(BREAKERS)(row)
                stats["broken"] += 1
            else:
                emails.append(row[0])
            writer.writerow(row)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=500_000, help="строк данных (без заголовка)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="куда записать файл")
    parser.add_argument("--seed", type=int, default=42, help="seed генератора")
    parser.add_argument("--duplicates", type=float, default=0.05, help="доля дублей email")
    parser.add_argument("--broken", type=float, default=0.01, help="доля битых строк")
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows должно быть не меньше 1")
    stats = generate(args.out, args.rows, args.seed, args.duplicates, args.broken)
    size = args.out.stat().st_size
    print(
        f"{args.out}: {stats['rows']} строк, дублей {stats['duplicates']}, "
        f"битых {stats['broken']}, {size / 1024 / 1024:.1f} МиБ"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
