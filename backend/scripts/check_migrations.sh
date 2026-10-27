#!/usr/bin/env bash
# Проверки миграций (раздел 9): job migrations в CI и make migrations-check.
# Нужна ПУСТАЯ база в DATABASE_URL: скрипт делает downgrade base — никогда не запускай его на базе стенда.
set -euo pipefail
: "${DATABASE_URL:?нужен DATABASE_URL пустой базы}"

heads=$(uv run alembic heads)
if [ "$(wc -l <<<"$heads")" -ne 1 ]; then
  echo "несколько голов — нужна merge-миграция (alembic merge heads):"
  echo "$heads"
  exit 1
fi
uv run alembic upgrade head     # на чистой базе
uv run alembic check            # модели и миграции не расходятся
uv run alembic downgrade base   # каждая миграция обратима
uv run alembic upgrade head     # и снова применяется после отката
