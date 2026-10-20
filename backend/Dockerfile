# syntax=docker/dockerfile:1

# --- Сборка: зависимости ставятся в /app/.venv через uv (без dev-зависимостей).
FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Сначала только файлы зависимостей: пока они не менялись, этот слой берётся из кэша.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

# --- Запуск: только venv и код, без uv и кэшей сборки.
FROM python:3.13-slim AS runtime
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY migrations ./migrations
COPY app ./app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
