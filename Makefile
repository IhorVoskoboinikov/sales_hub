# Команды проекта. Список с описаниями: make help
COMPOSE = docker compose -f deploy/compose/docker-compose.yml
BACKEND = cd backend &&

.DEFAULT_GOAL := help
.PHONY: help up infra down clean ps logs urls keys migrate revision run test test-unit test-integration lint fmt

help:  ## показать команды
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-17s %s\n", $$1, $$2}'

up: keys  ## поднять стенд: API, Postgres, Redis и все UI (ждёт, пока всё станет healthy)
	$(COMPOSE) up -d --build --wait
	@$(MAKE) --no-print-directory urls

infra:  ## только Postgres, Redis и Mailpit — для make run (API запускается без Docker)
	$(COMPOSE) up -d --wait postgres redis mailpit

down:  ## остановить стенд (данные сохраняются)
	$(COMPOSE) down

clean:  ## остановить стенд и удалить все данные (тома)
	$(COMPOSE) down -v

ps:  ## статус контейнеров
	$(COMPOSE) ps

logs:  ## логи API в реальном времени
	$(COMPOSE) logs -f api

urls:  ## адреса веб-интерфейсов
	@echo "Swagger      http://localhost:8000/docs   супер-админ admin@example.com / admin-password"
	@echo "pgAdmin      http://localhost:5050"
	@echo "RedisInsight http://localhost:5540"
	@echo "Mailpit      http://localhost:8025"
	@echo "RustFS (S3)  http://localhost:9001/rustfs/console/   логин saleshub / saleshub-secret"

keys:  ## создать приватный ключ JWT в backend/secrets/ (если его ещё нет)
	$(BACKEND) uv run python -m app.cli generate-jwt-key

migrate:  ## применить миграции в compose
	$(COMPOSE) run --rm --build migrate

revision:  ## новая миграция по моделям: make revision m="add users"
	$(BACKEND) uv run alembic revision --autogenerate -m "$(m)"

run: keys  ## API без Docker с автоперезапуском (сначала make infra; с make up конфликт по порту 8000)
	$(BACKEND) uv run uvicorn app.main:create_app --factory --reload --no-access-log

test:  ## все тесты (интеграционным нужен Docker)
	$(BACKEND) uv run pytest

test-unit:  ## только unit-тесты, без Docker
	$(BACKEND) uv run pytest tests/unit

test-integration:  ## тесты с настоящими Postgres и Redis (testcontainers)
	$(BACKEND) uv run pytest tests/integration

lint:  ## ruff + mypy, как в CI
	$(BACKEND) uv run ruff check . && uv run ruff format --check . && uv run mypy app

fmt:  ## отформатировать код и исправить то, что ruff умеет сам
	$(BACKEND) uv run ruff format . && uv run ruff check --fix .
