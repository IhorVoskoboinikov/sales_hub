# Команды проекта. Список с описаниями: make help
COMPOSE = docker compose -f deploy/compose/docker-compose.yml
# opa из Docker — ставить его локально не нужно. Версия — как в compose и CI.
OPA = docker run --rm -v $(CURDIR)/policies:/policies:ro openpolicyagent/opa:1.21.3
BACKEND = cd backend &&

.DEFAULT_GOAL := help
.PHONY: help up infra down clean ps logs logs-workers restart-workers urls keys migrate revision run test test-unit test-integration lint fmt policy-test policy-fmt refresh-reports seed-big seed-big-clean explain experiments

help:  ## показать команды
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-17s %s\n", $$1, $$2}'

up: keys  ## поднять стенд: API, воркеры, Postgres, Redis и все UI (ждёт, пока всё станет healthy)
	$(COMPOSE) up -d --build --wait
	@$(MAKE) --no-print-directory urls

infra:  ## только Postgres, Redis, Mailpit и OPA — для make run (API запускается без Docker)
	$(COMPOSE) up -d --wait postgres redis mailpit opa

down:  ## остановить стенд (данные сохраняются)
	$(COMPOSE) down

clean:  ## остановить стенд и удалить все данные (тома)
	$(COMPOSE) down -v

ps:  ## статус контейнеров
	$(COMPOSE) ps

logs:  ## логи API в реальном времени
	$(COMPOSE) logs -f api

logs-workers:  ## логи воркеров, beat и outbox relay в реальном времени
	$(COMPOSE) logs -f worker worker-bulk beat outbox-relay

restart-workers:  ## перезапустить воркеры, beat и relay (beat перечитывает расписание только при старте)
	$(COMPOSE) restart worker worker-bulk beat outbox-relay

urls:  ## адреса веб-интерфейсов
	@echo "Swagger      http://localhost:8000/docs   супер-админ admin@example.com / admin-password"
	@echo "pgAdmin      http://localhost:5050"
	@echo "RedisInsight http://localhost:5540"
	@echo "Mailpit      http://localhost:8025"
	@echo "Flower       http://localhost:5555   задачи и воркеры Celery"
	@echo "OPA          http://localhost:8181/v1/data/saleshub/roles"
	@echo "RustFS (S3)  http://localhost:9001/rustfs/console/   логин saleshub / saleshub-secret"

keys:  ## создать приватный ключ JWT в backend/secrets/ (если его ещё нет)
	$(BACKEND) uv run python -m app.cli generate-jwt-key

migrate:  ## применить миграции в compose
	$(COMPOSE) run --rm --build migrate

SCALE ?= 1
seed-big:  ## большие данные для замеров: 5 млн контактов, 20 млн активностей (SCALE=0.01 — 1%)
	$(COMPOSE) exec -T postgres psql -U saleshub -d saleshub -v scale=$(SCALE) < backend/scripts/seed_big.sql

seed-big-clean:  ## удалить данные seed-big (быстрее — make clean && make up)
	$(COMPOSE) exec -T postgres psql -U saleshub -d saleshub < backend/scripts/seed_big_clean.sql

explain:  ## планы всех списков и отчётов на seed-данных: нет ли Seq Scan по большим таблицам
	$(BACKEND) uv run python scripts/explain_lists.py

experiments:  ## замеры индексов «без / с» на seed-данных (результаты — docs/experiments.md)
	$(COMPOSE) exec -T postgres psql -U saleshub -d saleshub < backend/scripts/experiments.sql

refresh-reports:  ## обновить отчёты (materialized view deal_stats) в compose
	$(COMPOSE) exec api python -m app.cli refresh-reports

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

policy-test:  ## тесты политик OPA: формат, проверка, opa test с покрытием не ниже 90%
	$(OPA) fmt --list --fail /policies
	$(OPA) check --strict /policies
	$(OPA) test -b /policies -v
	$(OPA) test -b /policies --coverage --threshold 90 --format=json > /dev/null

policy-fmt:  ## отформатировать .rego
	docker run --rm -v $(CURDIR)/policies:/policies openpolicyagent/opa:1.21.3 fmt -w /policies
