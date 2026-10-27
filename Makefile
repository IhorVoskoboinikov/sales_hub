# Команды проекта. Список с описаниями: make help
COMPOSE = docker compose -f deploy/compose/docker-compose.yml
# opa из Docker — ставить его локально не нужно. Версия — как в compose и CI.
OPA = docker run --rm -v $(CURDIR)/policies:/policies:ro openpolicyagent/opa:1.21.3
# Regal — линтер Rego, тоже из Docker; конфиг .regal/config.yaml находится, потому что смонтирован весь репозиторий.
REGAL = docker run --rm -v $(CURDIR):/work:ro -w /work ghcr.io/open-policy-agent/regal:0.43.0
REGAL_ARGS ?=
# oasdiff — сравнение схем OpenAPI (раздел 12), версия — как в job openapi.
OASDIFF = docker run --rm -v $(CURDIR):/repo:ro -w /repo tufin/oasdiff:v1.34.1
BACKEND = cd backend &&
# Integration-тесты — в 4 процесса (pytest-xdist): у каждого свои контейнеры testcontainers (раздел 10).
# Больше не нужно: на раннере CI 4 ядра, а каждый процесс поднимает ~5 контейнеров. XDIST= — без xdist.
XDIST ?= -n 4 --dist worksteal

.DEFAULT_GOAL := help
.PHONY: help up infra down clean ps logs logs-workers restart-workers bot bot-logs urls keys migrate migrations-check revision openapi openapi-diff run test test-unit test-integration test-policy coverage lint fmt policy-lint policy-test policy-fmt refresh-reports seed-big seed-big-clean import-csv explain experiments bench-updates chaos

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

bot:  ## запустить Telegram-бота (профиль telegram; нужны TELEGRAM_BOT_TOKEN и _USERNAME в deploy/compose/.env)
	$(COMPOSE) --profile telegram up -d --build telegram-bot

bot-logs:  ## логи Telegram-бота в реальном времени
	$(COMPOSE) --profile telegram logs -f telegram-bot

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

ROWS ?= 500000
import-csv:  ## CSV для импорта контактов в backend/tmp/: 500 тыс. строк, 5% дублей, 1% битых (ROWS=1000)
	$(BACKEND) uv run python scripts/gen_import_csv.py --rows $(ROWS)

explain:  ## планы всех списков и отчётов на seed-данных: нет ли Seq Scan по большим таблицам
	$(BACKEND) uv run python scripts/explain_lists.py

experiments:  ## замеры индексов «без / с» на seed-данных (результаты — docs/experiments.md)
	$(COMPOSE) exec -T postgres psql -U saleshub -d saleshub < backend/scripts/experiments.sql

BENCH ?=
bench-updates:  ## замеры способов UPDATE на копии contacts, ~2 ч (BENCH="--methods U2,U3 --volumes 1m")
	$(BACKEND) uv run python scripts/bench_updates.py $(BENCH)

CHAOS ?=
chaos:  ## chaos-тест bulk на стенде make up: kill воркера посреди job, ~22 мин (CHAOS="--scenario a")
	$(BACKEND) uv run python scripts/chaos.py $(CHAOS)

PG_CHECK = saleshub-migrations-check
migrations-check:  ## проверки миграций, как в CI: одноразовый Postgres, одна голова, upgrade, check, downgrade, upgrade
	@docker rm -f $(PG_CHECK) > /dev/null 2>&1 || true
	@docker run -d --name $(PG_CHECK) -e POSTGRES_USER=saleshub -e POSTGRES_PASSWORD=saleshub \
		-e POSTGRES_DB=saleshub -p 127.0.0.1:55432:5432 postgres:18.6 > /dev/null
	@# Ждём не дольше 30 с; без --rm: упавший Postgres успеет показать логи, удаляет контейнер trap.
	@trap 'docker rm -f $(PG_CHECK) > /dev/null' EXIT; \
	for i in $$(seq 30); do \
		docker exec $(PG_CHECK) pg_isready -q -U saleshub -d saleshub -h 127.0.0.1 2> /dev/null && break; \
		if [ $$i = 30 ]; then echo "Postgres не поднялся за 30 с:"; docker logs --tail 20 $(PG_CHECK); exit 1; fi; \
		sleep 1; \
	done; \
	cd backend && DATABASE_URL=postgresql+asyncpg://saleshub:saleshub@127.0.0.1:55432/saleshub \
		bash scripts/check_migrations.sh

refresh-reports:  ## обновить отчёты (materialized view deal_stats) в compose
	$(COMPOSE) exec api python -m app.cli refresh-reports

revision:  ## новая миграция по моделям: make revision m="add users"
	$(BACKEND) uv run alembic revision --autogenerate -m "$(m)"

openapi:  ## записать схему API в backend/openapi.json (файл в git; CI проверяет, что он свежий)
	$(BACKEND) uv run python -m app.cli export-openapi

OPENAPI_BASE ?= origin/main
openapi-diff: openapi  ## изменения API относительно main (OPENAPI_BASE=<ветка>) и ломающие среди них, как в CI
	@mkdir -p backend/tmp
	git show $(OPENAPI_BASE):backend/openapi.json > backend/tmp/openapi.base.json
	$(OASDIFF) changelog backend/tmp/openapi.base.json backend/openapi.json
	$(OASDIFF) breaking backend/tmp/openapi.base.json backend/openapi.json

run: keys  ## API без Docker с автоперезапуском (сначала make infra; с make up конфликт по порту 8000)
	$(BACKEND) uv run uvicorn app.main:create_app --factory --reload --no-access-log

test:  ## все тесты (интеграционным нужен Docker)
	$(BACKEND) uv run pytest $(XDIST)

test-unit:  ## только unit-тесты, без Docker
	$(BACKEND) uv run pytest tests/unit

test-integration:  ## тесты с настоящими Postgres и Redis (testcontainers)
	$(BACKEND) uv run pytest tests/integration $(XDIST)

coverage:  ## все тесты с покрытием и порогами, как job integration в CI: services/ и authz/ — 80%, всё — 90%
	$(BACKEND) uv run pytest $(XDIST) --cov --cov-report=
	$(BACKEND) uv run coverage report --skip-covered --include='app/services/*' --fail-under=80
	$(BACKEND) uv run coverage report --skip-covered --include='app/authz/*' --fail-under=80
	$(BACKEND) uv run coverage report --format=total --fail-under=90

test-policy: policy-test  ## политики целиком: Regal, opa test и pytest-тесты кода, который обязан совпадать с policies/ (нужен Docker)
	$(BACKEND) uv run pytest -m policy $(XDIST)

lint:  ## ruff + mypy, как в CI
	$(BACKEND) uv run ruff check . && uv run ruff format --check . && uv run mypy app

fmt:  ## отформатировать код и исправить то, что ruff умеет сам
	$(BACKEND) uv run ruff format . && uv run ruff check --fix .

policy-lint:  ## линтер Rego (Regal)
	$(REGAL) lint $(REGAL_ARGS) policies

policy-test: policy-lint  ## тесты политик OPA: Regal, формат, проверка, opa test с покрытием не ниже 90%
	$(OPA) fmt --list --fail /policies
	$(OPA) check --strict /policies
	$(OPA) test -b /policies -v
	$(OPA) test -b /policies --coverage --threshold 90 --format=json > /dev/null

policy-fmt:  ## отформатировать .rego
	docker run --rm -v $(CURDIR)/policies:/policies openpolicyagent/opa:1.21.3 fmt -w /policies
