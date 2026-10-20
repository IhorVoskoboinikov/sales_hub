# SalesHub CRM

Учебный проект: бэкенд CRM для отдела продаж на FastAPI. На нём отрабатываются вход по JWT,
права на OPA (роли, уровни own / team / org), большие объёмы данных и фоновые задачи на Celery,
Alembic, тесты, логи и метрики, деплой в Kubernetes через Helm и Tilt.

Весь дизайн, план по этапам и принятые решения — в [docs/DESIGN.md](docs/DESIGN.md).

## Подготовка

Нужны: Docker, uv, Python 3.13 (полный список — этап 0 в `docs/DESIGN.md`).

```bash
uv tool install pre-commit
pre-commit install
pre-commit run --all-files
```

## Лицензия

MIT, см. [LICENSE](LICENSE).
