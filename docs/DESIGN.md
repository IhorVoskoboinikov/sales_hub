# SalesHub CRM — учебный проект: дизайн

Oct 19, 2026 · @ihor

## 1. Цель, домен и стек

SalesHub — мульти-тенантная CRM для отделов продаж: компании-клиенты, контакты, воронки и сделки, активности (звонки, встречи, задачи). На ней отрабатываются права доступа, bulk-обработка миллионов строк, очереди, деплой в Kubernetes и CI/CD. Домен взят из реальной жизни: в каждой CRM есть роли «администратор / руководитель отдела / менеджер / наблюдатель» и три ветки видимости — свои клиенты, клиенты отдела, вся компания.

**Реальные сценарии, на которых всё проверяется:**

- менеджер видит и редактирует только своих клиентов и сделки, руководитель — весь отдел, администратор — всю компанию;
- импорт 500 тыс. контактов из CSV с дедупликацией по email;
- менеджер увольняется — все его контакты, сделки и открытые задачи массово передаются другому;
- массовое закрытие просроченных сделок как «проиграна», массовые теги для сегментов;
- экспорт сегмента в CSV, отчёты по воронке и менеджерам.

**Что должно получиться в итоге:** можно принять приглашение из письма, залогиниться в Swagger, получить пару access/refresh-токенов и на каждом эндпоинте увидеть, что разные роли видят и меняют разное; можно запустить массовое обновление 1 млн контактов и смотреть прогресс; всё поднимается одной командой `tilt up` в локальном кластере.

### Стек

| Слой | Выбор | Почему |
| --- | --- | --- |
| API | FastAPI + Pydantic v2 | async, Depends, авто-Swagger |
| ORM / БД | SQLAlchemy 2.0 async + asyncpg, PostgreSQL 18 | партиции, частичные/покрывающие индексы, COPY |
| Миграции | Alembic (async env.py) |  |
| Auth | PyJWT, pwdlib/argon2 | вход по почте, access 1 ч, refresh 30 дней; письма через Jinja2, в dev — Mailpit |
| Очереди | Celery 5.6 + Redis (broker; result backend не нужен), Celery beat, Flower | этап 5, детали — раздел 15 |
| События (этап 9) | Transactional outbox → Redpanda (Kafka API), aiokafka или FastStream | сравнение с Celery |
| Логи | structlog (JSON), Loki |  |
| Метрики / алерты | Prometheus, Grafana, Alertmanager → Telegram |  |
| Тесты | pytest, pytest-asyncio, httpx, testcontainers, factory-boy, polyfactory |  |
| Качество | ruff, mypy, pre-commit |  |
| Пакеты | uv | быстрый lock и сборка образов |
| Деплой | Docker, docker compose → k3d + Helm + Tilt, Sealed Secrets |  |
| CI/CD | GitHub Actions, GHCR, Argo CD (локальный CD) |  |
| Фронт (в конце) | Vite + React + TanStack Query/Table |  |
| Авторизация | OPA (Open Policy Agent), правила на Rego, сайдкар | policy as code: правила в git, тесты opa test |
| Уведомления | aiogram 3 (Telegram-бот), письма через SMTP; отправка из воркеров через outbox | раздел 16 |
| Файлы | S3 API через aiobotocore; локально RustFS | импорт CSV (этап 5) |
| Поиск | Elasticsearch + Kibana | раздел 17 |

**Версии:** Python 3.13 и PostgreSQL 18. Python 3.13 — проверенная ветка: у всех библиотек (asyncpg, pydantic-core, argon2) есть готовые wheels, поддержка до октября 2029. PostgreSQL 18 — текущая стабильная ветка. Переход на Python 3.14/3.15 и PostgreSQL 19 — отдельное упражнение на этапе 10. Точные версии пакетов фиксируются в `uv.lock`.

**Версии этапа 5 и временные ограничения.** Стабильный Celery 5.6.3 тянет kombu 5.6, а он требует redis-py < 6.5; aiogram 3.31 требует pydantic < 2.14. Поэтому на этапе 5 стоят `celery[redis]==5.6.3` + `redis>=6.4,<6.5` и `aiogram==3.31.0` + `pydantic[email]>=2.13.5,<2.14`; `aiobotocore` тянет aiohttp 3.14 (потолок aiohttp < 3.15 от aiogram действует и на него). Код использует из redis-py и pydantic только то, что есть в этих версиях. У `testcontainers` экстра `redis` требует redis-py ≥ 7, поэтому в dev стоит `testcontainers[postgres]`: модулю Redis-контейнера нужен только импортируемый `redis`, а его даёт основная зависимость. Когда выйдут Celery 5.7 (kombu 5.7 разрешает redis < 9) и aiogram с поддержкой pydantic 2.14, ограничения снимаются одним коммитом `config(deps)` с полным прогоном тестов. Типы для mypy — `celery-types` и `types-aiobotocore-s3` (dev).

## 2. Архитектура слоёв

Один запрос = одна `AsyncSession` = один Unit of Work; коммит делает только сервис через UoW, репозитории никогда не коммитят.

&#91;embedded content: архитектура SalesHub · API, OPA, очереди, события, мониторинг\]

Права проверяются в два шага. OPA вызывается один раз на запрос — в `authorize()` на роутере: можно ли этой роли вызвать эндпоинт. Дальше сервис и репозиторий работают с готовым `Access`: проверяют объект, а для списков добавляют SQL-фильтр. Уровень (`own` / `team` / `org`) `Access` берёт из того же файла ролей `data.yaml`, что и OPA (раздел 5). Всё, что дольше секунды, уходит через Redis в воркеры, которые вызывают те же сервисы и UoW.

### Ответственность слоёв

| Слой | Знает о | Не знает о | Что делает |
| --- | --- | --- | --- |
| `api` (роутер) | HTTP, схемы Pydantic, Depends | SQL, сессии | валидация, `authorize() → OPA`, вызов сервиса, маппинг в response-схему |
| `deps` | всё, что собирает граф зависимостей | бизнес-правила | `get_session`, `get_uow`, `current_user`, `get_deal_service` |
| `service` | доменные правила, `Access`, UoW | HTTP, FastAPI | сценарий: проверить объектный доступ, изменить, записать события, `uow.commit()` |
| `uow` | сессию и набор репозиториев | HTTP | транзакция, `commit/rollback`, сбор доменных событий в outbox |
| `repository` | SQLAlchemy-модели, запросы | HTTP, OPA, роли (права получает готовыми в виде `Access`) | CRUD, фильтры, keyset-пагинация, bulk SQL; чтение принимает `Access` и само добавляет его фильтр |
| `db session` | engine, пул | — | `async_sessionmaker`, одна сессия на запрос |

### Как это выглядит в коде (эскиз)

```python
# deps.py
async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:   # одна сессия на запрос
        yield session                          # закроется после ответа

def get_uow(session: AsyncSession = Depends(get_session)) -> UnitOfWork:
    return UnitOfWork(session)

def get_deal_service(uow: UnitOfWork = Depends(get_uow)) -> DealService:
    return DealService(uow)

# uow.py
class UnitOfWork:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.contacts = ContactRepository(session)
        self.deals = DealRepository(session)
        self.outbox = OutboxRepository(session)
        self._events: list[DomainEvent] = []

    def add_event(self, e: DomainEvent) -> None:
        self._events.append(e)

    async def commit(self) -> None:
        await self.outbox.add_many(self._events)   # события в той же транзакции
        await self.session.commit()
        self._events.clear()

    async def rollback(self) -> None:
        await self.session.rollback()

# api/deals.py
@protected.patch("/deals/{deal_id}", response_model=DealOut)
async def update_deal(
    deal_id: int,
    body: DealUpdate,
    access: Access = Depends(authorize),        # спросили OPA: deal:write; уровень — из data.yaml
    svc: DealService = Depends(get_deal_service),
):
    return await svc.update(deal_id, body, access)

# services/deals.py
class DealService:
    async def update(self, deal_id: int, body: DealUpdate, access: Access) -> Deal:
        deal = await self.uow.deals.get_visible_or_404(deal_id, access)  # deleted_at IS NULL + фильтр Access; чужая → 404
        old_stage = deal.stage_id
        deal.apply(body)                      # версия +1; при смене owner_id team_id = отдел нового ответственного
        access.ensure_allowed(deal)           # после изменения сделка тоже в моём уровне, иначе 403
        if deal.stage_id != old_stage:
            self.uow.add_event(DealStageChanged(deal.id, old_stage, deal.stage_id))
        await self.uow.commit()
        return deal
```

**Celery-воркеры** используют те же сервисы, UoW и репозитории, но получают сессию не из FastAPI Depends, а из своих `Resources` (engine и фабрика сессий создаются лениво, один раз на процесс воркера — раздел 15). Так бизнес-логика пишется один раз.

### Структура репозитория

```text
sales_hub/
├── backend/
│   ├── app/
│   │   ├── main.py              # create_app(): роутеры, middleware, handlers
│   │   ├── core/                # config (pydantic-settings), logging, security (jwt, hash),
│   │   │                        # resources.py — ресурсы процесса для API, воркеров и бота
│   │   ├── db/                  # engine, session factory, base, naming convention
│   │   ├── models/              # SQLAlchemy-модели
│   │   ├── schemas/             # Pydantic in/out
│   │   ├── repositories/
│   │   ├── services/
│   │   ├── uow.py
│   │   ├── authz/               # OPA-клиент, authorize(), чтение data.yaml, Access (scope → SQL)
│   │   ├── api/
│   │   │   ├── deps.py          # get_session, get_engine, get_redis, current_user, сборка сервисов
│   │   │   ├── well_known.py    # /.well-known/jwks.json — публичные ключи JWT, без авторизации
│   │   │   ├── health.py        # /health/live, /health/ready — вне /api/v1, без авторизации
│   │   │   └── v1/              # auth, users, orgs, teams, companies, contacts, deals, activities, bulk, reports,
│   │   │                        # me (личные настройки: только current_user, без OPA)
│   │   ├── errors/              # AppError иерархия + exception handlers
│   │   ├── emails/              # EmailSender (SMTP) и Jinja2-рендер; с этапа 5 письма отправляет только воркер
│   │   ├── middleware/          # request_id, access log, unhandled errors (500), позже metrics
│   │   ├── events/              # типы событий, маршруты «событие → задача», outbox relay
│   │   ├── notifications/       # каталог событий и каналов, ключи дедупа, клиент Telegram Bot API
│   │   ├── storage/             # FileStorage: S3 API (aiobotocore)
│   │   ├── workers/             # celery_app.py, runtime.py, base.py, context.py, queue.py, schedule.py, tasks/ (раздел 15)
│   │   ├── bot/                 # Telegram-бот на aiogram (раздел 16)
│   │   ├── search/              # SearchRepository, индексатор (раздел 17)
│   │   ├── texts/               # украинские тексты: ошибки, письма, бот
│   │   └── cli.py               # create-superadmin, ensure-partitions, ensure-bucket, refresh-reports
│   ├── migrations/              # alembic
│   ├── tests/{unit,integration,e2e}/
│   ├── scripts/seed_big.sql     # генерация миллионов строк
│   ├── Dockerfile
│   ├── .env.example             # переменные для запуска API без Docker (make run)
│   ├── secrets/                 # dev-ключ JWT (make keys), в .gitignore
│   └── pyproject.toml
├── policies/saleshub/            # OPA: routes.rego, authz.rego, data.yaml (роли), *_test.rego
├── frontend/                    # в самом конце
├── deploy/
│   ├── compose/docker-compose.yml
│   ├── helm/saleshub/            # chart: api, worker, beat, flower, jobs
│   ├── envs/{local,staging,prod}/   # values, sealed-secrets, приложения Argo CD
│   ├── infra/                   # Pulumi: облачная инфраструктура (при переходе)
│   ├── monitoring/              # prometheus rules, grafana dashboards, alertmanager
│   └── k3d-config.yaml
├── Tiltfile
├── Makefile                     # make help — все команды: up, test, lint, migrate…
├── .github/workflows/{ci.yml,cd.yml}
└── docs/DESIGN.md               # этот документ — единственная копия
```

## 3. Модель данных

Тяжёлые таблицы — `contacts` (5 млн), `deals` (1 млн) и `activities` (20 млн, партиции по месяцам). На них отрабатываются bulk-операции, индексы и партиционирование. Остальные таблицы маленькие и нужны для прав и справочников. Каждая бизнес-строка несёт `org_id` (тенант), `team_id` (отдел) и `owner_id` (ответственный) — по ним бэкенд (`Access`, раздел 5) решает, кто что видит. `team_id` записи — всегда отдел её ответственного: его ставит сервис, а не клиент.

### Пользователи, тенанты, права

| Таблица | Ключевые поля | Заметки |
| --- | --- | --- |
| `users` | id (uuid), email (unique, citext), password\_hash, is\_active, is\_superadmin, **org\_id, team\_id, role**, email\_verified\_at, token\_version, timezone (null = как у организации), last\_login\_at, created\_at | один человек — одна организация и одна роль (раздел 5). `role` ∈ `org_admin` / `team_lead` / `sales_rep` / `viewer`. CHECK-и (в Postgres CHECK, который дал NULL, считается пройденным, поэтому условия написаны без сравнений с NULL): `role IN ('org_admin','team_lead','sales_rep','viewer')`; `(role IS NULL) = (org_id IS NULL)`; `NOT (is_superadmin AND org_id IS NOT NULL)`; `(team_id IS NOT NULL) = (role IS NOT NULL AND role IN ('team_lead','sales_rep'))`. FK `(team_id, org_id)` → `teams (id, org_id)` — отдел только из своей организации. Пользователей без организации и роли (не супер-админов), оставшихся с этапа 2, удаляет миграция этапа 3 (это только dev-данные; `downgrade` их не возвращает). Если такой всё же появится — он ничего не может: OPA отвечает 403 |
| `refresh_tokens` | id, user\_id, family\_id (= `sid`), token\_hash (unique), created\_at, expires\_at, revoked\_at, replaced\_by, user\_agent, ip | раздел 4 |
| `email_tokens` | id, user\_id, email, purpose (`invite` / `reset_password` / `telegram_link` — с этапа 5), token\_hash, payload jsonb (org\_id, team\_id, role для приглашения), created\_at, expires\_at, used\_at | одноразовые токены трёх целей (разделы 4 и 16) |
| `organizations` | id, name, slug (unique), plan, display\_currency (по умолчанию UAH), timezone (по умолчанию Europe/Kyiv), created\_at | тенант: компания, которая пользуется CRM; создаёт только супер-админ |
| `teams` | id, org\_id, name, created\_at | отдел продаж; UNIQUE (id, org\_id) для составного FK из `users`; UNIQUE (org\_id, name) — два одинаковых отдела в организации путают (409 `team.name_taken`); отдел с людьми удалить нельзя — 409 `team.has_members` |

### CRM-сущности

| Таблица | Ключевые поля | Объём / заметки |
| --- | --- | --- |
| `companies` | id, org\_id, team\_id, owner\_id, name, domain, industry, size, created\_at, updated\_at, deleted\_at, version | 300 тыс.; компании-клиенты — общий справочник: видны всей организации (`company:read` — org у всех ролей), меняются по уровню `company:write` |
| `contacts` | id, org\_id, team\_id, owner\_id, company\_id, first\_name, last\_name, email (citext), phone, status (`lead` / `customer` / `churned`), source, tags varchar(64)\[\] (до 20; в запросах — `ARRAY[:t]::varchar(64)[]`, раздел 6), created\_at, updated\_at, deleted\_at, version | **5 млн**; уникальность `(org_id, email)` для дедупликации при импорте |
| `pipelines`, `pipeline_stages` | pipeline: id, org\_id, name (UNIQUE в организации), created\_at; stage: id, pipeline\_id, name, position, probability (0–100); UNIQUE (id, pipeline\_id) — для составного FK из `deals` | воронки и этапы, настраивает org admin; новая организация создаётся без воронок |
| `deals` | id, org\_id, team\_id, owner\_id, contact\_id, company\_id, pipeline\_id, stage\_id, title, amount\_usd numeric(14,2), amount\_original numeric(14,2), currency\_original char(3), fx\_rate numeric(14,6), fx\_date, status (`open` / `won` / `lost`), expected\_close\_date, closed\_at, created\_at, updated\_at, deleted\_at, version | **1 млн**; FK `(stage_id, pipeline_id)` → `pipeline_stages (id, pipeline_id)`: этап всегда из воронки сделки, `pipeline_id` берётся из этапа. `won` / `lost` ставят `closed_at`, возврат в `open` его снимает |
| `activities` | id, org\_id, team\_id, owner\_id, deal\_id, contact\_id, type (`call` / `meeting` / `email` / `task`), subject, due\_at, done\_at, created\_at | **20 млн**, `PARTITION BY RANGE (created_at)` по месяцам, PK `(id, created_at)` (ключ партиционирования обязан входить в PK). Запланированная активность — с `due_at`, `done_at` пуст до отметки «выполнено»; без `due_at` — это запись о том, что уже было (звонок, письмо), `done_at = created_at` сразу. Поэтому «открытые» — ровно `done_at IS NULL` |
| `notes` | id, org\_id, entity\_type (`contact` / `company` / `deal`), entity\_id, author\_id, body, created\_at | заметки к контакту, компании, сделке; на этапе 4 эндпоинты только у контакта |

### Служебные

| Таблица | Назначение |
| --- | --- |
| `bulk_jobs` | id uuid; org\_id (NOT NULL, FK); team\_id (отдел автора на момент запуска, без FK); owner\_id — **автор** job (FK users; имя `owner_id`, чтобы `Access` работал для `bulk_job:read` / `bulk_job:cancel` без изменений); kind CHECK IN (`reassign_owner`, `move_team`, `import_contacts`, `close_overdue_deals`, `add_tags`, `remove_tags`) — `export` и `reindex` добавят расширяющие миграции этапов 10 и 9; status CHECK IN (`pending`, `running`, `cancelling`, `done`, `partial_failed`, `failed`, `cancelled`); stage (последний завершённый шаг импорта); params jsonb (параметры + `Access` автора); idempotency\_key varchar(64), request\_hash char(64); счётчики total, processed, failed\_rows, chunks\_total, failed\_chunks; result jsonb (итоги: `{created, updated}` импорта, строки по сущностям); error varchar(64) — код причины провала; reaps smallint; created\_at, started\_at, updated\_at (время последнего прогресса — по нему работает `bulk.reap_stuck`), finished\_at. Колонки `version` нет: переходы статуса — условными `UPDATE … WHERE status IN (…)`. Индексы: `UNIQUE (owner_id, idempotency_key)`; `UNIQUE (org_id, kind) WHERE status IN ('pending','running','cancelling') AND kind <> 'move_team'` — одна тяжёлая job вида на организацию (раздел 15) |
| `bulk_job_chunks` | job\_id (FK, ON DELETE CASCADE), no; PK (job\_id, no); entity CHECK IN (`companies`, `contacts`, `deals`, `activities`); lo, hi bigint — границы id; status CHECK IN (`pending`, `done`, `failed`, `skipped`); rows; error; finished\_at — учёт каждого чанка: повтор идемпотентен, прогресс не считается дважды, завершение job видно по базе (раздел 6) |
| `contacts_staging` | job\_id (FK, CASCADE), row\_no; PK (job\_id, row\_no); проверенные и нормализованные поля строки CSV (email — `text`, сравнение по `lower()`; first\_name, last\_name, phone, status, source, tags, owner\_email), owner\_id, error, merged — общая обычная (logged) таблица шагов импорта: любой воркер видит данные любого шага; строки job удаляются при финальном статусе |
| `import_errors` | job\_id (FK, CASCADE), row\_no; PK (job\_id, row\_no) — у строки одна, первая ошибка; code, field, raw jsonb (ячейки строки файла; у `duplicate_outside_scope` — без данных существующего контакта) |
| `outbox_events` | id bigint identity; event\_type; org\_id (без FK, NULL — у супер-админа); payload jsonb; request\_id; created\_at; published\_at. Частичный индекс `(id) WHERE published_at IS NULL`, триггер `AFTER INSERT … FOR EACH STATEMENT` → `pg_notify('outbox_new')`. Команды на отправку (`email.send`, `telegram.send`) и доменные события (раздел 6) |
| `audit_log` | кто что изменил: id, occurred\_at, org\_id, actor\_id (оба без FK — журнал переживает удаление), action (`member.updated`, `team.created`, `invite.created`…), entity\_type, entity\_id, changes jsonb (`{"role": ["viewer", "org_admin"]}` — было и стало), request\_id. Запись — в той же транзакции, что и изменение. Этап 3 — обычная таблица; с этапа 4 — `PARTITION BY RANGE (occurred_at)` по месяцам, PK (id, occurred\_at), индексы (org\_id, occurred\_at, id) и (org\_id, entity\_type, entity\_id, occurred\_at, id), `GET /audit`. В журнал пишутся и CRM-изменения: создание, изменение (diff), удаление и восстановление компаний, контактов, сделок, воронок; активности и заметки — нет, их слишком много. Массовая операция пишет **одну** строку на job (`action = bulk.<kind>`, `entity_type = bulk_job`, в `changes` — параметры и число строк по сущностям): история карточки массовых изменений не показывает, построчный аудит через `RETURNING OLD` (PostgreSQL 18) — эксперимент этапа 10 |
| fx\_rates | rate\_date, currency, rate\_uah numeric(14,6) (гривен за 1 единицу валюты), source = NBU, fetched\_at; PK (rate\_date, currency) — курсы НБУ |
| failed\_tasks | задачи Celery, упавшие после всех ретраев (раздел 15): id; task\_id (UNIQUE — повторная доставка не даёт дубля); task\_name, queue, args, kwargs jsonb; request\_id, user\_id (без FK); exc\_type, exc\_message (до 2 000 символов), traceback (до 20 000); retries; failed\_at; replayed\_at, replayed\_by, replay\_task\_id. Индекс (failed\_at, id) |
| sent\_notifications | dedup\_key (PK), channel CHECK IN (`email`, `telegram`), user\_id (без FK: у приглашения пользователя ещё нет), sent\_at; индекс (sent\_at) для очистки — защита от повторной отправки писем и сообщений |
| telegram\_links | user\_id (PK, FK users ON DELETE CASCADE), chat\_id bigint UNIQUE, username, linked\_at — один чат ↔ один пользователь (раздел 16) |
| notification\_settings | user\_id (FK, CASCADE), event\_type, channel; PK (user\_id, event\_type, channel); enabled — только отличия от умолчаний из кода (раздел 16) |

Таблиц `roles`, `permissions`, `role_permissions` и членств нет: права ролей живут в файле `policies/saleshub/data.yaml` (его читают OPA и бэкенд), в базе только `users.role` и `users.team_id`. У всех CRM-таблиц (компании, контакты, воронки, этапы, сделки, активности, заметки) id — `bigint` identity: для чанков по диапазонам id, keyset-пагинации и `generate_series` в генераторе данных это проще и быстрее; организации, отделы и пользователи остаются на uuid.

### Внешние ключи на больших таблицах

Каждый FK — это проверка при вставке (поиск по PK родителя) и поиск дочерних строк при удалении родителя (нужен **полный**, не частичный индекс по колонке, иначе — проход по миллионам строк). Поэтому FK ставятся осознанно:

| Колонка | FK | Почему |
| --- | --- | --- |
| `org_id`, `owner_id` у компаний, контактов, сделок | → `organizations`, `users` | родители не удаляются (пользователя отключают, а не удаляют) — нужна только дешёвая проверка при вставке |
| `team_id` у всех CRM-таблиц | **нет** | это копия отдела ответственного, её ставит сервис; FK потребовал бы полного индекса по `team_id` на каждой таблице. Удалить отдел с живыми записями не даёт сервис: 409 `team.has_records` |
| `contacts.company_id`, `deals.contact_id`, `deals.company_id` | `ON DELETE SET NULL` | окончательное удаление компании или контакта из корзины не трогает связанные записи; полные индексы — они же для фильтров `?company_id=`, `?contact_id=` |
| `deals (stage_id, pipeline_id)` | → `pipeline_stages (id, pipeline_id)` | этап не может быть из чужой воронки; удалить этап можно, только если на нём нет сделок, **включая корзину** (409 `pipeline.stage_has_deals`), полный индекс `deals (stage_id)` |
| `activities.deal_id` / `contact_id` | `ON DELETE CASCADE` / `SET NULL` | история уходит вместе со сделкой; удаление контакта не стирает историю его сделки |
| `activities.org_id`, `team_id`, `owner_id`; `notes.entity_id` | **нет** | 20 млн строк; у заметки родитель полиморфный (`entity_type`), заметки удаляет `trash.purge` вместе с родителем |

Связь «запись — та же организация» (`company_id` из своей организации) проверяет сервис: ссылка загружается через `Access` (раздел 5).

### Удаление: в корзину, а не насовсем

В реальных CRM удалённого клиента можно восстановить, поэтому `companies`, `contacts`, `deals` удаляются **мягко**: строка остаётся, ей ставится `deleted_at = now()`.

- Все запросы по умолчанию добавляют `deleted_at IS NULL` (общий фильтр в репозитории, а не в каждом запросе руками).
- Эндпоинты корзины: `DELETE` и `POST …/restore` у компаний, контактов и сделок; список удалённых — только `GET /contacts/trash` (уровень `contact:delete`, новые сначала).
- Восстановление возвращает запись в **текущий** отдел её ответственного; если ответственный отключён или больше не может вести записи — 409 `owner.inactive`. У контакта ещё проверяется, что живого контакта с тем же email нет (409 `contact.duplicate_email`).
- Через 30 дней задача beat `trash.purge` удаляет строки физически, чанками по 1 000 в своей транзакции: сделки → контакты → компании (от детей к родителям — меньше работы `SET NULL`), по каждой организации, `FOR UPDATE SKIP LOCKED` (строку, которую сейчас восстанавливают, задача пропускает). Вместе с записью удаляются её заметки; у контакта ещё и его активности без сделки (`deal_id IS NULL`), а активности с живой сделкой остаются (их `contact_id` обнуляет FK — история сделки не теряется). Ссылки `company_id` / `contact_id` обнуляет FK без `version + 1`. В журнал purge не пишет: мягкое удаление там уже есть. Запуск не дольше 200 секунд, остаток удалит следующий.
- Индексы делаются частичными: `... WHERE deleted_at IS NULL` — удалённые строки не раздувают индекс.
- Уникальность email тоже частичная: `UNIQUE (org_id, email) WHERE deleted_at IS NULL` — иначе нельзя завести контакт с email удалённого.
- `activities`, `notes`, `audit_log` удаляются жёстко (или старыми партициями целиком).

### Деньги: храним в USD, вводим и показываем в любой валюте

Все суммы хранятся в долларах, а пользователь вводит и видит их в гривне или другой валюте. Конвертер пересчитывает по официальному курсу НБУ на дату операции. Рядом с суммой в USD сохраняется то, что ввёл человек, и курс: карточка показывает сохранённую исходную сумму, а не результат обратного пересчёта (100 000,19 грн вместо 100 000 — из-за округления до центов).

**Поля в `deals`:**

| Поле | Пример | Зачем |
| --- | --- | --- |
| `amount_usd` | 2 410.32 | основа всех отчётов и сумм, одна валюта на всю базу |
| `amount_original` | 100 000.00 | ровно то, что ввёл пользователь |
| `currency_original` | `UAH` | валюта ввода |
| `fx_rate` | 41.4880 | сколько единиц исходной валюты стоит 1 USD по курсам НБУ на `fx_date` (для гривны — гривен за 1 USD, для евро — около 0.89) |
| `fx_date` | 2026-10-19 | на какую дату взят курс |

**Как считается.** НБУ публикует курс гривни к каждой валюте (`rate_uah` — гривен за 1 единицу). Кросс-курс к доллару: `fx_rate = rate_uah(USD) / rate_uah(валюта)`, 6 знаков (для гривны `rate_uah = 1`, то есть `fx_rate` — просто курс доллара). Дальше `amount_usd = amount / fx_rate`, до центов, банковское округление; считаем в `Decimal`. Обе величины сохраняются, поэтому пересчёт воспроизводим из строки сделки без таблицы курсов.

**Запись (создание и изменение сделки):** клиент присылает `{amount: 100000, currency: "UAH"}` → `FxService.to_usd(amount, "UAH", on_date)` (дата — сегодня по часовому поясу организации) → в базу пишутся все пять полей. Если сумма и валюта не менялись, курс не пересчитывается. Валюты, которой нет у НБУ, — 422 `fx.unknown_currency`.

**Чтение — «переводим обратно, если нужно»:**

- Карточка сделки показывает исходную сумму (`amount_original` + `currency_original`) — без пересчёта, ровно как ввели.
- Списки, канбан и отчёты принимают `?currency=UAH`: `amount_usd` пересчитывается по курсу на сегодня. В ответе всегда есть `fx: {currency, rate, date, stale}`, чтобы было видно, по какому курсу посчитано.
- У организации есть `display_currency` (по умолчанию `UAH`) — валюта ответа, если параметр не передан.
- Агрегаты (сумма воронки, выручка менеджера) всегда считаются в USD в SQL и пересчитываются один раз в конце, а не по каждой строке.

**Курсы НБУ.** Источник — открытый API НБУ: `GET https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=USD&date=YYYYMMDD&json`. Важные правила НБУ:

- курс устанавливается в конце рабочего дня и начинает действовать со следующего рабочего дня; публикуется на сайте до 18:00;
- курс, установленный в пятницу или перед праздником, действует все выходные и праздники.

Отсюда логика:

1. Задача beat `fx.fetch_nbu_rates` в будни в 18:30 (повтор в 19:30) загружает курсы на следующий день в `fx_rates`.
2. `FxService.get_rate(currency, date)`: Redis-кэш (1 ч) → строка `fx_rates` на эту дату → запрос к НБУ за все валюты на эту дату (`exchange?date=YYYYMMDD&json`, таймаут 3 с) и upsert. НБУ отдаёт курс и на выходные (тот, что действует в этот день), а на будущую дату — пустой список. Так курс подтягивается по первому запросу дня; с этапа 5 задача beat `fx.fetch_nbu_rates` загружает его заранее, а запрос остаётся резервом.
3. Если НБУ недоступен или пуст — последняя строка с `rate_date ≤ date`, в ответе `fx.stale: true` (такой курс не кэшируется, в лог — warning, алерт `FxRatesStale`, раздел 8); строк нет совсем — 503 `fx.unavailable`.
4. При импорте сделок из CSV конвертация идёт в SQL одним `JOIN` на `fx_rates`, а не построчно в Python.

**Тесты:** НБУ мокается (respx); проверяем выходные, праздник, недоступность НБУ (резерв и `stale`), что карточка отдаёт исходную сумму без изменений, а обратный пересчёт `amount_usd × fx_rate` отличается от неё не больше чем на `0.005 × fx_rate` (поэтому исходную сумму и храним).

Источники: [инструкция НБУ к API курсов](https://bank.gov.ua/admin_uploads/article/Instr_API_KURS_VAL_Full_eng.pdf), [порядок установления официального курса](https://news.dtkt.ua/finance/bank-system/33254-nacbank-zminiv-poriadok-vstanovlennia-oficiinogo-kursu-grivni).

### Индексы: базовые и экспериментальные

Каждый список — keyset-пагинация (`?limit=` до 100 и непрозрачный `?cursor=`, ответ `{items, next_cursor}`; OFFSET не поддерживается — он только в замере), а у каждого уровня `Access` свой индекс, который начинается с колонки уровня. Тогда «первые 50 строк уровня в порядке сортировки» — это один проход по индексу без сортировки. Индексы по `(…, created_at, id)` обходятся и в обратную сторону, поэтому `DESC` в них не нужен.

| Запрос | Индекс | Что изучаем |
| --- | --- | --- |
| списки контактов и сделок по уровням | `(org_id \| team_id \| owner_id, created_at, id) WHERE deleted_at IS NULL` — по три на `contacts` и `deals` | частичный составной индекс под каждый уровень; keyset против OFFSET на глубокой странице |
| «мои клиенты» менеджера с 50× клиентов | `contacts (owner_id, created_at, id)` против `(org_id, owner_id)` из первой версии дизайна | почему колонка сортировки должна быть в индексе: иначе — сортировка всех клиентов менеджера ради 50 строк |
| дедупликация при импорте и 409 | `UNIQUE contacts (org_id, email) WHERE deleted_at IS NULL` | `ON CONFLICT` на частичном уникальном индексе |
| поиск по имени / email (`?q=`, от 3 символов) | GIN `gin_trgm_ops` на выражении `lower(first_name \|\| ' ' \|\| last_name \|\| ' ' \|\| coalesce(email::text, ''))` | pg\_trgm, `LIKE '%ivan%'`; запрос пишет **то же** выражение, иначе индекс не применится; частый и редкий `q` дают разные планы |
| сегмент по тегам | GIN `contacts (tags)` | массивы, `@>` |
| компании по имени | `companies (org_id, name, id)` + GIN trgm на `name` | keyset по тексту |
| канбан воронки | `deals (pipeline_id, stage_id, id) INCLUDE (title, amount_usd, owner_id, team_id, org_id) WHERE status = 'open' AND deleted_at IS NULL` | покрывающий индекс, index-only scan (после `VACUUM`: без visibility map index-only scan всё равно читает таблицу); `org_id`, `team_id` в INCLUDE — чтобы фильтр `Access` тоже читался из индекса |
| просроченные сделки | `deals (org_id, expected_close_date, id) WHERE status = 'open' AND deleted_at IS NULL` | частичный индекс для списка `?overdue=true` и bulk «закрыть просроченные» |
| история карточки (активности сделки / контакта) | `activities (deal_id, created_at, id)`, `activities (contact_id, created_at, id)` | индекс на партиционированной таблице; keyset плюс простое `created_at <= :c` — по сравнению строк `(created_at, id) < (…)` Postgres партиции не отсекает |
| открытые задачи («на сегодня», просроченные) | `activities (owner_id \| team_id \| org_id, due_at, id) WHERE done_at IS NULL` | частичный индекс на партиционированной таблице: открытых задач несколько процентов от 20 млн |
| лента за период (эксперимент) | BRIN `activities (created_at)` + partition pruning | BRIN полезен, только когда строки лежат на диске в порядке времени; в API не используется — только замер |
| журнал организации | `audit_log (org_id, occurred_at, id)`, `(org_id, entity_type, entity_id, occurred_at, id)` | партиции по месяцам |
| корзина | `contacts \| deals \| companies (org_id, deleted_at, id) WHERE deleted_at IS NOT NULL` (у сделок и компаний — с этапа 5, `CONCURRENTLY`) | маленький частичный индекс «наоборот»: список корзины и `trash.purge` |

Предикаты частичных индексов (`status = 'open'`, `done_at IS NULL`) в запросах пишутся литералами, а не параметрами: asyncpg готовит запросы, и в общем (generic) плане с `status = $1` Postgres не может доказать, что частичный индекс подходит.

`pg_stat_statements` включён. Индексы новых, ещё пустых таблиц создаются в их миграции обычным `CREATE INDEX`; индекс, который добавляется к **уже заполненной** большой таблице, — только `CONCURRENTLY` (раздел 9). Замеры «без / с» — `make experiments` (`scripts/experiments.sql`): каждый в транзакции `BEGIN; DROP INDEX …; EXPLAIN (ANALYZE, BUFFERS) …; ROLLBACK` — DDL в Postgres транзакционен, база после замера та же. Результаты и выводы — `docs/experiments.md`. Главное из них: индекс уровня должен содержать колонку сортировки (у «лёгкого» менеджера 0,4 мс против 11 мс у `(org_id, owner_id)` и 95 мс без индекса); keyset на 100-тысячной строке — 0,2 мс против 15 мс у OFFSET; триграммы быстры для редкой подстроки (26 мс) и медленны для частой (≈ 1,2 с — pg\_trgm не умеет оценить её частоту), это закрывает Elasticsearch на этапе 9.

### Генерация данных

`scripts/seed_big.sql` через `generate_series` создаёт 20 организаций, по 3–10 отделов, 2 000 пользователей (у всех один пароль, его argon2-хэш вписан в скрипт), по 1–2 воронки на 5 этапов, 300 тыс. компаний, **5 млн контактов**, 1 млн сделок, 20 млн активностей за 24 месяца и курсы НБУ за это время. Распределение неравномерное (у части менеджеров клиентов в 50 раз больше), чтобы планировщик запросов вёл себя как в жизни. Запуск — `make seed-big` (psql в контейнере postgres); повторный запуск отказывается, если seed-данные уже есть.

Как загрузить десятки миллионов строк за минуты, а не за час: `SET session_replication_role = replica` (FK-триггеры не срабатывают — данные корректны по построению, так же делает `pg_restore --disable-triggers`), отдельный `INSERT … SELECT` на таблицу и на месяц активностей (строки внутри месяца по порядку `created_at`), GIN-индексы контактов удаляются перед загрузкой и строятся после, в конце — `setval` последовательностей id, `VACUUM (ANALYZE)` и обновление отчётов. Postgres в compose запускается с `shared_buffers=1GB`, `max_wal_size=8GB`, `maintenance_work_mem=512MB`. Для импорта (этап 5) — отдельный генератор CSV на 500 тыс. строк с 5% дублей и 1% битых строк.

## 4. Аутентификация (кто ты)

Вход по почте только по приглашению: публичной регистрации нет. Пользователь появляется, когда его приглашают: письмо с одноразовой ссылкой → он задаёт пароль и этим же подтверждает, что email его. Дальше — логин по email и паролю, access-токен JWT на 1 час, refresh-токен на 30 дней с ротацией, сброс пароля письмом. Письма в dev ловит Mailpit — их видно в веб-интерфейсе, поэтому весь сценарий проходится через Swagger без настоящего SMTP.

**Почему без регистрации.** Организации создаёт только супер-админ, поэтому человек, зарегистрировавшийся сам, ничего бы не увидел. Приглашение сразу привязывает его к организации и роли и одновременно подтверждает email — одна механика вместо двух.

**Важно различать:** аутентификация отвечает «кто ты» (логин, токены), авторизация — «что тебе можно» (раздел 5, OPA).

### Токены

|  | Access-токен | Refresh-токен |
| --- | --- | --- |
| Формат | JWT RS256, проверяется по `/.well-known/jwks.json` | случайная строка 256 бит (не JWT) |
| Срок | 1 час (`JWT_ACCESS_TTL=3600`) | 30 дней (`REFRESH_TTL_DAYS=30`) |
| Где хранится на сервере | нигде (stateless) | в таблице `refresh_tokens`, только SHA-256 хэш |
| Как отозвать | `token_version` пользователя или дождаться истечения | `revoked_at` в БД — сразу |
| Для чего | каждый запрос к API | только `POST /auth/refresh` |

Refresh сделан непрозрачным и хранится в БД намеренно: его можно отозвать по одному устройству и видеть список сессий. Access-токен остаётся JWT, чтобы API не ходило в БД на каждый запрос.

Содержимое access-токена:

```json
{
  "iss": "https://saleshub.local",
  "aud": "saleshub-api",
  "typ": "access",
  "sub": "3f1c…-user-uuid",
  "sid": "session-uuid",
  "ver": 3,
  "jti": "random",
  "iat": 1760900000,
  "exp": 1760903600
}
```

Права в токен не кладём: роль и отдел могут поменяться, а токен живёт час. Роль, организация и отдел лежат в `users` и грузятся вместе с пользователем (кэш Redis на 60 с); оттуда роль уходит в OPA (раздел 5).

### Ротация refresh и защита от кражи

1. `POST /auth/refresh {refresh_token}` → найти по хэшу, проверить `expires_at`, `revoked_at` и что пользователь `is_active`.
2. Выдать новую пару access + refresh; старый refresh пометить `revoked_at`, `replaced_by = новый`.
3. Все refresh одной сессии связаны `family_id`. Если пришёл **уже отозванный** refresh — его украли и использовали повторно: отзываем всю семью, пользователь выходит на этом устройстве.
4. Срок семьи не продлевается бесконечно: абсолютный предел сессии — 30 дней с логина (настраивается).
5. Всё, что выдаёт или отзывает refresh-токены (login, refresh, logout, logout-all, удаление сессии, смена и сброс пароля), сначала блокирует строку пользователя (`SELECT … FOR UPDATE`), и только потом — токены. Без этого refresh, начатый одновременно с logout-all, успевал вставить новый токен, которого `UPDATE` в logout-all уже не видел (так работает READ COMMITTED), и сессия переживала «выйти везде». Один порядок блокировок (пользователь → токены) исключает взаимные блокировки.

### Эндпоинты

| Метод | Путь | Что делает |
| --- | --- | --- |
| POST | `/api/v1/auth/login` | `OAuth2PasswordRequestForm` (username = email) → `{access_token, refresh_token, token_type, expires_in}`; работает с кнопкой **Authorize** в Swagger |
| POST | `/api/v1/auth/refresh` | ротация пары токенов |
| POST | `/api/v1/auth/logout` | отзыв текущей сессии (семьи refresh) |
| POST | `/api/v1/auth/logout-all` | отзыв всех сессий + `token_version += 1` (живые access тоже умирают) |
| GET | `/api/v1/auth/sessions` | мои активные сессии: устройство, IP, создана, последний refresh |
| DELETE | `/api/v1/auth/sessions/{sid}` | выйти на конкретном устройстве |
| POST | `/api/v1/auth/password/forgot` | `{email}` → письмо со ссылкой; ответ **всегда 202**, чтобы нельзя было проверить, есть ли email в базе |
| POST | `/api/v1/auth/password/reset` | `{token, new_password}` → новый пароль, отзыв всех сессий |
| POST | `/api/v1/auth/password/change` | `{old_password, new_password}` для залогиненного: отзыв остальных сессий и `token_version += 1` (их access-токены умирают сразу); в ответе — новая пара токенов для текущей сессии, иначе вышел бы и он сам |
| POST | `/api/v1/auth/invite/accept` | `{token, password}` — приглашённый задаёт пароль; в базе появляется пользователь с организацией, отделом и ролью из приглашения |
| GET | `/api/v1/auth/me` | профиль, роль, отдел и `permissions` — действия роли с уровнями из `data.yaml` (фронт по ним решает, что показывать). На этапе 2 ролей ещё нет — только профиль: id, email, `is_superadmin`, timezone |
| GET | `/.well-known/jwks.json` | публичный ключ для проверки подписи |
| POST | `/api/v1/admin/invites` | `{email}` — приглашение без организации, только супер-админ (на этапе 2 OPA ещё нет, поэтому проверяет зависимость `require_superadmin`: не супер-админ → 403 `forbidden`); живёт только на этапе 2, на этапе 3 удаляется — его заменяют `POST /api/v1/admin/orgs/{org_id}/invites` (первый админ организации) и `POST /api/v1/invites` (админ приглашает в свою организацию) |

Приглашение создаёт админ организации: `POST /api/v1/invites {email, role, team_id}` — всегда в свою организацию, `org_id` берётся из него самого. OPA проверяет, что вызывающий — `org_admin` (`member:manage`); правило «отдел обязателен для `team_lead` и `sales_rep`» проверяет сервис (раздел 5). Если email уже есть в базе, ответ тот же 202, а человеку уходит письмо «вы уже состоите в организации SalesHub»: админ чужой организации не узнаёт, что email занят (как в `password/forgot`). На этапе 2 такое приглашение давало 409 — с этапа 3 это поведение заменено.

### Одноразовые токены из писем

Одна таблица `email_tokens` для одноразовых токенов: `invite` (живёт 7 дней), `reset_password` (1 час) и с этапа 5 `telegram_link` (10 минут, раздел 16). В письмо уходит случайная строка, в БД — только её SHA-256 хэш; токен одноразовый (`used_at`), новый запрос на ту же цель гасит предыдущие — для приглашений только той же организации (`payload.org_id`): иначе админ одной организации мог бы гасить чужие приглашения, пригласив тот же email. Для приглашения в `payload` лежат организация, отдел и роль.

Письмо отправляет не API, а воркер. Сервис в той же транзакции, что токен и строка журнала, пишет в `outbox_events` **команду** `email.send` (`{to, template, context, user_id, dedup_key}`) — не доменное событие: в контексте лежит сырой токен, и на этапе 9 он не должен попасть в Kafka. После коммита relay публикует задачу `notifications.send_email(event_id)` (раздел 15). Так письмо не теряется, если коммит прошёл, а SMTP или Redis недоступны (ретраи), и не уходит, если коммит откатился. Ответ `password/forgot` по-прежнему не ждёт SMTP. До этапа 5 письмо отправлял `BackgroundTasks` FastAPI после коммита (ошибка SMTP только в лог, письмо терялось); с этапа 5 `Mailer` и `BackgroundMailer` удаляются, а API почту не отправляет вовсе — `EmailSender` создаёт только воркер. В dev — Mailpit (SMTP на 1025, веб на 8025), в prod — любой SMTP или Amazon SES.

**Токен в outbox.** В `email_tokens` лежит только хэш, а сырой токен живёт в `outbox_events.payload` от коммита до отправки (обычно секунды). Relay передаёт в Redis только `event_id`, поэтому в сообщениях брокера токена нет; `send_email` после успешной отправки в той же транзакции стирает его из payload (`payload #- '{context,token}'`), а опубликованные строки удаляются через 7 дней. Если отправка упала, токен остаётся — ретрай и перезапуск из `failed_tasks` работают. Токен лежит в контексте только под ключом `token` (`TOKEN_PATH` в `app/repositories/outbox.py`; шаблоны подставляют `{{ token }}`): готовых ссылок с токеном в контексте нет — иначе `#-` по одному ключу секрет бы не стёр. Контекст — только строки, готовые для шаблона. `user_id` команды: у сброса — пользователь, у приглашения — NULL, у письма «вы уже состоите» — существующий пользователь с этим email.

### Зависимость `current_user`

1. Достать Bearer-токен, проверить подпись по JWKS, `iss`, `aud`, `exp`, `typ=access`.
2. Найти пользователя по `sub` (кэш Redis на 60 с), проверить `is_active`, `email_verified_at` и `ver == token_version`. Изменение, после которого старые токены не должны работать (logout-all, смена и сброс пароля), после коммита ставит вместо записи кэша пустое «надгробие» на 5 секунд, а запись в кэш идёт с `SET NX`: иначе запрос, прочитавший пользователя до коммита, вернул бы в кэш старую `token_version` ещё на 60 секунд.
3. Вернуть `CurrentUser` (id, email, is\_superadmin, org\_id, team\_id, role) — роль и отдел приходят тем же запросом, отдельной загрузки прав нет (раздел 5).

Ошибки: нет токена, битая подпись, истёк → 401 `auth.invalid_token` / `auth.token_expired` (клиент по второму коду понимает, что пора делать refresh); email не подтверждён → 403 `auth.email_not_verified`; заблокирован → 403 `auth.user_inactive`.

### Безопасность

- Пароли — argon2 (pwdlib), минимум 10 символов, проверка на совпадение с email.
- Rate limit в Redis (фиксированное окно: `INCR` + `EXPIRE NX`): логин — 5 неудачных попыток / 15 мин на email+IP, `password/change` — 5 неверных старых паролей / 15 мин на пользователя, `password/forgot` — 3 / час на email (и для несуществующих тоже), приглашения — 50 / час на организацию (на этапе 2 — на супер-админа). Попытка засчитывается до проверки пароля, а успех обнуляет счётчик: проверка «сначала прочитать счётчик, потом увеличить» пропустила бы сотню одновременных запросов. Redis недоступен → 503: без счётчика пароли можно перебирать бесконечно.
- Ответы `login` и `forgot` не раскрывают, существует ли пользователь.
- Ключи RS256: приватный ключ — файл `JWT_PRIVATE_KEY_FILE`. В dev его создаёт `make keys` (`python -m app.cli generate-jwt-key`) в `backend/secrets/` — папка в `.gitignore`, compose монтирует её в `api`; в Kubernetes это Secret, смонтированный файлом; тесты создают ключ фикстурой. `kid` в заголовке JWT — отпечаток публичного ключа (RFC 7638), поэтому его не нужно настраивать. Ротация: новый ключ становится `JWT_PRIVATE_KEY_FILE`, а старый публичный кладётся в `JWT_PREVIOUS_PUBLIC_KEY_FILES` на время жизни access-токена (1 час) — JWKS отдаёт оба, и уже выданные токены продолжают проверяться.
- Для будущего фронта refresh переедет в httpOnly-cookie; пока, для Swagger, он в теле ответа.

### Супер-админ: откуда берётся

- Команда `python -m app.cli create-superadmin` читает `SUPERADMIN_EMAIL` и `SUPERADMIN_PASSWORD` из env/Secret, создаёт пользователя сразу с подтверждённым email; идемпотентна.
- В Kubernetes это Job `seed` после Job `migrate`; в compose — одноразовый сервис.
- Не в миграции: миграции описывают схему и не знают секретов окружения.
- `is_superadmin = true` передаётся в OPA; политика разрешает ему только админские эндпоинты `/api/v1/admin/*` (действия `admin:*`): создать организацию, пригласить её первого администратора, перезапустить упавшие задачи. В данные клиентов он не заходит; все его действия пишутся в `audit_log`.

### Сценарий проверки через Swagger

1. `create-superadmin` → **Authorize** под супер-админом.
2. `POST /api/v1/admin/invites {email}` (с этапа 3 — `POST /api/v1/admin/orgs/{org_id}/invites {email}` для первого админа, а он уже `POST /api/v1/invites {email, role, team_id}`) → открыть Mailpit (`localhost:8025`), скопировать токен из письма.
3. `invite/accept {token, password}` → **Authorize** под новым пользователем → `GET /auth/me`. На этапе 2 защищённых эндпоинтов ещё нет — проверяется только `GET /auth/me`; с этапа 3 пользователь работает в своей роли.
4. `refresh` с refresh-токеном → новый access; повторить старый refresh → 401 и отзыв всей семьи.
5. `password/forgot` → письмо → `password/reset` → старые токены больше не работают.

## 5. Авторизация через OPA (что тебе можно)

Права проверяются в два шага, и у каждого шага один хозяин:

| Шаг | Вопрос | Кто отвечает | Пример | Если нельзя |
| --- | --- | --- | --- | --- |
| 1 | Можно ли этой роли вызвать этот эндпоинт? | **OPA** по правилам на Rego | `sales_rep` может `PATCH /deals/{id}`, но не `DELETE /contacts/{id}` | 403 |
| 2 | Какие записи ему видны и какие он может менять? | **бэкенд** (Python, `Access`) | `sales_rep` видит только сделки, где он ответственный | список без чужих строк; чужой объект — 404; изменение, после которого объект выходит за его уровень, — 403 |

Оба шага читают **один файл** `policies/saleshub/data.yaml`, где для каждой роли записано, какие действия ей разрешены и на каком уровне. OPA смотрит только на сам факт «действие есть у роли», бэкенд — на уровень.

**Три слова, на которых держится раздел.**

- **Действие** — имя операции вида `ресурс:глагол`: `deal:write` = создать или изменить сделку. Одно действие могут выполнять несколько эндпоинтов (`POST /deals` и `PATCH /deals/{deal_id}` — оба `deal:write`); какой эндпоинт какое действие выполняет, записано в `routes.rego`.
- **Уровень** (в коде — `scope`) — какие записи затрагивает действие: `own` — свои (где я ответственный), `team` — своего отдела, `org` — всей организации.
- **`Access`** — маленький объект «действие + уровень + кто я (`user_id`, `org_id`, `team_id`)». Бэкенд собирает его после ответа OPA; сервис и репозиторий спрашивают у него, видна ли запись, и берут из него SQL-фильтр.

**Почему данные проверяет не OPA.** Спросить OPA про каждый из 5 млн контактов невозможно: список фильтрует SQL, и условие для `WHERE` всё равно пишется в коде. Кроме того, Telegram-бот и Celery-воркеры вызывают сервисы напрямую, без HTTP-маршрутов, — проверка данных в сервисах нужна им в любом случае. Поэтому OPA отвечает на простой вопрос «да/нет», а всё, что касается строк, живёт в одном месте — в `app/authz/access.py`.

### Роли

Один человек состоит ровно в одной организации и имеет ровно одну роль (так устроены многие реальные CRM, например Pipedrive и amoCRM). Роль, организация и отдел хранятся прямо в `users` — отдельных таблиц членства нет.

| Роль | Кто в жизни | Отдел | Видит данные | Меняет |
| --- | --- | --- | --- | --- |
| `super_admin` | владелец SaaS | — | в данные организаций не ходит | только через `/api/v1/admin/*`: создаёт организации, приглашает первого `org_admin`, перезапускает упавшие задачи. Это флаг `users.is_superadmin`, а не роль в `data.yaml`; организации у него нет |
| `org_admin` | администратор CRM в компании-клиенте | нет | всю организацию | всё: настройки, отделы, воронки, пользователи, данные |
| `team_lead` | руководитель отдела продаж | обязателен | свой отдел | данные отдела, массовые операции в отделе |
| `sales_rep` | менеджер по продажам | обязателен | только своё (компании — все) | только своё |
| `viewer` | директор, аналитик, бухгалтер | нет | всю организацию | ничего, только читает и выгружает |

### Матрица ролей (сводка `data.yaml`)

| Действие | Эндпоинты (примеры) | org\_admin | team\_lead | sales\_rep | viewer |
| --- | --- | --- | --- | --- | --- |
| `admin:orgs`, `admin:system` | `/api/v1/admin/*` | — | — | — | — |
| `org:read` | `GET /org`, `GET /teams`, `GET /pipelines` | org | org | org | org |
| `org:manage` | `PATCH /org`, `POST/PATCH/DELETE /teams`, `POST/PATCH /pipelines` | org | — | — | — |
| `member:read` | `GET /users` | org | team | team | org |
| `member:manage` | `POST /invites`, `PATCH /users/{id}` | org | — | — | — |
| `company:read` | компании — общий справочник | org | org | org | org |
| `contact:read`, `deal:read`, `activity:read` | списки, карточки, поиск, заметки | org | team | own | org |
| `company:write`, `contact:write`, `deal:write`, `activity:write` | `POST`, `PATCH` (активности — и `DELETE`) | org | team | own | — |
| `company:delete`, `contact:delete`, `deal:delete` | `DELETE`, `…/restore`, корзина | org | team | — | — |
| `bulk:run` | импорт, передача клиентов, теги, закрытие просроченных | org | team | — | — |
| `export:run` | выгрузка сегмента в CSV | org | team | — | org |
| `bulk_job:read`, `bulk_job:cancel` | прогресс и отмена job | org | team | — | own |
| `report:read` | отчёты по воронке и менеджерам | org | team | own | org |
| `audit:read` | журнал изменений | org | — | — | — |

Свою job автор видит всегда (`visible_jobs`: уровень `bulk_job:read` **или** `owner_id = я`): `team_id` job — отдел автора на момент запуска, и руководитель, которого перевели в другой отдел, иначе потерял бы свои job (а повтор по `Idempotency-Key` всё равно возвращал бы их).

`admin:*` нет ни у одной роли — эти эндпоинты проходит только супер-админ, и только их. Менеджер не удаляет даже свои записи — только руководитель: так клиенты не пропадают из отчётов отдела, а ошибку менеджер правит через `PATCH`.

**Что значит уровень для изменения.** Изменение проверяется дважды. Старая версия должна быть видна (репозиторий ищет объект через `get_visible_or_404`, иначе 404 — не раскрываем, что объект существует). Новая версия должна попадать в уровень (`ensure_allowed`, иначе 403 — объект пользователь уже видел, скрывать нечего). При создании проверяется только новая версия. Поэтому `sales_rep` создаёт клиента только на себя и не может отдать свою сделку коллеге, а `team_lead` может переназначить сделку внутри отдела, но не отдать её в чужой отдел.

Старая версия ищется по уровню **чтения** сущности, а затем проверяется уровнем действия: видна, но не моя — 403 (объект я уже видел), не видна — 404. Разница заметна у компаний: их читают все (`company:read` — org), а меняют по `company:write` — поэтому `PATCH` чужой компании от `sales_rep` даёт 403 `out_of_scope`, а не 404. У контактов и сделок уровни чтения и записи совпадают.

**Версия записи.** У компаний, контактов и сделок есть `version`; `PATCH` обязан прислать версию, которую видел. Не совпала — 409 `version.conflict` с `current_version`: кто-то успел изменить запись (оптимистическая блокировка). Гонку между чтением и записью ловит сам `UPDATE … WHERE id = :id AND version = :v` (`version_id_col` в SQLAlchemy) — тоже 409.

**Откуда берутся `org_id`, `owner_id` и `team_id` записи.** Клиент их не выбирает свободно:

- `org_id` — всегда из `Access` (организация того, кто делает запрос);
- `owner_id` — по умолчанию текущий пользователь; руководитель и админ могут указать другого, но ответственным может быть только активный `team_lead` или `sales_rep` той же организации (иначе 422 `owner.invalid`). `org_admin` и `viewer` сами записей не ведут: создавая запись, `org_admin` обязан указать ответственного (422 `owner.required`);
- `team_id` — всегда отдел ответственного; его ставит сервис, из тела запроса он не берётся.
- Ответственный проверяется и `team_id` пересчитывается только при создании и при смене `owner_id`; обычный `PATCH` их не трогает. Исключения — восстановление из корзины и (с этапа 5) повторное открытие закрытой сделки (`status` → `open`) или выполненной активности (`done: false`): `move_team` такие записи не переносит (это история), поэтому снова открытая запись возвращается в **текущий** отдел ответственного — как из корзины (`owner_for_restore`: ответственный больше не ведёт записи — 409 `owner.inactive`, новая версия вне моего уровня — 403 `out_of_scope`; новый `owner_id` в том же `PATCH` — отдел берётся от него). Строка ответственного читается с `FOR SHARE`: перевод его в другой отдел (`FOR UPDATE` в сервисе пользователей) ждёт, пока запись создастся, и потом её видит.
- Смена отдела человека, у которого есть живые компании, контакты, открытые сделки или открытые активности: с этапа 5 `users.team_id` меняется сразу, а в той же транзакции создаётся bulk job `move_team` — она «догоняет» записи чанками (раздел 6). Пока job идёт, `team_id` части записей отстаёт — это допустимая, описанная задержка. Смена роли на `org_admin` / `viewer` при живых записях по-прежнему 409 `member.has_records`: отдела у человека больше не будет, записи сначала передают через `POST /bulk/reassign-owner`. Отключать можно: записи остаются, их передадут bulk-ом. (До этапа 5 при живых записях запрещена была и смена отдела.) «Живые записи» для `member.has_records` и `team.has_records` (`LiveRecords.exist`) — компании, контакты и сделки не из корзины, **включая закрытые сделки** (они в отчётах отдела), и с этапа 5 — открытые активности (`done_at IS NULL`, частичные индексы `ix_activities_{team_id,owner_id}_due_at`); job `move_team` создаётся, только если есть что догонять (`LiveRecords.exist_outside_team` — то же условие, по которому её чанки режутся и обновляются: живые компании и контакты, открытые живые сделки, открытые активности с `team_id` не нового отдела).

Так `team_id` и `owner_id` никогда не расходятся, и проверка «новая версия в моём уровне» у `team_lead` автоматически означает «новый ответственный — из моего отдела».

**Ссылки на другие записи.** Каждый id в теле запроса (`company_id`, `contact_id`, `deal_id`) сервис загружает и проверяет через `Access` на чтение этой сущности (`access.for_action("contact:read", roles)` — тот же человек, уровень другого действия; нет такого действия у роли — ссылка невидима); невидимая ссылка → 422 `validation` с полем в `details`. Компании видны всей организации, поэтому к любой компании привязать можно, а к чужому контакту — нельзя. `stage_id` проверяется по организации воронки (у этапов нет ответственного).

**Дубль, которого не видно.** Email контакта уникален в организации (раздел 3), а видимость ограничена уровнем. Если менеджер создаёт контакт, меняет email или восстанавливает контакт из корзины, а такой email уже есть у записи вне его уровня, сервис возвращает 409 `contact.duplicate_email` и в `details` кладёт только ответственного: `{"owner": "ivan@acme.ua"}` — без id и данных контакта (имени у пользователей нет, поэтому это его рабочий email: внутри организации он не секрет). Менеджер знает, к кому обратиться, а передать клиента может руководитель (так делают контроль дублей в Bitrix24 и amoCRM). Дубль в моём уровне (`contact:read`) — тот же код и `{"contact_id": 123, "owner": …}`. Дублем считается только живой контакт; одновременное создание двух одинаковых ловит частичный UNIQUE — обработчик ошибок по имени ограничения отвечает тем же `contact.duplicate_email`.

**Активности и заметки видны по родителю.** Карточка (`GET /contacts/{id}/notes`, активности сделки) загружает родителя через `get_visible_or_404` и дальше по `Access` не фильтрует: кто видит сделку, видит всю её историю. Уровень `activity:read` применяется к спискам вне карточки — например, «мои задачи на сегодня» (`GET /activities?due=today`). Заметку добавляет тот, кто может менять родителя; править и удалять её может только автор (эндпоинты — позже). Задачи сделки или контакта из корзины остаются в списке задач, пока корзину не очистят: `trash.purge` удаляет их вместе со сделкой (FK CASCADE) или с контактом (активности без сделки).

### Как проходит запрос

1. `current_user` проверяет access-токен и загружает пользователя вместе с `role`, `org_id`, `team_id` (кэш Redis на 60 с, раздел 4).
2. Зависимость `authorize` на защищённом роутере берёт **шаблон** пути (`request.scope["route"].path`, например `/api/v1/deals/{deal_id}`, а не `/api/v1/deals/42`: в `routes.rego` записаны шаблоны, иначе пришлось бы перечислять каждый id), метод и роль.
3. Один запрос `POST http://localhost:8181/v1/data/saleshub/authz/decision` в OPA-сайдкар. Путь URL повторяет пакет и правило: `saleshub/authz/decision` = `package saleshub.authz`, правило `decision`.
4. OPA: путь → действие (`routes.rego`) → есть ли действие у роли (`data.yaml`) → `{allow, action}`.
5. `allow: false` → 403. Иначе бэкенд по роли и действию берёт уровень из того же `data.yaml` и передаёт в эндпоинт `Access`.
6. Сервис применяет `Access`, второго запроса к OPA нет: один объект — `get_visible_or_404(id, access)`, создание и изменение — `access.ensure_allowed(new_obj)`, список и bulk — `access.sql_filter()` в `WHERE`, поиск — `access.es_filter()`.

Вход и ответ OPA для `PATCH /api/v1/deals/42` от менеджера по продажам:

```json
// input
{
  "method": "PATCH",
  "route": "/api/v1/deals/{deal_id}",
  "user": {"id": "u3", "is_superadmin": false, "role": "sales_rep"}
}
// result
{"allow": true, "action": "deal:write"}
```

Для решения OPA нужны только роль и флаг супер-админа: организация и отдел ему не нужны, потому что данные он не проверяет. `id` передаётся только для decision logs — чтобы в Grafana было видно, кому отказали.

### Разбор на примере: что увидит каждая роль

Один и тот же запрос `GET /api/v1/deals` от разных людей организации `o1`. Эндпоинт и код одинаковые; разный только уровень — и из него получается разный SQL.

| Кто спрашивает | Ответ OPA | Уровень из `data.yaml` | Что `sql_filter()` добавит в `WHERE` |
| --- | --- | --- | --- |
| `sales_rep` u3 из отдела t1 | `allow: true` | `own` | `org_id = 'o1' AND owner_id = 'u3'` |
| `team_lead` отдела t1 | `allow: true` | `team` | `org_id = 'o1' AND team_id = 't1'` |
| `org_admin` или `viewer` | `allow: true` | `org` | `org_id = 'o1'` |
| `super_admin` | `allow: false` | — | 403: в данные организаций он не ходит |
| пользователь без роли (остался с этапа 2) | `allow: false` | — | 403 |

Итоговый SQL для менеджера:

```sql
-- GET /api/v1/deals?limit=50 от sales_rep u3
SELECT id, title, amount_usd, stage_id, owner_id
FROM deals
WHERE deleted_at IS NULL                        -- корзина: общий фильтр репозитория
  AND org_id = 'o1' AND owner_id = 'u3'         -- access.sql_filter()
ORDER BY created_at DESC, id DESC               -- keyset-пагинация
LIMIT 50;
```

А `PATCH /api/v1/deals/42` от того же менеджера: репозиторий ищет сделку по id **с тем же фильтром** (`get_visible_or_404`). Сделка чужая — 404 (не раскрываем, что она существует), своя — изменяется.

**Почему одно правило записано в трёх формах и как они не разойдутся.** `matches()` проверяет объект в памяти, `sql_filter()` — строки в Postgres, `es_filter()` — документы в Elasticsearch. Все три лежат рядом в `access.py`, и каждая — это три ветки `own / team / org`. Тест `test_access_forms` перебирает все сочетания «кто спрашивает» (уровень × пользователь × организация × отдел, включая «без отдела») и «чья запись» (организация × отдел × ответственный, включая NULL) — их всего несколько десятков, поэтому полный перебор надёжнее случайной генерации (hypothesis) — и проверяет на настоящем Postgres, что формы отвечают одинаково. Это главная защита от ситуации «в списке не видно, а по прямой ссылке открывается».

### Где что хранится

| Что | Пример | Где лежит | Кто и как меняет |
| --- | --- | --- | --- |
| Какое действие у эндпоинта | `PATCH /api/v1/deals/{deal_id}` → `deal:write` | `policies/saleshub/routes.rego` | разработчик, вместе с новым эндпоинтом, через PR |
| Что может каждая роль и на каком уровне | `sales_rep`: `deal:write: own` | `policies/saleshub/data.yaml` | через PR → `opa test` в CI → деплой |
| Логика проверки «роль → действие» | неописанный эндпоинт — 403; супер-админу — только `admin:*` | `policies/saleshub/authz.rego` | через PR, покрыто `opa test` |
| Кто какую роль имеет | Ihor — `team_lead` в отделе «Корпоративные продажи» | Postgres: `users.org_id`, `users.team_id`, `users.role` | через API: `org_admin` (супер-админ — для первого админа) |
| Как уровень применяется к данным | `own` → `owner_id = я` | `app/authz/access.py`: `matches`, `sql_filter`, `es_filter` | разработчик; тест проверяет, что три формы отвечают одинаково |

Таблиц `roles` и `permissions` в базе нет: роли — это код (policy as code), их меняют PR-ом и проверяют тестами. В базе только факт «у этого человека такая роль в таком отделе».

### Файл ролей `data.yaml`

OPA загружает YAML-данные, только если файл называется ровно `data.yaml`. Где они окажутся, складывается из двух частей: путь папки относительно корня, который передан в `opa run` / `opa test` (корень — `policies/`, папка `saleshub/` → `data.saleshub`), плюс ключи внутри файла (верхний ключ `roles` → `data.saleshub.roles`). Бэкенд читает тот же файл при старте. Ниже — файл как есть; в нём ровно то, что в матрице выше.

```yaml
# policies/saleshub/data.yaml — кто что может.
# Читают оба: OPA (есть ли у роли действие) и бэкенд (уровень видимости данных).
# Уровень: own — только свои записи, team — записи своего отдела, org — вся организация.
# super_admin здесь не описан: это флаг users.is_superadmin, ему разрешены только админские
# эндпоинты /api/v1/admin/* (действия admin:*), в данные организаций он не ходит.
roles:
  org_admin:
    title: Администратор организации
    permissions:
      org:read: org
      org:manage: org          # настройки организации, отделы, воронки
      member:read: org
      member:manage: org       # пригласить, сменить роль или отдел, отключить
      company:read: org
      company:write: org       # создать и изменить
      company:delete: org      # удалить в корзину и восстановить
      contact:read: org
      contact:write: org
      contact:delete: org
      deal:read: org
      deal:write: org
      deal:delete: org
      activity:read: org
      activity:write: org      # создать, изменить, удалить (активности удаляются насовсем)
      bulk:run: org            # импорт, передача клиентов, теги, закрытие просроченных
      bulk_job:read: org
      bulk_job:cancel: org
      export:run: org
      report:read: org
      audit:read: org

  team_lead:
    title: Руководитель отдела продаж
    permissions:
      org:read: org
      member:read: team
      company:read: org        # компании — общий справочник: видны всем, чтобы не плодить дубли
      company:write: team
      company:delete: team
      contact:read: team
      contact:write: team
      contact:delete: team
      deal:read: team
      deal:write: team
      deal:delete: team
      activity:read: team
      activity:write: team
      bulk:run: team
      bulk_job:read: team
      bulk_job:cancel: team
      export:run: team
      report:read: team

  sales_rep:
    title: Менеджер по продажам
    permissions:
      org:read: org
      member:read: team
      company:read: org
      company:write: own
      contact:read: own
      contact:write: own
      deal:read: own
      deal:write: own
      activity:read: own
      activity:write: own
      report:read: own

  viewer:
    title: Наблюдатель (директор, аналитик)
    permissions:
      org:read: org
      member:read: org
      company:read: org
      contact:read: org
      deal:read: org
      activity:read: org
      bulk_job:read: own       # свои выгрузки в S3 — с этапа 10 (job export); на этапе 5 своих job у viewer нет
      bulk_job:cancel: own
      export:run: org
      report:read: org
```

**Как поменять права.** Хотите, чтобы менеджер видел клиентов всего отдела, — меняете `contact:read: own` на `contact:read: team` у `sales_rep`. Хотите запретить действие — удаляете строку. Код при этом не меняется; тесты проверят, что опечаток нет, а массовые операции не стали шире прав на одну запись.

### Политики

В проекте — OPA 1.21.3 (compose, testcontainers и CI). `make policy-test` запускает OPA из Docker: `opa fmt --list --fail`, `opa check --strict`, `opa test -b policies -v` и покрытие `--coverage --threshold 90`; тот же make-таргет — job `policy` в CI. Тесты на маршруты этапов 4–9 добавляются в `authz_test.rego` вместе с маршрутами. Намеренная опечатка в имени действия, неверный уровень и слишком широкий `export:run` роняют тесты.

**Как читать Rego.** Обращение к отсутствующему ключу в Rego даёт не ошибку, а «нет значения» (undefined). Правило, в котором встретилось такое значение, просто не срабатывает, и берётся значение `default`. Поэтому для неописанного эндпоинта: `action` нет → ни одно `allow` не сработало → `allow = false`, `decision = {allow: false}`. Строка `permissions[action]` проверяет только, что ключ есть: значение `own` / `team` / `org` в условии считается истиной.

Ниже — `routes.rego` к концу этапа 9. Строки добавляются вместе с эндпоинтами: на этапе 3 в файле только первые два блока.

```rego
# policies/saleshub/routes.rego — какой эндпоинт какое действие выполняет.
# Новый эндпоинт без строки здесь получит 403 у всех, а pytest-тест test_routes_match_policy упадёт.
# Строки добавляются вместе с эндпоинтами: на этапе 3 — первые два блока, остальные — на этапах 4–9.
package saleshub.routes

actions := {
	# --- этап 3: только супер-админ (действий admin:* нет ни у одной роли в data.yaml)
	"GET /api/v1/admin/orgs": "admin:orgs",
	"POST /api/v1/admin/orgs": "admin:orgs",
	"POST /api/v1/admin/orgs/{org_id}/invites": "admin:orgs",
	# --- этап 3: организация, отделы, люди
	"GET /api/v1/org": "org:read",
	"PATCH /api/v1/org": "org:manage",
	"GET /api/v1/teams": "org:read",
	"POST /api/v1/teams": "org:manage",
	"PATCH /api/v1/teams/{team_id}": "org:manage",
	"DELETE /api/v1/teams/{team_id}": "org:manage",
	"GET /api/v1/users": "member:read",
	"PATCH /api/v1/users/{user_id}": "member:manage",
	"POST /api/v1/invites": "member:manage",
	# --- этап 4: воронки, компании, контакты, сделки, активности, отчёты
	"GET /api/v1/pipelines": "org:read",
	"POST /api/v1/pipelines": "org:manage",
	"PATCH /api/v1/pipelines/{pipeline_id}": "org:manage",
	"GET /api/v1/companies": "company:read",
	"GET /api/v1/companies/{company_id}": "company:read",
	"POST /api/v1/companies": "company:write",
	"PATCH /api/v1/companies/{company_id}": "company:write",
	"DELETE /api/v1/companies/{company_id}": "company:delete",
	"POST /api/v1/companies/{company_id}/restore": "company:delete",
	"GET /api/v1/contacts": "contact:read",
	"GET /api/v1/contacts/trash": "contact:delete",
	"GET /api/v1/contacts/{contact_id}": "contact:read",
	"POST /api/v1/contacts": "contact:write",
	"PATCH /api/v1/contacts/{contact_id}": "contact:write",
	"DELETE /api/v1/contacts/{contact_id}": "contact:delete",
	"POST /api/v1/contacts/{contact_id}/restore": "contact:delete",
	"GET /api/v1/contacts/{contact_id}/notes": "contact:read",
	"POST /api/v1/contacts/{contact_id}/notes": "contact:write",
	"GET /api/v1/deals": "deal:read",
	"GET /api/v1/deals/kanban": "deal:read",
	"GET /api/v1/deals/{deal_id}": "deal:read",
	"POST /api/v1/deals": "deal:write",
	"PATCH /api/v1/deals/{deal_id}": "deal:write",
	"DELETE /api/v1/deals/{deal_id}": "deal:delete",
	"POST /api/v1/deals/{deal_id}/restore": "deal:delete",
	"GET /api/v1/activities": "activity:read",
	"POST /api/v1/activities": "activity:write",
	"PATCH /api/v1/activities/{activity_id}": "activity:write",
	"DELETE /api/v1/activities/{activity_id}": "activity:write",
	"GET /api/v1/reports/pipeline": "report:read",
	"GET /api/v1/reports/managers": "report:read",
	"GET /api/v1/audit": "audit:read",
	# --- этап 5: массовые операции и выгрузка (каждый шаг добавляет свои строки)
	"POST /api/v1/bulk/reassign-owner": "bulk:run",
	"POST /api/v1/bulk/close-overdue-deals": "bulk:run",
	"POST /api/v1/bulk/add-tags": "bulk:run",
	"POST /api/v1/bulk/remove-tags": "bulk:run",
	"POST /api/v1/contacts/import": "bulk:run",
	"GET /api/v1/contacts/export": "export:run",
	"GET /api/v1/bulk-jobs/{job_id}": "bulk_job:read",
	"GET /api/v1/bulk-jobs/{job_id}/events": "bulk_job:read",
	"GET /api/v1/bulk-jobs/{job_id}/errors": "bulk_job:read",
	"POST /api/v1/bulk-jobs/{job_id}/cancel": "bulk_job:cancel",
	"GET /api/v1/admin/failed-tasks": "admin:system",
	"GET /api/v1/admin/failed-tasks/{failed_task_id}": "admin:system",
	"POST /api/v1/admin/failed-tasks/{failed_task_id}/replay": "admin:system",
	# --- этап 9: поиск
	"GET /api/v1/search/contacts": "contact:read",
	"GET /api/v1/search/suggest": "contact:read",
	"POST /api/v1/admin/search/reindex": "admin:system",
}
```

Пути организации не содержат `org_id`: организация у пользователя одна и берётся из него самого. `org_id` в пути есть только у эндпоинтов супер-админа. Не проходят через OPA: `/api/v1/auth/*`, `/api/v1/me/*` (с этапа 5: личные настройки и привязка Telegram — отдельный роутер `app/api/v1/me.py` только с `current_member` = `current_user` + проверка, что у пользователя есть организация, иначе 403; работают с записями самого пользователя; `test_routes_match_policy` их не проверяет, а unit-тест проверяет, что на этих маршрутах нет `opa_decision`), `/health/*`, `/metrics`, `/.well-known/jwks.json`, `/telegram/webhook` (появится при переходе в облако, этап 10.7; своя проверка секретным заголовком).

```rego
# policies/saleshub/authz.rego — можно ли этой роли вызвать этот эндпоинт.
# Данные (свои / отдела / организации) здесь НЕ проверяются — это делает бэкенд.
package saleshub.authz

# 1. Какое действие выполняет эндпоинт: "PATCH /api/v1/deals/{deal_id}" → "deal:write"
action := data.saleshub.routes.actions[concat(" ", [input.method, input.route])]

# 2. Разрешено ли действие. По умолчанию — нет.
default allow := false

allow if {
	data.saleshub.roles[input.user.role].permissions[action] # у роли есть это действие
}

allow if {
	input.user.is_superadmin # супер-админ работает только через /api/v1/admin/*
	startswith(action, "admin:")
}

# 3. Ответ бэкенду: можно ли и какое это действие (по нему бэкенд найдёт уровень в data.yaml).
default decision := {"allow": false}

decision := {"allow": allow, "action": action} if action
```

```rego
# policies/saleshub/authz_test.rego — запуск: opa test -b policies -v
package saleshub.authz_test

import data.saleshub.authz

super_admin := {"id": "u0", "is_superadmin": true, "role": null}

org_admin := {"id": "u1", "is_superadmin": false, "role": "org_admin"}

team_lead := {"id": "u2", "is_superadmin": false, "role": "team_lead"}

sales_rep := {"id": "u3", "is_superadmin": false, "role": "sales_rep"}

viewer := {"id": "u4", "is_superadmin": false, "role": "viewer"}

req(user, method, route) := {"user": user, "method": method, "route": route}

test_sales_rep_can_edit_deal if {
	d := authz.decision with input as req(sales_rep, "PATCH", "/api/v1/deals/{deal_id}")
	d.allow
	d.action == "deal:write"
}

test_sales_rep_cannot_delete_contact if {
	not authz.allow with input as req(sales_rep, "DELETE", "/api/v1/contacts/{contact_id}")
}

test_viewer_reads_but_cannot_write if {
	authz.allow with input as req(viewer, "GET", "/api/v1/deals")
	not authz.allow with input as req(viewer, "PATCH", "/api/v1/deals/{deal_id}")
}

test_only_org_admin_manages_members if {
	authz.allow with input as req(org_admin, "POST", "/api/v1/invites")
	not authz.allow with input as req(team_lead, "POST", "/api/v1/invites")
	not authz.allow with input as req(sales_rep, "POST", "/api/v1/invites")
	not authz.allow with input as req(viewer, "POST", "/api/v1/invites")
}

test_only_super_admin_creates_orgs if {
	authz.allow with input as req(super_admin, "POST", "/api/v1/admin/orgs")
	not authz.allow with input as req(org_admin, "POST", "/api/v1/admin/orgs")
}

# У супер-админа нет организации, поэтому в обычные эндпоинты он не ходит.
test_super_admin_only_admin_routes if {
	authz.allow with input as req(super_admin, "GET", "/api/v1/admin/failed-tasks")
	not authz.allow with input as req(super_admin, "GET", "/api/v1/deals")
	not authz.allow with input as req(super_admin, "POST", "/api/v1/invites")
}

test_unknown_route_denied_even_for_super_admin if {
	d := authz.decision with input as req(super_admin, "GET", "/api/v1/secret")
	not d.allow
}

test_user_without_role_denied if {
	not authz.allow with input as req({"id": "u9", "is_superadmin": false, "role": null}, "GET", "/api/v1/org")
}

# Каждое действие из routes.rego должно быть хотя бы у одной роли в data.yaml,
# либо быть админским (admin:*). Ловит опечатку в имени действия.
test_every_action_is_known if {
	known := {a | some r in data.saleshub.roles; some a, _ in r.permissions}
	unknown := {a | some a in data.saleshub.routes.actions; not a in known; not startswith(a, "admin:")}
	count(unknown) == 0
}

# Значения в data.yaml — только own / team / org.
test_scopes_are_valid if {
	bad := {[name, a, s] |
		some name, r in data.saleshub.roles
		some a, s in r.permissions
		not s in {"own", "team", "org"}
	}
	count(bad) == 0
}

rank := {"own": 1, "team": 2, "org": 3}

# Массовые операции и выгрузка не шире, чем права на одну запись:
# иначе через bulk можно было бы обойти ограничения роли.
test_bulk_and_export_not_wider_than_single_record if {
	# Множество пар, а не объект: у bulk:run несколько «базовых» действий.
	pairs := {
		["bulk:run", "company:write"], ["bulk:run", "contact:write"],
		["bulk:run", "deal:write"], ["bulk:run", "activity:write"],
		["export:run", "contact:read"], ["bulk_job:cancel", "bulk_job:read"],
	}
	bad := {[name, a] |
		some name, r in data.saleshub.roles
		some [a, base] in pairs
		r.permissions[a]
		not rank[r.permissions[a]] <= rank[r.permissions[base]]
	}
	count(bad) == 0
}
```

Полную матрицу «каждая роль × каждый эндпоинт» в Rego руками не пишем: pytest берёт список маршрутов у живого OPA (`GET /v1/data/saleshub/routes/actions`), роли и уровни — из `data.yaml`, и прогоняет запросы (раздел 10).

### Сторона FastAPI

```python
# authz/opa.py
class OpaClient:
    async def query(self, rule: str, input: dict) -> Any:
        r = await self.http.post(f"{self.url}/v1/data/{rule}", json={"input": input}, timeout=0.5)
        r.raise_for_status()
        return r.json().get("result")

# authz/roles.py — тот же data.yaml, что читает OPA (папка смонтирована и в api, и в opa)
ROLES: dict = yaml.safe_load(Path(settings.policies_dir, "saleshub/data.yaml").read_text())["roles"]

def scope_for(user: CurrentUser, action: str) -> Scope:
    level = ROLES.get(user.role, {}).get("permissions", {}).get(action)
    if level is None:            # OPA разрешил, а у бэкенда в data.yaml такого действия нет — файлы разошлись
        raise ServiceUnavailable("authz.policy_out_of_sync")
    return level

# authz/deps.py
async def opa_decision(request: Request,
                       user: CurrentUser = Depends(current_user),
                       opa: OpaClient = Depends(get_opa)) -> dict:
    inp = {"method": request.method,
           "route": request.scope["route"].path,      # шаблон: /api/v1/deals/{deal_id}
           "user": {"id": str(user.id), "role": user.role, "is_superadmin": user.is_superadmin}}
    try:
        d = await opa.query("saleshub/authz/decision", inp)
    except httpx.HTTPError:
        raise ServiceUnavailable("authz")             # OPA недоступен → никого не пускаем
    if not d or not d["allow"]:
        raise Forbidden(action=(d or {}).get("action"))
    return d

async def authorize(user: CurrentUser = Depends(current_user),
                    d: dict = Depends(opa_decision)) -> Access:
    return Access.for_user(user, d["action"])       # уровень — из data.yaml

# Полный префикс — у роутера, эндпоинты объявляются прямо на нём (@protected.get(...)),
# роутер подключается только в app: без include_router(prefix=...) и без вложения роутера
# в роутер. Тогда scope["route"].path — полный шаблон, как ключи в routes.rego. У вложенного
# роутера FastAPI 0.151 оставляет только его собственный префикс, и OPA ответила бы 403 всем.
protected = APIRouter(prefix="/api/v1", dependencies=[Depends(authorize)])          # данные организации
admin = APIRouter(prefix="/api/v1/admin", dependencies=[Depends(opa_decision)])     # супер-админ, без Access
```

Статические пути (`/contacts/trash`, `/deals/kanban`) объявляются раньше путей с параметром (`/contacts/{contact_id}`), иначе Starlette сопоставит `trash` с `{contact_id}` и ответит 422. Конвертеры типов в пути (`{contact_id:int}`) не используются: они попадают в шаблон пути, и он перестаёт совпадать с ключом в `routes.rego`.

`authorize` стоит и на роутере, и в параметрах эндпоинта (`access: Access = Depends(authorize)`), но FastAPI кэширует зависимость в пределах запроса — OPA спрашивается один раз.

```python
# authz/access.py — одно правило «своё / отдел / организация» в трёх формах
Scope = Literal["own", "team", "org"]

@dataclass(frozen=True)
class Access:
    action: str
    scope: Scope
    user_id: UUID
    org_id: UUID
    team_id: UUID | None         # у org_admin и viewer отдела нет — уровень team им и не выдаётся
    role: str | None = None      # для for_action(): уровень другого действия того же человека

    @classmethod
    def for_user(cls, user, action: str, roles: RoleCatalog) -> "Access | None":
        # процессы без HTTP (бот, воркеры): None — у роли нет действия или нет организации
        scope = roles.scope_or_none(user.role, action) if user.org_id else None
        return None if scope is None else cls(action, scope, user.id, user.org_id,
                                              user.team_id, user.role)

    def own(self) -> "Access":                            # бот: «мои задачи», «мои сделки»
        return replace(self, scope="own")

    def to_params(self) -> dict: ...                      # bulk: Access автора в bulk_jobs.params (uuid → str)
    @classmethod
    def from_params(cls, d: dict) -> "Access": ...        # воркер восстанавливает его в каждом чанке

    def matches(self, obj) -> bool:                       # 1) объект в памяти
        match self.scope:
            case "org":
                return obj.org_id == self.org_id
            case "team":
                return obj.org_id == self.org_id and obj.team_id == self.team_id
            case "own":
                return obj.org_id == self.org_id and obj.owner_id == self.user_id
        raise ValueError(f"неизвестный уровень: {self.scope}")

    def sql_filter(self, m) -> ColumnElement[bool]:       # 2) список и bulk → WHERE
        match self.scope:
            case "org":
                return m.org_id == self.org_id
            case "team":
                return and_(m.org_id == self.org_id, m.team_id == self.team_id)
            case "own":
                return and_(m.org_id == self.org_id, m.owner_id == self.user_id)
        raise ValueError(f"неизвестный уровень: {self.scope}")

    def es_filter(self) -> dict: ...                      # 3) поиск → фильтр Elasticsearch (раздел 17)

    def for_action(self, action: str, roles: RoleCatalog) -> "Access | None":
        # сделку пишут с deal:write, а привязанный контакт проверяют на contact:read
        scope = roles.scope_or_none(self.role, action)
        return None if scope is None else replace(self, action=action, scope=scope)

    def ensure_allowed(self, new_obj) -> None:            # создать или изменить за пределами своего уровня → 403
        if not self.matches(new_obj):
            raise Forbidden(reason="out_of_scope")
```

**Где применять `Access`.** Репозиторий не отдаёт данные без `Access`: и список `uow.deals.list(access, filters, page)`, и один объект `uow.deals.get_visible_or_404(deal_id, access)` сами добавляют `access.sql_filter(Deal)` — так же, как общий фильтр `deleted_at IS NULL` (для корзины — `get_deleted_or_404`). Забыть проверку нельзя: без `Access` метод не вызвать. Вручную в сервисе остаётся только `ensure_allowed` для новой версии объекта. `Access` работает с таблицами, где есть `org_id`, `team_id`, `owner_id`; для `users` уровень `team` = «мой отдел», заметки проверяются через родителя.

### Кто управляет пользователями и ролями

1. Супер-админ создаётся командой при деплое (раздел 4). Только он создаёт организации (`POST /api/v1/admin/orgs`) и приглашает в них первого `org_admin` (`POST /api/v1/admin/orgs/{org_id}/invites`). Самостоятельной регистрации нет.
2. `org_admin` приглашает людей в свою организацию: `POST /api/v1/invites {email, role, team_id}`. Пользователь появляется в базе, когда принимает приглашение.
3. `org_admin` меняет роль, отдел или отключает пользователя: `PATCH /api/v1/users/{user_id} {role, team_id, is_active}`. Остальные роли пользователями не управляют.
4. Правила проверяет сервис пользователей (это бизнес-логика, а не вопрос «можно ли вызвать эндпоинт»):
   - нельзя менять роль, отдел или статус **самому себе**;
   - в организации всегда остаётся хотя бы один активный `org_admin`: сервис в той же транзакции блокирует строку организации (`SELECT … FROM organizations WHERE id = :org FOR UPDATE`) и считает админов, иначе 409 `member.last_admin` — так два админа не смогут одновременно снять друг друга;
   - у `team_lead` и `sales_rep` отдел обязателен, у `org_admin` и `viewer` его нет; отдел — из той же организации;
   - `super_admin` нельзя назначить через API;
   - приглашение на email, который уже есть в базе, не создаётся, но ответ тот же 202, а человеку уходит письмо «у вас уже есть учётная запись в SalesHub» — админ чужой организации не узнаёт, что email занят (как в `password/forgot`). В журнал организации в обоих случаях пишется одно и то же `invite.created`: журнал читают её админы (`audit:read`), и отдельная запись выдала бы занятый email. При `invite/accept` сервис ещё раз проверяет, что email свободен и отдел существует (иначе 409 `invite.stale`).
   Причина отказа возвращается в теле ответа: 403 `member.cannot_change_self`, 409 `member.last_admin`, 422 `member.team_required` / `member.team_not_allowed` / `member.team_not_found`. Через обычный API `member.last_admin` недостижим (сам себе админ ничего не меняет, значит, пока он снимает другого, остаётся он сам) — правило срабатывает при гонке, и проверяет его тест на настоящих транзакциях.
5. Перевод менеджера в другой отдел запускает bulk job `move_team`: `team_id` его живых компаний и контактов, открытых сделок и открытых активностей переносится теми же чанками, что и при передаче клиентов (раздел 6), — клиенты переходят в новый отдел вместе с ним. `PATCH /users/{id}` отвечает 200 с полем `bulk_job_id` (null, если переносить нечего; схема ответа — `MemberUpdateOut`, наследник `MemberOut`: в `GET /users` этого поля нет). Смена роли на `org_admin` / `viewer` при живых записях — 409 `member.has_records`.
6. Любое изменение `role`, `team_id`, `is_active` → **после** `commit` удалить пользователя из кэша Redis + запись в `audit_log`. Токен перевыпускать не нужно: роли нет в JWT, новая роль действует со следующего запроса (60 с — лишь страховка, если удаление ключа не прошло). Отключение пользователя дополнительно делает `token_version += 1` и отзывает все его refresh-токены — доступ пропадает сразу.
7. Изменить права роли (что может `sales_rep`) — PR в `data.yaml` + `opa test` в CI + деплой.

### Как OPA развёрнут

- **Сайдкар** в поде `api`: `opa run --server --addr=127.0.0.1:8181 --watch --ignore='..*' /policies`. Запрос по localhost — около миллисекунды.
- **Kubernetes.** Политики попадают в под через ConfigMap: Helm собирает `routes.rego`, `authz.rego` и `data.yaml` (без `*_test.rego`). ConfigMap не хранит подпапки, поэтому он монтируется в `mountPath: /policies/saleshub` — тогда `data.yaml` оказывается в `data.saleshub.roles`. `--ignore='..*'` нужен из-за служебных папок `..data` и `..<время>`, которые Kubernetes кладёт в каждый ConfigMap: без него OPA видит файлы трижды и не стартует, а с `--bundle` кладёт данные не туда, и все получают 403 (проверено на OPA 1.4.2; на этапе 7 перепроверить на 1.21.3). Тот же ConfigMap монтируется и в контейнер `api` (`POLICIES_DIR=/policies`), поэтому OPA и бэкенд читают один файл. В шаблоне пода — аннотация `checksum/policies`: при правке политик Helm перезапускает под, и обе стороны перечитывают правила. Позже (этап CD) — бандлом: CI делает `opa build` и публикует bundle в GHCR как OCI-артефакт.
- **Compose.** Папка `./policies` монтируется и в `opa` (та же команда плюс `--ignore=*_test.rego` и decision logs в консоль), и в `api`; uvicorn запускается с `--reload --reload-dir /policies --reload-include 'data.yaml'`, иначе он следит только за `.py`. В образе OPA нет shell, wget и curl, поэтому healthcheck — `opa eval --fail` с `http.send` к самому серверу: он проверяет, что `/v1/data/saleshub/roles` не пустой, и `api` стартует только после этого. Правка `.rego` и `data.yaml` применяется сразу в обоих. В Tilt — то же через `sync`.
- **Проверка после деплоя:** `curl localhost:8181/v1/data/saleshub/roles` не должен быть пустым.
- **Fail closed:** OPA недоступен → 503, алерт `OpaUnavailable` (раздел 8).
- **Decision logs:** OPA пишет каждое решение (вход + результат) в stdout → Loki → Grafana: видно, кому и почему отказали.

### Права в Swagger и тест покрытия

- При старте приложение читает `data.saleshub.routes.actions` из OPA (до 5 попыток раз в секунду; не ответил — Swagger без прав, приложение всё равно стартует) и дописывает в описание каждой операции «Требуется: `deal:write`», а также роли и уровни из `data.yaml`: «org\_admin — org, team\_lead — team, sales\_rep — own». То же действие — в расширении `x-required-action` для генераторов клиентов.
- `GET /api/v1/auth/me` возвращает роль и `permissions` — действия роли из `data.yaml` с уровнями; фронт по ним прячет кнопки. У супер-админа — `is_superadmin: true` и пустой `permissions`.
- `test_routes_match_policy` (pytest): множество маршрутов роутеров `protected_router` и `admin_router` (`app/api/v1/__init__.py`) должно совпадать с ключами `actions` в `routes.rego`. Маршруты берутся у самих роутеров: в FastAPI 0.151 `include_router` кладёт в `app.routes` не маршруты, а обёртки `_IncludedRouter`. Новый эндпоинт без правила или устаревшее правило без эндпоинта — CI падает. Unit-тест `test_authz_deps` подменяет OPA и проверяет, что `opa_decision` отправляет шаблон пути с полным префиксом (`/api/v1/test/items/{item_id}`, а не реальный URL), а OPA спрашивается один раз; ошибку с префиксом на настоящих маршрутах ловит тест-матрица — такой маршрут получил бы 403 у всех.

### Нужен ли Auth0

Нет. Auth0 — провайдер входа (пароли, MFA, SSO, соцсети); всё это делает свой модуль из раздела 4. Row-level доступ (own / team / org) Auth0 не решает в любом случае — это задача бэкенда. Попробовать Auth0 можно на этапе 10: проверка токена уже идёт через JWKS, меняются только `ISSUER`, `AUDIENCE` и `JWKS_URL`.

## 6. Большие объёмы данных и фоновая обработка

Массовая операция — это асинхронная job: API проверяет права и параметры, создаёт запись `bulk_jobs` и сразу отвечает `202` с job. Воркер считает объём, режет выборку на чанки по диапазонам id и обновляет каждый чанк одним set-based SQL в своей транзакции. В очередь уходит одна команда, а SQL выполняется пачками — не одной транзакцией на миллион строк и не по одной строке.

> Система воркеров (очереди, ретраи, масштабирование, мониторинг, async внутри Celery) описана отдельным модулем в разделе 15. Здесь зафиксирован только общий подход к объёму данных.

### Сценарии CRM

| Сценарий | Объём | Техника |
| --- | --- | --- |
| Импорт контактов из CSV с дедупликацией по email | 500 тыс. строк | файл в S3 (RustFS) → проверка строк в Python (те же правила полей, что у `POST /contacts`) → `COPY` в `contacts_staging` → проверки по файлу и базе SQL-ом (дубли в файле, ответственный) → чанки `INSERT … ON CONFLICT (org_id, email) WHERE deleted_at IS NULL DO UPDATE SET … WHERE <фильтр Access из params>`: существующий контакт обновляется, только если он в уровне того, кто запустил импорт, остальные дубли → `import_errors` (`duplicate_outside_scope`). Подробно — «Поток: импорт контактов из CSV» |
| Менеджер уволился: передать всех его клиентов | до 100 тыс. контактов + компании, открытые сделки и открытые активности | чанки `UPDATE … SET owner_id = :new WHERE owner_id = :old AND id BETWEEN …` по четырём таблицам |
| Закрыть просроченные сделки как `lost` | десятки тысяч | ручной запуск руководителем; частичный индекс `(org_id, expected_close_date) WHERE status = 'open'` + чанки |
| Добавить или снять тег у сегмента | до 1 млн контактов | `UPDATE … SET tags = array_append(tags, :t) WHERE NOT tags @> ARRAY[:t]` (снятие — `array_remove`) — идемпотентно |
| Экспорт сегмента в CSV | до 1 млн | серверный курсор + `StreamingResponse`; больше — 422 «сузьте фильтр». Файл в S3 и ссылка для больших выгрузок — этап 10 |
| Очистка корзины | всё, что удалено > 30 дней | задача beat, `DELETE` чанками (раздел 3) |

### Способы обновить много строк

| Способ | Когда | Скорость | Риск |
| --- | --- | --- | --- |
| ORM в цикле | никогда для объёма | очень медленно | N запросов, память |
| Один `UPDATE … WHERE` на всё | до \~50 тыс. строк | максимум | долгая транзакция, блокировки, всплеск WAL |
| `UPDATE … WHERE id BETWEEN a AND b` чанками по 5–10 тыс. | основной вариант | высокая | нужен прогресс и идемпотентность |
| `UPDATE … FROM (SELECT unnest(:ids), unnest(:vals))` | разные значения на строку, десятки тысяч | высокая | размер параметров |
| `COPY` в staging → `INSERT … ON CONFLICT DO UPDATE` | импорт, сотни тысяч+ | максимум | нужен файл или поток |
| SQLAlchemy 2.0 bulk `update()` со списком dict-ов | средние объёмы | средняя | следить за размером батча |

Каждый способ замеряется на 1 и 5 млн контактов (шаг 5.9): `make bench-updates` (`scripts/bench_updates.py`) после `make seed-big` копирует `contacts` в `bench_contacts (LIKE contacts INCLUDING ALL)` — рабочая таблица не трогается, в конце копия удаляется. Меняется то же, что при передаче клиентов (`owner_id`, `team_id`, `version`); ORM в цикле — только на 10 тыс. строк с экстраполяцией. Метрики: время, строк в секунду, объём WAL (`pg_wal_lsn_diff`), HOT- и мёртвые строки (`pg_stat_user_tables`) и задержка «пробника» — параллельной сессии, которая каждые 100 мс правит одну строку сегмента (сколько bulk держит ручную правку). Отдельно: `SET owner_id` (индексированная колонка — не HOT, обновляются все индексы, включая GIN) против `SET source` (HOT) и границы чанков «равные диапазоны id» против «по строкам». Результаты — в `docs/experiments.md`.

### Поток: «передать клиентов уволенного менеджера»

1. `POST /api/v1/bulk/reassign-owner {from_user_id, to_user_id, include?}` + необязательный заголовок `Idempotency-Key`. `include` ⊆ {`companies`, `contacts`, `open_deals`, `open_activities`}, по умолчанию все четыре. Закрытые сделки и выполненные активности остаются у прежнего ответственного — это история («выиграно менеджером» — тем, кто выиграл), отчёт `/reports/managers` не меняется. Корзина не передаётся: фильтр `deleted_at IS NULL`.
2. API: `authorize()` → OPA: `bulk:run` → `from_user` должен быть виден по `member:read` автора (иначе 422 `validation`, поле `from_user_id`); `from == to` → 422 `bulk.same_owner`; `to_user` проверяет тот же `resolve_owner`, что у одной записи (422 `owner.invalid` / 403 `out_of_scope`) → повтор по `Idempotency-Key` → `bulk_jobs(status=pending, total=0, params={from_user_id, to_user_id, include, access})` → `uow.commit()` → задача `bulk.plan(job_id)` (best-effort, см. ниже) → `202 BulkJobOut` и `Location: /api/v1/bulk-jobs/{id}`. Вторая активная job того же вида в организации → 409 `bulk.already_running` с `details.job_id` (частичный UNIQUE). Счёт строк — не в API: count по сегменту в миллион строк занял бы сотни миллисекунд HTTP-запроса.
3. `bulk.plan(job_id)`: одна транзакция с условием `WHERE status = 'pending'` (повтор ничего не сделает). Для каждой сущности строит границы чанков по реальным строкам: `SELECT id FROM (SELECT id, row_number() OVER (ORDER BY id) rn FROM <t> WHERE <фильтр job>) s WHERE (rn - 1) % :size = 0` плюс `max(id)` (в коде — один запрос: `count(*) OVER ()` рядом с `row_number()` и условие `(rn - 1) % :size = 0 OR rn = cnt` — последняя строка даёт верхнюю границу, а `cnt` — `total` сущности; не `rn % :size = 1`: при `BULK_CHUNK_SIZE = 1` оно не выполнялось бы никогда) — получаются чанки ≈ `BULK_CHUNK_SIZE` строк (10 000), а не равные диапазоны id: у менеджера со 100 тыс. клиентов, разбросанных по 5 млн id, равные диапазоны дали бы ~500 почти пустых чанков (замер — 5.9). В той же транзакции: строки `bulk_job_chunks`, `total`, `chunks_total`, `status = running`, `started_at`. После коммита — обычная `group(bulk.chunk.s(job_id, no) …)`, без chord. Пустая выборка — сразу `done`. Записи, созданные после plan, в job не входят: это снимок на момент запуска. Снимок — диапазоны id, а не список строк: запись, которая вошла в выборку после plan (восстановлена из корзины, передана прежнему ответственному, снова открыта) и чей id попал в диапазон ещё не выполненного чанка, чанк тоже обновит. Поэтому шаг (6) чанка ставит `total = GREATEST(total, processed)` — прогресс не больше 100 %.
4. `bulk.chunk(job_id, no)`, одна транзакция: (1) строка чанка `FOR UPDATE` с `status = 'pending'` — нет строки, значит чанк уже выполнен, выход (повторная доставка безопасна); (2) job в `cancelling` или с заполненным `error` → свой чанк `skipped`; (3) автор `FOR SHARE`: активен, организация, роль и отдел совпадают с `params.access`, а `roles.scope_or_none(role, 'bulk:run')` равен уровню из params — иначе `UPDATE bulk_jobs SET error = 'bulk.author_changed' WHERE id = :id AND error IS NULL` и свой чанк `skipped` (чужие строки чанков не трогаем — иначе два чанка ждали бы друг друга; остальные увидят `error` на шаге 2); получатель `FOR SHARE`: может вести записи и в уровне автора — иначе так же `bulk.target_changed`; `to_team` — из **текущей** строки получателя; (4) `UPDATE <t> SET owner_id = :to, team_id = :to_team, version = version + 1, updated_at = now() WHERE id BETWEEN :lo AND :hi AND owner_id = :from AND <access.sql_filter> AND <живые / открытые>` (у активностей нет `version`, `updated_at` и `deleted_at`: для них только `SET owner_id, team_id`, «открытые» — `done_at IS NULL`); (5) чанк `done`, `rows`; (6) последним — `UPDATE bulk_jobs SET processed = processed + :n, total = GREATEST(total, processed + :n), updated_at = now()`. После коммита — `finalize_if_complete(job_id)`. Порядок блокировок: строка чанка → пользователи → строки данных → job; он совместим с сервисом людей (организация → пользователь `FOR UPDATE`): перевод получателя ждёт конца чанка и видит его записи. Ко всем таблицам job применяется один уровень — `bulk:run` (тест политик проверяет, что он не шире записи по каждой сущности). Чанк не пишет ни аудит, ни события.
5. `finalize_if_complete(job_id)` — один запрос: `UPDATE bulk_jobs SET status = CASE WHEN status = 'cancelling' THEN 'cancelled' WHEN error IS NOT NULL THEN 'failed' WHEN failed_chunks = 0 AND failed_rows = 0 THEN 'done' WHEN chunks_total > 0 AND failed_chunks = chunks_total THEN 'failed' ELSE 'partial_failed' END, finished_at = now() WHERE id = :id AND kind <> 'import_contacts' AND status IN ('running', 'cancelling') AND NOT EXISTS (SELECT 1 FROM bulk_job_chunks WHERE job_id = :id AND status = 'pending') RETURNING status` (в коде `RETURNING status, kind, org_id, owner_id, params` — победителю хватает этого для итога и журнала без чтения job). Его вызывают чанк после коммита (и повторно доставленный чанк, который нашёл свою строку уже не `pending`: вдруг первая копия упала между коммитом чанка и finalize), упавший чанк (`on_failure` базового класса `ChunkTask`: после последнего ретрая, а для неповторяемой ошибки — сразу; чанк `failed`, `failed_chunks += 1`), plan при пустой выборке и `bulk.reap_stuck`. Победитель в той же транзакции вторым `UPDATE` пишет `result = {"rows": {сущность: строк}}` (строки по всем сущностям job, нули включительно). Два чанка закончили одновременно — строку получит только один `UPDATE`, поэтому итог пишется ровно один раз: в той же транзакции — одна строка `audit_log` (`bulk.reassign_owner`, `actor_id` — автор job, в `changes` — параметры без `access`, итоговый `status` и `rows` — строки по сущностям из `GROUP BY entity`) и (с шага 5.12) одно событие `bulk_job.completed` → новому ответственному «вам передано N клиентов», автору — итог job.
6. `GET /api/v1/bulk-jobs/{id}` отдаёт прогресс, `GET …/events` — тот же прогресс потоком SSE (раздел 15).

Чанк обязан быть **идемпотентным**: повторный запуск того же чанка ничего не делает (строка чанка уже не `pending`), а повторный `UPDATE` того же диапазона ничего не находит (`owner_id = :from`). Поэтому `acks_late=True` и ретраи безопасны — упавший воркер не теряет и не дублирует работу, а `processed` не считается дважды.

**Почему не chord.** Группа с финальной задачей (chord) в Celery 5.6 не запускает финальную задачу, если хоть один чанк упал после всех ретраев (тело уходит в `ChordError`), а её счётчик в Redis — ещё одно состояние, которое считает повторно доставленный чанк дважды. Учёт в `bulk_job_chunks` и условный `UPDATE` проще и честнее: упавший чанк даёт `partial_failed`, а не повисшую job. Chord можно показать отдельным экспериментом (этап 10).

**Как API ставит задачи.** Строка `bulk_jobs` — сама себе outbox: API после коммита best-effort ставит `bulk.plan` / `imports.import_contacts` через порт `TaskQueue` (`asyncio.to_thread(celery_app.send_task, …)`, без ретраев публикации). Ошибку брокера только логирует (`task_enqueue_failed`) и всё равно отвечает 202: job в `pending` через 2 минуты подберёт `bulk.reap_stuck`. Через `outbox_events` идут только письма, сообщения и доменные события — у них нет своей таблицы-состояния. В тестах API порт подменяется фейком.

**Смена отдела человека (`move_team`).** `PATCH /users/{id} {team_id}` в своей транзакции (блокировки как на этапе 4: организация → пользователь `FOR UPDATE`) меняет `users.team_id`, пишет `member.updated` и, если у человека есть живые компании, контакты, открытые сделки или открытые активности вне нового отдела, вставляет `bulk_jobs(kind = move_team, owner_id = админ, params = {user_id})`; после коммита — `bulk.plan`. Записи, созданные до перевода, plan видит (создание читает ответственного `FOR SHARE`), созданные после — уже получают новый отдел. Чанк: `SET team_id = :current_team, version = version + 1 WHERE id BETWEEN … AND org_id = :org AND owner_id = :user AND team_id <> :current_team AND <живые / открытые>` (у активностей — без `version`), где `current_team` — из строки человека, прочитанной в чанке `FOR SHARE`: при цепочке переводов записи всегда догоняют текущий отдел. Проверки автора у `move_team` нет — это следствие уже сделанного изменения; отменить её нельзя (409 `bulk.not_cancellable`). Несколько `move_team` разных людей идут параллельно (они не входят в индекс «одна job вида на организацию»): строки у них не пересекаются. Повторный `PATCH` с тем же отделом создаёт новую job, если записи вне отдела остались, — так чинится упавшая.

Как сделано (шаг 5.4, часть B). Job вставляет `MemberService.change` (функция `new_move_team_job` в `app/services/bulk.py`; сервис людей получил порт `TaskQueue`), `params = {user_id, access}` (`Access` админа — для истории, чанк его не проверяет), `team_id` job — отдел админа (обычно NULL), ключа повтора и `request_hash` нет. Проверка «есть что догонять» идёт, когда в `PATCH` передан `team_id` (и тот же) у роли с отделом — так же работает и `viewer` → `sales_rep`, если у человека остались записи. Дальше — общие `plan` / `chunk` / `finalize_if_complete`; выборка — одна функция `move_team_filter` для plan, чанка и `exist_outside_team`. Уточнения к тексту выше: (1) строку человека `FOR SHARE` читает и **plan** — его снимок и отдел не расходятся, пока он режет чанки; (2) чанк ставит ещё `updated_at = now()` (как у `reassign_owner`); у активностей — только `team_id`; (3) отдела у человека нет (стал `org_admin` / `viewer` — это возможно, только когда живых записей не осталось) — выборка пустая (`false()`: в SQLAlchemy `team_id != None` превратилось бы в `IS NOT NULL`), чанк отмечается `done` с 0 строк; (4) журнал — одна строка `bulk.move_team` с `changes = {user_id, status, rows}`.

**Повтор запроса (`Idempotency-Key`).** Заголовок необязателен, 1–64 печатных ASCII-символа (иначе 422). `request_hash` — sha256 канонического JSON `{kind, body}` (`sort_keys`; body — нормализованные параметры: `include` по умолчанию раскрыт во все четыре значения в постоянном порядке, поэтому «не передан» и «все четыре в любом порядке» — один запрос). «Печатные» — `0x20–0x7E`, пробел включительно. Job с тем же `(owner_id = я, key)` и тем же хэшем → 202 с этой job в любом статусе; хэш другой → 422 `bulk.idempotency_key_reused`. Два одинаковых запроса одновременно: второй `INSERT` ловит уникальный индекс → откат, перечитать job по ключу и вернуть её же. Имя нарушенного индекса сервис берёт из исключения asyncpg (`constraint_name`): `uq_bulk_jobs_owner_id_idempotency_key` → повтор, `uq_bulk_jobs_org_id_kind_active` → 409 `bulk.already_running`; если активная job успела закончиться до перечитывания или она не видна мне (перечитывается по уровню `bulk_job:read` автора и «своя» — как `GET /bulk-jobs/{id}`; job админа с `team_id` NULL руководителю не видна), `details.job_id` — `null`: id, который даёт 404, клиенту ни к чему. У импорта хэш берётся только из `{kind}`: ключ проверяется до приёма файла, и тот же ключ — та же job.

### Поток: импорт контактов из CSV

1. `POST /api/v1/contacts/import` — `multipart/form-data`: `file` (CSV) и необязательный `owner_id` (ответственный строк без `owner_email`; по умолчанию — я; `org_admin` обязан указать — 422 `owner.required`). Обработчик не объявляет `UploadFile`: FastAPI читает тело формы до зависимостей, и неаутентифицированный клиент успел бы залить 100 МБ до 401. Поэтому порядок: `authorize` → нет `Content-Length` (`Transfer-Encoding: chunked` прочитался бы без предела) → 411 `import.length_required`; больше `IMPORT_MAX_BYTES` (100 МиБ) → 413 `import.file_too_large` → повтор по `Idempotency-Key` → уже идёт импорт в организации → 409 `bulk.already_running` → `async with request.form(max_files=1, max_fields=1) as form` (временный файл формы закрывается сразу после загрузки в S3; лишний файл или поле — ошибка парсера Starlette 400 → 422 `import.invalid_form`) → проверка заголовка CSV по первым 64 КиБ (ошибки формата — 422 сразу, без job) → `owner_id` через `resolve_owner` → файл в S3 `imports/{org_id}/{uuid}.csv` → `bulk_jobs(kind = import_contacts, params = {file_key, file_name, delimiter, columns, default_owner_id, access})` → commit → `imports.import_contacts(job_id)` → 202 `BulkJobOut` с `import_info: {delimiter, columns, ignored_columns}`. Схема формы для Swagger задаётся через `openapi_extra`.
2. **Формат.** UTF-8, BOM допускается; не декодируется заголовок → 422 `import.invalid_encoding`. Разделитель — `,` или `;` (тот, что даёт больше колонок в заголовке; Excel с украинской локалью сохраняет `;`). Заголовок обязателен, имена без учёта регистра: `email`\*, `first_name`\*, `last_name`, `phone`, `status`, `source`, `tags` (через запятую внутри ячейки), `owner_email`. Нет обязательной или повтор колонки → 422 `import.invalid_header`; пустой файл → 422 `import.empty_file`. Незнакомые колонки (в том числе `id`, `created_at`, `company_id` из нашего экспорта) игнорируются и возвращаются в `ignored_columns`, поэтому свой экспорт загружается обратно. Ведущий апостроф перед `= + - @` снимается (пара к экранированию в экспорте). Лимиты: файл 100 МиБ, `IMPORT_MAX_ROWS` = 1 млн строк (больше → job `failed`, `import.too_many_rows`), поле до 10 000 символов; битый CSV (незакрытая кавычка) → `failed`, `import.malformed_csv` с номером строки. Компания на этапе 5 не импортируется (имя компании не уникально).
3. **Задача** `imports.import_contacts(job_id)` (очередь `imports`, лимит 10 / 12 мин на запуск задачи, повтор продолжает с последнего завершённого шага) идёт шагами; `bulk_jobs.stage` хранит последний завершённый, повтор задачи пропускает готовые шаги. В начале задача берёт `pg_try_advisory_lock(hashtextextended(job_id::text, 0))` на отдельном соединении и держит его до конца: не взялся — работает другая копия (reaper переотправил или Redis выдал повторно), выход без изменений. Перед каждым шагом и чанком — статус (`cancelling` → стоп) и автор (`bulk.author_changed`), после каждого — `updated_at`.
   - **parse**: файл из S3 скачивается во временный файл воркера и читается `csv.reader` (синхронный разбор многострочных полей из async-потока невозможен). Каждая строка проверяется pydantic-моделью `ContactImportRow` с теми же типами полей, что `ContactIn` (общие `Annotated`-типы в `schemas/crm.py`): API и импорт принимают одинаковые email и телефоны. В импорте email обязателен — иначе повторная загрузка файла плодила бы дубли. Одна транзакция: `copy_records_to_table` в `contacts_staging` (только валидные, уже нормализованные строки; `email` в staging — `text`, а не `citext`: у asyncpg нет бинарного кодека для типа из расширения, и COPY упал бы; дубли в stage и сопоставление с результатом merge сравниваются по `lower(email)`; `raw` передаётся строкой `json.dumps`) и в `import_errors` (коды `invalid_columns`, `invalid_encoding`, `validation` с полем, `email_required`), `total`, `stage = parse`.
   - **stage**: одна транзакция set-based `UPDATE` по staging: дубли email в файле — побеждает первая строка, остальные `duplicate_in_file`; `owner_email` → активный `team_lead` / `sales_rep` организации, иначе `owner_invalid`; вне уровня запустившего → `owner_out_of_scope`; пустой — `default_owner_id`. Ошибки — в `import_errors`, `failed_rows`, `stage = stage`.
   - **merge**: последовательные чанки по 5 000 строк `WHERE NOT merged ORDER BY row_no`, каждый — своя транзакция: ответственные чанка `FOR SHARE` (стали неактивными или вышли из уровня → `owner_invalid`; `team_id` новой записи — из `users` на момент merge), затем один statement `INSERT … SELECT … ON CONFLICT (org_id, email) WHERE deleted_at IS NULL DO UPDATE SET … WHERE <access.sql_filter> RETURNING email, (xmax = 0) AS inserted` в CTE, и тем же запросом строки без результата → `import_errors` `duplicate_outside_scope` (raw без данных существующего контакта); `merged = true`, `processed`, `failed_rows`, `result.created` / `result.updated`. Параллельные merge по одному уникальному индексу давали бы конфликты, поэтому чанки идут в одной задаче один за другим (500 тыс. строк — около минуты).
   - **Обновление существующего контакта** (так делает HubSpot): непустая ячейка перезаписывает поле, пустая не трогает; `first_name` обязателен и перезаписывается; теги объединяются с имеющимися без дублей, не больше 20 (лишние новые отбрасываются, строка не считается ошибкой); `status` меняется, только если указан; ответственный и отдел не меняются (передача — это `reassign-owner`); `version + 1`. Email, который есть только у контакта в корзине, не конфликтует (UNIQUE частичный) — создаётся новый контакт.
   - **финал**: свой условный `UPDATE … WHERE status IN ('running', 'cancelling')` (`done`, `partial_failed`, если `failed_rows > 0`, или `cancelled`), одна строка журнала `bulk.import_contacts`, (с 5.12) событие `bulk_job.completed` автору. Строки staging удаляются в той же транзакции, что **любой** финальный переход импорта — и отмена, и `failed`, и reaper.
4. Ошибки строк — `GET /api/v1/bulk-jobs/{job_id}/errors?limit=&cursor=` (keyset по `row_no`), job ищется по уровню `bulk_job:read`. Файлы S3 завершённых импортов старше 7 дней и сироты `imports/*` старше суток (файл залит, а коммит job не прошёл) удаляет `maintenance.cleanup`.

### Остальные массовые операции и выгрузка

- **Закрыть просроченные сделки** — только ручной запуск (автоматическое закрытие без человека молча портит воронку): `POST /api/v1/bulk/close-overdue-deals {before?: date, pipeline_id?}` + `Idempotency-Key`. «Просрочена» — то же, что у `GET /deals?overdue=true`: `status = 'open' AND deleted_at IS NULL AND expected_close_date < :before`; `before` по умолчанию — сегодня по часовому поясу организации, дата позже сегодняшней → 422. Дата фиксируется в `params`, все чанки работают с одной границей. Общий plan, чанк `UPDATE deals SET status = 'lost', closed_at = now(), version = version + 1 …`; этап сделки не меняется. Уведомлений `deal.closed` по каждой сделке нет — одно `bulk_job.completed` автору.
- **Теги сегмента** — `POST /api/v1/bulk/add-tags` и `/bulk/remove-tags` с телом `{tag, filter: {q?, status?, tag?, owner_id?, company_id?}}`: фильтр — те же поля и правила, что у `GET /contacts` (общая функция `ContactRepository.filter_conditions`), пустой — весь уровень. Типизированные массивы (`ARRAY[:t]::varchar(64)[]`), иначе не сработает GIN-индекс. Контакты, у которых уже 20 тегов, пропускаются и считаются в `failed_rows` → `partial_failed`.
- **Выгрузка** — `GET /api/v1/contacts/export?q=&status=&tag=&owner_id=&company_id=` (`export:run`) → `text/csv; charset=utf-8` с BOM, `Content-Disposition: attachment`. Те же фильтры и порядок, что у списка (выгрузка не шире видимости), серверный курсор `session.stream(stmt, execution_options={"yield_per": 2000})`. Колонки: id, email, first\_name, last\_name, phone, status, source, tags, owner\_email, company\_id, created\_at, updated\_at — импорт принимает файл обратно. Значение, начинающееся с `= + - @ \t \r`, получает префикс `'` (CSV-инъекция). Заранее `count` с `LIMIT EXPORT_MAX_ROWS + 1`: больше 1 млн → 422 `export.too_large` («сузьте фильтр»); 10 выгрузок в час на пользователя → 429. В лог — пользователь, фильтр и число строк, без `audit_log`.

### Остальное про объём

- **Пагинация**: keyset (`WHERE (created_at, id) < (:c, :i)`) против OFFSET — замер на 100-й тысяче контактов.
- **Отчёты**: сумма сделок по менеджерам и этапам — materialized view `deal_stats` (живые сделки, группировка по организации, отделу, ответственному, воронке, этапу, статусу и месяцу), `REFRESH … CONCURRENTLY` задачей beat (до этапа 5 — `make refresh-reports`); сравнить с прямым запросом. Отчёт отстаёт от данных на интервал обновления — это нормально для аналитики.
- **Партиции**: `activities` и `audit_log` по месяцам; старые месяцы — `DETACH PARTITION` + архив вместо `DELETE`.
- **Конкурентность**: чанки упорядочены по id против дедлоков; оптимистическая блокировка через `version` для ручных правок карточки; очередь в таблице через `SELECT … FOR UPDATE SKIP LOCKED` — отдельный эксперимент.

### Celery: общая схема

| Компонент | Роль |
| --- | --- |
| Broker (Redis) | очередь сообщений-команд |
| Workers | очереди `default`, `bulk`, `imports`, `notifications`; в compose — два воркера, в Kubernetes (этап 7) — Deployment на очередь |
| Beat | планировщик, **1 реплика**: курсы НБУ, `bulk.reap_stuck`, `trash.purge`, партиции, `reports.refresh`, очистка, напоминания, дайджест |
| Outbox relay | отдельный долгоживущий процесс, а не задача beat (раздел 15) |
| Result backend | не нужен: результаты задач никто не читает (`task_ignore_result=True`), завершение bulk job видно по базе |
| Flower | веб-мониторинг |

Детали (async внутри задач, `visibility_timeout`, `prefetch_multiplier`, маршрутизация, масштабирование) — в разделе 15.

### Событийная система: outbox, потом Kafka

Сервисы не шлют события в брокер напрямую: они пишут их в `outbox_events` в той же транзакции, что и данные. Relay читает неопубликованные события (`FOR UPDATE SKIP LOCKED`) и публикует. Событие не теряется, если коммит прошёл, а брокер недоступен, и не уходит, если коммит откатился.

- **Этап 5:** relay публикует в Celery: команды `email.send` и `telegram.send` и доменные события `record.assigned`, `deal.closed`, `bulk_job.completed` (их разбирает notifier, раздел 16). Команды на отправку в Kafka на этапе 9 не попадают — в них секреты и тексты для конкретного человека.
- **Этап 9:** relay публикует в Redpanda (Kafka API): топики `saleshub.contacts.v1`, `saleshub.deals.v1`, ключ = `org_id`. Потребители — отдельные consumer group: `audit-writer`, `reports` (обновляет агрегаты воронки), `notifier`, `search-indexer`.

|  | Celery + Redis | Redis Streams | RabbitMQ | Kafka |
| --- | --- | --- | --- | --- |
| Модель | очередь задач (команды) | лог с consumer groups | очередь сообщений | распределённый лог событий |
| Повторное чтение | нет | ограниченно | нет | да, по offset |
| Несколько независимых потребителей | неудобно | да | через exchange | да, consumer groups |
| Порядок | нет гарантий | в стриме | в очереди | внутри партиции |
| Для чего в SalesHub | bulk-задачи, импорт, расписание | — | — | доменные события: «сделка выиграна», «контакт создан» |

Вывод: Kafka не заменяет Celery. Celery — «сделай работу», Kafka — «произошло событие, кому интересно — читайте». Для массового UPDATE Kafka не нужна; она нужна, когда одно событие потребляют несколько систем или нужен replay.

## 7. Middleware, ошибки, логирование, Swagger

Каждый запрос получает `request_id`, который проходит через логи API, задачи Celery и события outbox; все ошибки отдаются в одном формате с машинным кодом.

### Middleware (порядок снаружи внутрь)

1. `RequestIdMiddleware` — берёт `X-Request-ID` (если он не длиннее 64 символов и без спецсимволов, иначе через него можно подделать строки лога) или генерирует, кладёт в `contextvars` и в ответ.
2. `AccessLogMiddleware` — метод, путь-шаблон (`/api/v1/deals/{deal_id}`, а не реальный id; query не пишем — в нём бывают токены; без маршрута — `<unmatched>`), статус, длительность, с этапа 2 — user\_id. `/health/*` пишется на уровне debug, чтобы пробы не засоряли лог.
3. `MetricsMiddleware` — гистограмма латентности и счётчик запросов по шаблону пути и статусу (раздел 8, этап 8).
4. `CORSMiddleware` — для фронта (добавляется вместе с фронтом, раздел 13).
5. (опц.) `GZipMiddleware`, лимит размера тела.
6. `UnhandledErrorMiddleware` — самый внутренний слой: любое неожиданное исключение превращает в 500 общего формата, traceback пишет только в лог.

Механика на чистом ASGI (а не `BaseHTTPMiddleware`) — заодно разобраться, почему `BaseHTTPMiddleware` плохо дружит со стримингом.

**Почему 500 ловит middleware, а не `add_exception_handler(Exception)`.** Обработчик `Exception` Starlette вызывает в `ServerErrorMiddleware` — снаружи всех наших слоёв. Тогда ответ 500 ушёл бы без `X-Request-ID`, в логе ошибки не было бы `request_id`, а uvicorn напечатал бы traceback второй раз. `UnhandledErrorMiddleware` стоит внутри `RequestIdMiddleware` и `AccessLogMiddleware`, поэтому оба видят ошибку как обычный ответ 500.

### Ошибки

```python
class AppError(Exception):
    status = 500
    code = "internal"
    def __init__(self, code: str | None = None, **details): ...   # code уточняет класс, details → в ответ
    @property
    def title(self) -> str: ...   # текст из app/texts/uk.py по code, иначе — текст класса

class NotFound(AppError):        status, code = 404, "not_found"
class Forbidden(AppError):       status, code = 403, "forbidden"
class Unauthorized(AppError):    status, code = 401, "auth.invalid_token"
class Conflict(AppError):        status, code = 409, "conflict"          # version mismatch, дубль
class ValidationFailed(AppError): status, code = 422, "validation"
class PayloadTooLarge(AppError): status, code = 413, "payload_too_large"  # файл импорта больше лимита (этап 5)
class RateLimited(AppError):     status, code = 429, "rate_limited"
class ServiceUnavailable(AppError): status, code = 503, "unavailable"   # Postgres, Redis, OPA, SMTP

raise NotFound(entity="deal", id=123)      # → 404, code=not_found, details={"entity": "deal", "id": 123}
raise Conflict("member.last_admin")        # → 409, code=member.last_admin, title «Конфлікт даних»
```

Сервисы и репозитории бросают только `AppError`-наследников, ничего не знают про HTTP-коды фреймворка. Человеческий текст (`title`) в коде не пишется — он берётся из `app/texts/uk.py` по `code`. Обработчики:

| Исключение | Ответ | Лог |
| --- | --- | --- |
| `AppError` | его status + тело ниже | warning (4xx), error (5xx) |
| `RequestValidationError` | 422, `code=validation`, в `details.errors` — `loc`, `type` и украинское `message`; введённое значение (`input`) не возвращается — там может быть пароль | info |
| `HTTPException` Starlette (нет маршрута, не тот метод) | 404 `not_found`, 405 `method_not_allowed` с заголовком `Allow` | — |
| `IntegrityError` (unique, SQLSTATE 23505) | 409 `conflict` (у ограничений со своим кодом — он: `uq_contacts_org_id_email` → `contact.duplicate_email`); остальные нарушения (FK, CHECK) — ошибка в коде → 500 | warning |
| `DBAPIError` с SQLSTATE класса 22 (число вне диапазона, NUL в тексте) | 422 `validation` — ввод, который пропустила схема | info |
| `StaleDataError` (версия записи изменилась между чтением и записью) | 409 `version.conflict` | warning |
| любое другое | 500 `internal`, без стектрейса наружу (`UnhandledErrorMiddleware`) | error + traceback |

Формат тела (по мотивам RFC 9457 problem+json):

```json
{
  "type": "https://saleshub.local/errors/not_found",
  "code": "not_found",
  "title": "Не знайдено",
  "status": 404,
  "request_id": "7b0c…",
  "details": {"entity": "deal", "id": 123}
}
```

**Язык.** Все тексты для людей — украинские: `title` ошибок, сообщения валидации, письма, сообщения Telegram-бота. Поле `code` — стабильный английский ключ: по нему работают клиенты и тесты, его никогда не переводят. Тексты лежат в одном месте (`app/texts/uk.py` и Jinja2-шаблоны писем), а не по коду — так позже можно добавить второй язык. Стандартные сообщения валидации Pydantic (английские) переводятся в обработчике `RequestValidationError` по типу ошибки.

### Логирование

- structlog, JSON в stdout (в dev — цветной консольный рендер), одна настройка для API и воркеров.
- В каждой строке: `ts`, `level`, `event`, `request_id`, `user_id`, `path`, для задач — `task_id`, `job_id`.
- `request_id` передаётся в Celery через заголовки задачи (`before_task_publish` / `task_prerun`) и в колонку `outbox_events.request_id`; у задач beat, где запроса нет, `request_id` = `task_id`. Воркеры настраивают логи той же функцией `configure_logging` (сигнал `setup_logging`), поэтому формат строк у API и воркеров один.
- SQL-логи — только в dev и только медленные запросы (событие `before/after_cursor_execute`, порог 200 мс).
- Никогда не логируются: пароли, токены, тело `/auth/login`.
- Сбор логов в кластере: Loki + Grafana Alloy (собирает логи с подов), поиск по request\_id в той же Grafana.

### Swagger / OpenAPI

- `OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")` → кнопка **Authorize** работает из коробки; access живёт 1 час: после ошибки auth.token\_expired вызвать POST /auth/refresh и вставить новый токен или снова нажать Authorize.
- Теги по ресурсам, версия API в префиксе `/api/v1`.
- У каждой операции: требуемое право в описании (раздел 5) и `responses={401: …, 403: …, 404: …}` с моделью `ErrorOut`.
- Примеры тел (`model_config = {"json_schema_extra": {"examples": [...]}}`) для bulk-эндпоинтов.
- Swagger отключается в prod-профиле флагом `DOCS_ENABLED`; схема экспортируется в CI в `openapi.json` и на её основе генерируется TS-клиент для фронта.

### Health-эндпоинты

`/health/live` — процесс жив (для liveness); внешнее не проверяет, иначе при падении БД Kubernetes перезапускал бы все поды. `/health/ready` — отвечают Postgres (`SELECT 1`) и Redis (`PING`), на каждую проверку 2 секунды (для readiness, без него pod не получает трафик); иначе 503 `unavailable` и `details.failed: ["postgres", "redis"]`.

## 8. Метрики, дашборды и алерты

Prometheus собирает метрики (pull по `/metrics`), Grafana их показывает, а алерты описываются правилами Prometheus и доставляются Alertmanager-ом в Telegram. Сама Grafana тоже умеет алертить, но в проекте источник правды — правила в git (`deploy/monitoring/rules/*.yaml`).

### Что собираем

| Источник | Как | Ключевые метрики |
| --- | --- | --- |
| API | `prometheus-client`, свой middleware, `/metrics` | `http_requests_total{route,method,status}`, `http_request_duration_seconds` (histogram), `authz_denied_total{action}` |
| Бизнес | счётчики в сервисах | `bulk_jobs_total{kind,status}`, `bulk_rows_processed_total`, `outbox_lag_seconds` |
| Celery | celery-exporter (по событиям воркеров) | задачи по статусам, время выполнения, длина очередей |
| PostgreSQL | postgres\_exporter | соединения, долгие транзакции, размер таблиц, cache hit, dead tuples |
| Redis | redis\_exporter | память, клиенты, длина списков-очередей |
| Kubernetes | kube-prometheus-stack | CPU/память подов, рестарты, OOMKilled |
| OPA | встроенный /metrics сайдкара | время решения, ошибки, число запросов; плюс authz\_denied\_total{action} со стороны API |

Несколько процессов uvicorn в одном поде → `PROMETHEUS_MULTIPROC_DIR`; в Kubernetes проще один процесс на под и масштабирование репликами.

### Дашборды (как код, JSON в `deploy/monitoring/dashboards/`)

1. **API RED**: rate, errors, duration p50/p95/p99 по маршрутам.
2. **Bulk jobs**: активные job, строк/сек, ошибки чанков, длина очереди `bulk`.
3. **Postgres**: соединения, медленные запросы (pg\_stat\_statements), блокировки.
4. **Authz**: отказы 403 по правам — видно, если матрица ролей настроена неверно.

### Алерты

| Алерт | Условие | Severity |
| --- | --- | --- |
| `ApiHighErrorRate` | доля 5xx > 2% за 5 мин | critical |
| `ApiSlowP95` | p95 > 500 мс за 10 мин | warning |
| `CeleryQueueBacklog` | очередь `bulk` > 1 000 задач 10 мин | warning |
| `BulkJobStuck` | `bulk.reap_stuck` сдался: job в `failed` с `bulk.stuck` после трёх повторных отправок | warning |
| `OutboxLag` | старейшее неопубликованное событие > 60 с | critical |
| `CeleryBeatDown` | heartbeat beat старше 3 мин | critical |
| `PostgresLongTransaction` | транзакция > 5 мин | warning |
| `PodCrashLooping` | > 3 рестарта за 15 мин | critical |
| OpaUnavailable | ответы 503 unavailable от authorize > 0 за 2 мин | critical |
| EmailSendFailing | ошибки notifications.send\_email > 5 за 10 мин | warning |
| FxRatesStale | в рабочий день после 20:00 нет курса НБУ на завтра | warning |
| CeleryTaskFailureRate | > 5% задач очереди падают окончательно (после ретраев) за 10 мин | critical |
| SearchIndexerLag | consumer lag search-indexer > 10 000 сообщений 5 мин или кластер Elasticsearch в статусе red | warning |

Путь алерта: правило Prometheus → Alertmanager (группировка, подавление дублей, тишина на время работ, маршрутизация по severity) → Telegram-бот (receiver `telegram_configs` есть в Alertmanager из коробки). Warning идут в один чат, critical — в другой с упоминанием.

Правила тестируются `promtool test rules` в CI — это тоже часть «понять, как работает».

## 9. Миграции Alembic

Миграции запускаются отдельным шагом до выкатки нового кода (Job `migrate` в Kubernetes, отдельный job в CI), а не при старте каждого пода — иначе несколько реплик одновременно пытаются мигрировать.

### Правила

- `env.py` асинхронный (`create_async_engine(url, poolclass=NullPool)` + `run_sync`), URL из той же `Settings`, что и приложение; тесты передают свой через `config.attributes["database_url"]`.
- `MetaData(naming_convention=…)` — у всех индексов и constraint-ов предсказуемые имена, иначе `downgrade` и autogenerate ломаются.
- Autogenerate — черновик, миграция всегда читается и правится руками.
- Схема и данные — разные ревизии. Данные (backfill) на больших таблицах — не в миграции, а bulk job-ом из раздела 6.
- Индексы на больших таблицах: `op.create_index(..., postgresql_concurrently=True)` внутри `with op.get_context().autocommit_block():` — `CONCURRENTLY` не работает в транзакции.
- Каждая миграция имеет рабочий `downgrade` (проверяется в CI).
- Партиции `activities` и `audit_log`: родитель и SQL-функцию `ensure_monthly_partitions(таблица, с, по)` создаёт миграция; месячные партиции `<таблица>_pYYYY_MM` — заранее на 2 месяца вперёд. Функция идемпотентна (`CREATE TABLE IF NOT EXISTS … PARTITION OF`), её вызывают: сервис `migrate` после `alembic upgrade` (`python -m app.cli ensure-partitions`), `seed_big.sql` для своего окна в 24 месяца и, с этапа 5, задача beat. DEFAULT-партиции нет: строка вне диапазона — ошибка, а не тихо растущая «прочая» партиция. Партиции — не модели, поэтому `env.py` Alembic пропускает таблицы `*_pYYYY_MM` при сравнении схемы; materialized view отчётов тоже описан вне `Base.metadata`.

### Изменения без простоя (expand → contract)

Пример: перевести суммы в целые центы — с `deals.amount_usd numeric(14,2)` на `deals.amount_usd_cents bigint`.

1. **Expand**: добавить nullable-колонку `amount_usd_cents`; код пишет в обе.
2. **Backfill**: bulk job заполняет новую колонку чанками.
3. **Switch**: код читает только новую колонку; `NOT NULL` через `CHECK … NOT VALID` → `VALIDATE CONSTRAINT` (без долгой блокировки).
4. **Contract**: отдельным релизом удалить старую колонку.

### Проверки в CI

1. `alembic upgrade head` на чистой базе.
2. `alembic check` — модели и миграции не расходятся (нет незаписанных изменений).
3. `alembic downgrade base` → `upgrade head` — миграции обратимы.
4. Проверка «одна голова» (`alembic heads` = 1), чтобы параллельные ветки не сломали историю.

## 10. Тесты

Три уровня: unit — сервисы и authz без БД (fake UoW), integration — реальные Postgres и Redis в контейнерах, e2e — несколько сценариев против поднятого стенда. Самый ценный набор — матрица прав «роль × эндпоинт × чужой/свой объект», генерируемая из кода.

### Уровни

| Уровень | Что тестируем | Инструменты | Скорость |
| --- | --- | --- | --- |
| unit | `Access.matches` / `ensure_allowed`, `sql_filter` (компиляция SQL), сервисы с `FakeUnitOfWork`, правила управления пользователями (нельзя менять себе роль, отдел обязателен), нарезка чанков, маппинг ошибок | pytest, pytest-asyncio, polyfactory | секунды |
| integration | репозитории на настоящем Postgres, API через `httpx.AsyncClient(transport=ASGITransport(app))`, миграции, Celery-задачи с реальным воркером на Redis из testcontainers, бот через `feed_update` | testcontainers (postgres, redis), factory-boy | десятки секунд |
| e2e (smoke) | логин → создать контакт → передать клиентов (bulk) → дождаться job | pytest против `tilt`/kind-стенда | минуты, в CD |
| нагрузка (опц.) | список контактов и bulk на 5 млн | locust / k6 | вручную |
| policy (Rego) | роль × эндпоинт (allow / 403), опечатки в действиях и уровнях `data.yaml` | opa test --coverage, Regal (линтер) | миллисекунды |
| auth flow | приглашение → письмо → accept → login → refresh → повтор старого refresh (отзыв семьи) → reset | integration + Mailpit API (чтение писем из теста), freezegun для сроков | секунды |

### Изоляция БД в integration-тестах

- Один контейнер Postgres на сессию pytest, `alembic upgrade head` один раз.
- Каждый тест — во внешней транзакции с `SAVEPOINT` (`join_transaction_mode="create_savepoint"`), откат после теста: быстро и без очистки таблиц. Подменённая `get_session` при ошибке в запросе откатывает его SAVEPOINT — как настоящий запрос, который при ошибке ничего не сохраняет. После такого отката объекты сессии «протухают», поэтому значения, нужные тесту после ответа 4xx (например, email), держат в переменных.
- Сессия подменяется через `app.dependency_overrides[get_session]`.
- Тесты bulk-операций, которые коммитят в нескольких транзакциях (воркер), используют `TRUNCATE` в фикстуре — отдельный маркер `@pytest.mark.committing`. На этапах 2–4 такой тест был один (гонка refresh и logout-all) — он сам удаляет своего пользователя.
- Outbox внутри тестовой транзакции: строки не коммитятся, и отдельный relay их не видит. Поэтому фикстура `deliver_outbox` вызывает ту же функцию `publish_batch` со встроенным «издателем» (`InlinePublisher`), который не ставит задачу в Celery, а запоминает её; после коммита батча фикстура вызывает обработчик (`send_email`, `notify`…) в сессии теста — как воркер после relay, а не внутри открытой транзакции батча (иначе откат обработчика откатил бы и батч), — и так по кругу, пока неопубликованные строки не кончатся (notify порождает новые команды). Тесты писем читают Mailpit через `inbox(email)` = `deliver_outbox` + чтение писем. Настоящий путь relay → Redis → воркер проверяет отдельный `committing`-тест. Письма (шаг 5.3, `tests/integration/test_notifications.py`): повтор того же события — одно письмо и одна строка `sent_notifications`; обрыв SMTP — строки нет, токен в payload, повтор отправляет; после успеха токена в payload нет; два события с одним `dedup_key` — одно письмо; откат транзакции приглашения после `add_email` — ни команды, ни токена (что команда была записана в откатившейся транзакции, тест проверяет в подменённом `commit` до ошибки); `committing`: relay + `celery_worker` → письмо в Mailpit, токен стёрт; повторная публикация того же `event_id` второго письма не даёт — повторяется письмо без токена (`already_member`), иначе дубль погас бы на стёртом токене (`StrictUndefined`), а не на дедупе; следом в ту же очередь идёт письмо-маркер: воркер solo берёт задачи по порядку. SMTP Mailpit-контейнера воркеру передаёт `runtime.configure` в фикстуре `celery_worker`. Сам relay (шаг 5.2, `tests/integration/test_outbox.py`): `publish_batch` — в транзакции теста с поддельным издателем (порядок, `request_id`, откат при ошибке издателя и неизвестном типе); цикл — `committing`-тестами со своими соединениями и `TRUNCATE outbox_events` после: NOTIFY будит relay быстрее секунды при опросе раз в 30 с, без LISTEN работает опрос, после `pg_terminate_backend` relay подписывается снова, два батча с `SKIP LOCKED` не берут одно событие, остановка дожидается батча; тестовый маршрут события ставит `monkeypatch` в `ROUTES`, тестовая задача на `celery_app` отдаёт id события и `request_id` через `queue.Queue`.
- Письма: контейнер Mailpit на сессию pytest, тест читает письма через его REST API (`/api/v1/search`, `/api/v1/message/{id}`); у каждого теста свой email, потому что Mailpit и Redis общие на весь запуск.
- Массовые операции (шаг 5.4): `tests/integration/test_bulk.py` — сервис в транзакции теста (plan и чанки вызываются напрямую): границы чанков по строкам, повтор plan и чанка ничего не меняет и не считает `processed` дважды, `bulk.author_changed` (отключён, переведён) и `bulk.target_changed`, отдел получателя — на момент чанка, упавший чанк → `partial_failed`, все упали → `failed`, пустая выборка → сразу `done`, чанки по одной строке (`BULK_CHUNK_SIZE = 1`), запись, вошедшая в выборку после plan внутри диапазона чанка, — обновлена, и `total` вырос вместе с `processed`, передаются только живые и открытые записи, `version + 1`, одна строка журнала со строками по сущностям; `test_bulk_api.py` — все коды ошибок `POST /bulk/reassign-owner`, повтор и переиспользование `Idempotency-Key`, гонка по ключу (первая проверка подменена и «не видит» job), 409 с `job_id` только видимой job (job админа руководителю — `null`), автор видит свою job и после перевода в другой отдел, SSE готовой job; `test_bulk_worker.py` (`committing`) — два чанка заканчивают одновременно (строку job держит третья транзакция) и два finalize наперегонки → один итог, и конец в конец на `celery_worker` с `BULK_CHUNK_SIZE = 50` (размер подменяется в `runtime._settings`). Генератор событий SSE — unit-тест `tests/unit/test_bulk.py` (и `: ping`, пока снимок не меняется). Смена отдела (`move_team`, часть B шага): `tests/integration/test_move_team.py` — через `PATCH /users/{id}` и фикстуру `run_job` (plan и все чанки в сессии теста): переходят только живые и открытые записи (закрытые сделки, выполненные активности и корзина остаются в прежнем отделе), одна открытая активность — уже повод для job, а одна история — нет; цепочка из двух переводов сходится к последнему отделу (чанки первой job идут после второго перевода, запись, созданная между переводами, достаётся второй); перевод обратно до запуска job — новой job нет, первая ничего не меняет; две `move_team` разных людей одной организации активны одновременно; повторный `PATCH` с тем же отделом чинит упавшую job; закрытая сделка и выполненная активность, снова открытые после перевода, — в новом отделе, а прежний руководитель открыть их не может (403 `out_of_scope`). Тесты этапа 4, ждавшие 409 `member.has_records` на смену отдела (`test_crm_rules.py`, `test_deals.py`), теперь ждут 200 с `bulk_job_id` и после job — записи в новом отделе; гонка «создание записи против перевода ответственного» (`test_members_concurrency.py`) — перевод ждёт создание, создаёт job, и после неё запись в новом отделе.
- Отмена, reaper и упавшие задачи (шаг 5.5): `tests/integration/test_bulk_cancel.py` — все переходы и коды `POST /bulk-jobs/{job_id}/cancel` (pending → `cancelled` с журналом, и plan после этого ничего не делает; running → `cancelling`; повтор и `cancelled` — 202 без изменений; финальные и `move_team` — 409; невидимая — 404; уровень отмены уже чтения — 403; автор отменяет свою job после перевода), отмена посреди job (чанки после неё `skipped`, финал `cancelled`, `processed` и журнал — строки успевших чанков); `test_bulk_reaper.py` — каждое правило в транзакции теста (`updated_at` сдвигается назад), повтор прохода сразу ничего не шлёт, занятая очередь `bulk` откладывает (3) и не считает попытку, граница `BULK_MAX_REAPS` для pending, чанковой, отменяемой и импортной job, `broker_queue_length` на Redis контейнера; `test_bulk_worker.py` (`committing`) — два прохода одновременно → одна отправка, job под чужой блокировкой пропускается, сдавшийся reaper не ждёт строку выполняющегося чанка, и `bulk.reap_stuck` на `celery_worker` поднимает job с потерянным plan до `done`; `test_failed_tasks.py` — строка из `task_failure` на `celery_worker` (контекст, очередь, аргументы, traceback), повторная доставка того же `task_id` — одна строка, провал после всех ретраев — одна строка с `retries = 5`, список, карточка, 404, перезапуск по имени с журналом, 409 трёх видов, 503 без изменений; unit — усечение текстов, eager не пишется, ошибка записи только в лог, `replayable` из реестра, расписание reaper.

### Матрица прав (генерируется)

```python
# tests/integration/test_access_matrix.py (сокращённо)
CASES = {   # как вызвать каждый маршрут routes.rego: тело и чьи объекты подставлять в путь
    "PATCH /api/v1/teams/{team_id}": Case(lambda: {"name": unique("Відділ")}, ("same_org", "other_org")),
    ...
}

def test_every_route_has_a_case(route_actions):          # маршруты — у живого OPA
    assert set(route_actions) == set(CASES)              # новый маршрут без кейса — CI падает

@pytest.mark.parametrize(("role", "route", "target"), all_cases())
async def test_access(role, route, target, route_actions, client, ...):
    world = await build_world(...)                       # две организации, люди всех ролей
    resp = await client.request(method, url_for(route, target, world), ...)
    assert resp.status_code == expected_status(role, route_actions[route], target, success)
```

На этапе 3 цели — `same_org` и `other_org` (объекты организации). С этапа 4 у CRM-маршрутов цели `own` (запись `sales_rep`-а из «мира»), `same_team` (коллеги из его отдела), `same_org` (другой отдел) и `other_org`; тело запроса зависит от роли (ответственный подставляется из «мира», `version` — текущая), а у `Case` есть query-параметры (канбан требует `?pipeline_id=`). Каждый шаг этапа 4, который добавляет маршруты, добавляет и их кейсы — иначе `test_every_route_has_a_case` падает. Этап 5 — так же: у `reassign-owner` цель — `from_user` из `same_team` / `same_org` / `other_org` (вне `member:read` автора — 422), у `bulk-jobs/{job_id}` — job из фабрики `make_job` в финальном статусе (SSE сразу отдаёт снимок и закрывается); цель `own` здесь — job **самого вызывающего** (у viewer `bulk_job:read` — own: его собственные job), `same_team` / `same_org` / `other_org` — job менеджеров, как у CRM-записей; у `cancel` — job в `running` (у каждой цели свой `kind`: активная job вида в организации одна), у `/admin/failed-tasks/{failed_task_id}` — цель `system`, строка `failed_tasks` (2xx только у супер-админа); импорт идёт с маленьким CSV, фейковыми `FileStorage` и `TaskQueue` через `dependency_overrides` (фейк `TaskQueue` подменён для всех тестов API в фикстуре `app`: иначе `bulk.plan` ушёл бы в Redis теста, и сессионный воркер выполнял бы задачи над незакоммиченными данными); выгрузка — 200 `text/csv` без чужих строк. Чтобы сотня кейсов шла за секунды, хэш пароля считается один раз на запуск, а access-токен выпускается напрямую через `KeySet` — без логина с argon2.

Ожидаемый ответ считается из `data.yaml`: действия нет у роли → 403; чтение или изменение одного объекта: объект в уровне роли → 2xx, вне уровня чтения → 404, в уровне чтения, но вне уровня действия (чужая компания у `sales_rep`) → 403; создание или изменение, после которого объект выходит из уровня (чужой ответственный или отдел) → 403 `out_of_scope`; список → 200, но без строк вне уровня. Супер-админ на обычных эндпоинтах → 403.

Плюс `test_routes_match_policy` и тест «список не возвращает чужого»: создать контакты в двух организациях, проверить, что `GET /contacts` от sales\_rep org A не содержит ничего из org B, а bulk-update не трогает чужие строки.

**Планы запросов (этап 4).** `make explain` (`scripts/explain_lists.py`) на сгенерированных данных строит запросы всех списков и отчётов теми же методами `*_query()` репозиториев, что и API, для пользователей каждой роли из самых крупных организаций (и для менеджера с 50× клиентов), на первой и глубокой странице, с частым и редким `?q=`, и прогоняет `EXPLAIN (ANALYZE, BUFFERS)` — обычный и с `plan_cache_mode = force_generic_plan` (так выполняет asyncpg после пяти запусков). Ошибка — `Seq Scan` по `companies`, `contacts`, `deals`, `activities` (и её партициям) больше 1 000 страниц; запрос дольше 300 мс — предупреждение (индекс используется, но данных много — частая подстрока в `?q=`). Это автоматическая проверка «Готово, когда» этапа 4: на seed-данных 224 плана, Seq Scan — 0 (`docs/experiments.md`).

### Celery в тестах

- `task_always_eager` не используем: тесты асинхронные, и eager-задача внутри async-теста вызвала бы `run_until_complete` при уже запущенном loop pytest (RuntimeError); к тому же eager пропускает сериализацию, заголовки, маршрутизацию и acks.
- unit: сервисы и функции (`FxService.load_rates`, `ensure_partitions`, `finalize_if_complete`, правила получателей, ключи дедупа) вызываются напрямую; задача — тонкая обёртка, её отдельно не тестируем.
- integration (маркеры `worker` и `committing`): сессионная фикстура `celery_worker` запускает **наше** приложение `celery.contrib.testing.worker.start_worker(celery_app, pool="solo", perform_ping_check=False)` (ping-задаче нужен result backend, а его нет) в отдельном потоке со своим event loop, брокер — db 1 Redis из testcontainers (адрес брокера `celery_app` меняет сессионная autouse-фикстура `celery_broker` — для всех integration-тестов, не только с воркером: иначе `.delay()` из эндпоинта ушёл бы в `localhost:6379` из `get_settings()`), `runtime.configure(settings)` с адресами контейнеров; воркер слушает все четыре очереди, после сессии фикстура вызывает `runtime.shutdown()`. Тест ждёт результат опросом базы или Redis (тестовые задачи в самом модуле теста отдают его через `queue.Queue` — воркер в том же процессе); данные коммитятся, после теста — `TRUNCATE`. НБУ и Telegram подменяет respx — он работает и в потоке воркера.
- Бот: `create_dispatcher(...)` и `dp.feed_update(bot, update)` без сети — обработчики возвращают метод (`return message.answer(...)`), тест сравнивает его текст.
- Время: freezegun для расписания, напоминаний и дайджеста.

Покрытие собирается `pytest-cov`, порог в CI — 80% для `services/` и `authz/`, остальное без жёсткого порога.

## 11. Деплой: compose → Kubernetes → Tilt

Tilt — не альтернатива Kubernetes, а dev-инструмент поверх него: он собирает образы, применяет манифесты в локальный кластер и синхронизирует изменённый код в поды без пересборки. Поэтому порядок такой: сначала docker compose (быстрый старт, этапы 1–6), затем локальный кластер k3d + Helm-чарт (этап 7), и уже над ним Tilt.

### Шаг 1 — docker compose

Сервисы: `api`, `worker`, `worker-bulk`, `beat`, `outbox-relay`, `flower`, `postgres`, `redis`, `migrate` (одноразовый), `seed` (одноразовый), `telegram-bot` (по профилю). `make up` — и можно работать. Этого достаточно для разделов 2–10.

Плюс два сервиса для этого проекта: `opa` (политики из `./policies`, `--watch` — правка `.rego` применяется сразу) и `mailpit` (SMTP-ловушка: SMTP на 1025, веб на `localhost:8025`).

Файл — `deploy/compose/docker-compose.yml`, команды — в `Makefile` (`make help`). Что есть после этапа 1:

| Сервис | Порт на хосте | Заметки |
| --- | --- | --- |
| `api` | 8000 | код смонтирован с хоста, `--reload` подхватывает правки без пересборки образа; `backend/secrets` (ключ JWT) — только на чтение; стартует после `migrate` и `seed` |
| `migrate` | — | одноразовый `alembic upgrade head`, `ensure-partitions`, с этапа 5 — `ensure-bucket` (бакет в RustFS, идемпотентно: RustFS сам бакеты не создаёт); `api` стартует только после его успешного завершения |
| `seed` (с этапа 2) | — | одноразовый `create-superadmin` после `migrate`; dev-логин супер-админа задан в compose, команда идемпотентна |
| `postgres` 18 | `${POSTGRES_PORT:-5432}` | том монтируется в `/var/lib/postgresql` (у образа 18 данные в `/var/lib/postgresql/18/docker`) |
| `redis` 8 | `${REDIS_PORT:-6379}` | если порт занят другим проектом — поменять в `deploy/compose/.env` (шаблон `.env.example` рядом) и в `backend/.env` |
| `pgadmin` | 5050 | без экрана входа; сервер и пароль заданы заранее (`servers.json` + `pgpass`, права 600 ставятся при старте контейнера) |
| `redisinsight` | 5540 | подключение к Redis стенда создаётся само (`RI_REDIS_HOST`) |
| `mailpit` | 1025, 8025 | |
| `rustfs` | 9000 (S3), 9001 (консоль `/rustfs/console/`) | S3-совместимое хранилище вместо MinIO: официальные образы MinIO больше не публикуются |
| `worker` (этап 5) | — | `celery worker -Q default,notifications -c 4`: короткие задачи и письма; экземпляр один, поэтому лимит писем 50 в минуту общий |
| `worker-bulk` (этап 5) | — | `-Q bulk,imports -c 4`: чанки и импорт — тяжёлая работа, её убивает chaos-тест, не трогая письма. Оба воркера в dev запускаются через `watchfiles` (у Celery нет `--reload`; на SIGINT он дорабатывает задачу; SIGTERM от `docker stop` watchfiles превращает в SIGINT, `--sigint-timeout 110`), смонтированы код и `policies/` (файл ролей), `stop_grace_period: 120s` — больше лимита чанка (90 с), healthcheck — `celery inspect ping -d <имя>@$HOSTNAME` (воркеры запускаются с `-n worker@%h` / `-n bulk@%h`; ping без адресата был бы зелёным, пока жив хоть один воркер). Четыре отдельных Deployment по очередям — на этапе 7 |
| `beat` (этап 5) | — | `celery beat -s /tmp/celerybeat-schedule`, ровно один; правку расписания применяет `make restart-workers`. Без healthcheck: `make up --wait` для таких сервисов ждёт только запуска; что beat жив, видно по ключу `workers:heartbeat` |
| `outbox-relay` (этап 5) | — | `python -m app.events.relay` через `watchfiles` (`--sigint-timeout 25`: SIGINT — relay дорабатывает батч и выходит), смонтирован только код (файл ролей не нужен), `stop_grace_period: 30s`; без healthcheck — `make up --wait` ждёт только запуска, как у beat; логи и перезапуск — `make logs-workers` / `make restart-workers` |
| `flower` (этап 5) | 5555 | образ `mher/flower:2.2.0`, без входа (порт только на localhost), брокер — db 1, healthcheck — `/healthcheck`; в Kubernetes — basic auth из SealedSecret |
| `telegram-bot` (этап 5) | — | `profiles: [telegram]`: `make up` его не запускает, `make bot` — запускает. Токен и имя бота — `TELEGRAM_BOT_TOKEN`, `TELEGRAM_BOT_USERNAME` в `deploy/compose/.env`; без токена бот завершается с понятной ошибкой, отправка в Telegram пропускается, а `POST /me/telegram/link` отвечает 503 `telegram.disabled` |

Все порты открыты только на `127.0.0.1`: у pgAdmin нет входа, у Redis нет пароля, поэтому из сети стенд не виден. `make up` ждёт, пока все сервисы станут healthy (`--wait`), и печатает адреса UI. Для запуска API без Docker (`make run`, отладка в IDE) — `make infra`: только Postgres, Redis и Mailpit. `make up` и `make run` сначала вызывают `make keys`.

### Шаг 2 — Kubernetes локально

Кластер: **kind** (Kubernetes in Docker) или **k3d** (k3s in Docker) — оба поднимаются одной командой и работают с Tilt. Выбран k3d — он легче по памяти; kind используется только в CI для e2e. Пакетирование — свой Helm-чарт `deploy/helm/saleshub`; Postgres и Redis — готовые Bitnami-чарты (в проде их обычно заменяет managed-сервис).

| Объект Kubernetes | Для чего в SalesHub |
| --- | --- |
| Deployment `api` | FastAPI, 2 реплики, liveness `/health/live`, readiness `/health/ready` |
| Deployment `worker-default`, `worker-bulk` | Celery-воркеры на разные очереди и с разными ресурсами; плюс worker-imports и worker-notifications (раздел 15) |
| Deployment `beat` | `replicas: 1`, стратегия `Recreate` — чтобы при выкатке не было двух beat |
| Deployment `flower` | мониторинг Celery |
| Job `migrate` (Helm hook `pre-install,pre-upgrade`) | `alembic upgrade head` до выкатки кода |
| Job `seed` (hook `post-install`) | супер-админ из Secret (идемпотентно) |
| Service / Ingress (ingress-nginx) | `api.saleshub.localhost`, `flower.saleshub.localhost`, `grafana.saleshub.localhost` |
| ConfigMap / Secret | настройки; секреты приходят как SealedSecret, контроллер превращает их в обычный Secret |
| HPA (после этапа 7) | масштабирование `api` по CPU |
| KEDA ScaledObject (опц.) | масштабирование `worker-bulk` по длине очереди Redis — наглядно на bulk job |
| kube-prometheus-stack | Prometheus, Grafana, Alertmanager (раздел 8) |
| Сайдкар opa в поде api | решения о доступе по localhost:8181; политики из ConfigMap (позже — OCI-бандл) |
| Deployment mailpit (только dev) | ловит письма приглашений и сброса пароля; mail.saleshub.localhost |
| Deployment outbox-relay | публикация событий outbox в Celery, позже в Kafka (раздел 15) |
| Deployment telegram-bot | 1 реплика; OPA не нужен — бот показывает только данные самого пользователя (раздел 16) |

Полезно один раз пройти руками: `kubectl get pods -w` во время выкатки, `kubectl logs -f`, `kubectl describe pod` для упавшего пода, `kubectl port-forward`, `helm upgrade --atomic` и откат `helm rollback`.

### Секреты: Sealed Secrets с первого дня

Секреты проекта: пароль Postgres, приватный RS256-ключ для JWT, пароль супер-админа, SMTP, токен пользовательского Telegram-бота (раздел 16) и отдельный токен бота для алертов, ключи S3-хранилища (RustFS), basic auth Flower.

| Где | Инструмент | Как работает |
| --- | --- | --- |
| docker compose (шаг 1) | `.env` в `.gitignore` + `.env.example` в git | compose подставляет переменные; в git только шаблон без значений |
| Kubernetes, с первого дня | **Sealed Secrets** | в кластере стоит контроллер с приватным ключом. `make seal` (обёртка над `kubeseal`) шифрует секрет публичным ключом кластера → файл `SealedSecret` в `deploy/envs/local/sealed-secrets/`, его **коммитим в git**. Argo CD применяет его, контроллер расшифровывает и создаёт обычный Secret, под читает его как env или файл |
| Облако (при переходе) | Sealed Secrets своего кластера или External Secrets Operator + AWS Secrets Manager / GCP Secret Manager | у облачного кластера свой ключ, секреты пересоздаются под него; код и чарт не меняются |
| Опционально | HashiCorp Vault + External Secrets Operator | отдельный сервер секретов: политики доступа, аудит, ротация, динамические пароли к БД |

Правила работы с Sealed Secrets:

- **Бэкап ключа контроллера** — первое, что делаем после установки (`make backup-sealing-key`, файл хранится вне git). Без него после пересоздания k3d-кластера старые SealedSecret не расшифровать; с ним — восстанавливаем ключ до установки контроллера, и всё работает.
- Секрет зашифрован под конкретный кластер и namespace: для каждого окружения (`local`, `staging`, `prod`) — свои файлы.
- Обычный `kubectl create secret` руками не делаем: всё, что попадает в кластер, есть в git.
- В CI проверка: в репозитории нет незашифрованных Secret (`kubeconform` + grep по `kind: Secret` в `deploy/`).

Код приложения от выбора не зависит: он всегда читает секреты из env или файла через `pydantic-settings`.

### Шаг 3 — Tilt

```python
# Tiltfile (эскиз)
docker_build(
    'saleshub-api', './backend',
    live_update=[
        sync('./backend/app', '/app/app'),          # код без пересборки образа
        run('uv sync', trigger=['./backend/pyproject.toml']),
    ],
)

k8s_yaml(helm('./deploy/helm/saleshub', values=['./deploy/helm/values-dev.yaml']))

k8s_resource('api', port_forwards=8000, resource_deps=['migrate'], labels=['backend'])
k8s_resource('worker-bulk', resource_deps=['api'], labels=['workers'])
k8s_resource('flower', port_forwards=5555, labels=['workers'])
k8s_resource('mailpit', port_forwards=8025, labels=['infra'])
watch_file('./policies')                     # правка .rego/data.yaml → новый ConfigMap → под api перезапустится (checksum)
local_resource('opa-test', 'opa test policies -v', deps=['./policies'], labels=['tests'])

local_resource('unit-tests', 'cd backend && uv run pytest tests/unit -q',
               deps=['./backend/app'], labels=['tests'], auto_init=False)
local_resource('seed-5m', 'kubectl exec deploy/postgres -- psql -f /seed/seed_big.sql',
               trigger_mode=TRIGGER_MODE_MANUAL, labels=['data'])
```

В dev-образе uvicorn запущен с `--reload`, воркер — через `watchfiles`, поэтому `sync` сразу подхватывается. В UI Tilt видны логи всех сервисов и кнопки ручных ресурсов (тесты, генерация данных).

### Группы сервисов в Tilt

Весь стек сразу займёт большую часть памяти Docker, поэтому сервисы разбиты на группы. `core` запущен всегда, остальное включается, когда нужно: `tilt up -- --groups=search,monitoring` (в Tiltfile — `config.define_string_list("groups")`, в Helm — флаги `enabled` у сабчартов).

| Группа | Сервисы | Когда включать |
| --- | --- | --- |
| `core` | api + OPA, воркеры, beat, outbox-relay, telegram-bot, Postgres, Redis, RustFS (S3), Mailpit, Sealed Secrets, pgAdmin, RedisInsight, Flower | всегда |
| `events` | Redpanda, Redpanda Console | этап 9 и всё, что про Kafka |
| `search` | Elasticsearch, Kibana, search-indexer (требует `events`) | работа над поиском |
| `monitoring` | Prometheus, Grafana, Alertmanager, Loki, exporters | метрики, дашборды, алерты |
| `cd` | Argo CD | проверка GitOps-деплоя |

Без включённой группы приложение работает: при выключенном `search` поиск идёт через trigram Postgres, при выключенном `events` relay публикует события только в Celery.

### Веб-интерфейсы для всех ресурсов

У каждого ресурса стенда должен быть UI: смотреть, что происходит внутри, — главная часть обучения. Все интерфейсы поднимаются вместе со стендом в compose и в кластере, открываются через Ingress на `*.saleshub.localhost`, а в Tilt доступны ещё и через port-forward. Это только dev-профиль: в prod-чарте они выключены или закрыты авторизацией.

| Ресурс | UI | Адрес (Ingress / порт) | Что смотреть |
| --- | --- | --- | --- |
| API | Swagger UI, ReDoc | `api.saleshub.localhost/docs` · 8000 | все эндпоинты, Authorize, требуемые права |
| PostgreSQL | pgAdmin 4 | `db.saleshub.localhost` · 5050 | таблицы, `EXPLAIN ANALYZE` с графическим планом, блокировки, pg\_stat\_statements |
| Redis | RedisInsight | `redis.saleshub.localhost` · 5540 | ключи кэша (пользователи с ролями, rate limit), очереди Celery, память |
| Celery | Flower | `flower.saleshub.localhost` · 5555 | воркеры, задачи, ретраи, длина очередей |
| Kafka (Redpanda) | Redpanda Console | `kafka.saleshub.localhost` · 8080 | топики, сообщения, consumer groups и их lag |
| Prometheus | встроенный UI Prometheus | `prometheus.saleshub.localhost` · 9090 | метрики, PromQL, targets, активные правила |
| Дашборды | Grafana | `grafana.saleshub.localhost` · 3000 | дашборды из раздела 8, логи (Loki) |
| Алерты | Alertmanager UI | `alerts.saleshub.localhost` · 9093 | сработавшие алерты, тишина (silences) |
| Почта | Mailpit | `mail.saleshub.localhost` · 8025 | письма приглашений и сброса пароля |
| Файлы (S3) | консоль RustFS | `s3.saleshub.localhost` · 9001, путь `/rustfs/console/` | файлы импорта (`imports/…`); выгрузки в S3 — с этапа 10 |
| Kubernetes | Tilt UI + Headlamp | `localhost:10350` · `k8s.saleshub.localhost` | логи и статус ресурсов, поды, Jobs, события кластера |
| OPA | отдельного UI нет | — | решения видны в decision logs (Grafana → Loki); правила удобно отлаживать в Rego Playground |
| CD | Argo CD UI | argocd.saleshub.localhost | синхронизация с git, история выкаток, откат |
| Elasticsearch | Kibana | kibana.saleshub.localhost · 5601 | индексы, документы, отладка запросов в Dev Tools |

pgAdmin и RedisInsight сразу настроены на подключения стенда (провижининг через `servers.json` и переменные окружения), чтобы после `tilt up` ничего не вводить руками. Ссылки на все UI выводятся в Tilt UI через `links=` у каждого ресурса.

## 12. CI/CD

CI проверяет каждый PR (качество, тесты, миграции, сборка, чарт); CD собирает образ один раз, тегирует его SHA коммита и выкатывает тот же образ дальше. Для локального кластера push-деплой из GitHub невозможен (раннер не видит твой ноутбук), поэтому CD изучается в двух вариантах: e2e-стенд kind прямо в раннере и pull-based GitOps через Argo CD.

### CI — `.github/workflows/ci.yml` (на pull request)

| Job | Шаги | Зависит от |
| --- | --- | --- |
| `lint` | `ruff check`, `ruff format --check`, `mypy app` | — |
| `unit` | `pytest tests/unit --cov` | — |
| `integration` | `pytest tests/integration`: Postgres и Redis поднимает testcontainers, `alembic upgrade head` — в сессионной фикстуре; там же проверки миграций (одна голова, `alembic check`, downgrade → upgrade) | lint |
| `migrations` | upgrade → `alembic check` → downgrade base → upgrade; одна голова | lint |
| `openapi` | экспорт `openapi.json`, diff с main (ломающие изменения подсвечиваются) | lint |
| `build` | `docker buildx` с кэшем GHA, без push | unit |
| `helm` | `helm lint`, `helm template` + `kubeconform` | — |
| `monitoring` | `promtool check rules`, `promtool test rules` | — |
| `frontend` (если есть) | `npm ci`, lint, `vitest`, `vite build` | — |
| `policy` | `make policy-test`: `opa fmt --list --fail`, `opa check --strict`, `opa test -b policies` и `--coverage --threshold 90` (OPA 1.21.3 из Docker); Regal (линтер Rego) — на этапе 6 | — |

Параллельность: всё, что не зависит друг от друга, идёт одновременно; кэш uv и Docker-слоёв обязателен, иначе CI будет медленным. Ветка `main` защищена: мерж только при зелёных обязательных проверках.

### CD — `.github/workflows/cd.yml` (на push в main и теги `v*`)

1. Собрать образ один раз → push в GHCR с тегами `sha-<short>` и, для тега, `v1.2.3`.
2. **e2e**: поднять kind в раннере → `helm upgrade --install --atomic` этого образа → Job `migrate` → smoke-тесты из раздела 10 → кластер удаляется.
3. **Выкатка (вариант GitOps)**: workflow обновляет `image.tag` в `deploy/envs/local/values.yaml` (коммитом в репозиторий). Argo CD, установленный в твоём локальном кластере, сам замечает изменение и синхронизирует — так деплой работает и на ноутбуке.
4. **Выкатка (вариант push, опц.)**: если появится облачный кластер или VPS с k3s — `helm upgrade --atomic --wait` из GitHub Environment `staging` с ручным approve для `prod`.

### Что важно понять на практике

- Один артефакт: образ не пересобирается между окружениями, меняются только values/Secrets.
- Миграции идут до кода и должны быть совместимы со старой версией кода (expand/contract, раздел 9) — иначе rolling update ломается на середине.
- `--atomic` откатывает релиз, если поды не стали ready; откат схемы БД при этом не происходит — отсюда правило обратной совместимости миграций.
- Секреты: в GitHub — Environments secrets; в кластере — Sealed Secrets (раздел 11).
- pre-commit локально повторяет `lint` из CI, чтобы не ждать раннер ради опечатки.

### Локальный CD: что для этого нужно

GitHub Actions работает в облаке и не видит ноутбук, поэтому деплой в локальный кластер строится наоборот: кластер сам забирает изменения (pull, GitOps). Для этого нужно:

1. **Репозиторий на твоём GitHub** с двумя ветками ответственности: код и `deploy/envs/local/values.yaml` (тег образа).
2. **Образы в GHCR** (`ghcr.io/<твой-логин>/saleshub-api`). Если пакет приватный — Secret `imagePullSecret` с personal access token (`read:packages`) в кластере.
3. **Workflow `cd.yml`**: сборка образа → push в GHCR с тегом `sha-<коммит>` → коммит нового тега в `deploy/envs/local/values.yaml` (или это делает Argo CD Image Updater).
4. **Argo CD в k3d-кластере** (ставится Helm-чартом, у него есть свой UI — `argocd.saleshub.localhost`). Он следит за репозиторием: видит новый тег → `helm upgrade` → Jobs `migrate` и `seed` → rolling update. Для приватного репозитория — deploy key только на чтение.
5. **Ноутбук с интернетом**: Argo CD опрашивает GitHub раз в 3 минуты (для обучения хватает, вебхуки снаружи до ноутбука не дойдут).

Репозиторий и образы в GHCR **публичные**, поэтому pull-секрет и deploy key не нужны: кластер и Argo CD читают их без авторизации. Правила для публичного репозитория:

- В git только зашифрованные секреты (SealedSecret) и `.env.example`; реальные `.env` — в `.gitignore`.
- В настройках GitHub включены secret scanning и push protection — случайно закоммиченный токен заблокируется.
- Workflow с секретами запускаются на `push` и `pull_request` из своих веток; `pull_request_target` для PR из форков не используется.
- Dependabot для pip, npm, GitHub Actions и Docker-образов.

Альтернатива для сравнения — **self-hosted runner**: GitHub Actions runner, запущенный на ноутбуке (как под в кластере или процесс). Тогда job деплоя выполняется локально и делает `helm upgrade` напрямую (push-модель). Оба варианта стоит попробовать: GitOps — основной, runner — чтобы понять разницу.

Итог цепочки: `git push` → CI (тесты) → сборка образа → новый тег в репозитории → Argo CD видит изменение → миграции → новая версия в локальном кластере. Откат — `git revert` коммита с тегом.

### Переход в облако без переписывания

Локальный кластер — просто первое окружение из трёх (`local`, `staging`, `prod`). Приложение, Helm-чарт, образы, политики OPA и пайплайн одинаковые везде. Отличия живут только в values окружения и в инфраструктурном коде на Pulumi (Python), который пишется в момент переезда.

```text
deploy/
├── helm/saleshub/          # один чарт для всех окружений
├── envs/
│   ├── local/              # k3d: values.yaml, sealed-secrets/, argocd-app.yaml
│   ├── staging/            # облако: те же файлы, другие значения
│   └── prod/
├── argocd/                 # ApplicationSet: по приложению Argo CD на окружение
└── infra/                  # Pulumi: кластер, БД, Redis, DNS, IAM — только для облака
```

| Что | Локально (k3d) | Облако (EKS / GKE) | Как переключается |
| --- | --- | --- | --- |
| PostgreSQL | Bitnami-чарт в кластере | RDS / Cloud SQL | `postgresql.enabled: false` + `externalDatabase.host` |
| Redis | чарт в кластере | ElastiCache / Memorystore | `redis.enabled: false` + `externalRedis.host` |
| Kafka | Redpanda в кластере | Redpanda Cloud / MSK | `kafka.bootstrapServers` |
| Файлы | RustFS (S3 API) | S3 / GCS | только endpoint и ключи: клиент один (S3 API) |
| Ingress и TLS | ingress-nginx, `*.saleshub.localhost`, без TLS | ingress-nginx или ALB, свой домен, cert-manager + Let's Encrypt | `ingress.className`, `ingress.host`, `tls.enabled` |
| Диски | local-path | gp3 / pd-ssd | `storageClass` |
| Образы | GHCR | GHCR (или ECR / Artifact Registry) | без изменений |
| Секреты | Sealed Secrets, ключ локального кластера | Sealed Secrets облачного кластера или External Secrets + Secrets Manager | пересоздать SealedSecret под новый ключ |
| Деплой | Argo CD в том же кластере | Argo CD в облачном кластере, тот же репозиторий | новая папка `envs/staging` |
| Почта | Mailpit | Amazon SES / любой SMTP | SMTP-настройки |
| Мониторинг | kube-prometheus-stack + Loki | то же (или managed Prometheus / Grafana Cloud) | values |
| Инфраструктура | `make cluster` (k3d) | `pulumi up -s staging` | — |

**Правила с первого дня, чтобы переезд был лёгким:**

- Вся конфигурация — из переменных окружения (12-factor); в коде нет хостов, путей и названий бакетов.
- Внешние зависимости в чарте — опциональные сабчарты с флагом `enabled`; приложение не знает, где запущена база.
- Файлы — только через S3 API (`aiobotocore` — живая async-обёртка botocore; `aioboto3` заброшен и держит старый aiobotocore), а не через специфичный клиент хранилища: так RustFS локально и S3 в облаке взаимозаменяемы. Для RustFS — path-style адреса и `request_checksum_calculation = when_required` (новые botocore по умолчанию считают CRC, а часть S3-совместимых хранилищ его не понимает).
- Пробы `live`/`ready`, graceful shutdown по SIGTERM, `requests`/`limits` у каждого пода — без этого в облаке поды убивают.
- Миграции — отдельный Job и совместимы со старой версией кода (раздел 9).
- Образ один на все окружения, тег = SHA коммита; продвижение в staging/prod — коммит тега в папку окружения.
- В CI уже сейчас `helm template` прогоняется для всех трёх окружений, чтобы values не разъехались.

Когда придёт время облака: `pulumi up` поднимает кластер и managed-сервисы → ставим Argo CD и контроллер Sealed Secrets → пересоздаём секреты под новый кластер → добавляем `envs/staging` → Argo CD деплоит ту же версию, что работает локально.

## 13. Фронтенд (в самом конце)

Пишется последним; до этого всё проверяется через Swagger. Небольшой SPA на Vite + React + TypeScript, задача которого — показать права и bulk-операции глазами пользователя, а не быть продуктом. TS-клиент генерируется из `openapi.json` (`openapi-typescript`), поэтому типы всегда совпадают с бэкендом.

| Экран | Что демонстрирует |
| --- | --- |
| Логин + переключатель «войти как» (dev) | разные роли за один клик |
| Канбан воронки: сделки по этапам, drag-and-drop | смена этапа, событие `DealStageChanged`, права `own` / `team` |
| Список контактов (TanStack Table, keyset-подгрузка, виртуальный скролл, поиск) | фильтр видимости `Access`, trigram-поиск на 5 млн строк |
| Карточка контакта: сделки, активности, заметки | связи, оптимистическая блокировка (`version`) |
| Импорт CSV: загрузка → прогресс → отчёт об ошибках | job, `import_errors`, дедупликация |
| Массовые действия: выбрать сегмент → передать другому менеджеру / добавить тег | bulk job, число строк — из `total` job после plan, прогресс по SSE |
| Корзина | мягкое удаление и восстановление |
| Отчёты: воронка, сумма сделок по менеджерам | materialized view, права `report:read` |
| Команда: отделы, участники, приглашения | назначение ролей и отделов, правила вроде «нельзя менять роль себе» (причина отказа видна в UI) |

Кнопки прячутся по `GET /auth/me → permissions`, но это только UX: решение всегда принимает бэкенд. В Kubernetes фронт — отдельный Deployment с nginx и статикой, в Tilt — `vite dev` как `local_resource` с проксированием `/api`.

## 14. Дорожная карта и открытые вопросы

Двенадцать этапов (0–11), каждый разбит на шаги размером от нескольких часов до дня и заканчивается работающим результатом и тестами; к Kubernetes переходим только после того, как всё работает в compose. Этапы 0–8 — ядро (всё из исходной задачи), 9–11 — расширения по желанию.

**Как работать по плану:**

- Один этап — одна ветка `stage-N` и один PR в `main`; шаги — отдельными коммитами; после этапа CI зелёный.
- Отмечай шаг галочкой прямо здесь, когда он смержен.
- Не переходи к следующему этапу, пока не выполнено «Готово, когда».
- В скобках у шага — раздел документа, где описаны детали.
- Застрял на шаге дольше дня — разбей его на два и запиши здесь.

> **Ядро — этапы 0–8.** Закрывает все исходные цели: вход и права (JWT, роли, OPA, own / team / org), большие объёмы (bulk, Celery, beat, индексы), таблицы с индексами для экспериментов, Kubernetes и Tilt, тесты, CI, Alembic, логи, метрики и алерты. Сюда же входят Telegram-бот и валюты — они недорогие и опираются на то, что уже построено.

### Этап 0 — Подготовка

- [ ] 0.1 Установить: Docker, uv, Python 3.13, k3d, kubectl, helm, tilt, opa, kubeseal, gh CLI. Уже есть: Docker, uv, Python 3.13, pre-commit (`uv tool install pre-commit`); остальное понадобится не раньше этапа 3 (opa) и этапа 7 (Kubernetes).
- [x] 0.2 Создать публичный репозиторий на GitHub (`IhorVoskoboinikov/sales_hub`): README, лицензия MIT, `.gitignore`, `.editorconfig`.
- [x] 0.3 Перенести этот документ в `docs/DESIGN.md` — дальше правим только его, копий нет.
- [x] 0.4 pre-commit: ruff, ruff-format, mypy, gitleaks (поиск секретов). Ruff ограничен файлами `.py`: новые версии форматируют и Python-фрагменты в Markdown, а эскизы в этом документе специально написаны компактно.
- [ ] 0.5 В настройках GitHub включить secret scanning, push protection, Dependabot, защиту ветки `main`.

**Готово, когда:** `pre-commit run --all-files` проходит, первый коммит в `main`.

### Этап 1 — Скелет приложения

- [x] 1.1 `pyproject.toml` через uv, структура папок (раздел 2).
- [x] 1.2 `Settings` на pydantic-settings, профили dev/test/prod, `.env.example`.
- [x] 1.3 `create_app()`, роутер `/api/v1`, `/health/live`.
- [x] 1.4 structlog (JSON), `RequestIdMiddleware`, access-лог (раздел 7).
- [x] 1.5 Иерархия `AppError`, обработчики, формат ошибки, тексты на украинском (раздел 7).
- [x] 1.6 Dockerfile (multi-stage) и docker compose: api, Postgres, Redis, pgAdmin, RedisInsight, Mailpit, RustFS (S3; вместо MinIO — его образы больше не публикуются).
- [x] 1.7 Async engine, сессия на запрос, `Base` с naming convention, `/health/ready`.
- [x] 1.8 Alembic с async `env.py`, первая пустая миграция, `make migrate`.
- [x] 1.9 pytest: `conftest`, testcontainers Postgres, первый тест health.
- [x] 1.10 Минимальный `ci.yml`: lint + unit + integration; первый зелёный прогон — PR #1 и push в `main`.

**Готово, когда:** `make up` поднимает всё, все UI открываются (раздел 11), `/health/ready` отвечает 200, CI зелёный.

**Статус: ✅ этап закрыт** — смержен в `main` (PR #1), CI зелёный.

### Этап 2 — Вход по почте (раздел 4)

- [x] 2.1 Модели `users`, `refresh_tokens`, `email_tokens` + миграция.
- [x] 2.2 Хеширование argon2, правила пароля.
- [x] 2.3 Ключи RS256 (`make keys`), выпуск и проверка JWT, `/.well-known/jwks.json`.
- [x] 2.4 Команда `create-superadmin` — первый пользователь, под которым можно войти; в compose — одноразовый сервис `seed`.
- [x] 2.5 `login`, зависимость `current_user`, кнопка Authorize в Swagger.
- [x] 2.6 `refresh` с ротацией и защитой от повторного использования.
- [x] 2.7 `logout`, `logout-all`, `sessions`, удаление сессии.
- [x] 2.8 `EmailSender` (пока через `BackgroundTasks` в Mailpit), шаблоны писем на Jinja2.
- [x] 2.9 Приглашения: супер-админ приглашает по email → письмо → `invite/accept` задаёт пароль и подтверждает email. Организация и роль в приглашении — на этапе 3.
- [x] 2.10 `password/forgot`, `password/reset`, `password/change`.
- [x] 2.11 Rate limit на Redis для логина, `forgot` и приглашений.
- [x] 2.12 Интеграционный тест всего сценария: письма читаются через API Mailpit, сроки — через freezegun.

**Готово, когда:** сценарий проверки из раздела 4 проходит руками в Swagger и автотестом.

**Статус: ✅ этап выполнен и закрыт** (2026-10-20, ветка `stage-2`) — сценарий раздела 4 пройден на стенде compose и автотестом `test_auth_flow.py`; 113 тестов зелёные.

### Этап 3 — Организации и права на OPA (раздел 5)

- [x] 3.1 Модели `organizations` (валюта, часовой пояс), `teams`; в `users` — `org_id`, `team_id`, `role` с CHECK-ограничениями + миграция.
- [x] 3.2 OPA в compose; `routes.rego` — только маршруты этапа 3 (`/admin/orgs…`, `/org`, `/teams…`, `/users…`, `/invites`), `data.yaml`, `authz.rego`; первые `opa test`.
- [x] 3.3 `OpaClient`, `authorize()`, чтение `data.yaml`, `Access`.
- [x] 3.4 Защищённый роутер и тест `test_routes_match_policy`.
- [x] 3.5 Эндпоинты организаций (супер-админ) и отделов (org admin).
- [x] 3.6 Приглашения: `POST /api/v1/admin/orgs/{org_id}/invites` (первый админ) и `POST /api/v1/invites` (с ролью и отделом), письмо, `invite/accept`; занятый email — 202 и письмо «вы уже состоите в организации»; удалить `POST /api/v1/admin/invites`, а тестовых пользователей этапа 2 без организации удаляет миграция 3.1.
- [x] 3.7 Смена роли и отдела, отключение пользователя; правила в сервисе (нельзя себе, отдел обязателен), причина отказа в ответе.
- [x] 3.8 `audit_log` (пока без партиций) и сброс кэша пользователя при смене роли.
- [x] 3.9 «Требуется: …» в описании операций Swagger.
- [x] 3.10 Тест-матрица «роль × эндпоинт × свой/чужой» + `opa test --coverage` в CI.

**Готово, когда:** супер-админ создаёт организацию → приглашает org admin → тот создаёт отдел и приглашает менеджера; каждый видит и меняет только то, что разрешено матрицей: организация, отделы, пользователи (CRM-данные — на этапе 4).

**Статус: ✅ этап выполнен и закрыт** (2026-10-20, ветка `stage-3`) — сценарий пройден на стенде compose; тест-матрица (81 случай), `opa test` (12 тестов, покрытие ≥ 90%) и 249 тестов pytest зелёные.

### Этап 4 — CRM-домен и объём (раздел 3)

- [x] 4.1 `companies` и `contacts` с мягким удалением и `version`; общий фильтр `deleted_at IS NULL` в репозитории; keyset-пагинация; правила ответственного (`owner.*`) и `member.has_records`, `team.has_records`.
- [x] 4.2 CRUD компаний и контактов с `Access` (`list`, `get_visible_or_404`, `ensure_allowed`), `?q=`, `?tag=`, ошибки `contact.duplicate_email`, `version.conflict` + строки в `routes.rego`, тесты в `authz_test.rego`, тест-матрица с целями own / same\_team / same\_org / other\_org.
- [x] 4.3 Воронки, этапы, `deals` (пока только USD), эндпоинт канбана + строки в `routes.rego` и кейсы матрицы.
- [x] 4.4 `fx_rates`, `FxService` (НБУ, кэш, резерв), суммы сделок в любой валюте, параметр `?currency=`.
- [x] 4.5 `activities` с партициями по месяцам, `ensure_monthly_partitions`, `notes`; `audit_log` на партициях и `GET /audit` + строки в `routes.rego`.
- [x] 4.6 Корзина: удаление, восстановление, список удалённых + строки в `routes.rego`.
- [x] 4.7 Отчёты: materialized view `deal_stats` и эндпоинты + строки в `routes.rego`.
- [x] 4.8 `seed_big.sql` (5 млн контактов, 1 млн сделок, 20 млн активностей), `make seed-big`.
- [x] 4.9 Индексы из раздела 3: замеры «без / с» → `docs/experiments.md`; `make explain`.
- [x] 4.10 Тесты: «список не возвращает чужого», конвертер валют, корзина, гонки (версия, ответственный).

**Готово, когда:** на сгенерированных данных все списки и отчёты идут по индексам (в `EXPLAIN` нет Seq Scan по большим таблицам — проверяет `make explain`), замеры записаны.

**Статус: ✅ этап выполнен и закрыт** (2026-10-21, ветка `stage-4`, смержен в `main` — PR #4) — `make seed-big` загружает 5 млн контактов, 1 млн сделок и 20 млн активностей за 10 минут; `make explain`: 224 плана, Seq Scan по большим таблицам — 0 (предупреждение — частая подстрока в `?q=`, её закрывает Elasticsearch на этапе 9); замеры — `docs/experiments.md`; тест-матрица (496 кейсов «роль × эндпоинт × цель»), `opa test` (20 тестов, покрытие ≥ 90%) и 726 тестов pytest зелёные. Каждый шаг прошёл независимое ревью; подтверждённые находки исправлены отдельными коммитами `fix(…)`.

**API этапа 4** (пути — из `routes.rego`, раздел 5; все списки — keyset `?limit=&cursor=`):

| Эндпоинт | Что делает |
| --- | --- |
| `GET /companies?q=` | компании организации по имени; в `q` — хотя бы 3 буквы или цифры подряд (иначе у pg\_trgm нет триграмм и пришлось бы читать всю организацию), без управляющих символов — иначе 422 |
| `GET /contacts?q=&status=&tag=&owner_id=&company_id=` | контакты в уровне `contact:read`, новые сначала |
| `POST /companies`, `POST /contacts` | `owner_id` необязателен (по умолчанию — я); `org_id` и `team_id` в теле не принимаются |
| `PATCH /companies/{id}`, `PATCH /contacts/{id}` | поля, которых нет в теле, не меняются; `version` обязателен |
| `GET /pipelines`, `POST /pipelines {name, stages}` | воронки с этапами; `stages` — список `{name, probability}`, порядок в списке = `position` |
| `PATCH /pipelines/{id} {name?, stages?}` | `stages` — целиком: элемент с `id` — изменить, без `id` — добавить, пропавший — удалить (если на этапе есть сделки, в том числе в корзине, — 409 `pipeline.stage_has_deals`) |
| `GET /deals?status=&pipeline_id=&stage_id=&owner_id=&contact_id=&company_id=&overdue=&currency=` | сделки в уровне; `overdue=true` — открытые с прошедшей `expected_close_date`, сортировка по ней |
| `POST /deals`, `PATCH /deals/{id}` | `{title, amount, currency, stage_id, contact_id?, company_id?, owner_id?, expected_close_date?, status?}`; `pipeline_id` — из этапа |
| `GET /deals/kanban?pipeline_id=&currency=&per_stage=20` | по каждому этапу: число и сумма открытых сделок в уровне + первые `per_stage` (новые сначала); воронка чужой организации — 404 |
| `GET /activities?deal_id= \| contact_id=` | история карточки: родитель по уровню чтения, дальше без фильтра `Access` |
| `GET /activities?due=today\|overdue` | открытые задачи в уровне `activity:read`, по `due_at`; «сегодня» — по часовому поясу пользователя или организации |
| `POST /activities`, `PATCH /activities/{id}` (`done: true` → `done_at`), `DELETE /activities/{id}` | ответственный — по общему правилу; удаление жёсткое |
| `GET`, `POST /contacts/{id}/notes` | заметки контакта: читает тот, кто видит контакт, добавляет тот, кто может его менять |
| `DELETE`, `POST …/restore` у компаний, контактов, сделок; `GET /contacts/trash` | корзина (раздел 3) |
| `GET /reports/pipeline?pipeline_id=&currency=` | по этапам: число и сумма открытых сделок в уровне `report:read` |
| `GET /reports/managers?from=&to=&currency=` | по ответственным: открытые сделки и выигранные / проигранные за месяцы `from`–`to` (`YYYY-MM`) |
| `GET /audit?entity_type=&entity_id=&actor_id=&action=&from=&to=` | журнал своей организации (`audit:read` — только org), новые сначала |

### Этап 5 — Воркеры, bulk и Telegram-бот (разделы 6, 15, 16)

- [x] 5.1 Зависимости (Celery 5.6, раздел 1), `Resources` для API и воркеров, `celery_app` и маршруты очередей, event loop на процесс, `BaseTask` (что повторять), `request_id` в заголовках задачи; `schedule.py` с heartbeat; воркеры, beat и Flower в compose.
- [x] 5.2 `outbox_events` с NOTIFY-триггером и процесс relay (`LISTEN/NOTIFY` + опрос), `outbox-relay` в compose.
- [x] 5.3 Письма через outbox → очередь `notifications` (`send_email`) + `sent_notifications`; удалить `Mailer` и `BackgroundMailer`, стирать токен после отправки.
- [x] 5.4 `bulk_jobs`, `bulk_job_chunks`, `plan` / `chunk` / `finalize_if_complete` (без chord), `TaskQueue`, `POST /bulk/reassign-owner` с `Idempotency-Key`, `move_team` при смене отдела, `GET /bulk-jobs/{id}` и SSE + строки в `routes.rego`, кейсы матрицы и тесты в `authz_test.rego` (множество пар «bulk не шире записи», sales\_rep без bulk и выгрузки, viewer выгружает, но не запускает bulk).
- [x] 5.5 Отмена job, `bulk.reap_stuck` (в расписании), `failed_tasks`, список и перезапуск + строки в `routes.rego`.
- [ ] 5.6 S3 (`FileStorage`, `ensure-bucket`, RustFS в testcontainers); генератор CSV на 500 тыс. строк (`make import-csv`); импорт CSV: загрузка → parse → `COPY` в staging → stage → merge → `import_errors` и `GET /bulk-jobs/{id}/errors` + строки в `routes.rego`.
- [ ] 5.7 Закрытие просроченных сделок, добавление и снятие тегов, потоковый экспорт в CSV + строки в `routes.rego`.
- [ ] 5.8 Периодическое обслуживание в beat (раздел 15): НБУ, корзина (+ индексы корзины сделок и компаний), партиции, отчёты, очистка.
- [ ] 5.9 Замеры способов UPDATE → `docs/experiments.md`.
- [ ] 5.10 Chaos-тест: убить воркер посреди job (`make chaos` сам создаёт менеджеру 100 тыс. контактов через `generate_series`): job завершается, строки не посчитаны дважды.
- [ ] 5.11 Telegram-бот на aiogram (polling), привязка через `/start <token>` (миграция: `telegram_link` в CHECK `email_tokens.purpose`, `telegram_links`), роутер `/api/v1/me` и `/me/telegram`.
- [ ] 5.12 Доменные события (`record.assigned`, `deal.closed`, `bulk_job.completed`), notifier, `notification_settings` и `/me/notifications`, отправка в Telegram.
- [ ] 5.13 Команды `/today`, `/deals`, `/stop`.
- [ ] 5.14 Напоминания за 15 минут и утренний дайджест по часовому поясу.
- [ ] 5.15 Приёмка этапа: e2e-тест «одно письмо и одно сообщение» и `make chaos` с проверкой уведомлений.

**Готово, когда:** передача 100 тыс. клиентов идёт с прогрессом, переживает рестарт воркера, новый ответственный получает одно сообщение в Telegram и одно письмо (автор job — своё одно письмо и одно сообщение с итогом).

**API этапа 5** (пути — из `routes.rego`, раздел 5; все POST массовых операций принимают `Idempotency-Key` и отвечают `202 BulkJobOut` + `Location`):

| Эндпоинт | Что делает |
| --- | --- |
| `POST /bulk/reassign-owner {from_user_id, to_user_id, include?}` | передать записи человека (раздел 6); 422 `bulk.same_owner`, `owner.invalid`, `validation` (`from_user_id` вне `member:read`); 403 `out_of_scope` (получатель вне уровня); 409 `bulk.already_running` |
| `POST /bulk/close-overdue-deals {before?, pipeline_id?}` | закрыть просроченные открытые сделки как `lost`; `before` позже сегодняшней даты — 422 `validation` (поле `before`) |
| `POST /bulk/add-tags`, `POST /bulk/remove-tags` `{tag, filter}` | тег сегменту; `filter` — поля `GET /contacts` |
| `POST /contacts/import` (multipart: `file`, `owner_id?`) | импорт CSV; 411 `import.length_required`; 413 `import.file_too_large`; 422 `import.invalid_form`, `import.invalid_header`, `import.invalid_encoding`, `import.empty_file`; в ответе ещё `import_info` |
| `GET /contacts/export?q=&status=&tag=&owner_id=&company_id=` | CSV потоком; 422 `export.too_large`, 429 — больше 10 в час |
| `GET /bulk-jobs/{job_id}` | `{id, kind, status, total, processed, failed_rows, chunks_total, failed_chunks, result, error, created_at, started_at, finished_at}` |
| `GET /bulk-jobs/{job_id}/events` | SSE: `progress` при изменениях, `end` на финальном статусе, не дольше 5 минут |
| `GET /bulk-jobs/{job_id}/errors?limit=&cursor=` | ошибки строк импорта: `{row_no, code, field, raw}` |
| `POST /bulk-jobs/{job_id}/cancel` | 202; 409 `bulk.not_cancellable` |
| `PATCH /users/{user_id}` | теперь и смена отдела человека с записями: 200 с `bulk_job_id` (`MemberUpdateOut`; null — переносить нечего); смена роли на `org_admin` / `viewer` при живых записях (с открытыми активностями) — 409 `member.has_records` |
| `GET /admin/failed-tasks?task_name=&replayed=`, `GET /admin/failed-tasks/{id}`, `POST …/{id}/replay` | супер-админ; 409 `failed_task.already_replayed`, `failed_task.unknown_task`, `failed_task.not_replayable` |
| `POST /me/telegram/link`, `GET`, `DELETE /me/telegram` | привязка бота (без OPA); 503 `telegram.disabled` |
| `GET`, `PUT /me/notifications` | каналы по событиям (без OPA) |

Коды причин в `bulk_jobs.error`: `bulk.author_changed` (автор отключён, сменил роль или отдел, или изменился уровень `bulk:run`), `bulk.target_changed` (получатель больше не может вести записи или вышел из уровня), `bulk.stuck`, `import.too_many_rows`, `import.malformed_csv`. Повтор ключа с другим телом — 422 `bulk.idempotency_key_reused`. Гонку двух одновременных запусков ловит частичный UNIQUE: сервис перехватывает `IntegrityError` по имени индекса, перечитывает активную job и отвечает тем же 409 `bulk.already_running` с `details.job_id` (`null`, если job мне не видна по `bulk_job:read`). Тексты всех кодов — в `app/texts/uk.py`.

**Настройки этапа 5** (`Settings`, `.env.example`, compose): `CELERY_BROKER_URL`, `CELERY_VISIBILITY_TIMEOUT` (1200); `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET` (saleshub), `S3_REGION` (us-east-1); `BULK_CHUNK_SIZE` (10 000, не меньше 1 — иначе ошибка при старте); `IMPORT_MAX_BYTES` (100 МиБ), `IMPORT_MAX_ROWS` (1 млн), `EXPORT_MAX_ROWS` (1 млн), `EXPORT_RATE_PER_HOUR` (10); `BULK_REAP_PENDING_SECONDS` (120), `BULK_REAP_STALL_SECONDS` (300), `IMPORT_REAP_STALL_SECONDS` (900), `BULK_MAX_REAPS` (3); `OUTBOX_BATCH_SIZE` (100), `OUTBOX_POLL_SECONDS` (2); `TRASH_RETENTION_DAYS` (30); `TELEGRAM_BOT_TOKEN`, `TELEGRAM_BOT_USERNAME`, `TELEGRAM_API_URL`, `TELEGRAM_LINK_TTL_MINUTES` (10).

### Этап 6 — Тесты и CI целиком (разделы 10, 12)

CI растёт с этапа 1; здесь он доводится до полного набора из раздела 12.

- [ ] 6.1 Разделить тесты на `unit` / `integration` / `policy`, маркеры pytest, пороги покрытия.
- [ ] 6.2 Job `migrations`: upgrade → `alembic check` → downgrade → upgrade, одна голова.
- [ ] 6.3 Job `openapi`: экспорт схемы и сравнение с `main`.
- [ ] 6.4 Job `build`: сборка образа с кэшем, без публикации.
- [ ] 6.5 Кэш uv и Docker в Actions, параллельные jobs.
- [ ] 6.6 Обязательные проверки в защите ветки `main`.

**Готово, когда:** PR нельзя смержить без зелёного CI, полный прогон занимает меньше 10 минут.

### Этап 7 — Kubernetes и Tilt (раздел 11)

- [ ] 7.1 `make cluster`: k3d-кластер, ingress-nginx, домены `*.saleshub.localhost`.
- [ ] 7.2 Helm-чарт: api с OPA-сайдкаром, пробы, ресурсы, Service, Ingress.
- [ ] 7.3 Воркеры по очередям, beat (1 реплика, `Recreate`), relay, telegram-bot, Flower.
- [ ] 7.4 Сабчарты Postgres, Redis, RustFS, Mailpit, pgAdmin, RedisInsight с флагами `enabled`.
- [ ] 7.5 Контроллер Sealed Secrets, бэкап ключа, `make seal`, все секреты как SealedSecret.
- [ ] 7.6 Jobs `migrate` и `seed` как Helm hooks.
- [ ] 7.7 Tiltfile: live\_update, группы сервисов, ссылки на все UI, ручные ресурсы (тесты, seed).
- [ ] 7.8 `deploy/envs/local`, заготовки `staging` и `prod`; `helm lint` и `helm template` всех окружений в CI.
- [ ] 7.9 Руками: rolling update, `helm rollback`, удалить под и посмотреть, как он поднимется.

**Готово, когда:** `make cluster && tilt up` — всё зелёное, правка Python-кода применяется в кластере без пересборки образа.

### Этап 8 — Наблюдаемость (раздел 8)

- [ ] 8.1 `/metrics` в API и бизнес-метрики (bulk, outbox, отказы прав).
- [ ] 8.2 kube-prometheus-stack в группе `monitoring`; экспортёры Postgres, Redis, Celery.
- [ ] 8.3 Loki: логи всех подов в Grafana, поиск по `request_id`.
- [ ] 8.4 Дашборды как код: API, Workers, Bulk jobs, Postgres, Authz.
- [ ] 8.5 Правила алертов и `promtool test rules` в CI.
- [ ] 8.6 Alertmanager → отдельный Telegram-бот для алертов.

**Готово, когда:** остановил Postgres — в Telegram пришёл алерт; по `request_id` из ответа API нашёл в Grafana всю цепочку логов, включая воркер.

> **Ядро готово, когда:** в локальном кластере работает CRM с входом по приглашению; каждая роль видит только своё, и это доказывают тест-матрица и `opa test`; передача 100 тыс. клиентов идёт фоном с прогрессом и переживает рестарт воркера; замеры индексов записаны; CI зелёный; при падении сервиса приходит алерт в Telegram. На этом проект можно считать завершённым.

> **Расширения — этапы 9–11.** Делаются только после ядра и в любом порядке: Kafka и поиск на Elasticsearch, GitOps-деплой через Argo CD, эксперименты, фронтенд. Ядро от них не зависит: без них работает Celery вместо Kafka, trigram-индексы вместо Elasticsearch и `tilt up` вместо Argo CD.

### Этап 9 — События, поиск и CD (разделы 6, 12, 17)

- [ ] 9.1 Redpanda и Redpanda Console в группе `events`; relay публикует в Kafka.
- [ ] 9.2 Потребители `audit-writer` и `reports`.
- [ ] 9.3 Elasticsearch и Kibana в группе `search`, mapping индекса `contacts`, украинская морфология.
- [ ] 9.4 `search-indexer` и полная переиндексация через алиас.
- [ ] 9.5 Search API, `Access.es_filter()`, тест «менеджер не находит чужих клиентов» + строки в `routes.rego`.
- [ ] 9.6 `cd.yml`: сборка → GHCR → коммит нового тега в `deploy/envs/local`.
- [ ] 9.7 Argo CD в группе `cd`, приложение на `envs/local`; проверить `git push` → выкатка → `git revert` → откат.
- [ ] 9.8 (опционально) Self-hosted runner и push-деплой — для сравнения.

**Готово, когда:** `git push` в `main` сам приводит к новой версии в локальном кластере, а поиск находит контакт с опечаткой через секунду после его создания.

### Этап 10 — Эксперименты (по желанию, в любом порядке)

- [ ] 10.0 Отложено с этапа 5: «тихие часы» уведомлений (`available_at` в outbox, переход через полночь и летнее время); экспорт в S3 по ссылке (`POST /exports`, job `export`, multipart-upload, скачивание через API); chord вместо учёта чанков в базе — сравнение; построчный аудит массовых операций через `RETURNING OLD`.
- [ ] 10.1 OPA partial evaluation: перенести видимость данных в OPA (`/v1/compile` сам возвращает условия для SQL) и сравнить с `Access` — как это делают в больших системах.
- [ ] 10.2 Taskiq вместо Celery на одной очереди.
- [ ] 10.3 KEDA для `worker-bulk`.
- [ ] 10.4 Vault + External Secrets.
- [ ] 10.5 Auth0 dev-tenant как провайдер входа.
- [ ] 10.6 Переход на Python 3.14+ и PostgreSQL 19.
- [ ] 10.7 Pulumi-стек и staging в облаке (раздел 12, «Переход в облако»); webhook Telegram-бота вместо polling (раздел 16).

### Этап 11 — Фронтенд (раздел 13)

- [ ] 11.1 Vite + React + TypeScript, клиент из `openapi.json`, логин и обновление токена.
- [ ] 11.2 Список контактов и карточка.
- [ ] 11.3 Канбан воронки.
- [ ] 11.4 Импорт CSV и массовые действия с прогрессом.
- [ ] 11.5 Отчёты, корзина, команда и приглашения.
- [ ] 11.6 Deployment фронта в чарте и в Tilt.

### Утверждённые решения

| Вопрос | Принято |
| --- | --- |
| Документ дизайна | один файл `docs/DESIGN.md`; все договорённости вносятся в него |
| Коммиты | `тип(модуль) сообщение`, например `docs(design) …`; типы: feat, fix, refactor, docs, config, deploy (test, chore — если не подходит другое) |
| Хранилище файлов (S3) | RustFS вместо MinIO: официальные образы MinIO больше не публикуются; код работает только через S3 API, поэтому хранилище заменяемо |
| Роутеры FastAPI | полный префикс у роутера (`/api/v1`, `/api/v1/admin`), эндпоинты прямо на нём, подключение только в `app`; вложенные роутеры не используем — у них `scope["route"].path` теряет внешний префикс (FastAPI 0.151) |
| Ключи JWT | приватный RS256-ключ — файл `JWT_PRIVATE_KEY_FILE` (в dev — `make keys` в `backend/secrets/`, в Kubernetes — Secret файлом); `kid` — отпечаток ключа (RFC 7638); при ротации старый публичный ключ остаётся в JWKS на 1 час |
| Письма | до этапа 5 — `BackgroundTasks` после коммита (ошибка SMTP — в лог, письмо терялось); с этапа 5 — команда `email.send` в outbox в той же транзакции → relay → `notifications.send_email(event_id)`; токен не попадает в Redis и стирается из payload после отправки |
| Выдача и отзыв токенов | сначала блокировка строки пользователя, потом токены — refresh не обгоняет logout-all, смену и сброс пароля |
| Rate limit | `INCR` до проверки, успех обнуляет счётчик; Redis недоступен → 503 |
| Смена пароля | отзыв остальных сессий + `token_version += 1`, в ответе новая пара токенов для текущей сессии |
| Неожиданные ошибки (500) | ловит `UnhandledErrorMiddleware` внутри `RequestId` и `AccessLog`, а не `exception_handler(Exception)` — так в ответе есть `X-Request-ID`, а в логе `request_id` |
| Домен | мульти-тенантная CRM: организации → отделы → компании, контакты, сделки, активности |
| Кто создаёт организации | только супер-админ; дальше всё через приглашения |
| Приглашение на занятый email | 202 и письмо «вы уже состоите в организации» — по ответу нельзя проверить, есть ли email в системе |
| Пользователи этапа 2 без организации | удаляются миграцией этапа 3 (dev-данные, `downgrade` их не возвращает) |
| Блокировки при управлении людьми | организация → пользователь → его токены; отдел — `FOR UPDATE` при удалении и `FOR KEY SHARE`, когда в него кладут человека: гонки дают 409/422, а не 500. С этапа 4 ответственный записи читается с `FOR SHARE`: перевод человека в другой отдел ждёт создания его записи и видит её (на этапе 4 — 409 `member.has_records`, с этапа 5 запись догоняет `move_team`: plan её видит). Организацию сервис людей блокирует `FOR NO KEY UPDATE`, а не `FOR UPDATE`: вставка CRM-записи проверяет FK на организацию с `FOR KEY SHARE`, и с `FOR UPDATE` создание записи и перевод её ответственного ждали бы друг друга (deadlock) |
| Версия OPA | 1.21.3: образ `openpolicyagent/opa:1.21.3` в compose и testcontainers, `opa test` в CI — та же версия |
| Удаление | мягкое (корзина 30 дней) для компаний, контактов, сделок; жёсткое для остального |
| 2FA | не делаем |
| Версии | Python 3.13, PostgreSQL 18 |
| Аутентификация | своя, по почте: access 1 ч, refresh 30 дней с ротацией |
| Движок прав | OPA + Rego решает только доступ роли к эндпоинту; роли и уровни — `policies/saleshub/data.yaml` в git |
| Объём данных | 5 млн контактов, 1 млн сделок, 20 млн активностей |
| Репозиторий и CI/CD | твой GitHub, Actions, GHCR; деплой локально через Argo CD, структура готова к переходу в облако |
| Логи | Loki — ставим, ресурсов хватает |
| Секреты | Sealed Secrets с первого дня; Vault — опционально |
| Брокер событий | Redpanda (Kafka API) |
| Локальный кластер | k3d (16 ГБ и 8 ядер под Docker) |
| Фронт | React, в самом конце |
| Кастомные поля | нет, фиксированный набор полей |
| Валюта | храним в USD; ввод и вывод в любой валюте через конвертер по курсу НБУ, исходная сумма и курс сохраняются |
| Воркеры | отдельный модуль (раздел 15): Celery, один event loop на процесс, outbox relay отдельным процессом |
| Уведомления | email + Telegram-бот на aiogram (раздел 16) |
| Поиск | Elasticsearch + Kibana, индексация через Kafka (раздел 17); резерв — trigram Postgres |
| Видимость репозитория | публичный (портфолио); образы GHCR тоже публичные |
| Язык | украинский: письма, бот, сообщения ошибок; коды ошибок — стабильные английские ключи |
| Часовой пояс | всё время в UTC; timezone у организации (Europe/Kyiv) и, опционально, у пользователя |
| Локальные ресурсы | группы в Tilt: core всегда, events / search / monitoring / cd по необходимости |
| Объём | ядро — этапы 0–8 (исходная задача), расширения — 9–11 по желанию |
| Регистрация | нет, вход только по приглашению (оно же подтверждает email) |
| Проверка прав | OPA отвечает только «может ли роль вызвать эндпоинт» (`allow` + `action`); видимость данных `own` / `team` / `org` применяет `Access` в Python; правила управления пользователями — в сервисе |
| Роли | `super_admin` (флаг) + `org_admin`, `team_lead`, `sales_rep`, `viewer` — простые, но как в настоящих CRM |
| Членство | один человек — одна организация и одна роль; `org_id`, `team_id`, `role` прямо в `users`, таблиц членства нет |
| Файл прав | `policies/saleshub/data.yaml`: действия роли и уровень видимости; один файл читают OPA и бэкенд |
| id CRM-таблиц | `bigint` identity у всех CRM-таблиц; uuid — у организаций, отделов, пользователей |
| Пагинация | keyset везде: `?limit=` (до 100) и непрозрачный `?cursor=`, ответ `{items, next_cursor}`; битый курсор — 422 `pagination.invalid_cursor` |
| Ответственный записи | по умолчанию — я; проверяется при создании и смене `owner_id` (активный `team_lead` / `sales_rep`), `team_id` — его отдел; с этапа 5 смена отдела человека с записями сразу меняет `users.team_id` и запускает `move_team` (закрытые сделки и выполненные активности она не переносит — снова открытые, они получают текущий отдел ответственного), смена роли на `org_admin` / `viewer` при записях — 409 `member.has_records` |
| Чтение перед изменением | старая версия ищется по уровню чтения: не видна — 404, видна, но вне уровня действия — 403 |
| Оптимистическая блокировка | `version` в теле `PATCH`, 409 `version.conflict`; `version_id_col` SQLAlchemy ловит гонку |
| FK на больших таблицах | на `org_id`, `owner_id` и ссылки между сущностями — да, на `team_id` — нет (раздел 3) |
| Курс в сделке | `fx_rate` — единиц исходной валюты за 1 USD; курсы НБУ подтягиваются по первому запросу дня, при недоступности — последний известный и `fx.stale` |
| Партиции | `activities`, `audit_log` по месяцам, идемпотентная `ensure_monthly_partitions` на 2 месяца вперёд, без DEFAULT; с этапа 5 — ещё и ежедневная задача beat |
| Открытые активности | запланированная — с `due_at` и пустым `done_at`; без `due_at` — уже состоялась (`done_at = created_at`) |
| Отчёты | MV `deal_stats` по месяцам, `REFRESH CONCURRENTLY`; активности в отчёты не входят; с этапа 5 — beat каждые 5 минут под advisory lock |
| Проверка планов | `make explain` на seed-данных, в том числе generic-план; предикаты частичных индексов и шаблон `?q=` — литералами |
| Поиск по подстроке | pg\_trgm до этапа 9: быстрый для редких подстрок, медленный (≈ 1 с) для частых — ограничение триграмм, его закрывает Elasticsearch |
| Версии этапа 5 | `celery[redis]==5.6.3` + `redis>=6.4,<6.5` (и `testcontainers[postgres]` без экстра `redis`), `aiogram==3.31.0` + `pydantic>=2.13.5,<2.14`, `aiobotocore` (aiohttp 3.14); ограничения снимаются после Celery 5.7 и aiogram с pydantic 2.14 (раздел 1) |
| Завершение bulk job | без chord: обычная group чанков, учёт в `bulk_job_chunks`, `finalize_if_complete` — условный `UPDATE … WHERE NOT EXISTS pending`; result backend не нужен |
| Постановка задач | bulk и импорт — best-effort `send_task` после коммита, потерю подбирает `bulk.reap_stuck` (job — сама себе outbox); письма, сообщения, доменные события — через `outbox_events`, relay передаёт только `event_id` |
| Надёжность Celery | `visibility_timeout` 20 мин, `acks_late`, prefetch 1, повтор по SQLSTATE и сетевым ошибкам с паузой до 60 с, reaper переотправляет чанки через 5 мин без прогресса (3 попытки) |
| Передача клиентов | компании, контакты, открытые сделки и открытые активности; закрытые сделки остаются у того, кто их закрыл |
| Импорт CSV | UTF-8, `,` или `;`; строки проверяются теми же типами, что API; существующий контакт: непустая ячейка перезаписывает, пустая не трогает, теги объединяются (до 20), статус — только если указан, ответственный не меняется; вне уровня — `duplicate_outside_scope` |
| Аудит массовых операций | одна строка на job (`bulk.<kind>`); по строкам — нет |
| Выгрузка | потоковый CSV до 1 млн строк; файл в S3 по ссылке — этап 10 |
| Telegram | aiogram только в боте (polling, профиль compose); отправка из воркера — httpx в Bot API; токен привязки — `email_tokens` (`telegram_link`); тихие часы — этап 10 |

### Открытые вопросы

- [x] Уведомления — email и Telegram-бот (раздел 16).
- [x] Поиск — Elasticsearch (раздел 17).
- [ ] Новых открытых вопросов нет; этапы 1–4 закрыты, идёт этап 5 (ветка `stage-5`).

## 15. Модуль: система воркеров

Воркеры — отдельный модуль `app/workers` со своими процессами, но без своей бизнес-логики: задача — тонкая обёртка, которая восстанавливает контекст, открывает UoW и вызывает тот же сервис, что и API. Celery доставляет задачу «как минимум один раз», поэтому каждая задача идемпотентна: повтор не ломает данные и не дублирует письма.

### Процессы

| Процесс | Команда | Реплики локально | Назначение |
| --- | --- | --- | --- |
| `worker-default` | `celery worker -Q default -c 2` | 1 | короткие задачи: notifier, Telegram, beat-задачи, reaper |
| `worker-bulk` | `-Q bulk -c 4` | 1–4 (KEDA по длине очереди) | чанки массовых операций |
| `worker-imports` | `-Q imports -c 2` | 1–2 | разбор и загрузка CSV |
| `worker-notifications` | `-Q notifications -c 2` | **ровно 1** | только письма: `rate_limit` Celery действует на экземпляр воркера, поэтому один экземпляр = общий лимит 50 в минуту. Ведро токенов Celery вмещает один токен, поэтому `50/m` — это не «50 сразу, потом пауза», а ровный шаг 1,2 с между письмами |
| `beat` | `celery beat` | **ровно 1**, стратегия `Recreate` | расписание; две копии продублируют задачи |
| `outbox-relay` | `python -m app.events.relay` | 1 (можно 2+) | публикация outbox: в Celery, позже в Kafka |
| `flower` | `celery flower` | 1 | UI |

Это раскладка для Kubernetes (этап 7). В compose очередей две пары: `worker` (`default,notifications`) и `worker-bulk` (`bulk,imports`) — раздел 11.

Outbox relay — не задача beat, а долгоживущий цикл: `LISTEN outbox_new` (триггер шлёт `NOTIFY` при вставке, уведомление доставляется только после коммита) плюс опрос раз в 2 секунды на случай пропуска. Несколько копий не мешают друг другу благодаря `FOR UPDATE SKIP LOCKED`. Так задержка события — миллисекунды, а очередь не засоряется задачами «проверь outbox».

**Как работает relay.** Для `LISTEN` — отдельное соединение `asyncpg.connect()` (DSN — `make_url(database_url).set(drivername="postgresql")`: asyncpg не понимает схему `postgresql+asyncpg`), не из пула SQLAlchemy (иначе соединение с подпиской вернулось бы в пул); `add_listener('outbox_new', …)` будит цикл, обрыв соединения — переподключение, а пока соединения нет, работает только опрос. Цикл: батч → если строк ровно 100, сразу следующий, иначе ждать `NOTIFY` не дольше 2 секунд. Батч — одна транзакция: `SELECT id, event_type, request_id FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED` → для каждой строки `request_id_var.set(row.request_id)` и `celery_app.send_task(ROUTES[event_type], args=[row.id])` → `UPDATE … SET published_at = now()` → коммит. Сначала публикация, потом отметка: «как минимум один раз», дубли гасит `sent_notifications`. Ошибка брокера или базы → откат батча, error `outbox_publish_failed`, пауза 2 → 4 → … → 30 с (успех её сбрасывает). Маршруты всех строк батча ищутся до первой публикации: тип без маршрута (`UnknownEventType` — событие записала версия кода новее relay) останавливает батч, не отдав в брокер его соседей; батч стоит, пока relay не обновят, — это видно по логу и алерту `OutboxLag`. `CeleryPublisher` вызывает `send_task(..., retry=False)` через `asyncio.to_thread` (контекст с `request_id` копируется в поток): при недоступном Redis ошибка приходит после попыток подключения kombu, примерно через 6 с, при зависшем (соединение есть, ответа нет) — через 5 с по `socket_timeout` из `broker_transport_options`, а паузу держит цикл relay. SIGTERM / SIGINT — мягкая остановка: текущий батч дорабатывает, следующего нет (и если сигнал пришёл, пока relay подключался к LISTEN). В задачу уходит только `event_id`: payload потребитель читает из базы, поэтому токенов из писем в Redis нет. Порядок событий не гарантируется (несколько воркеров его всё равно теряют), потребители от него не зависят. Ядро — `publish_batch(session, publisher, batch_size)` с протоколом `Publisher`: в проде `CeleryPublisher`, в тестах — встроенный (раздел 10). Relay собирает не `Resources`, а только свой engine (`pool_size=1`, `max_overflow=0`): Redis-клиент, HTTP, OPA и файл ролей ему не нужны, а `Resources.create` прочитал бы `data.yaml` и собрал SMTP-отправителя; одно соединение пула плюс LISTEN — те самые «relay 2» в бюджете соединений. `EventType` и `ROUTES` на шаге 5.2 пустые: типы и маршруты появляются вместе с потребителями (5.3 — `email.send`, 5.12 — остальные); unit-тест сравнивает множества в обе стороны — тип без маршрута relay не опубликует, маршрут без типа никто не запишет.

Маршруты событий (`app/events/routes.py`, тест: у каждого `EventType` есть маршрут):

| Тип в outbox | Задача | Что делает |
| --- | --- | --- |
| `email.send` | `notifications.send_email` | письмо по шаблону из payload |
| `telegram.send` | `notifications.send_telegram` | сообщение с готовым текстом из payload |
| `record.assigned`, `deal.closed`, `bulk_job.completed` | `notifications.notify` | notifier: кому и в какой канал (раздел 16) |

### Структура модуля

```text
app/core/resources.py    # Resources: engine, session_factory, redis, http, nbu, roles, storage,
                         #   email_sender и telegram (только у воркеров) — для API, воркеров и бота
app/workers/
├── celery_app.py        # Celery(), конфиг, task_routes
├── runtime.py           # event loop и Resources на процесс, run_async()
├── context.py           # request_id / user_id через заголовки задачи, настройка логов
├── base.py              # BaseTask: какие ошибки повторять, backoff, replayable
├── queue.py             # TaskQueue: постановка задачи из API после коммита (порт, в тестах — фейк)
├── schedule.py          # расписание beat
└── tasks/
    ├── bulk.py          # plan, chunk, reap_stuck (finalize_if_complete — метод BulkService)
    ├── imports.py       # import_contacts: parse → stage → merge
    ├── notifications.py # send_email, send_telegram, notify, send_reminders, morning_digest
    ├── fx.py            # fetch_nbu_rates
    └── maintenance.py   # trash.purge, create_partitions, reports.refresh, cleanup, heartbeat
app/events/types.py      # EventType и схемы payload
app/events/routes.py     # тип события → имя задачи
app/events/relay.py      # outbox relay
```

У каждой задачи явное стабильное `name=` (`bulk.chunk`, `fx.fetch_nbu_rates`). Очередь в декораторе не пишется: маршруты — одна таблица `task_routes` по полным именам (`bulk.chunk` → `bulk`; `imports.import_contacts` → `imports`; `notifications.send_email` → `notifications`; всё остальное, включая `bulk.plan` и `bulk.reap_stuck`, → `default`). Шаблон `bulk.*` отправил бы короткие plan и reaper в очередь за тысячей чужих чанков. `finalize_if_complete` — метод сервиса, а не задача Celery. API модули задач не импортирует: ставит их по имени (`send_task`).

### Async внутри Celery

Celery вызывает обычные функции, а сервисы и репозитории асинхронные. Варианты:

| Вариант | Как | Минусы |
| --- | --- | --- |
| `asyncio.run()` в каждой задаче | новый event loop на задачу | пул соединений не переживает задачу: нужен `NullPool`, каждое подключение заново |
| **Один loop на процесс (выбрано)** | loop и `Resources` создаются лениво, один раз на процесс; `run_async(coro)` = `loop.run_until_complete(coro)` | только prefork-пул (не gevent/eventlet); одна задача на процесс одновременно — это нормально |
| Sync SQLAlchemy в воркерах | psycopg без async | дублирование репозиториев |
| Taskiq вместо Celery | async-нативные очереди | меньше экосистема; эксперимент этапа 10 |

```python
# workers/runtime.py
_runtime: Runtime | None = None      # loop + Resources этого процесса
_settings: Settings | None = None    # configure() — для тестов; иначе get_settings()

def configure(settings: Settings) -> None: ...

def get_runtime() -> Runtime:
    # Лениво и по pid: worker_process_init срабатывает только в дочерних процессах prefork,
    # а не в тестовом воркере (pool=solo) и не в родительском процессе, где обрабатываются
    # сигналы. После fork ресурсы родителя не годятся — создаём свои.
    global _runtime
    if _runtime is None or _runtime.pid != os.getpid():
        # Сначала ресурсы, потом loop: create бросает (нет файла ролей — его Resources.create
        # читает первым, до пула и HTTP-клиента) — ничего не создано и не утекло.
        # max_overflow=0: задача одна на процесс. for_worker=True — EmailSender (с 5.12 и Telegram)
        # только у воркеров; у API email_sender = None: с шага 5.3 он почту не отправляет.
        resources = Resources.create(settings, pool_size=2, max_overflow=0, for_worker=True)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _runtime = Runtime(os.getpid(), loop, resources)
    return _runtime

@worker_process_shutdown.connect
def shutdown_process(**_):
    if _runtime is not None:
        _runtime.loop.run_until_complete(_runtime.resources.aclose())
        _runtime.loop.close()

def run_async(coro):
    loop = get_runtime().loop
    task = loop.create_task(coro)
    try:
        return loop.run_until_complete(task)
    except BaseException:            # SoftTimeLimitExceeded прерывает run_until_complete:
        task.cancel()                # без отмены корутина проснулась бы во время следующей задачи
        loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
        raise

# workers/tasks/bulk.py
@celery_app.task(base=BaseTask, name="bulk.chunk", bind=True, replayable=False,
                 soft_time_limit=60, time_limit=90)
def chunk(self, job_id: str, no: int) -> None:
    run_async(bulk_service().run_chunk(UUID(job_id), no))   # своя сессия и UoW
```

`Resources` собирает и lifespan API (кладёт в `app.state` те же атрибуты, что раньше, поэтому `api/deps.py` не меняется), и воркер, и бот — с одними таймаутами; отличаются размер пула и то, что `EmailSender` и клиент Telegram создаются только у воркеров. Файл ролей API читает при сборке приложения (нет `data.yaml` — `create_app` падает сразу) и передаёт в `Resources.create(settings, roles=...)`; воркер и бот читают его в `create`. Воркеры закрывают ресурсы по `worker_process_shutdown` (`runtime.shutdown()`), тестовая фикстура — сама после остановки потока воркера. Бюджет соединений к Postgres: API 2 пода × (5 + 10 overflow) + воркеры 10 процессов × 2 + relay 2 + бот 5 ≈ 57 при `max_connections = 100`. При росте — PgBouncer в режиме transaction.

### Настройки надёжности

| Настройка | Значение | Зачем |
| --- | --- | --- |
| Брокер | Redis db 1 (`CELERY_BROKER_URL`); кэш, rate limit и heartbeat — db 0 | очереди отдельно от кэша: `FLUSHDB` кэша не сотрёт задачи, KEDA читает длину списка `bulk` в известной базе |
| Result backend | нет, `task_ignore_result=True` | результаты задач никто не читает; завершение bulk видно по базе |
| Сериализация | `json`, `accept_content=["json"]` | аргументы — только id и простые значения |
| `task_acks_late` | `True` | сообщение подтверждается после выполнения: упал воркер — задачу возьмёт другой |
| `task_reject_on_worker_lost` | `True` | то же при OOM и `kill -9` процесса пула |
| `worker_prefetch_multiplier` | 1 | воркер не забирает лишние длинные задачи |
| `visibility_timeout` (Redis) | **20 минут**, одинаковый у всех сервисов (`broker_transport_options`, `visibility_timeout`) | больше самой долгой задачи (запуск импорта — 12 минут), иначе Redis выдаст её повторно; сообщение воркера, убитого целиком (`docker kill`), вернётся через это время, а `bulk.reap_stuck` переотправит чанк раньше — через 5 минут без прогресса |
| `socket_timeout`, `socket_connect_timeout` (Redis) | **5 с** у всех сервисов (`broker_transport_options`) | у kombu по умолчанию нет таймаута: зависший Redis (пауза контейнера, потерянные пакеты без RST) держал бы `send_task` бесконечно — relay стоял бы в открытой транзакции батча с блокировками строк и не останавливался по SIGTERM. С таймаутом — ошибка через 5 с, как при отказе в соединении. `BRPOP` воркера ждёт 1 с и в таймаут укладывается |
| `worker_cancel_long_running_tasks_on_connection_loss` | `True` | при потере брокера задача отменяется, а не выполняется второй раз параллельно с повторной доставкой |
| `soft_time_limit` / `time_limit` | по умолчанию 240 / 300 с; чанк 60 / 90 с; запуск `imports.import_contacts` 10 / 12 мин | зависшая задача не держит воркер. Жёсткий `time_limit` убивает процесс пула: `on_failure` нет, `task_failure` Celery шлёт в главном процессе воркера (prefork), где нет контекста задачи, — обработчик его пропускает (иначе главный процесс поднял бы свой пул Postgres и Redis вне бюджета соединений); сообщение подтверждается — такой чанк остаётся `pending`, его переотправит `bulk.reap_stuck`; в `failed_tasks` строки нет, это видно в логе (`Hard time limit exceeded`) и во Flower |
| Ретраи | `BaseTask`: функция `is_retryable(exc)` + `self.retry(countdown=backoff)`, `max_retries=5`, пауза экспоненциальная с jitter, не больше 60 с | повторяем только временные сбои (ниже); countdown всегда меньше `visibility_timeout`. Задержки больше 60 с (ETA) в задачах не используются — напоминания делает опрос beat |
| `worker_send_task_events`, `task_send_sent_event` | `True` | Flower видит задачи по событиям воркера (`task_track_started` не нужен: без result backend состояние STARTED некуда писать) |
| Время | `timezone="Europe/Kyiv"`, `enable_utc=True` | расписание beat — по Киеву, данные — в UTC |
| Остановка | `stop_grace_period: 120s` (в k8s — `terminationGracePeriodSeconds: 120`) | больше лимита чанка: SIGTERM даёт warm shutdown, невзятые сообщения сразу возвращаются в очередь |

**Что повторять.** У asyncpg ошибки Postgres приходят как `sqlalchemy.exc.DBAPIError`, а не `OperationalError`, поэтому повтор решается по SQLSTATE, а не по классу: класс `08` (соединение), `40001` (serialization failure), `40P01` (deadlock — два job разных видов могут обновлять одни строки в разном порядке), `57P01`–`57P03` (Postgres перезапускается), `53300` (нет свободных соединений); плюс `InterfaceError`; из сети — встроенные `ConnectionError` и `TimeoutError` и `socket.gaierror` (имя хоста не разрешилось — сервис в compose ещё не поднялся), а не весь `OSError`: его наследники `FileNotFoundError`, `PermissionError` и `jinja2.TemplateNotFound` — ошибка в коде или настройке; `httpx.TransportError`; `redis.exceptions.ConnectionError` и `redis.exceptions.TimeoutError` (у redis-py свои классы, не встроенные; таймаут подключения — тоже он); SMTP — обрыв и таймаут (наследники `ConnectionError` и `TimeoutError`) и ответы 4xx, а 5xx (адрес отклонён, нет авторизации) — нет; отказ на RCPT TO aiosmtplib бросает списком `SMTPRecipientsRefused` (он не наследник `SMTPResponseException`) — повторяется, если коды всех отказов 4xx (greylisting 451, ящик переполнен 452), а у нас получатель в письме один; `ServiceUnavailable` с кодами `unavailable` и `fx.unavailable`. `authz.policy_out_of_sync` — ошибка конфигурации, её не повторяем. Остальные `AppError` (404 / 409 / 422) — сразу провал: в лог — warning `task_rejected` с кодом, а `BaseTask.throws = (AppError,)` — и Celery пишет провал как ожидаемый, строкой INFO без traceback (сигнал `task_failure` приходит всё равно). Перед повтором — warning `task_retry` с паузой; на последней попытке повтора уже не будет — вместо него error `task_retries_exhausted`. Unit-тесты — на поддельных `DBAPIError` с `orig.sqlstate`; integration-тест: настоящий deadlock двух соединений (`pg_advisory_xact_lock` крест-накрест) распознаётся как повторяемый.

### Идемпотентность и защита от дублей

- **Чанк bulk:** строка `bulk_job_chunks` со статусом `pending` берётся `FOR UPDATE`; повтор выполненного чанка ничего не делает, а сам `UPDATE` с условием (`owner_id = :from`) после первого прогона ничего не находит.
- **Импорт:** одна копия задачи на job (сессионный advisory lock по `job_id`); шаги `parse → stage → merge` с отметкой в `bulk_jobs.stage`; повтор начинается с последнего незавершённого шага; чанки merge отмечают строки staging `merged`; `merge` через `ON CONFLICT`.
- **Письма и сообщения:** `sent_notifications(dedup_key PK)`. Отправитель в **одной** транзакции: `INSERT … ON CONFLICT (dedup_key) DO NOTHING RETURNING` — ничего не вернулось, значит уже отправлено, выход; параллельная копия задачи ждёт на уникальном индексе → отправка (SMTP и Telegram с таймаутом 10 с) → успех: коммит (для письма — заодно стереть токен из payload); ошибка: откат, ключ исчезает, Celery повторяет. Дубль возможен только при падении процесса между ответом SMTP и коммитом (миллисекунды) — осознанное «как минимум один раз». Шаг 5.3: `NotificationService.send_email(event_id)` (`app/services/notifications.py`) — строки события нет (удалила очистка) → warning `email_event_missing` и выход; ключ занят → info `email_duplicate`, откат и выход; таймаут — 10 с на каждую команду SMTP (`timeout` aiosmtplib: подключение, команды, QUIT), а не `asyncio.timeout` на всю отправку: тот захватил бы и QUIT после принятого DATA — сервер письмо уже принял, а откат и ретрай отправили бы его снова; ошибки QUIT aiosmtplib сам игнорирует. Медленный сервер от зависания воркера страхует `task_soft_time_limit`; таймаут команды — `TimeoutError`, повторяется. Задача `notifications.send_email` (`rate_limit="50/m"`) — обёртка: сессия из `Resources` процесса, `email_sender` из них же.
- **Ключи дедупа** строят функции `app/notifications/keys.py` (unit-тест на формат): письма сервисов — `email:{event_id}`, если команда не задаёт свой; события notifier — `{event_type}:{subject}:{user_id}:{channel}`, где `subject` — `event_id` для `record.assigned` и `deal.closed` (каждое изменение — отдельный факт) и `job:{job_id}` для `bulk_job.completed` (одно сообщение на job, сколько бы раз ни прошёл finalize); напоминание — `activity.reminder:{activity_id}:{unix due_at}:telegram` (перенос встречи — новое напоминание); дайджест — `digest.morning:{user_id}:{YYYY-MM-DD}:telegram`.
- **Запуск job:** заголовок `Idempotency-Key` → уникальный индекс `bulk_jobs(owner_id, idempotency_key)` и `request_hash` (раздел 6).
- **Одна тяжёлая job на организацию:** частичный уникальный индекс `bulk_jobs(org_id, kind) WHERE status IN ('pending', 'running', 'cancelling') AND kind <> 'move_team'` — второй запуск получит 409 `bulk.already_running`; `cancelling` тоже держит слот, иначе старые и новые чанки шли бы параллельно. Задачи супер-админа без организации (`reindex`, этап 9) добавят свой индекс вместе с колонкой.
- **Курсы НБУ:** `INSERT … ON CONFLICT (rate_date, currency) DO UPDATE` — повторная загрузка безопасна.

### Жизненный цикл bulk job

Статусы: `pending` → `running` → `done` | `partial_failed` | `failed`; отмена: `pending`/`running` → `cancelling` → `cancelled`.

&#91;embedded content: статусы bulk job · 4 финальных, отмена из pending и running\]

Отменить можно только незавершённую job. Куда перейдёт running — в done, partial\_failed или failed, — решают `finalize_if_complete` и `bulk.reap_stuck`.

- `POST /api/v1/bulk-jobs/{id}/cancel` (`bulk_job:cancel`; job ищется по уровню `bulk_job:read` — не видна → 404): `pending` → сразу `cancelled` (plan с условием `status = 'pending'` ничего не сделает); `running` → `cancelling`; `cancelling` / `cancelled` → 202 без изменений; финальные статусы и `move_team` → 409 `bulk.not_cancellable`. Чанк читает статус job без блокировки — один начатый чанк может доработать; следующие становятся `skipped`, `finalize_if_complete` ставит `cancelled`. Уже изменённые строки остаются изменёнными — в ответе видно, сколько успели.
- `bulk.reap_stuck` (раз в минуту, очередь `default`). Правила (1)–(3) — для `kind <> 'import_contacts'`: (1) `pending` дольше 2 минут → снова `bulk.plan` (потерянная постановка из API); (2) `running` / `cancelling` без `pending`-чанков → `finalize_if_complete` (потерянный finalize); (3) `running` / `cancelling` без прогресса 5 минут (`updated_at`) **и пустая очередь `bulk`** (`LLEN bulk` в db 1: если там сообщения, чанки, скорее всего, ждут своей очереди за чужой job — не переотправлять и не считать попытку) → снова отправить `pending`-чанки, `reaps += 1`; при `reaps ≥ 3` → `error = bulk.stuck`, `pending` → `skipped`, job → `failed`, алерт `BulkJobStuck`. Для `kind = 'import_contacts'`: `pending` дольше 2 минут или `running` без прогресса 15 минут (больше лимита запуска) → снова `imports.import_contacts`, та же граница `reaps ≥ 3`. Повторная доставка того же чанка позже безопасна. Пороги — в `Settings`.
- Прогресс: `processed / total` в `bulk_jobs`. SSE-эндпоинт `/bulk-jobs/{id}/events` опрашивает job раз в секунду короткой сессией и шлёт `event: progress`, когда что-то изменилось; если ничего не отправлял 15 с — комментарий `: ping` (`KEEPALIVE_COMMENT` из `fastapi.sse`, как у FastAPI в пути `response_class`: прокси с таймаутом простоя — 60 с у nginx — не рвёт поток, пока job ждёт в `pending` или долгий чанк); на финальном статусе — `event: end`, и поток закрывается; поток живёт не дольше 5 минут (`retry: 3000` — клиент переподключится со свежим токеном). Сессия запроса возвращается в пул до начала потока (`uow.rollback()`): иначе каждая открытая вкладка держала бы соединение, и полтора десятка вкладок исчерпали бы пул (5 + 10). Аутентификация — обычный `Authorization: Bearer`: браузерный `EventSource` заголовков не шлёт, поэтому клиент читает поток через `fetch` (на этапе 11 — `@microsoft/fetch-event-source`), руками — `curl -N -H 'Authorization: Bearer …'`; токен в URL не кладём — он осел бы в логах прокси. Реализация — встроенный в FastAPI `EventSourceResponse`, но не через `response_class=EventSourceResponse`: с ним FastAPI 0.151 ждёт эндпоинт-генератор и выполняет его код уже внутри ответа 200 — 404 на чужую job и `uow.rollback()` до потока стали бы невозможны. Поэтому обычный эндпоинт проверяет job, возвращает соединение в пул и отдаёт готовый `EventSourceResponse(job_events(...))` (события — `format_sse_event`), а для Swagger — `response_class=StreamingResponse` и `responses` с `text/event-stream`. Короткая сессия опроса — зависимость `get_session_scope` (фабрика сессий из `app.state`; в integration-тестах подменена на сессию теста). Redis pub/sub ради десятка событий не нужен.

### Задачи, упавшие после всех ретраев

У Celery с Redis нет dead-letter очереди, поэтому делаем свою (шаг 5.5): сигнал `task_failure` (после исчерпания ретраев; на `Retry` его нет) → строка в `failed_tasks` (`ON CONFLICT (task_id) DO NOTHING` — повторно доставленная задача не даёт дубля) своей сессией; ошибка записи только логируется. Плюс метрика `celery_task_failed_total` и алерт `CeleryTaskFailureRate` (этап 8). До шага 5.5 провалы видны во Flower и в логах.

Супер-админ (`admin:system`): `GET /api/v1/admin/failed-tasks?task_name=&replayed=` — keyset-список без traceback, `GET …/{failed_task_id}` — со всеми полями, `POST …/{failed_task_id}/replay` → 202 `{task_id}`. Replay: строка `FOR UPDATE`; уже перезапущена → 409 `failed_task.already_replayed`; имени нет среди задач → 409 `failed_task.unknown_task`; задачи bulk и импорта (`replayable=False`: `bulk.plan`, `bulk.chunk`, `imports.import_contacts`) → 409 `failed_task.not_replayable` — их повторяют новой job, условия идемпотентны. Иначе `send_task` (через `asyncio.to_thread`; брокер недоступен → 503 и откат) → `replayed_at`, `replayed_by`, `replay_task_id`, строка журнала `failed_task.replayed` → коммит. Публикация идёт до коммита: упадёт коммит — задача выполнится лишний раз, это безопасно.

**Как сделано (шаг 5.5).**

- **Отмена** — `BulkService.cancel` (`POST /bulk-jobs/{job_id}/cancel`, 202 `BulkJobOut`): job берётся по уровню `bulk_job:read` **с `FOR UPDATE`** (`get_visible_or_404(..., for_update=True)`) — переход не обгонит plan, который берёт ту же строку; дальше `ensure_allowed` по уровню `bulk_job:cancel`, но **свою job автор отменяет всегда** — так же, как всегда её видит (руководитель, переведённый в другой отдел, иначе получил бы 403 на собственную job). `move_team` → 409 `bulk.not_cancellable` в любом статусе (`details.status`). `pending` → `cancelled` с `finished_at`, итогом `result = {"rows": …}` (нули) и строкой журнала `bulk.<kind>` (`status = cancelled`) — пишет тот, кто сделал финальный переход; `running` → `cancelling`, `updated_at` не трогается (это время прогресса, по нему работает reaper). Итог и журнал любого финального перехода — один метод `_record_final` (finalize, отмена pending, reaper, который сдался); строки staging импорта удалит в нём же шаг 5.6.
- **Reaper** — `BulkService.reap_stuck(ReapPolicy, queue_length)`, задача `bulk.reap_stuck` (очередь `default`, beat каждую минуту, `expires = 60`). Условия — функции `stale_pending`, `lost_finalize`, `stalled_chunks`, `stalled_import` в `app/repositories/bulk.py`; «давно» везде считается по `updated_at` (у pending он равен `created_at`, повторная отправка ставит `updated_at = now()` и `reaps + 1` — следующая попытка не раньше, чем через тот же порог). **Одновременные проходы** (beat поставил следующий, пока медленный идёт): кандидаты читаются без блокировок, затем каждая job — своя короткая транзакция `SELECT … FOR UPDATE SKIP LOCKED` с повторной проверкой условия; занятая строка пропускается, а после коммита первой копии вторая видит свежий `updated_at`. Advisory lock не подошёл: finalize коммитит свою транзакцию, и транзакционная блокировка отпускалась бы посреди прохода, а сессионная на пуле соединений «утекла» бы. Задачи ставятся **после** коммита (`TaskQueue`, best-effort): потерю подберёт следующий проход. Длина очереди для правила (3) — `broker_queue_length(CELERY_BROKER_URL, "bulk")` (`LLEN` в db 1, таймауты 5 с), читается только когда есть кандидаты; в тестах — подмена. Попытки кончились (`reaps ≥ BULK_MAX_REAPS`): job без чанков (pending, импорт) — сразу `failed` с `error = coalesce(error, 'bulk.stuck')` и итогом; чанковая — `set_error('bulk.stuck')`, pending-чанки → `skipped` **с `SKIP LOCKED`** (строку выполняющегося чанка держит он сам и в конце ждёт строку job, которую держит reaper, — без `SKIP LOCKED` был бы deadlock), затем `finalize_if_complete` → `failed` (или `cancelled`, если job отменяли — finalize проверяет `cancelling` первым); лог error `bulk_job_stuck`. Если чанк, пропущенный `SKIP LOCKED`, потом умер, `set_error` уже обновил `updated_at` — job дожмёт следующий проход через `BULK_REAP_STALL_SECONDS`. Правило импорта (`kind = 'import_contacts'`) — и для `cancelling` (не только `running`): переотправленная задача увидит `cancelling` и завершит job как `cancelled`. Сама задача `imports.import_contacts` появится в 5.6 — reaper ставит её по имени; правило (2) импорт не трогает (у него нет чанков, а `FINALIZE` исключает его и сам).
- **`failed_tasks`** — модель `FailedTask`, `FailedTaskRepository.record` (`INSERT … ON CONFLICT (task_id) DO NOTHING RETURNING`), обработчик `record_failed_task` в `app/workers/base.py` (подключается импортом `base` в `celery_app.py`): строка собирается в контексте задачи (`request_id_var` и `user_id` из structlog — `task_postrun` их ещё не очистил; очередь — `delivery_info.routing_key`), пишется через `run_async` своей сессией. `exc_type` — модуль и имя класса; `exc_message` — первые 2 000 символов, traceback — **последние** 20 000 (внизу — само место ошибки). Eager-вызовы (`task.apply()` в unit-тестах) не записываются: их не брал воркер, и у такого процесса может не быть базы. Жёсткий `time_limit` строки не даёт: `task_failure` в этом случае приходит в главный процесс prefork, где `request.id` не совпадает с `task_id`, и обработчик его пропускает (лог `failed_task_skipped_outside_task`) — остаются лог и Flower.
- **Перезапуск** — порт `TaskSender` (`app/workers/queue.py`): `replayable(name)` — `None`, если задачи нет в `celery_app.tasks`, иначе атрибут `replayable` (у встроенных задач Celery его нет — не перезапускаются); `send` — `send_task(..., retry=False)` в `asyncio.to_thread`, любая ошибка брокера → 503 `unavailable`. **Отступление:** чтобы знать атрибут задачи, API один раз импортирует модули задач (`celery_app.loader.import_default_modules()`, список `include`) — ставит по-прежнему по имени, но модули теперь загружены и в процессе API. До шага 5.6 задачи `imports.import_contacts` нет — её строка дала бы `failed_task.unknown_task`, а не `not_replayable`. Список — keyset по `(failed_at, id)` по убыванию, фильтры `task_name` (точное имя) и `replayed`; `limit`/`cursor` — как у остальных списков. В тестах API `TaskSender` подменён фейком (реестр — настоящий, публикация запоминается).

### Контекст и логи

`before_task_publish` берёт `request_id` (из `request_id_var`) и `user_id` (из contextvars structlog) и кладёт в заголовки задачи `saleshub_request_id`, `saleshub_user_id`; организацию не переносим — она есть в аргументах или в job. `task_prerun` восстанавливает их в `request_id_var` (его читает `AuditRepository.record` — иначе в журнале был бы NULL) и в structlog вместе с `task_id` и `task_name`; у задач beat `request_id` = `task_id`. `task_postrun` всё очищает: процесс prefork выполняет много задач, и поля одной не должны попасть в логи следующей. Задачи, которые ставит сам воркер (plan → чанки), получают тот же `request_id` автоматически. Relay перед публикацией ставит `request_id` из колонки `outbox_events.request_id`. В логах одна цепочка: HTTP-запрос → задача → событие outbox → потребитель Kafka. Тест: `request_id` из HTTP-запроса появляется в строке `audit_log`, которую записала задача.

### Расписание beat (часовой пояс Europe/Kyiv)

`app/workers/schedule.py` появляется в шаге 5.1 с одной записью (heartbeat), каждый шаг добавляет свои. У каждой записи `options={"expires": <интервал>}`: если воркер лежал, задачи не копятся в очереди. Все — в очереди `default`.

| Задача | Когда | Что делает |
| --- | --- | --- |
| `workers.heartbeat` | каждую минуту | `SET workers:heartbeat <unix ts> EX 600` в db 0; старше 3 минут — алерт `CeleryBeatDown` (этап 8) |
| `bulk.reap_stuck` | каждую минуту | правила выше |
| `notifications.send_reminders` | каждую минуту | напоминания за 15 минут (раздел 16) |
| `notifications.morning_digest` | каждые 15 минут | дайджест тем, у кого по их часовому поясу 09:00–12:00 и он ещё не отправлен сегодня (раздел 16) |
| `reports.refresh` | каждые 5 минут | `REFRESH MATERIALIZED VIEW CONCURRENTLY deal_stats` под `pg_try_advisory_xact_lock`: занято — пропуск, два REFRESH не встают друг за другом; `make refresh-reports` вызывает тот же метод |
| `fx.fetch_nbu_rates` | ежедневно 18:30 и 19:30 | `FxService.load_rates`: курсы на сегодня и завтра по Киеву → `fx_rates`; ошибка НБУ — ретрай, пустой ответ на завтра — не ошибка. Курс по-прежнему подтягивается и по первому запросу дня (раздел 3) |
| `maintenance.create_partitions` | ежедневно 01:00 | партиции `activities` и `audit_log` на 2 месяца вперёд — та же функция `ensure_partitions` (`app/db/partitions.py`), что вызывает `migrate`. Ежедневно, а не 1-го числа: если beat в тот день лежал, партиции всё равно появятся |
| `trash.purge` | ежедневно 03:00 | корзина старше 30 дней, чанками (раздел 3) |
| `maintenance.cleanup` | ежедневно 04:00 | по шагам, каждый в своих транзакциях, ошибка шага не останавливает остальные: истёкшие refresh- и email-токены; опубликованные `outbox_events` старше 7 дней; `sent_notifications` старше 30 дней (ключ живёт дольше строки outbox, которую можно доставить повторно); файлы S3 завершённых импортов старше 7 дней и сироты `imports/*` старше суток; `bulk_jobs` старше 90 дней (чанки, staging и ошибки — каскадом). Неопубликованные строки outbox не удаляются никогда — для них алерт `OutboxLag` |

Beat хранит своё состояние в файле на `emptyDir` (в compose — `/tmp`); после перезапуска пропущенный запуск просто выполнится в следующий раз — задачи идемпотентны. Переход на летнее и зимнее время: в Киеве час 03:00–04:00 в октябре повторяется, а в марте пропадает — для идемпотентных задач это допустимо. Для отказоустойчивого beat позже можно попробовать RedBeat (блокировка в Redis).

### Масштабирование и остановка

- Каждая очередь — свой Deployment со своими ресурсами: `bulk` нагружает базу, поэтому `-c 4`; `imports` ест память, поэтому `-c 2` и лимит памяти выше.
- KEDA `ScaledObject` масштабирует `worker-bulk` от 1 до 4 реплик по длине Redis-списка `bulk` (db 1).
- SIGTERM → warm shutdown: воркер дорабатывает текущую задачу. `terminationGracePeriodSeconds: 120` больше лимита чанка (90 с). Длинный импорт разбит на шаги, поэтому рестарт теряет не больше одного шага.
- Новая версия кода выполняет задачи, поставленные старой: аргументы совместимы, а переименованная задача на один релиз оставляет старое имя как алиас.

### Мониторинг

- Flower — UI (раздел 11).
- celery-exporter → Prometheus: задачи по статусам, время выполнения, длина очередей.
- Дашборд «Workers»: длина очередей, задач в секунду, p95 времени по задачам, ретраи, `failed_tasks`, активные bulk job.
- Алерты: `CeleryQueueBacklog`, `CeleryTaskFailureRate`, `CeleryBeatDown`, `BulkJobStuck`, `OutboxLag`, `FxRatesStale`.

### Тесты воркеров

- **Unit:** сервис вызывается напрямую; задача — обёртка, её логику не тестируем отдельно.
- **Integration:** фикстура `celery_worker` с реальным Redis (раздел 10) — группа чанков и завершение job, ретраи, отмена, reaper, relay → письмо.
- **Chaos (5.10):** `make chaos` против стенда `make up`: менеджер со 100 тыс. контактов → `POST /bulk/reassign-owner` → дождаться `processed > 30%` → сценарий A: `kill -9` процесса пула (PID из `celery inspect stats`) — `task_reject_on_worker_lost` сразу возвращает задачу; сценарий B: `docker compose kill -s KILL worker-bulk` и `up -d` — сообщение вернёт Redis через `visibility_timeout`, а reaper переотправит чанк минут через 5. Проверки: job `done`, `processed == total`, у старого ответственного 0 записей, у нового — все, ни один чанк не посчитан дважды. В CI не запускается, результат — в `docs/experiments.md`; на этапе 7 тот же сценарий через `kubectl delete pod`.
- **Приёмка этапа (5.15):** integration-тест `test_bulk_reassign_e2e.py` (настоящие воркер и relay, Telegram через respx): reassign → чанки → finalize → outbox → notify → у получателя и автора привязан Telegram; в Mailpit ровно одно письмо получателю и одно автору, ровно два вызова `sendMessage` — по одному каждому; повторный finalize и повторная публикация того же события ничего не добавляют. Плюс `make chaos` с проверкой писем (Telegram — если задан токен).
- **Время и внешние API:** freezegun для расписания, respx для НБУ, Telegram и SMTP-ошибок.

## 16. Модуль: уведомления и Telegram-бот

Пользователь получает уведомления CRM в Telegram и на почту — по своим настройкам. Бот на aiogram 3 — отдельный процесс, который делает две вещи: привязывает Telegram-аккаунт к пользователю CRM и отвечает на пару команд. Отправка сообщений идёт не из бота, а из воркеров, тем же путём, что и письма: всё, что отправляется, сначала записывается командой в `outbox_events` (`email.send` или `telegram.send`), relay ставит задачу, а отправитель дедуплицирует по `sent_notifications`.

В проекте два разных бота: **пользовательский** (этот раздел) и **бот для алертов** Alertmanager (раздел 8). У них разные токены и разные получатели — пользователи CRM и команда разработки.

### События и каналы

| Событие (`event_type`) | Когда пишется | Кому | Каналы по умолчанию (разрешённые) |
| --- | --- | --- | --- |
| `record.assigned` | создание контакта или сделки с чужим ответственным, смена ответственного (`CrmService`, хук `_events` перед коммитом; у компаний — нет, это общий справочник); bulk-передача — одно сообщение «вам передано 120 клиентов» через `bulk_job.completed` | новому ответственному | Telegram + email (Telegram, email) |
| `deal.closed` | сделка перешла в `won` / `lost` через `PATCH` (`DealService`); создание сразу закрытой и массовое закрытие просроченных события не дают | ответственному и активным руководителям его отдела | Telegram (Telegram, email) |
| `activity.reminder` | beat, за 15 минут до звонка или встречи | ответственному | Telegram (только Telegram) |
| `digest.morning` | beat, 09:00–12:00 по часовому поясу пользователя | каждому `team_lead` и `sales_rep` с привязанным Telegram | Telegram (только Telegram) |
| `bulk_job.completed` | финал job (одна строка на job) | автору — итог с числом ошибок; получателю `reassign_owner` — «вам передано N клиентов» (по его настройке `record.assigned`) | Telegram + email (Telegram, email) |
| приглашение, сброс пароля | сервисы раздела 4 | пользователю | только email, вне настроек |

Payload событий (`app/events/types.py`): `record.assigned` — `{entity_type, entity_id, title, owner_id, actor_id}`; `deal.closed` — `{deal_id, title, status, amount_original, currency_original, owner_id, team_id, actor_id}`; `bulk_job.completed` — `{job_id, kind, status, total, processed, failed_rows, failed_chunks, rows: {contacts: n, …} | null, author_id, to_user_id | null}`. Массовые операции по строкам событий не пишут — иначе 100 тыс. сообщений вместо одного.

**Настройки.** Таблица `notification_settings(user_id, event_type, channel, enabled)` хранит только отличия от умолчаний; умолчания и разрешённые пары — в коде (`app/notifications/catalog.py`: `ALLOWED`, `DEFAULTS`). Итог = строка пользователя, иначе умолчание. Эндпоинты `GET/PUT /api/v1/me/notifications`: GET отдаёт `{telegram_linked, channels: [{event_type, channel, enabled, is_default}]}` по всем восьми разрешённым парам; PUT `{channels: [...]}` — полная замена (удалить строки пользователя, вставить только отличия от умолчаний); неизвестная или неразрешённая пара, повтор пары — 422 `validation`. Личные настройки в журнал не пишутся. Эндпоинты `/api/v1/me/*` стоят на отдельном роутере только с `current_member`, без OPA (раздел 5). «Тихие часы» (несрочное — до утра) перенесены на этап 10.

### Как сообщение доходит до пользователя

1. Сервис (например, `DealService.update`) пишет доменное событие в outbox в той же транзакции.
2. Relay публикует `notifications.notify(event_id)`. Notifier (`NotificationService.notify`) выбирает получателей: у `record.assigned` и `deal.closed` исключает автора изменения (`actor_id` — себе не пишем); у `bulk_job.completed` автор job (`author_id`) — законный получатель итога; всегда исключает неактивных и пользователей без организации и схлопывает повторы (получатель передачи, он же автор, получает одно сообщение с итогом); для каждой пары «получатель × канал» с итоговым `enabled` пишет команду: email → `email.send` с шаблоном и контекстом, Telegram (только если есть `telegram_links`) → `telegram.send {user_id, text, dedup_key}` с текстом из того же шаблона `.txt`. Один коммит. Повтор notify создаёт команды с теми же `dedup_key` — отправитель их пропустит.
3. `notifications.send_email(event_id)` (очередь `notifications`, `rate_limit='50/m'`) рендерит шаблон Jinja2 и отправляет; `notifications.send_telegram(event_id)` (очередь `default`, без rate limit) отправляет готовый текст. Шаблоны — `app/texts/emails/{record_assigned,deal_closed,bulk_job_completed,bulk_reassigned}.{txt,html}`.
4. Дедупликация через `sent_notifications(dedup_key)` (раздел 15) — повтор задачи не отправит второе сообщение.
5. Ответы Telegram: 429 → `retry(countdown = min(retry_after + 1, 60))`; 403 или 400 «chat not found» → откат, удалить привязку `WHERE user_id = :u AND chat_id = :c` (свежую перепривязку это не задевает), коммит, без ретрая — канал выключен, пользователь включит его снова через `/start`; 5xx и сетевые ошибки — обычный ретрай; прочие 4xx — провал в `failed_tasks`.

Отправка в Telegram из воркера — не через aiogram, а одним HTTP-вызовом: `app/notifications/telegram.py: TelegramClient(http, token, base_url)` → `POST {TELEGRAM_API_URL}/bot{token}/sendMessage {chat_id, text, link_preview_options: {is_disabled: true}}`, таймаут 10 с. httpx уже есть, respx его подменяет; aiogram с его aiohttp-сессией нужен только процессу бота. Текст — простой, без `parse_mode`: не нужно экранировать названия сделок. Токен бота стоит в URL, поэтому: логгеры `httpx` и `httpcore` — на уровне WARNING (иначе каждая строка `HTTP Request: POST …/bot<token>/…` попала бы в Loki), а `TelegramClient` не вызывает `raise_for_status()` и бросает своё `TelegramApiError(status, error_code, description)` без URL — токена нет ни в логе, ни в `failed_tasks`. Unit-тест это проверяет.

### Привязка Telegram

1. В профиле `POST /api/v1/me/telegram/link` → 201 `{url: "https://t.me/<TELEGRAM_BOT_USERNAME>?start=<token>", expires_at}`. Токен — тот же механизм, что у писем: `email_tokens` с целью `telegram_link`, живёт 10 минут, хранится хэш, новый запрос гасит предыдущий. 43 символа `[A-Za-z0-9_-]` укладываются в лимит Telegram для параметра `start` (64). Бот не настроен (`TELEGRAM_BOT_USERNAME` или токен пусты) → 503 `telegram.disabled`. `GET /api/v1/me/telegram` → `{linked, username, linked_at}`, `DELETE /api/v1/me/telegram` → 204 всегда, даже без привязки.
2. Пользователь открывает ссылку, Telegram отправляет боту `/start <token>`.
3. Бот (`TelegramService.link`, одна транзакция): гасит токен (`use_email_token`, блокировка строки токена) → пользователь неактивен или без организации — «доступ закрыто» → `DELETE … WHERE chat_id = :chat AND user_id <> :user` (чат переходит к владельцу токена) → `INSERT … ON CONFLICT (user_id) DO UPDATE` (новый чат заменяет старый) → коммит → «Аккаунт привязан». Токен доказывает владение аккаунтом CRM, апдейт — владение чатом, поэтому молча перепривязать безопасно. Гонка двух `/start` из одного чата (`IntegrityError`) — откат, токен не погашен, «спробуйте ще раз».
4. Отвязка — `/stop` в боте или `DELETE /api/v1/me/telegram`. Если пользователь заблокировал бота, Telegram вернёт 403 — привязка удаляется автоматически.

### Команды бота

| Команда | Что делает |
| --- | --- |
| `/start <token>` | привязка аккаунта; `/start` без токена — «вы подключены как …» или инструкция, как привязать |
| `/today` | мои открытые задачи на сегодня (до 20) и просроченные с прошлых дней (до 10), без пересечения списков; время — `HH:MM` в моём часовом поясе |
| `/deals` | мои открытые сделки (10 новых) и итог по всем открытым: число и сумма в валюте организации через конвертер; при `fx.stale` — «(курс на <дата>)» |
| `/stop` | отвязать и выключить уведомления (работает и у отключённого пользователя) |

Любой другой текст — список команд. Бот отвечает только в личных чатах (`F.chat.type == "private"`): в группе личные данные CRM попали бы в общий чат.

Бот вызывает те же сервисы, что и API, от имени привязанного пользователя. Через HTTP-маршруты он не ходит, поэтому OPA ему не нужен. Перед каждой командой middleware `LinkedUserMiddleware` находит привязку по `chat_id`, загружает пользователя (тот же кэш Redis, что у `current_user`) и проверяет `is_active`, подтверждённый email и организацию; отключённому отвечает, что доступ закрыт, и удаляет `telegram_links`. Команды бота показывают только данные самого пользователя: бот строит `Access.for_user(user, "activity:read" | "deal:read", roles)` и сужает уровень до `own()`; если у роли нет действия — отвечает «недоступно». `/today` и дайджест собирает один `AgendaService`. Ошибки: `AppError` → текст по коду из `app/texts/uk.py`, остальное — «Сталася помилка, спробуйте пізніше» и лог с `request_id` апдейта. Тексты бота — `BOT` и `BOT_COMMANDS` в `app/texts/uk.py`; текст сообщений пользователя в лог не пишется.

Структура: `app/bot/__main__.py` (`python -m app.bot`: `Resources`, `Bot`, `delete_webhook`, `set_my_commands`, `start_polling(allowed_updates=["message"], tasks_concurrency_limit=4)`), `dispatcher.py` (`create_dispatcher(...)` — её же вызывают тесты), `middlewares.py` (сессия БД на апдейт, как сессия на запрос в API; привязанный пользователь), `handlers.py`. FSM не используется, поэтому `aiogram` ставится без extras (у `aiogram[redis]` свой потолок redis-py).

### Напоминания и дайджест

- **Напоминание за 15 минут** — `notifications.send_reminders` каждую минуту. Один запрос от привязанных активных пользователей: `telegram_links ⨝ users ⨝ LATERAL (открытые звонки и встречи этого ответственного с due_at в (now, now + 15 минут], до 20)`, минус уже отправленные ключи `activity.reminder:{id}:{unix due_at}:telegram` и минус выключенные настройки (умолчание подставляется параметром из `DEFAULTS`, чтобы не было второго источника правды). На каждую строку — команда `telegram.send` с текстом «Через N хв (о HH:MM) — дзвінок: <тема>». Окно «до начала события» догоняет пропущенные тики beat и активности, созданные меньше чем за 15 минут; перенос встречи даёт новый ключ; выполненная или удалённая не попадает в выборку. Индекс — `activities (owner_id, due_at, id) WHERE done_at IS NULL` в каждой партиции (`make explain`).
- **Утренний дайджест** — `notifications.morning_digest` каждые 15 минут: `team_lead` и `sales_rep` с привязанным Telegram, у которых локальное время (`coalesce(users.timezone, organizations.timezone)`) в окне 09:00–12:00 и ключа `digest.morning:{user_id}:{локальная дата}:telegram` ещё нет. Повестка (`AgendaService.day`) собирается в момент постановки — та же, что `/today`, с заголовком «Доброго ранку! План на <дата>». Пустая повестка — ничего не отправлять (следующий тик проверит снова). После 12:00 местного времени дайджест за этот день уже не уходит: окно переживает перезапуск beat, но «утренний» дайджест не придёт вечером.

### Запуск

- Локально бот работает через **long polling** — внешний адрес не нужен. В compose — сервис `telegram-bot` с профилем `telegram` (`make bot`, `make bot-logs`): у пользователя может не быть токена, и `make up` без него работает. Без токена бот завершается с `telegram_bot_token_missing`, `send_telegram` пропускает сообщение (`skipped`), а логика бота и отправки полностью покрыта тестами без Telegram.
- Настройки: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_BOT_USERNAME` (для ссылки без запроса `getMe`), `TELEGRAM_API_URL` (по умолчанию `https://api.telegram.org`), `TELEGRAM_LINK_TTL_MINUTES` = 10.
- В облаке — **webhook**: `POST /telegram/webhook` за Ingress с секретным заголовком `X-Telegram-Bot-Api-Secret-Token`; переключается настройкой. Появится при переходе в облако (этап 10.7): без внешнего адреса его не проверить.
- Токен бота — в SealedSecret; для local и prod разные боты.
- В Tilt и Kubernetes — отдельный Deployment `telegram-bot`, 1 реплика (при polling две копии конфликтуют).
- Тесты: обработчики aiogram тестируются без Telegram (`dp.feed_update` и ответ-метод), отправка — через respx, напоминания и дайджест — под freezegun.

## 17. Модуль: поиск на Elasticsearch

Elasticsearch — отдельный поисковый движок, который хранит **копию** данных в виде, удобном для поиска. Postgres остаётся главной базой: все записи и изменения идут туда, а в Elasticsearch данные попадают с задержкой в секунду-две через события Kafka. Поиск в CRM работает через Elasticsearch; если он недоступен — через trigram-индексы Postgres.

### Что это и чем отличается от Postgres

Postgres хранит строки и быстро находит их по точным условиям: «контакты менеджера X со статусом lead». Elasticsearch строит **обратный индекс**: для каждого слова — список документов, где оно встречается. Это как предметный указатель в конце книги. Поэтому он хорошо делает то, что Postgres делает с трудом:

| Задача | Postgres (trigram, FTS) | Elasticsearch |
| --- | --- | --- |
| Найти «Ivanov» по запросу «ivanv» (опечатка) | частично, через `similarity`, медленно на 5 млн | да, `fuzzy` из коробки |
| «Иванов», «Иванова», «Іванову» — одна фамилия | слабо, морфология ограничена | да, анализаторы для русского и украинского языков |
| Подсказки при вводе (autocomplete) | сложно | да, `search_as_you_type` |
| Сортировка по релевантности (имя совпало важнее, чем заметка) | примитивная | да, весá полей |
| Фасеты: сколько найдено по статусам, менеджерам, тегам — одним запросом | несколько `GROUP BY` | да, aggregations |
| Транзакции, связи, точные суммы денег | да | нет, это не основная база |

В проекте используем Elasticsearch: его чаще спрашивают на собеседованиях, UI — Kibana. **OpenSearch — это не то же самое**, а форк Elasticsearch 7.10.2, который AWS сделал в 2021 году после смены лицензии Elastic; сейчас им управляет Linux Foundation. Запросы и идеи у них общие, но с тех пор проекты разошлись: разные Python-клиенты (`elasticsearch-py` и `opensearch-py`, новый клиент Elasticsearch к OpenSearch не подключается), разные UI (Kibana и OpenSearch Dashboards). Поэтому весь код поиска живёт за интерфейсом `SearchRepository`: переход на OpenSearch (например, на Amazon OpenSearch Service в облаке) — это новая реализация одного класса и смена клиента, а не переписывание модуля.

### Что индексируем

Индекс `contacts` — один документ на контакт, в нём уже «склеено» всё, что нужно для поиска, без JOIN:

```json
{
  "id": 123,
  "org_id": "o1", "team_id": "t1", "owner_id": "u1",
  "full_name": "Іван Петренко",
  "email": "ivan@acme.ua", "phone": "+380671234567",
  "company": "Acme", "status": "customer", "tags": ["vip", "kyiv"],
  "open_deals_usd": 24100.0,
  "last_activity_at": "2026-10-18T14:20:00Z",
  "deleted": false
}
```

Поле `full_name` хранится в нескольких вариантах (mapping с `fields`): для точного совпадения, для морфологии и для автодополнения. Позже — индекс `deals` по той же схеме.

### Как данные попадают в Elasticsearch

&#91;embedded content: поток данных поиска · запись через Kafka, чтение напрямую\]

Верхний ряд — как изменение контакта доходит до индекса (секунда–две), нижний — как идёт поиск: сразу в Elasticsearch с фильтром прав, а при его недоступности — в trigram-индексы Postgres.

1. `ContactService` меняет контакт и пишет событие `contact.updated` в outbox (та же транзакция).
2. Relay публикует его в Kafka, топик `saleshub.contacts.v1`, ключ — `org_id`.
3. Отдельный потребитель `search-indexer` (consumer group) читает события пачками, перечитывает актуальное состояние контакта из Postgres и отправляет в Elasticsearch через Bulk API.
4. Документ версионируется полем `version` контакта (`version_type=external`): старое событие не перезапишет более новое, даже если события пришли не по порядку.

Это первый настоящий потребитель Kafka в проекте: если индексатор упал, он продолжит с последнего offset, а для переиндексации можно перечитать топик с начала.

### Полная переиндексация без простоя

При изменении mapping или после восстановления данных:

1. Создаём новый индекс `contacts_v2` рядом со старым.
2. Bulk job читает 5 млн контактов из Postgres keyset-пачками и заливает через Bulk API — это ещё одно упражнение по большим объёмам (раздел 6).
3. Индексатор во время заливки пишет в оба индекса.
4. Переключаем алиас `contacts` → `contacts_v2` атомарно; приложение всегда обращается к алиасу.

### Права в поиске

Поиск обязан видеть ровно то же, что и списки. `Access` из раздела 5 получает метод `es_filter()` — тот же уровень, только в виде фильтра Elasticsearch:

```python
def es_filter(self) -> dict:
    org = {"term": {"org_id": str(self.org_id)}}
    match self.scope:
        case "org":
            return org
        case "team":
            return {"bool": {"filter": [org, {"term": {"team_id": str(self.team_id)}}]}}
        case "own":
            return {"bool": {"filter": [org, {"term": {"owner_id": str(self.user_id)}}]}}
    raise ValueError(f"неизвестный уровень: {self.scope}")
```

Фильтр `deleted: false` и фильтр `Access` добавляются в каждый запрос в одном месте — в `SearchRepository`, вручную в эндпоинтах их не пишут. Отдельный тест проверяет, что `sales_rep` не находит чужих клиентов ни в списке, ни в поиске.

### API

| Метод | Путь | Что делает |
| --- | --- | --- |
| GET | `/api/v1/search/contacts?q=…&status=…&tags=…` | поиск с опечатками, фасеты по статусу, тегам, ответственному |
| GET | `/api/v1/search/suggest?q=…` | подсказки при вводе: первые 10 имён и компаний |
| POST | `/api/v1/admin/search/reindex` | полная переиндексация (только супер-админ, `admin:system`), это bulk job `reindex` без `Access` — она системная |

### Запуск и ресурсы

- Локально — один узел Elasticsearch с памятью JVM 2 ГБ плюс Kibana: около 3–4 ГБ, на ноутбуке с 32 ГБ помещается рядом со всем остальным.
- Kibana — UI: смотреть индексы и документы, отлаживать запросы в Dev Tools (добавляется в таблицу UI, раздел 11).
- Для украинского языка — плагин морфологии `analysis-ukrainian`, ставится при сборке образа.
- В облаке — Elastic Cloud или Amazon OpenSearch Service; переключается адресом в values, как остальные внешние сервисы.
- Метрики — elasticsearch-exporter; алерты: кластер `red`, отставание индексатора (consumer lag > 10 000 сообщений).
