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
| Очереди | Celery + Redis (broker + result backend), Celery beat, Flower | этап 5, детали — раздел 15 |
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
| Уведомления | aiogram (Telegram-бот), письма через SMTP | раздел 16 |
| Поиск | Elasticsearch + Kibana | раздел 17 |

**Версии:** Python 3.13 и PostgreSQL 18. Python 3.13 — проверенная ветка: у всех библиотек (asyncpg, pydantic-core, argon2) есть готовые wheels, поддержка до октября 2029. PostgreSQL 18 — текущая стабильная ветка. Переход на Python 3.14/3.15 и PostgreSQL 19 — отдельное упражнение на этапе 10. Точные версии пакетов фиксируются в `uv.lock`.

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

**Celery-воркеры** используют те же сервисы, UoW и репозитории, но получают сессию не из FastAPI Depends, а из своей фабрики (`worker_session_factory`, engine создаётся на процесс воркера в сигнале `worker_process_init`). Так бизнес-логика пишется один раз.

### Структура репозитория

```text
sales_hub/
├── backend/
│   ├── app/
│   │   ├── main.py              # create_app(): роутеры, middleware, handlers
│   │   ├── core/                # config (pydantic-settings), logging, security (jwt, hash)
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
│   │   │   └── v1/              # auth, users, orgs, teams, companies, contacts, deals, activities, bulk, reports
│   │   ├── errors/              # AppError иерархия + exception handlers
│   │   ├── emails/              # EmailSender (SMTP), Mailer (пока BackgroundTasks), Jinja2-рендер
│   │   ├── middleware/          # request_id, access log, unhandled errors (500), позже metrics
│   │   ├── events/              # доменные события, outbox relay
│   │   ├── workers/             # celery_app.py, runtime.py, schedule.py, tasks/ (раздел 15)
│   │   ├── bot/                 # Telegram-бот на aiogram (раздел 16)
│   │   ├── search/              # SearchRepository, индексатор (раздел 17)
│   │   ├── texts/               # украинские тексты: ошибки, письма, бот
│   │   └── cli.py               # create-superadmin, seed
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
| `email_tokens` | id, user\_id, email, purpose (`invite` / `reset_password`), token\_hash, payload jsonb (org\_id, team\_id, role для приглашения), created\_at, expires\_at, used\_at | раздел 4 |
| `organizations` | id, name, slug (unique), plan, display\_currency (по умолчанию UAH), timezone (по умолчанию Europe/Kyiv), created\_at | тенант: компания, которая пользуется CRM; создаёт только супер-админ |
| `teams` | id, org\_id, name, created\_at | отдел продаж; UNIQUE (id, org\_id) для составного FK из `users`; отдел с людьми удалить нельзя — 409 |

### CRM-сущности

| Таблица | Ключевые поля | Объём / заметки |
| --- | --- | --- |
| `companies` | id, org\_id, team\_id, owner\_id, name, domain, industry, size, created\_at, deleted\_at | 300 тыс.; компании-клиенты |
| `contacts` | id (bigint), org\_id, team\_id, owner\_id, company\_id, first\_name, last\_name, email (citext), phone, status (`lead` / `customer` / `churned`), source, tags text\[\], created\_at, updated\_at, deleted\_at, version | **5 млн**; уникальность `(org_id, email)` для дедупликации при импорте |
| `pipelines`, `pipeline_stages` | id, org\_id, name; stage: pipeline\_id, name, position, probability | воронки и этапы, настраивает org admin |
| `deals` | id (bigint), org\_id, team\_id, owner\_id, contact\_id, company\_id, pipeline\_id, stage\_id, title, amount\_usd numeric(14,2), amount\_original numeric(14,2), currency\_original char(3), fx\_rate numeric(14,6), fx\_date, status (`open` / `won` / `lost`), expected\_close\_date, closed\_at, created\_at, updated\_at, deleted\_at, version | **1 млн** |
| `activities` | id, org\_id, team\_id, owner\_id, deal\_id, contact\_id, type (`call` / `meeting` / `email` / `task`), subject, due\_at, done\_at, created\_at | **20 млн**, `PARTITION BY RANGE (created_at)` по месяцам |
| `notes` | id, org\_id, entity\_type, entity\_id, author\_id, body, created\_at | заметки к контакту, компании, сделке |

### Служебные

| Таблица | Назначение |
| --- | --- |
| `bulk_jobs` | id, kind (`import_contacts`, `reassign_owner`, `move_team`, `close_stale_deals`, `add_tags`, `export`, `reindex`), status, stage (шаг импорта), params jsonb (фильтр + `Access` того, кто запустил), idempotency\_key, total, processed, failed, error, org\_id (NULL у задач супер-админа), team\_id, owner\_id (кто запустил — по этим полям работают `bulk_job:read` и `bulk_job:cancel`), timestamps |
| `import_errors` | job\_id, row\_no, raw jsonb, error — строки CSV, которые не прошли валидацию |
| `outbox_events` | transactional outbox: события и письма |
| `audit_log` | кто что изменил, `PARTITION BY RANGE (occurred_at)` |
| fx\_rates | rate\_date, currency, rate\_uah numeric(14,6) (гривен за 1 единицу валюты), source = NBU, fetched\_at; PK (rate\_date, currency) — курсы НБУ |
| failed\_tasks | задачи Celery, упавшие после всех ретраев: имя, аргументы, ошибка, request\_id, replayed\_at (раздел 15) |
| sent\_notifications | dedup\_key (unique), channel, user\_id, sent\_at — защита от повторной отправки писем и сообщений |
| telegram\_links, notification\_settings | привязка chat\_id к пользователю; настройки «событие × канал» (раздел 16) |

Таблиц `roles`, `permissions`, `role_permissions` и членств нет: права ролей живут в файле `policies/saleshub/data.yaml` (его читают OPA и бэкенд), в базе только `users.role` и `users.team_id`. `contacts.id` и `deals.id` — bigint: для чанков по диапазонам id и keyset-пагинации это проще и быстрее.

### Удаление: в корзину, а не насовсем

В реальных CRM удалённого клиента можно восстановить, поэтому `companies`, `contacts`, `deals` удаляются **мягко**: строка остаётся, ей ставится `deleted_at = now()`.

- Все запросы по умолчанию добавляют `deleted_at IS NULL` (общий фильтр в репозитории, а не в каждом запросе руками).
- Эндпоинты корзины: `GET /contacts/trash`, `POST /contacts/{id}/restore`.
- Через 30 дней задача beat `trash.purge` удаляет строки физически, чанками.
- Индексы делаются частичными: `... WHERE deleted_at IS NULL` — удалённые строки не раздувают индекс.
- Уникальность email тоже частичная: `UNIQUE (org_id, email) WHERE deleted_at IS NULL` — иначе нельзя завести контакт с email удалённого.
- `activities`, `notes`, `audit_log` удаляются жёстко (или старыми партициями целиком).

### Деньги: храним в USD, вводим и показываем в любой валюте

Все суммы хранятся в долларах, а пользователь вводит и видит их в гривне или другой валюте. Конвертер пересчитывает по официальному курсу НБУ на дату операции. Рядом с суммой в USD сохраняется то, что ввёл человек, и курс: тогда обратный пересчёт даёт ровно исходную цифру, а не 99 999,98 грн вместо 100 000.

**Поля в `deals`:**

| Поле | Пример | Зачем |
| --- | --- | --- |
| `amount_usd` | 2 410.32 | основа всех отчётов и сумм, одна валюта на всю базу |
| `amount_original` | 100 000.00 | ровно то, что ввёл пользователь |
| `currency_original` | `UAH` | валюта ввода |
| `fx_rate` | 41.4880 | курс НБУ (гривен за 1 USD), по которому пересчитали |
| `fx_date` | 2026-10-19 | на какую дату взят курс |

**Как считается.** НБУ публикует курс гривни к каждой валюте. В USD пересчитываем через гривну: `amount_usd = amount × курс(валюта→UAH) / курс(USD→UAH)`. Для гривны — просто делим на курс доллара. Считаем в `Decimal`, округляем до центов один раз в конце (банковское округление).

**Запись (создание и изменение сделки):** клиент присылает `{amount: 100000, currency: "UAH"}` → `FxService.to_usd(amount, "UAH", on_date)` → в базу пишутся все пять полей. Если сумма не менялась, курс не пересчитывается.

**Чтение — «переводим обратно, если нужно»:**

- Карточка сделки показывает исходную сумму (`amount_original` + `currency_original`) — без пересчёта, ровно как ввели.
- Списки, канбан и отчёты принимают `?currency=UAH`: `amount_usd` пересчитывается по курсу на сегодня (или на дату отчёта). В ответе всегда есть `fx: {currency, rate, date}`, чтобы было видно, по какому курсу посчитано.
- У организации есть `display_currency` (по умолчанию `UAH`) — валюта ответа, если параметр не передан.
- Агрегаты (сумма воронки, выручка менеджера) всегда считаются в USD в SQL и пересчитываются один раз в конце, а не по каждой строке.

**Курсы НБУ.** Источник — открытый API НБУ: `GET https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=USD&date=YYYYMMDD&json`. Важные правила НБУ:

- курс устанавливается в конце рабочего дня и начинает действовать со следующего рабочего дня; публикуется на сайте до 18:00;
- курс, установленный в пятницу или перед праздником, действует все выходные и праздники.

Отсюда логика:

1. Задача beat `fx.fetch_nbu_rates` в будни в 18:30 (повтор в 19:30) загружает курсы на следующий день в `fx_rates`.
2. `FxService.get_rate(currency, date)`: Redis-кэш → `fx_rates` → если строки на эту дату нет, берём последнюю с `rate_date ≤ date` (выходные и праздники).
3. Если НБУ недоступен и курса на сегодня нет — используем последний известный, в ответе флаг `fx_stale: true`, алерт `FxRatesStale` (раздел 8).
4. При импорте сделок из CSV конвертация идёт в SQL одним `JOIN` на `fx_rates`, а не построчно в Python.

**Тесты:** НБУ мокается (respx); проверяем выходные, праздник, недоступность НБУ, а также что `to_usd` → обратно по тому же курсу даёт исходную сумму с точностью до копейки.

Источники: [инструкция НБУ к API курсов](https://bank.gov.ua/admin_uploads/article/Instr_API_KURS_VAL_Full_eng.pdf), [порядок установления официального курса](https://news.dtkt.ua/finance/bank-system/33254-nacbank-zminiv-poriadok-vstanovlennia-oficiinogo-kursu-grivni).

### Индексы: базовые и экспериментальные

Каждый индекс проверяется парой «без / с» через `EXPLAIN (ANALYZE, BUFFERS)`; результаты — в `docs/experiments.md`.

| Запрос | Индекс | Что изучаем |
| --- | --- | --- |
| «мои клиенты» менеджера | `contacts (org_id, owner_id) WHERE deleted_at IS NULL` | частичный составной индекс под scope `own` |
| клиенты отдела, keyset | `contacts (team_id, created_at DESC, id DESC) WHERE deleted_at IS NULL` | keyset против OFFSET на глубокой странице |
| дедупликация при импорте | `UNIQUE contacts (org_id, email) WHERE deleted_at IS NULL` | `ON CONFLICT` на частичном уникальном индексе |
| канбан воронки | `deals (pipeline_id, stage_id) INCLUDE (title, amount_usd, owner_id) WHERE status = 'open'` | покрывающий индекс, index-only scan |
| просроченные сделки | `deals (org_id, expected_close_date) WHERE status = 'open'` | частичный индекс для bulk «закрыть просроченные» |
| поиск по имени / email | GIN `trgm` на `first_name, last_name, email` | pg\_trgm, `ILIKE '%ivan%'` |
| сегмент по тегам | GIN `contacts (tags)` | массивы, `@>` |
| лента активностей | BRIN `activities (created_at)` + partition pruning | BRIN и партиции |
| задачи менеджера на сегодня | `activities (owner_id, due_at) WHERE done_at IS NULL` | частичный индекс на партиционированной таблице |

`pg_stat_statements` включён; индексы на больших таблицах создаются только `CONCURRENTLY` (раздел 9).

### Генерация данных

`scripts/seed_big.sql` через `generate_series` создаёт 20 организаций, по 3–10 отделов, 2 000 пользователей, 300 тыс. компаний, **5 млн контактов**, 1 млн сделок и 20 млн активностей. Распределение неравномерное (у части менеджеров клиентов в 50 раз больше), чтобы планировщик запросов вёл себя как в жизни. Для импорта — отдельный генератор CSV на 500 тыс. строк с 5% дублей и 1% битых строк.

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

Одна таблица `email_tokens` для двух целей: `invite` (живёт 7 дней) и `reset_password` (1 час). В письмо уходит случайная строка, в БД — только её SHA-256 хэш; токен одноразовый (`used_at`), новый запрос на ту же цель гасит предыдущие. Для приглашения в `payload` лежат организация, отдел и роль.

Письмо отправляет не API, а воркер: сервис пишет событие `member.invited` / `password.reset_requested` в outbox → задача `notifications.send_email` (Jinja2-шаблоны). Приглашение не падает, если SMTP недоступен, — письмо уйдёт с ретраем. До этапа 5 (воркеры) письмо отправляет `BackgroundTasks` FastAPI через тот же интерфейс `EmailSender` — после коммита и уже после ответа клиенту: ответ не ждёт SMTP — по времени ответа `password/forgot` не видно отправки письма (небольшая разница остаётся: для существующего email сервис ещё записывает токен в базу), а ошибка SMTP только пишется в лог (письмо теряется; ретраи появятся вместе с очередью). На этапе 5 меняется только место вызова `EmailSender`: вместо `BackgroundTasks` — outbox и задача. В dev — Mailpit (SMTP на 1025, веб на 8025), в prod — любой SMTP или Amazon SES.

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

`admin:*` нет ни у одной роли — эти эндпоинты проходит только супер-админ, и только их. Менеджер не удаляет даже свои записи — только руководитель: так клиенты не пропадают из отчётов отдела, а ошибку менеджер правит через `PATCH`.

**Что значит уровень для изменения.** Изменение проверяется дважды. Старая версия должна быть видна (репозиторий ищет объект через `get_visible_or_404`, иначе 404 — не раскрываем, что объект существует). Новая версия должна попадать в уровень (`ensure_allowed`, иначе 403 — объект пользователь уже видел, скрывать нечего). При создании проверяется только новая версия. Поэтому `sales_rep` создаёт клиента только на себя и не может отдать свою сделку коллеге, а `team_lead` может переназначить сделку внутри отдела, но не отдать её в чужой отдел.

**Откуда берутся `org_id`, `owner_id` и `team_id` записи.** Клиент их не выбирает свободно:

- `org_id` — всегда из `Access` (организация того, кто делает запрос);
- `owner_id` — по умолчанию текущий пользователь; руководитель и админ могут указать другого, но ответственным может быть только активный `team_lead` или `sales_rep` той же организации;
- `team_id` — всегда отдел ответственного; его ставит сервис, из тела запроса он не берётся.

Так `team_id` и `owner_id` никогда не расходятся, и проверка «новая версия в моём уровне» у `team_lead` автоматически означает «новый ответственный — из моего отдела».

**Ссылки на другие записи.** Каждый id в теле запроса (`company_id`, `contact_id`, `deal_id`, `stage_id`) сервис загружает и проверяет через `Access` на чтение этой сущности; невидимая ссылка → 422 `validation` с полем в `details`. Компании видны всей организации, поэтому к любой компании привязать можно, а к чужому контакту — нельзя.

**Дубль, которого не видно.** Email контакта уникален в организации (раздел 3), а видимость ограничена уровнем. Если менеджер создаёт контакт, меняет email или восстанавливает контакт из корзины, а такой email уже есть у записи вне его уровня, сервис возвращает 409 `contact.duplicate_email` и в `details` кладёт только имя ответственного: `{"owner": "Іван Петренко"}` — без id и данных контакта. Менеджер знает, к кому обратиться, а передать клиента может руководитель (так делают контроль дублей в Bitrix24 и amoCRM).

**Активности и заметки видны по родителю.** Карточка (`GET /contacts/{id}/notes`, активности сделки) загружает родителя через `get_visible_or_404` и дальше по `Access` не фильтрует: кто видит сделку, видит всю её историю. Уровень `activity:read` применяется к спискам вне карточки — например, «мои задачи на сегодня» (`GET /activities?due=today`). Заметку добавляет тот, кто может менять родителя; править и удалять её может только автор.

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

**Почему одно правило записано в трёх формах и как они не разойдутся.** `matches()` проверяет объект в памяти, `sql_filter()` — строки в Postgres, `es_filter()` — документы в Elasticsearch. Все три лежат рядом в `access.py`, и каждая — это три ветки `own / team / org`. Тест на hypothesis генерирует сотни случайных пользователей и объектов и проверяет, что все три формы отвечают одинаково. Это главная защита от ситуации «в списке не видно, а по прямой ссылке открывается».

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
      bulk_job:read: own       # свои выгрузки
      bulk_job:cancel: own
      export:run: org
      report:read: org
```

**Как поменять права.** Хотите, чтобы менеджер видел клиентов всего отдела, — меняете `contact:read: own` на `contact:read: team` у `sales_rep`. Хотите запретить действие — удаляете строку. Код при этом не меняется; тесты проверят, что опечаток нет, а массовые операции не стали шире прав на одну запись.

### Политики

Проверены `opa test` на OPA 1.4.2: 11 тестов проходят, `opa fmt` и `opa check` без замечаний; намеренная опечатка в имени действия, неверный уровень и слишком широкий `export:run` роняют тесты.

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
	# --- этап 5: массовые операции и выгрузка
	"POST /api/v1/contacts/import": "bulk:run",
	"POST /api/v1/bulk/reassign-owner": "bulk:run",
	"POST /api/v1/bulk/close-stale-deals": "bulk:run",
	"POST /api/v1/bulk/add-tags": "bulk:run",
	"POST /api/v1/exports": "export:run",
	"GET /api/v1/bulk-jobs/{job_id}": "bulk_job:read",
	"GET /api/v1/bulk-jobs/{job_id}/events": "bulk_job:read",
	"POST /api/v1/bulk-jobs/{job_id}/cancel": "bulk_job:cancel",
	"GET /api/v1/admin/failed-tasks": "admin:system",
	"POST /api/v1/admin/failed-tasks/{task_id}/replay": "admin:system",
	# --- этап 9: поиск
	"GET /api/v1/search/contacts": "contact:read",
	"GET /api/v1/search/suggest": "contact:read",
	"POST /api/v1/admin/search/reindex": "admin:system",
}
```

Пути организации не содержат `org_id`: организация у пользователя одна и берётся из него самого. `org_id` в пути есть только у эндпоинтов супер-админа. Не проходят через OPA: `/api/v1/auth/*`, `/api/v1/me/*` (личные настройки и привязка Telegram — только `current_user`, работают с записями самого пользователя), `/health/*`, `/metrics`, `/.well-known/jwks.json`, `/telegram/webhook` (своя проверка секретным заголовком).

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
	pairs := {"bulk:run": "contact:write", "export:run": "contact:read"}
	bad := {[name, a] |
		some name, r in data.saleshub.roles
		some a, base in pairs
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

    @classmethod
    def for_user(cls, user: CurrentUser, action: str) -> "Access":
        return cls(action, scope_for(user, action), user.id, user.org_id, user.team_id)

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
   - приглашение на email, который уже есть в базе, не создаётся, но ответ тот же 202, а человеку уходит письмо «вы уже состоите в другой организации» — админ чужой организации не узнаёт, что email занят (как в `password/forgot`). При `invite/accept` сервис ещё раз проверяет, что email свободен и отдел существует (иначе 409 `invite.stale`).
   Причина отказа возвращается в теле ответа (`code` = `member.cannot_change_self`, `member.team_required` и т. д.).
5. Перевод менеджера в другой отдел запускает bulk job `move_team`: `team_id` всех его записей переносится теми же чанками, что и при передаче клиентов (раздел 6), — клиенты переходят в новый отдел вместе с ним.
6. Любое изменение `role`, `team_id`, `is_active` → **после** `commit` удалить пользователя из кэша Redis + запись в `audit_log`. Токен перевыпускать не нужно: роли нет в JWT, новая роль действует со следующего запроса (60 с — лишь страховка, если удаление ключа не прошло). Отключение пользователя дополнительно делает `token_version += 1` и отзывает все его refresh-токены — доступ пропадает сразу.
7. Изменить права роли (что может `sales_rep`) — PR в `data.yaml` + `opa test` в CI + деплой.

### Как OPA развёрнут

- **Сайдкар** в поде `api`: `opa run --server --addr=127.0.0.1:8181 --watch --ignore='..*' /policies`. Запрос по localhost — около миллисекунды.
- **Kubernetes.** Политики попадают в под через ConfigMap: Helm собирает `routes.rego`, `authz.rego` и `data.yaml` (без `*_test.rego`). ConfigMap не хранит подпапки, поэтому он монтируется в `mountPath: /policies/saleshub` — тогда `data.yaml` оказывается в `data.saleshub.roles`. `--ignore='..*'` нужен из-за служебных папок `..data` и `..<время>`, которые Kubernetes кладёт в каждый ConfigMap: без него OPA видит файлы трижды и не стартует, а с `--bundle` кладёт данные не туда, и все получают 403 (проверено на OPA 1.4.2). Тот же ConfigMap монтируется и в контейнер `api` (`POLICIES_DIR=/policies`), поэтому OPA и бэкенд читают один файл. В шаблоне пода — аннотация `checksum/policies`: при правке политик Helm перезапускает под, и обе стороны перечитывают правила. Позже (этап CD) — бандлом: CI делает `opa build` и публикует bundle в GHCR как OCI-артефакт.
- **Compose.** Папка `./policies` монтируется и в `opa` (та же команда), и в `api`; uvicorn запускается с `--reload --reload-dir /policies --reload-include 'data.yaml'`, иначе он следит только за `.py`. Правка `.rego` и `data.yaml` применяется сразу в обоих. В Tilt — то же через `sync`.
- **Проверка после деплоя:** `curl localhost:8181/v1/data/saleshub/roles` не должен быть пустым.
- **Fail closed:** OPA недоступен → 503, алерт `OpaUnavailable` (раздел 8).
- **Decision logs:** OPA пишет каждое решение (вход + результат) в stdout → Loki → Grafana: видно, кому и почему отказали.

### Права в Swagger и тест покрытия

- При старте приложение читает `data.saleshub.routes.actions` из OPA и дописывает в описание каждой операции «Требуется: `deal:write`», а также роли и уровни из `data.yaml`: «org\_admin — org, team\_lead — team, sales\_rep — own».
- `GET /api/v1/auth/me` возвращает роль и `permissions` — действия роли из `data.yaml` с уровнями; фронт по ним прячет кнопки. У супер-админа — `is_superadmin: true` и пустой `permissions`.
- `test_routes_match_policy` (pytest): множество маршрутов роутеров `protected` и `admin` должно совпадать с ключами `actions` в `routes.rego`. Новый эндпоинт без правила или устаревшее правило без эндпоинта — CI падает. Второй тест перехватывает `input`, который `opa_decision` реально отправляет в OPA, и проверяет, что `METHOD route` есть среди ключей, — так ловится ошибка с префиксом пути.

### Нужен ли Auth0

Нет. Auth0 — провайдер входа (пароли, MFA, SSO, соцсети); всё это делает свой модуль из раздела 4. Row-level доступ (own / team / org) Auth0 не решает в любом случае — это задача бэкенда. Попробовать Auth0 можно на этапе 10: проверка токена уже идёт через JWKS, меняются только `ISSUER`, `AUDIENCE` и `JWKS_URL`.

## 6. Большие объёмы данных и фоновая обработка

Массовая операция — это асинхронная job: API проверяет права, считает объём, создаёт запись `bulk_jobs` и сразу отвечает `202 + job_id`. Воркер режет выборку на чанки по диапазонам id и обновляет каждый чанк одним set-based SQL в своей транзакции. В очередь уходит одна команда, а SQL выполняется пачками — не одной транзакцией на миллион строк и не по одной строке.

> Система воркеров (очереди, ретраи, масштабирование, мониторинг, async внутри Celery) описана отдельным модулем в разделе 15. Здесь зафиксирован только общий подход к объёму данных.

### Сценарии CRM

| Сценарий | Объём | Техника |
| --- | --- | --- |
| Импорт контактов из CSV с дедупликацией по email | 500 тыс. строк | файл в S3 (RustFS) → `COPY` в `contacts_staging` → валидация SQL-ом → `INSERT … ON CONFLICT (org_id, email) WHERE deleted_at IS NULL DO UPDATE SET … WHERE <фильтр Access из params>`: существующий контакт обновляется, только если он в уровне того, кто запустил импорт, остальные дубли → `import_errors` (`duplicate_outside_scope`). Новые строки: ответственный — из колонки `owner_email` (в уровне запустившего), иначе сам запустивший; `team_id` — отдел ответственного; битые строки → `import_errors` |
| Менеджер уволился: передать всех его клиентов | до 100 тыс. контактов + сделки + открытые активности | чанки `UPDATE … SET owner_id = :new WHERE owner_id = :old AND id BETWEEN …` по трём таблицам |
| Закрыть просроченные сделки как `lost` | десятки тысяч | частичный индекс `(org_id, expected_close_date) WHERE status = 'open'` + чанки |
| Добавить тег сегменту | до 1 млн контактов | `UPDATE … SET tags = array_append(tags, :t) WHERE NOT tags @> ARRAY[:t]` — идемпотентно |
| Экспорт сегмента в CSV | до 5 млн | серверный курсор + `StreamingResponse`; для больших — файл в S3 (RustFS) и ссылка |
| Очистка корзины | всё, что удалено > 30 дней | задача beat, `DELETE` чанками |

### Способы обновить много строк

| Способ | Когда | Скорость | Риск |
| --- | --- | --- | --- |
| ORM в цикле | никогда для объёма | очень медленно | N запросов, память |
| Один `UPDATE … WHERE` на всё | до \~50 тыс. строк | максимум | долгая транзакция, блокировки, всплеск WAL |
| `UPDATE … WHERE id BETWEEN a AND b` чанками по 5–10 тыс. | основной вариант | высокая | нужен прогресс и идемпотентность |
| `UPDATE … FROM (SELECT unnest(:ids), unnest(:vals))` | разные значения на строку, десятки тысяч | высокая | размер параметров |
| `COPY` в staging → `INSERT … ON CONFLICT DO UPDATE` | импорт, сотни тысяч+ | максимум | нужен файл или поток |
| SQLAlchemy 2.0 bulk `update()` со списком dict-ов | средние объёмы | средняя | следить за размером батча |

Каждый способ замеряется на 1 и 5 млн контактов, результаты — в `docs/experiments.md`.

### Поток: «передать клиентов уволенного менеджера»

1. `POST /api/v1/bulk/reassign-owner {from_user_id, to_user_id, include: [contacts, deals, open_activities]}` + заголовок `Idempotency-Key`.
2. API: `authorize()` → OPA: `bulk:run` → проверка, что новый ответственный — активный `team_lead` или `sales_rep` в уровне запустившего (для `team_lead` — из его отдела, для `org_admin` — любой в организации) → фильтр `owner_id = :from` AND `access.sql_filter()` → `count`, `min(id)`, `max(id)` по каждой таблице → `bulk_jobs(status=pending, total=…, params={…, access})` → `uow.commit()` → задача `bulk.plan` → `202 {job_id}`.
3. `bulk.plan`: режет диапазоны id по 10 000 и запускает группу задач `bulk.chunk` с финальной `bulk.finalize`.
4. `bulk.chunk(job_id, table, lo, hi)`: одна транзакция: `UPDATE … SET owner_id = :to, team_id = :to_team, version = version + 1 WHERE id BETWEEN lo AND hi AND owner_id = :from AND <фильтр Access из params>`; воркер восстанавливает `Access` из `bulk_jobs.params`, иначе руководитель отдела мог бы массово переназначить чужих клиентов; перед работой чанк проверяет, что автор job всё ещё активен и его роль и отдел не менялись (иначе job → `failed`, `bulk.author_changed`); `processed += rowcount`; событие в outbox.
5. `bulk.finalize`: статус `done` или `partial_failed`, событие `bulk_job.completed` → письмо новому ответственному «вам передано N клиентов».
6. `GET /api/v1/bulk-jobs/{id}` отдаёт прогресс.

Чанк обязан быть **идемпотентным**: повторный запуск того же диапазона даёт тот же результат (условие `owner_id = :from` после первого прогона уже ничего не найдёт). Поэтому `acks_late=True` и ретраи безопасны — упавший воркер не теряет и не дублирует работу.

### Остальное про объём

- **Пагинация**: keyset (`WHERE (created_at, id) < (:c, :i)`) против OFFSET — замер на 100-й тысяче контактов.
- **Отчёты**: сумма открытых сделок по менеджерам и этапам — materialized view, `REFRESH … CONCURRENTLY` задачей beat; сравнить с прямым запросом.
- **Партиции**: `activities` и `audit_log` по месяцам; старые месяцы — `DETACH PARTITION` + архив вместо `DELETE`.
- **Конкурентность**: чанки упорядочены по id против дедлоков; оптимистическая блокировка через `version` для ручных правок карточки; очередь в таблице через `SELECT … FOR UPDATE SKIP LOCKED` — отдельный эксперимент.

### Celery: общая схема

| Компонент | Роль |
| --- | --- |
| Broker (Redis) | очередь сообщений-команд |
| Workers | очереди `default`, `bulk`, `imports`, `notifications` — отдельные Deployment |
| Beat | планировщик, **1 реплика**: курсы НБУ, `bulk.reap_stuck`, `trash.purge`, партиции, `reports.refresh` |
| Outbox relay | отдельный долгоживущий процесс, а не задача beat (раздел 15) |
| Result backend (Redis) | статусы задач, нужен для групп с финальной задачей |
| Flower | веб-мониторинг |

Детали (async внутри задач, `visibility_timeout`, `prefetch_multiplier`, маршрутизация, масштабирование) — в разделе 15.

### Событийная система: outbox, потом Kafka

Сервисы не шлют события в брокер напрямую: они пишут их в `outbox_events` в той же транзакции, что и данные. Relay читает неопубликованные события (`FOR UPDATE SKIP LOCKED`) и публикует. Событие не теряется, если коммит прошёл, а брокер недоступен, и не уходит, если коммит откатился.

- **Этап 5:** relay публикует в Celery (письма, уведомления).
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
| `IntegrityError` (unique, SQLSTATE 23505) | 409 `conflict`; остальные нарушения (FK, CHECK) — ошибка в коде → 500 | warning |
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
- `request_id` передаётся в Celery через заголовки задачи (`before_task_publish` / `task_prerun`) и в `outbox_events.payload.meta`.
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
| `BulkJobStuck` | job в `running` без прогресса 15 мин | warning |
| `OutboxLag` | старейшее неопубликованное событие > 60 с | critical |
| `CeleryBeatDown` | нет heartbeat beat 2 мин | critical |
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
- Партиции `audit_log`: родитель создаётся миграцией, месячные партиции — задачей beat заранее на 2 месяца вперёд.

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
| integration | репозитории на настоящем Postgres, API через `httpx.AsyncClient(transport=ASGITransport(app))`, миграции, Celery-задачи в eager-режиме и с реальным воркером | testcontainers (postgres, redis), factory-boy | десятки секунд |
| e2e (smoke) | логин → создать контакт → передать клиентов (bulk) → дождаться job | pytest против `tilt`/kind-стенда | минуты, в CD |
| нагрузка (опц.) | список контактов и bulk на 5 млн | locust / k6 | вручную |
| policy (Rego) | роль × эндпоинт (allow / 403), опечатки в действиях и уровнях `data.yaml` | opa test --coverage, Regal (линтер) | миллисекунды |
| auth flow | приглашение → письмо → accept → login → refresh → повтор старого refresh (отзыв семьи) → reset | integration + Mailpit API (чтение писем из теста), freezegun для сроков | секунды |

### Изоляция БД в integration-тестах

- Один контейнер Postgres на сессию pytest, `alembic upgrade head` один раз.
- Каждый тест — во внешней транзакции с `SAVEPOINT` (`join_transaction_mode="create_savepoint"`), откат после теста: быстро и без очистки таблиц. Подменённая `get_session` при ошибке в запросе откатывает его SAVEPOINT — как настоящий запрос, который при ошибке ничего не сохраняет. После такого отката объекты сессии «протухают», поэтому значения, нужные тесту после ответа 4xx (например, email), держат в переменных.
- Сессия подменяется через `app.dependency_overrides[get_session]`.
- Тесты bulk-операций, которые коммитят в нескольких транзакциях (воркер), используют `TRUNCATE` в фикстуре — отдельный маркер `@pytest.mark.committing`. Пока такой тест один (гонка refresh и logout-all) — он сам удаляет своего пользователя.
- Письма: контейнер Mailpit на сессию pytest, тест читает письма через его REST API (`/api/v1/search`, `/api/v1/message/{id}`); у каждого теста свой email, потому что Mailpit и Redis общие на весь запуск.

### Матрица прав (генерируется)

```python
CASES = build_matrix(app.routes)   # маршруты — из OPA (data.saleshub.routes.actions), уровни — из data.yaml

@pytest.mark.parametrize("role,route,target,expected", CASES)
async def test_access(role, route, target, expected, client_as):
    # target: own | same_team | same_org | other_org
    resp = await client_as(role).request(route.method, route.url_for(target))
    assert resp.status_code == expected   # 2xx / 403 / 404
```

Ожидаемый ответ считается из `data.yaml`: действия нет у роли → 403; чтение или изменение одного объекта: объект в уровне роли → 2xx, вне уровня → 404; создание или изменение, после которого объект выходит из уровня (чужой ответственный или отдел) → 403 `out_of_scope`; список → 200, но без строк вне уровня. Супер-админ на обычных эндпоинтах → 403.

Плюс `test_routes_match_policy` и тест «список не возвращает чужого»: создать контакты в двух организациях, проверить, что `GET /contacts` от sales\_rep org A не содержит ничего из org B, а bulk-update не трогает чужие строки.

### Celery в тестах

- unit: функция задачи вызывается напрямую (логика вынесена в сервис, задача — тонкая обёртка).
- integration: `task_always_eager=True` для простых сценариев; для chord и ретраев — фикстура `celery_worker` с реальным Redis.

Покрытие собирается `pytest-cov`, порог в CI — 80% для `services/` и `authz/`, остальное без жёсткого порога.

## 11. Деплой: compose → Kubernetes → Tilt

Tilt — не альтернатива Kubernetes, а dev-инструмент поверх него: он собирает образы, применяет манифесты в локальный кластер и синхронизирует изменённый код в поды без пересборки. Поэтому порядок такой: сначала docker compose (быстрый старт, этапы 1–6), затем локальный кластер k3d + Helm-чарт (этап 7), и уже над ним Tilt.

### Шаг 1 — docker compose

Сервисы: `api`, `worker`, `worker-bulk`, `beat`, `flower`, `postgres`, `redis`, `migrate` (одноразовый), `seed` (одноразовый). `make up` — и можно работать. Этого достаточно для разделов 2–10.

Плюс два сервиса для этого проекта: `opa` (политики из `./policies`, `--watch` — правка `.rego` применяется сразу) и `mailpit` (SMTP-ловушка: SMTP на 1025, веб на `localhost:8025`).

Файл — `deploy/compose/docker-compose.yml`, команды — в `Makefile` (`make help`). Что есть после этапа 1:

| Сервис | Порт на хосте | Заметки |
| --- | --- | --- |
| `api` | 8000 | код смонтирован с хоста, `--reload` подхватывает правки без пересборки образа; `backend/secrets` (ключ JWT) — только на чтение; стартует после `migrate` и `seed` |
| `migrate` | — | одноразовый `alembic upgrade head`; `api` стартует только после его успешного завершения |
| `seed` (с этапа 2) | — | одноразовый `create-superadmin` после `migrate`; dev-логин супер-админа задан в compose, команда идемпотентна |
| `postgres` 18 | `${POSTGRES_PORT:-5432}` | том монтируется в `/var/lib/postgresql` (у образа 18 данные в `/var/lib/postgresql/18/docker`) |
| `redis` 8 | `${REDIS_PORT:-6379}` | если порт занят другим проектом — поменять в `deploy/compose/.env` (шаблон `.env.example` рядом) и в `backend/.env` |
| `pgadmin` | 5050 | без экрана входа; сервер и пароль заданы заранее (`servers.json` + `pgpass`, права 600 ставятся при старте контейнера) |
| `redisinsight` | 5540 | подключение к Redis стенда создаётся само (`RI_REDIS_HOST`) |
| `mailpit` | 1025, 8025 | |
| `rustfs` | 9000 (S3), 9001 (консоль `/rustfs/console/`) | S3-совместимое хранилище вместо MinIO: официальные образы MinIO больше не публикуются |

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

Секреты проекта: пароль Postgres, приватный RS256-ключ для JWT, пароль супер-админа, SMTP, токен Telegram-бота для алертов, ключи S3-хранилища (RustFS).

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
| Файлы (S3) | консоль RustFS | `s3.saleshub.localhost` · 9001, путь `/rustfs/console/` | файлы импорта и экспорта |
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
| policy | opa fmt --diff, opa check, regal lint, opa test policies --coverage (порог 90%) | — |

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
- Файлы — только через S3 API (`aioboto3`), а не через специфичный клиент хранилища: так RustFS локально и S3 в облаке взаимозаменяемы.
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
| Массовые действия: выбрать сегмент → передать другому менеджеру / добавить тег | bulk job, предпросмотр количества, прогресс по SSE |
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

- [ ] 0.1 Установить: Docker, uv, Python 3.13, k3d, kubectl, helm, tilt, opa, kubeseal, gh CLI. Уже есть: Docker, uv, Python 3.13, pre-commit (`uv tool install pre-commit`); остальное понадобится не раньше этапа 3 (opa) и этапа 6 (Kubernetes).
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

- [ ] 3.1 Модели `organizations` (валюта, часовой пояс), `teams`; в `users` — `org_id`, `team_id`, `role` с CHECK-ограничениями + миграция.
- [ ] 3.2 OPA в compose; `routes.rego` — только маршруты этапа 3 (`/admin/orgs…`, `/org`, `/teams…`, `/users…`, `/invites`), `data.yaml`, `authz.rego`; первые `opa test`.
- [ ] 3.3 `OpaClient`, `authorize()`, чтение `data.yaml`, `Access`.
- [ ] 3.4 Защищённый роутер и тест `test_routes_match_policy`.
- [ ] 3.5 Эндпоинты организаций (супер-админ) и отделов (org admin).
- [ ] 3.6 Приглашения: `POST /api/v1/admin/orgs/{org_id}/invites` (первый админ) и `POST /api/v1/invites` (с ролью и отделом), письмо, `invite/accept`; занятый email — 202 и письмо «вы уже состоите в организации»; удалить `POST /api/v1/admin/invites`, а тестовых пользователей этапа 2 без организации удаляет миграция 3.1.
- [ ] 3.7 Смена роли и отдела, отключение пользователя; правила в сервисе (нельзя себе, отдел обязателен), причина отказа в ответе.
- [ ] 3.8 `audit_log` (пока без партиций) и сброс кэша пользователя при смене роли.
- [ ] 3.9 «Требуется: …» в описании операций Swagger.
- [ ] 3.10 Тест-матрица «роль × эндпоинт × свой/чужой» + `opa test --coverage` в CI.

**Готово, когда:** супер-админ создаёт организацию → приглашает org admin → тот создаёт отдел и приглашает менеджера; каждый видит и меняет только то, что разрешено матрицей: организация, отделы, пользователи (CRM-данные — на этапе 4).

### Этап 4 — CRM-домен и объём (раздел 3)

- [ ] 4.1 `companies` и `contacts` с мягким удалением и `version`; общий фильтр `deleted_at IS NULL` в репозитории.
- [ ] 4.2 CRUD компаний и контактов с `Access` (`list`, `get_visible_or_404`, `ensure_allowed`), keyset-пагинация, ошибка `contact.duplicate_email` + строки в `routes.rego` и тест-матрица.
- [ ] 4.3 Воронки, этапы, `deals`, эндпоинт канбана + строки в `routes.rego`.
- [ ] 4.4 `fx_rates`, `FxService` (НБУ, кэш, резерв), суммы сделок в USD, параметр `?currency=`.
- [ ] 4.5 `activities` с партициями по месяцам и `notes`.
- [ ] 4.6 Корзина: удаление, восстановление, список удалённых + строки в `routes.rego`.
- [ ] 4.7 Отчёты: materialized view и эндпоинт + строки в `routes.rego`.
- [ ] 4.8 `seed_big.sql` (5 млн контактов, 1 млн сделок, 20 млн активностей), `make seed-big`.
- [ ] 4.9 Индексы из раздела 3: замеры «без / с» → `docs/experiments.md`.
- [ ] 4.10 Тесты: «список не возвращает чужого», конвертер валют, корзина.

**Готово, когда:** на сгенерированных данных все списки и отчёты идут по индексам (в `EXPLAIN` нет Seq Scan по большим таблицам), замеры записаны.

### Этап 5 — Воркеры, bulk и Telegram-бот (разделы 6, 15, 16)

- [ ] 5.1 `celery_app`, очереди, event loop на процесс, `BaseTask`, передача `request_id`; воркер, beat и Flower в compose.
- [ ] 5.2 Письма через очередь `notifications` + `sent_notifications` (без дублей).
- [ ] 5.3 `outbox_events` и процесс relay (`LISTEN/NOTIFY` + опрос).
- [ ] 5.4 `bulk_jobs`, `plan` / `chunk` / `finalize`, эндпоинт передачи клиентов, `move_team` при смене отдела, прогресс по SSE + строки в `routes.rego`.
- [ ] 5.5 Отмена job, `reap_stuck`, `failed_tasks` и перезапуск + строки в `routes.rego`.
- [ ] 5.6 Импорт CSV: загрузка в S3 (RustFS) → `COPY` → merge → `import_errors`.
- [ ] 5.7 Закрытие просроченных сделок, теги, потоковый экспорт в CSV + строки в `routes.rego`.
- [ ] 5.8 Расписание beat из раздела 15 (НБУ, корзина, партиции, отчёты, токены, heartbeat).
- [ ] 5.9 Замеры способов UPDATE → `docs/experiments.md`.
- [ ] 5.10 Chaos-тест: убить воркер посреди job.
- [ ] 5.11 Telegram-бот на aiogram (polling), привязка через `/start <token>`.
- [ ] 5.12 `notification_settings` и обработчик notifier → Telegram и email.
- [ ] 5.13 Команды `/today`, `/deals`, `/stop`.
- [ ] 5.14 Напоминания за 15 минут и утренний дайджест по часовому поясу.

**Готово, когда:** передача 100 тыс. клиентов идёт с прогрессом, переживает рестарт воркера, новый ответственный получает одно сообщение в Telegram и одно письмо.

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

- [ ] 10.1 OPA partial evaluation: перенести видимость данных в OPA (`/v1/compile` сам возвращает условия для SQL) и сравнить с `Access` — как это делают в больших системах.
- [ ] 10.2 Taskiq вместо Celery на одной очереди.
- [ ] 10.3 KEDA для `worker-bulk`.
- [ ] 10.4 Vault + External Secrets.
- [ ] 10.5 Auth0 dev-tenant как провайдер входа.
- [ ] 10.6 Переход на Python 3.14+ и PostgreSQL 19.
- [ ] 10.7 Pulumi-стек и staging в облаке (раздел 12, «Переход в облако»).

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
| Письма до этапа 5 | `BackgroundTasks` после коммита и ответа: ответ не ждёт SMTP; ошибка SMTP — в лог; на этапе 5 заменяется outbox и очередью |
| Выдача и отзыв токенов | сначала блокировка строки пользователя, потом токены — refresh не обгоняет logout-all, смену и сброс пароля |
| Rate limit | `INCR` до проверки, успех обнуляет счётчик; Redis недоступен → 503 |
| Смена пароля | отзыв остальных сессий + `token_version += 1`, в ответе новая пара токенов для текущей сессии |
| Неожиданные ошибки (500) | ловит `UnhandledErrorMiddleware` внутри `RequestId` и `AccessLog`, а не `exception_handler(Exception)` — так в ответе есть `X-Request-ID`, а в логе `request_id` |
| Домен | мульти-тенантная CRM: организации → отделы → компании, контакты, сделки, активности |
| Кто создаёт организации | только супер-админ; дальше всё через приглашения |
| Приглашение на занятый email | 202 и письмо «вы уже состоите в организации» — по ответу нельзя проверить, есть ли email в системе |
| Пользователи этапа 2 без организации | удаляются миграцией этапа 3 (dev-данные, `downgrade` их не возвращает) |
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

### Открытые вопросы

- [x] Уведомления — email и Telegram-бот (раздел 16).
- [x] Поиск — Elasticsearch (раздел 17).
- [ ] Новых открытых вопросов нет; этапы 1 и 2 закрыты, следующий шаг — этап 3 дорожной карты.

## 15. Модуль: система воркеров

Воркеры — отдельный модуль `app/workers` со своими процессами, но без своей бизнес-логики: задача — тонкая обёртка, которая восстанавливает контекст, открывает UoW и вызывает тот же сервис, что и API. Celery доставляет задачу «как минимум один раз», поэтому каждая задача идемпотентна: повтор не ломает данные и не дублирует письма.

### Процессы

| Процесс | Команда | Реплики локально | Назначение |
| --- | --- | --- | --- |
| `worker-default` | `celery worker -Q default -c 2` | 1 | короткие задачи: пересчёты, реакция на события |
| `worker-bulk` | `-Q bulk -c 4` | 1–4 (KEDA по длине очереди) | чанки массовых операций |
| `worker-imports` | `-Q imports -c 2` | 1–2 | разбор и загрузка CSV |
| `worker-notifications` | `-Q notifications -c 2` | 1 | письма, лимит 50 в минуту |
| `beat` | `celery beat` | **ровно 1**, стратегия `Recreate` | расписание; две копии продублируют задачи |
| `outbox-relay` | `python -m app.events.relay` | 1 (можно 2+) | публикация outbox: в Celery, позже в Kafka |
| `flower` | `celery flower` | 1 | UI |

Outbox relay — не задача beat, а долгоживущий цикл: `LISTEN outbox_new` (триггер шлёт `NOTIFY` при вставке) плюс опрос раз в 2 секунды на случай пропуска. Несколько копий не мешают друг другу благодаря `FOR UPDATE SKIP LOCKED`. Так задержка события — миллисекунды, а очередь не засоряется задачами «проверь outbox».

### Структура модуля

```text
app/workers/
├── celery_app.py        # Celery(), конфиг, маршрутизация очередей
├── runtime.py           # event loop и engine на процесс, run_async()
├── context.py           # request_id / user_id / org_id через заголовки задачи
├── base.py              # BaseTask: ретраи, логи, метрики, запись в failed_tasks
├── schedule.py          # расписание beat
└── tasks/
    ├── bulk.py          # plan, chunk, finalize, reap_stuck
    ├── imports.py       # import_contacts: parse → stage → merge
    ├── notifications.py # send_email
    ├── fx.py            # fetch_nbu_rates
    └── maintenance.py   # trash.purge, партиции, reports.refresh, очистка токенов
app/events/relay.py      # outbox relay
```

### Async внутри Celery

Celery вызывает обычные функции, а сервисы и репозитории асинхронные. Варианты:

| Вариант | Как | Минусы |
| --- | --- | --- |
| `asyncio.run()` в каждой задаче | новый event loop на задачу | пул соединений не переживает задачу: нужен `NullPool`, каждое подключение заново |
| **Один loop на процесс (выбрано)** | loop и engine создаются в `worker_process_init`; `run_async(coro)` = `loop.run_until_complete(coro)` | только prefork-пул (не gevent/eventlet); одна задача на процесс одновременно — это нормально |
| Sync SQLAlchemy в воркерах | psycopg без async | дублирование репозиториев |
| Taskiq вместо Celery | async-нативные очереди | меньше экосистема; эксперимент этапа 10 |

```python
# workers/runtime.py
_loop: asyncio.AbstractEventLoop | None = None

@worker_process_init.connect
def init_process(**_):
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    db.init_engine(settings.database_url, pool_size=2)    # engine на процесс, не на задачу

@worker_process_shutdown.connect
def shutdown_process(**_):
    _loop.run_until_complete(db.dispose_engine())
    _loop.close()

def run_async(coro):
    return _loop.run_until_complete(coro)

# workers/tasks/bulk.py
@app.task(base=BaseTask, name="bulk.chunk", queue="bulk", bind=True)
def chunk(self, job_id: str, table: str, lo: int, hi: int) -> int:
    return run_async(BulkService.run_chunk(job_id, table, lo, hi))   # своя сессия и UoW
```

Бюджет соединений к Postgres: API 2 пода × 10 + воркеры 10 процессов × 2 + relay 2 ≈ 42 при `max_connections = 100`. При росте — PgBouncer в режиме transaction.

### Настройки надёжности

| Настройка | Значение | Зачем |
| --- | --- | --- |
| `task_acks_late` | `True` | сообщение подтверждается после выполнения: упал воркер — задачу возьмёт другой |
| `task_reject_on_worker_lost` | `True` | то же при OOM и `kill -9` |
| `worker_prefetch_multiplier` | 1 | воркер не забирает лишние длинные задачи |
| `visibility_timeout` (Redis) | 1 час | должен быть больше самой долгой задачи, иначе Redis выдаст её повторно |
| `soft_time_limit` / `time_limit` | чанк 60 / 90 с; шаг импорта 10 / 12 мин | зависшая задача не держит воркер |
| Ретраи | `autoretry_for=(OperationalError, httpx.TransportError, SMTPException)`, `retry_backoff`, `retry_jitter`, `max_retries=5` | временные сбои БД, SMTP, НБУ |
| Аргументы | только JSON и только id / простые значения | задачи из очереди переживают деплой новой версии |
| `task_track_started` | `True` | видно в Flower, что задача выполняется |

### Идемпотентность и защита от дублей

- **Чанк bulk:** set-based `UPDATE` с условием, которое после первого прогона ничего не находит (`owner_id = :from`).
- **Импорт:** шаги `parse → stage → merge` с отметкой в `bulk_jobs.stage`; повтор начинается с последнего незавершённого шага; `merge` через `ON CONFLICT`.
- **Письма:** таблица `sent_notifications(dedup_key UNIQUE)`, ключ вида `deal_reassigned:{job_id}:{user_id}` — повтор не отправит письмо второй раз.
- **Запуск job:** заголовок `Idempotency-Key` → уникальный индекс `bulk_jobs(owner_id, idempotency_key)`.
- **Одна тяжёлая job на организацию:** частичный уникальный индекс `bulk_jobs(org_id, kind) WHERE status IN ('pending', 'running')` — второй запуск получит 409. Для задач супер-админа (`org_id` NULL, а NULL-ы в уникальном индексе не совпадают) — отдельный `UNIQUE (kind) WHERE org_id IS NULL AND status IN ('pending', 'running')`.
- **Курсы НБУ:** `INSERT … ON CONFLICT (rate_date, currency) DO UPDATE` — повторная загрузка безопасна.

### Жизненный цикл bulk job

Статусы: `pending` → `running` → `done` | `partial_failed` | `failed`; отмена: `pending`/`running` → `cancelling` → `cancelled`.

&#91;embedded content: статусы bulk job · 4 финальных, отмена из pending и running\]

Отменить можно только незавершённую job. Куда перейдёт running — в done, partial\_failed или failed, — решают bulk.finalize и bulk.reap\_stuck.

- `POST /api/v1/bulk-jobs/{id}/cancel` ставит `cancelling`; каждый чанк перед стартом проверяет статус и пропускает работу; `finalize` ставит `cancelled`. Уже изменённые строки остаются изменёнными — в ответе видно, сколько успели.
- `bulk.reap_stuck` (раз в минуту): job в `running` без роста `processed` 15 минут → `failed` + алерт `BulkJobStuck`.
- Прогресс: `processed / total` в `bulk_jobs`; изменение публикуется в Redis pub/sub, SSE-эндпоинт `/bulk-jobs/{id}/events` отдаёт его клиенту.

### Задачи, упавшие после всех ретраев

У Celery с Redis нет dead-letter очереди, поэтому делаем свою: сигнал `task_failure` (после исчерпания ретраев) → строка в `failed_tasks` (имя, аргументы, ошибка, traceback, request\_id, время) + метрика `celery_task_failed_total` + алерт `CeleryTaskFailureRate`. Супер-админ видит их в `GET /api/v1/admin/failed-tasks` и может перезапустить `POST /api/v1/admin/failed-tasks/{id}/replay` — это безопасно, потому что задачи идемпотентны.

### Контекст и логи

`before_task_publish` берёт `request_id`, `user_id`, `org_id` из contextvars API и кладёт в заголовки задачи; `task_prerun` восстанавливает их в structlog. В логах одна цепочка: HTTP-запрос → задача → событие outbox → потребитель Kafka.

### Расписание beat (часовой пояс Europe/Kyiv)

| Задача | Когда | Что делает |
| --- | --- | --- |
| `fx.fetch_nbu_rates` | будни 18:30, повтор 19:30; при старте, если нет курса на завтра | курсы НБУ на следующий рабочий день → `fx_rates` |
| `bulk.reap_stuck` | каждую минуту | зависшие job → `failed` |
| `trash.purge` | ежедневно 03:00 | удаляет из корзины записи старше 30 дней, чанками |
| `maintenance.create_partitions` | 1-го числа месяца | партиции `activities` и `audit_log` на 2 месяца вперёд |
| `reports.refresh` | каждые 5 минут | `REFRESH MATERIALIZED VIEW CONCURRENTLY` |
| `auth.cleanup_tokens` | ежедневно 04:00 | удаляет истёкшие refresh- и email-токены |
| `workers.heartbeat` | каждую минуту | пишет время в Redis; если старше 3 минут — алерт `CeleryBeatDown` |
| notifications.morning\_digest | каждые 15 минут | отправляет дайджест тем, у кого по их часовому поясу наступило 09:00 и он ещё не отправлен сегодня (dedup\_key = user + локальная дата) |

Beat хранит своё состояние в файле на `emptyDir`; после перезапуска пропущенный запуск просто выполнится в следующий раз — задачи идемпотентны. Для отказоустойчивого beat позже можно попробовать RedBeat (блокировка в Redis).

### Масштабирование и остановка

- Каждая очередь — свой Deployment со своими ресурсами: `bulk` нагружает базу, поэтому `-c 4`; `imports` ест память, поэтому `-c 2` и лимит памяти выше.
- KEDA `ScaledObject` масштабирует `worker-bulk` от 1 до 4 реплик по длине Redis-списка `bulk`.
- SIGTERM → warm shutdown: воркер дорабатывает текущую задачу. `terminationGracePeriodSeconds: 120` больше лимита чанка (90 с). Длинный импорт разбит на шаги, поэтому рестарт теряет не больше одного шага.
- Новая версия кода выполняет задачи, поставленные старой: аргументы совместимы, а переименованная задача на один релиз оставляет старое имя как алиас.

### Мониторинг

- Flower — UI (раздел 11).
- celery-exporter → Prometheus: задачи по статусам, время выполнения, длина очередей.
- Дашборд «Workers»: длина очередей, задач в секунду, p95 времени по задачам, ретраи, `failed_tasks`, активные bulk job.
- Алерты: `CeleryQueueBacklog`, `CeleryTaskFailureRate`, `CeleryBeatDown`, `BulkJobStuck`, `OutboxLag`, `FxRatesStale`.

### Тесты воркеров

- **Unit:** сервис вызывается напрямую; задача — обёртка, её логику не тестируем отдельно.
- **Integration:** фикстура `celery_worker` с реальным Redis — группы задач с финальной задачей, ретраи, отмена job.
- **Chaos:** bulk job на 1 млн контактов, посреди — `kubectl delete pod` у воркера → job всё равно завершается, число изменённых строк верное, письмо одно.
- **Время и внешние API:** freezegun для расписания, respx для НБУ и SMTP.

## 16. Модуль: уведомления и Telegram-бот

Пользователь получает уведомления CRM в Telegram и на почту — по своим настройкам. Бот на aiogram — отдельный процесс, который делает две вещи: привязывает Telegram-аккаунт к пользователю CRM и отвечает на пару команд. Отправка сообщений идёт не из бота, а из воркеров очереди `notifications`, тем же путём, что и письма.

В проекте два разных бота: **пользовательский** (этот раздел) и **бот для алертов** Alertmanager (раздел 8). У них разные токены и разные получатели — пользователи CRM и команда разработки.

### События и каналы

| Событие | Кому | Канал по умолчанию |
| --- | --- | --- |
| Вам назначили сделку или клиента (в том числе bulk-передача — одно сообщение «вам передано 120 клиентов») | новому ответственному | Telegram + email |
| Сделка перешла в `won` / `lost` | ответственному и руководителю отдела | Telegram |
| Напоминание: встреча или звонок через 15 минут | ответственному | Telegram |
| Утренний дайджест в 09:00 по часовому поясу пользователя: задачи на сегодня, просроченные | каждому менеджеру | Telegram |
| Импорт или массовая операция завершены (с числом ошибок) | автору job | Telegram + email |
| Приглашение, сброс пароля | пользователю | только email |

Настройки: таблица `notification_settings(user_id, event_type, channel, enabled)` и эндпоинты `GET/PUT /api/v1/me/notifications`. Эндпоинты `/api/v1/me/*` — личные настройки: они стоят на роутере только с `current_user`, без OPA, и работают с записями самого пользователя. «Тихие часы» (например, 22:00–08:00) — несрочное откладывается до утра.

### Привязка Telegram

1. В профиле `POST /api/v1/me/telegram/link` → одноразовый токен (живёт 10 минут) и ссылка вида `https://t.me/<bot>?start=<token>`.
2. Пользователь открывает ссылку, Telegram отправляет боту `/start <token>`.
3. Бот проверяет токен через сервис, сохраняет `telegram_links(user_id, chat_id, linked_at)` и отвечает «Аккаунт привязан».
4. Отвязка — `/stop` в боте или `DELETE /api/v1/me/telegram`. Если пользователь заблокировал бота, Telegram вернёт 403 — канал отключается автоматически.

### Команды бота

| Команда | Что делает |
| --- | --- |
| `/start <token>` | привязка аккаунта |
| `/today` | мои задачи и встречи на сегодня |
| `/deals` | мои открытые сделки с суммой (в валюте организации, через конвертер) |
| `/stop` | отвязать и выключить уведомления |

Бот вызывает те же сервисы, что и API, от имени привязанного пользователя. Через HTTP-маршруты он не ходит, поэтому OPA ему не нужен. Перед каждой командой бот загружает пользователя (тот же кэш, что у `current_user`) и проверяет `is_active`; отключённому отвечает, что доступ закрыт, и удаляет `telegram_links`. Команды бота показывают только данные самого пользователя: бот строит `Access.for_user(user, "deal:read")` и сужает уровень до `own` («мои задачи», «мои сделки»); если у роли нет действия — отвечает «недоступно».

### Как сообщение доходит до пользователя

1. Сервис (например, `DealService.reassign`) пишет доменное событие в outbox.
2. Relay публикует событие; обработчик `notifier` по `notification_settings` решает, кому и в какой канал.
3. Задача `notifications.send_telegram` (очередь `notifications`) отправляет через Bot API.
4. Дедупликация через `sent_notifications(dedup_key)` — повтор задачи не отправит второе сообщение.
5. Лимиты Telegram: rate limit задачи и ретрай с паузой из ответа 429 (`retry_after`).

### Запуск

- Локально бот работает через **long polling** — внешний адрес не нужен.
- В облаке — **webhook**: `POST /telegram/webhook` за Ingress с секретным заголовком; переключается настройкой.
- Токен бота — в SealedSecret; для local и prod разные боты.
- В Tilt и Kubernetes — отдельный Deployment `telegram-bot`, 1 реплика (при polling две копии конфликтуют).
- Тесты: обработчики aiogram тестируются без Telegram (подмена `Bot`), отправка — через respx.

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
