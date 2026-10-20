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

Один запрос = одна `AsyncSession` = один Unit of Work; коммит делает только сервис через UoW, репозитории никогда не коммитят. Права проверяются в двух местах: на входе в эндпоинт (есть ли permission вообще) и в сервисе/репозитории (на какой объект/строки он распространяется).

&#91;embedded content: архитектура SalesHub · API, OPA, очереди, события, мониторинг\]

Права проверяются дважды; OPA спрашиваем один раз: `authorize()` на роутере (можно ли это действие вообще) и `Grant` в сервисе (можно ли с этим объектом; для списков — SQL-фильтр). Всё, что дольше секунды, уходит через Redis в воркеры, которые вызывают те же сервисы и UoW.

### Ответственность слоёв

| Слой | Знает о | Не знает о | Что делает |
| --- | --- | --- | --- |
| `api` (роутер) | HTTP, схемы Pydantic, Depends | SQL, сессии | валидация, `authorize() → OPA`, вызов сервиса, маппинг в response-схему |
| `deps` | всё, что собирает граф зависимостей | бизнес-правила | `get_session`, `get_uow`, `current_user`, `get_deal_service` |
| `service` | доменные правила, `Grant`, UoW | HTTP, FastAPI | сценарий: проверить объектный доступ, изменить, записать события, `uow.commit()` |
| `uow` | сессию и набор репозиториев | HTTP | транзакция, `commit/rollback`, сбор доменных событий в outbox |
| `repository` | SQLAlchemy-модели, запросы | HTTP, права | CRUD, фильтры, keyset-пагинация, bulk SQL; принимает готовый scope-фильтр |
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
    grant: Grant = Depends(authorize),          # спросили OPA: deal:update
    svc: DealService = Depends(get_deal_service),
):
    return await svc.update(deal_id, body, grant)

# services/deals.py
class DealService:
    async def update(self, deal_id: int, body: DealUpdate, grant: Grant) -> Deal:
        deal = await self.uow.deals.get_or_404(deal_id)          # deleted_at IS NULL внутри
        grant.ensure_object(deal)             # own / team / org: то же условие, что в SQL-фильтре
        old_stage = deal.stage_id
        deal.apply(body)                                         # версия +1, проверка version
        if deal.stage_id != old_stage:
            self.uow.add_event(DealStageChanged(deal.id, old_stage, deal.stage_id))
        await self.uow.commit()
        return deal
```

**Celery-воркеры** используют те же сервисы, UoW и репозитории, но получают сессию не из FastAPI Depends, а из своей фабрики (`worker_session_factory`, engine создаётся на процесс воркера в сигнале `worker_process_init`). Так бизнес-логика пишется один раз.

### Структура репозитория

```text
saleshub/
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
│   │   ├── authz/               # OPA-клиент, authorize(), Grant, scope→SQL
│   │   ├── api/
│   │   │   ├── deps.py
│   │   │   └── v1/              # auth, users, orgs, teams, companies, contacts, deals, activities, bulk, reports, health
│   │   ├── errors/              # AppError иерархия + exception handlers
│   │   ├── middleware/          # request_id, access log, metrics
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
│   └── pyproject.toml
├── policies/saleshub/            # OPA: routes.rego, authz.rego, data.json, *_test.rego
├── frontend/                    # в самом конце
├── deploy/
│   ├── compose/docker-compose.yml
│   ├── helm/saleshub/            # chart: api, worker, beat, flower, jobs
│   ├── envs/{local,staging,prod}/   # values, sealed-secrets, приложения Argo CD
│   ├── infra/                   # Pulumi: облачная инфраструктура (при переходе)
│   ├── monitoring/              # prometheus rules, grafana dashboards, alertmanager
│   └── k3d-config.yaml
├── Tiltfile
├── .github/workflows/{ci.yml,cd.yml}
└── docs/DESIGN.md               # этот документ, экспорт
```

## 3. Модель данных

Тяжёлые таблицы — `contacts` (5 млн), `deals` (1 млн) и `activities` (20 млн, партиции по месяцам). На них отрабатываются bulk-операции, индексы и партиционирование. Остальные таблицы маленькие и нужны для прав и справочников. Каждая бизнес-строка несёт `org_id` (тенант), `team_id` (отдел) и `owner_id` (ответственный) — по ним OPA и SQL решают, кто что видит.

### Пользователи, тенанты, права

| Таблица | Ключевые поля | Заметки |
| --- | --- | --- |
| `users` | id (uuid), email (unique, citext), password\_hash, is\_active, is\_superadmin, email\_verified\_at, token\_version, timezone (null = как у организации), last\_login\_at, created\_at |  |
| `refresh_tokens` | id, user\_id, family\_id (= `sid`), token\_hash (unique), created\_at, expires\_at, revoked\_at, replaced\_by, user\_agent, ip | раздел 4 |
| `email_tokens` | id, user\_id, email, purpose (`invite` / `reset_password`), token\_hash, payload jsonb (org\_id, team\_id, role для приглашения), expires\_at, used\_at | раздел 4 |
| `organizations` | id, name, slug (unique), plan, display\_currency (по умолчанию UAH), timezone (по умолчанию Europe/Kyiv), created\_at | тенант: компания, которая пользуется CRM; создаёт только супер-админ |
| `teams` | id, org\_id, name, created\_at | отдел продаж |
| `org_memberships` | user\_id, org\_id, role (`org_admin` / `viewer` / `member`), granted\_by, created\_at | PK (user\_id, org\_id) |
| `team_memberships` | user\_id, org\_id, team\_id, role (`team_lead` / `sales_rep`), granted\_by | PK (user\_id, team\_id) |

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
| `bulk_jobs` | id, kind (`import_contacts`, `reassign_owner`, `close_stale_deals`, `add_tags`, `export`), status, params jsonb (фильтр + scope-фильтр из Grant), total, processed, failed, error, created\_by, timestamps |
| `import_errors` | job\_id, row\_no, raw jsonb, error — строки CSV, которые не прошли валидацию |
| `outbox_events` | transactional outbox: события и письма |
| `audit_log` | кто что изменил, `PARTITION BY RANGE (occurred_at)` |
| fx\_rates | rate\_date, currency, rate\_uah numeric(14,6) (гривен за 1 единицу валюты), source = NBU, fetched\_at; PK (rate\_date, currency) — курсы НБУ |
| failed\_tasks | задачи Celery, упавшие после всех ретраев: имя, аргументы, ошибка, request\_id, replayed\_at (раздел 15) |
| sent\_notifications | dedup\_key (unique), channel, user\_id, sent\_at — защита от повторной отправки писем и сообщений |
| telegram\_links, notification\_settings | привязка chat\_id к пользователю; настройки «событие × канал» (раздел 16) |

Таблиц `roles`, `permissions`, `role_permissions` нет: матрица ролей живёт в OPA (`policies/saleshub/data.json`), в базе только членства. `contacts.id` и `deals.id` — bigint: для чанков по диапазонам id и keyset-пагинации это проще и быстрее.

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

|  | Access token | Refresh token |
| --- | --- | --- |
| Формат | JWT RS256, проверяется по `/.well-known/jwks.json` | случайная строка 256 бит (не JWT) |
| Срок | 1 час (`JWT_ACCESS_TTL=3600`) | 30 дней (`REFRESH_TTL_DAYS=30`) |
| Где хранится на сервере | нигде (stateless) | в таблице `refresh_tokens`, только SHA-256 хэш |
| Как отозвать | `token_version` пользователя или дождаться истечения | `revoked_at` в БД — сразу |
| Для чего | каждый запрос к API | только `POST /auth/refresh` |

Refresh сделан непрозрачным и хранится в БД намеренно: его можно отозвать по одному устройству и видеть список сессий. Access остаётся JWT, чтобы API не ходило в БД на каждый запрос.

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

Права в токен не кладём: они зависят от членств, которые меняются, а токен живёт час. Членства грузятся из БД (кэш Redis) и передаются в OPA (раздел 5).

### Ротация refresh и защита от кражи

1. `POST /auth/refresh {refresh_token}` → найти по хэшу, проверить `expires_at` и `revoked_at`.
2. Выдать новую пару access + refresh; старый refresh пометить `revoked_at`, `replaced_by = новый`.
3. Все refresh одной сессии связаны `family_id`. Если пришёл **уже отозванный** refresh — его украли и использовали повторно: отзываем всю семью, пользователь выходит на этом устройстве.
4. Срок семьи не продлевается бесконечно: абсолютный предел сессии — 30 дней с логина (настраивается).

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
| POST | `/api/v1/auth/password/change` | `{old_password, new_password}` для залогиненного, отзыв остальных сессий |
| POST | `/api/v1/auth/invite/accept` | `{token, password}` — приглашённый задаёт пароль и сразу состоит в организации |
| GET | `/api/v1/auth/me` | профиль, членства и роли (фронт по ним решает, что показывать) |
| GET | `/.well-known/jwks.json` | публичный ключ для проверки подписи |
| POST | /api/v1/admin/invites | приглашение без организации, только супер-админ (этап 2); с этапа 3 основное — POST /api/v1/orgs/{org\_id}/invites с ролью |

Приглашение создаёт админ организации: `POST /api/v1/orgs/{org_id}/invites {email, role}` (проверка прав — в OPA, включая «нельзя пригласить на роль выше своей»).

### Одноразовые токены из писем

Одна таблица `email_tokens` для двух целей: `invite` (живёт 7 дней) и `reset_password` (1 час). В письмо уходит случайная строка, в БД — только её SHA-256 хэш; токен одноразовый (`used_at`), новый запрос на ту же цель гасит предыдущие. Для приглашения в `payload` лежат организация, отдел и роль.

Письмо отправляет не API, а воркер: сервис пишет событие `member.invited` / `password.reset_requested` в outbox → задача `notifications.send_email` (Jinja2-шаблоны). Приглашение не падает, если SMTP недоступен, — письмо уйдёт с ретраем. До этапа 5 (воркеры) письма отправляются синхронно через тот же интерфейс `EmailSender`. В dev — Mailpit (SMTP на 1025, веб на 8025), в prod — любой SMTP или Amazon SES.

### Зависимость `current_user`

1. Достать Bearer-токен, проверить подпись по JWKS, `iss`, `aud`, `exp`, `typ=access`.
2. Найти пользователя по `sub` (кэш Redis на 60 с), проверить `is_active`, `email_verified_at` и `ver == token_version`.
3. Вернуть `CurrentUser` (id, email, is\_superadmin). Членства грузятся отдельно, только для защищённых эндпоинтов (раздел 5).

Ошибки: нет токена, битая подпись, истёк → 401 `auth.invalid_token` / `auth.token_expired` (клиент по второму коду понимает, что пора делать refresh); email не подтверждён → 403 `auth.email_not_verified`; заблокирован → 403 `auth.user_inactive`.

### Безопасность

- Пароли — argon2 (pwdlib), минимум 10 символов, проверка на совпадение с email.
- Rate limit в Redis: логин 5 попыток / 15 мин на email+IP, `password/forgot` — 3 / час на email, приглашения — 50 / час на организацию.
- Ответы `login` и `forgot` не раскрывают, существует ли пользователь.
- Ключи RS256: приватный — в Secret, поддержка ротации через `kid` в заголовке JWT и несколько ключей в JWKS.
- Для будущего фронта refresh переедет в httpOnly-cookie; пока, для Swagger, он в теле ответа.

### Супер-админ: откуда берётся

- Команда `python -m app.cli create-superadmin` читает `SUPERADMIN_EMAIL` и `SUPERADMIN_PASSWORD` из env/Secret, создаёт пользователя сразу с подтверждённым email; идемпотентна.
- В Kubernetes это Job `seed` после Job `migrate`; в compose — одноразовый сервис.
- Не в миграции: миграции описывают схему и не знают секретов окружения.
- `is_superadmin = true` передаётся в OPA, и политика разрешает ему всё; все его действия пишутся в `audit_log`.

### Сценарий проверки через Swagger

1. `create-superadmin` → **Authorize** под супер-админом.
2. `POST /api/v1/admin/invites {email}` (с этапа 3 — `POST /api/v1/orgs/{org_id}/invites {email, role}`) → открыть Mailpit (`localhost:8025`), скопировать токен из письма.
3. `invite/accept {token, password}` → **Authorize** под новым пользователем → вызвать любой защищённый эндпоинт.
4. `refresh` с refresh-токеном → новый access; повторить старый refresh → 401 и отзыв всей семьи.
5. `password/forgot` → письмо → `password/reset` → старые токены больше не работают.

## 5. Авторизация через OPA (что тебе можно)

Решения о доступе принимает OPA (Open Policy Agent) по правилам на Rego, которые лежат в git рядом с кодом. FastAPI на каждый защищённый запрос отправляет в OPA «вход» — метод, шаблон пути, пользователя с его ролями — и получает ответ: разрешено ли и на какие данные (`own` — свои клиенты, `team` — клиенты отдела, `org` — вся компания, `all` — всё, только супер-админ).

### Где что хранится

| Что | Пример | Где лежит | Кто и как меняет |
| --- | --- | --- | --- |
| Какие действия есть у API | `PATCH /api/v1/deals/{deal_id}` → `deal:update` | `policies/saleshub/routes.rego` | разработчик, вместе с новым эндпоинтом, через PR |
| Что разрешено каждой роли | `sales_rep`: `deal:update` со scope `own` | `policies/saleshub/data.json` (матрица ролей) | через PR → деплой → OPA перечитывает файлы |
| Логика проверки | роль → области доступа; нельзя выдать роль выше своей | `policies/saleshub/authz.rego` | через PR, покрыто `opa test` |
| Кто какую роль имеет и где | Ihor — `team_lead` в отделе «Корпоративные продажи» | Postgres: `org_memberships`, `team_memberships` | через API: org admin и team lead назначают роли, OPA проверяет, можно ли им |
| Супер-админ | `users.is_superadmin` | Postgres | только командой `create-superadmin` при деплое |
| Как область применяется к данным | own → owner\_id = я | app/authz/grant.py: matches, sql\_filter, es\_filter | разработчик; тест проверяет, что три формы отвечают одинаково |

Команды для синхронизации прав нет, таблиц прав тоже. Правила и матрица — это код (policy as code): их меняют PR-ом и проверяют тестами. В базе только факты «кто в какой компании и отделе с какой ролью» — их меняют через API.

### Роли

| Роль | Уровень | Где | Кто это в жизни |
| --- | --- | --- | --- |
| `super_admin` | — | вся система | владелец SaaS: создаёт организации-тенанты |
| `org_admin` | 40 | организация | администратор CRM в компании-клиенте: отделы, воронки, пользователи |
| `team_lead` | 30 | отдел | руководитель отдела продаж: видит и перераспределяет клиентов отдела |
| `sales_rep` | 20 | отдел | менеджер по продажам: работает со своими клиентами |
| `viewer` | 10 | организация | аналитик, бухгалтер, директор: читает всё, ничего не меняет |
| `member` | 0 | организация | участник без отдела: видит только карточку организации |

### Матрица ролей (содержимое `data.json`)

| Действие | super\_admin | org\_admin | team\_lead | sales\_rep | viewer |
| --- | --- | --- | --- | --- | --- |
| `org:update`, `team:manage`, `pipeline:manage` | all | org | — | — | — |
| `member:invite` | all | org | — | — | — |
| `member:assign` | all | org | team | — | — |
| `contact:read`, `company:read`, `deal:read` | all | org | team | own | org |
| `contact:create`, `deal:create` | all | org | team | team | — |
| `contact:update`, `deal:update` | all | org | team | own | — |
| `contact:delete`, `deal:delete`, `trash:restore` | all | org | team | — | — |
| `activity:manage` | all | org | team | own | — |
| `contact:import` | all | org | team | — | — |
| `contact:export` | all | org | team | — | org |
| `bulk:reassign_owner`, `bulk:update` | all | org | team | — | — |
| `report:read` | all | org | team | own | org |
| `bulk_job:read` | all | org | own | own | own |
| `audit:read` | all | org | — | — | — |

`sales_rep` создаёт клиента в своём отделе (scope `team`), но потом видит и меняет только тех, где он ответственный (`own`). Руководитель отдела не видит соседний отдел — только администратор организации.

### Как проходит запрос

1. `current_user` проверяет access-токен (раздел 4).
2. Глобальная зависимость `authorize` на защищённом роутере берёт **шаблон** пути (`request.scope["route"].path`), метод и членства пользователя (Redis-кэш, иначе БД).
3. Один запрос `POST http://localhost:8181/v1/data/saleshub/authz/decision` в OPA-сайдкар.
4. OPA: путь → действие (`routes.rego`) → роли пользователя → области (`data.json`) → ответ.
5. `allow: false` → 403. Иначе в эндпоинт приходит `Grant` с областями доступа.
6. Дальше области применяет сам Python, второго запроса к OPA нет: один объект — `grant.ensure_object(obj)`, список и bulk — `grant.sql_filter()` в `WHERE`, поиск — `grant.es_filter()`.
7. Исключение — назначение ролей и приглашения: правило анти-эскалации есть только в Rego, поэтому сервис отдельно спрашивает `allow_assign`, передав организацию, отдел и роль из тела запроса.

Вход и ответ OPA для `PATCH /api/v1/deals/42` от менеджера по продажам:

```json
// input
{
  "method": "PATCH",
  "route": "/api/v1/deals/{deal_id}",
  "user": {
    "id": "u1",
    "is_superadmin": false,
    "org_memberships": [{"org_id": "o1", "role": "member"}],
    "team_memberships": [{"org_id": "o1", "team_id": "t1", "role": "sales_rep"}]
  }
}
// result
{"allow": true, "action": "deal:update", "all": false,
 "org_ids": [], "team_ids": [], "own_org_ids": ["o1"]}
```

### Разбор на примере: что увидит каждая роль

Один и тот же запрос `GET /api/v1/deals` от разных людей организации `o1`. Эндпоинт и код одинаковые; разный только ответ OPA — и из него получается разный SQL.

| Кто спрашивает | Ответ OPA | Что `sql_filter()` добавит в `WHERE` |
| --- | --- | --- |
| `sales_rep` u1 в отделе t1 | `own_org_ids: [o1]` | `org_id IN ('o1') AND owner_id = 'u1'` |
| `team_lead` отдела t1 | `team_ids: [t1]` | `team_id IN ('t1')` |
| `org_admin` или `viewer` организации o1 | `org_ids: [o1]` | `org_id IN ('o1')` |
| `super_admin` | `all: true` | ничего |
| `member` без отдела | `allow: false` | запрос не дойдёт до SQL — 403 |

Итоговый SQL для менеджера:

```sql
-- GET /api/v1/deals?limit=50 от sales_rep u1
SELECT id, title, amount_usd, stage_id, owner_id
FROM deals
WHERE deleted_at IS NULL                        -- корзина: общий фильтр репозитория
  AND (org_id IN ('o1') AND owner_id = 'u1')    -- Grant.sql_filter()
ORDER BY created_at DESC, id DESC               -- keyset-пагинация
LIMIT 50;
```

А `PATCH /api/v1/deals/42` от того же менеджера: сервис загружает сделку по id, и `grant.ensure_object(deal)` проверяет то же условие уже на объекте. Сделка чужая — 404, своя — изменяется.

Если у человека несколько ролей (менеджер в отделе t1 и админ в организации o2), области складываются через `OR`: `org_id IN ('o2') OR (org_id IN ('o1') AND owner_id = 'u1')`.

**Почему одно правило записано в трёх формах и как они не разойдутся.** `matches()` проверяет объект в памяти, `sql_filter()` — строки в Postgres, `es_filter()` — документы в Elasticsearch. Все три лежат рядом в `grant.py`. Тест на hypothesis генерирует сотни случайных пользователей и объектов и проверяет, что все три формы отвечают одинаково. Это главная защита от ситуации «в списке не видно, а по прямой ссылке открывается».

### Политики (эскиз)

```rego
# policies/saleshub/routes.rego — «файл с путями API и actions»
package saleshub.routes

actions := {
    "GET /api/v1/contacts": "contact:read",
    "GET /api/v1/contacts/{contact_id}": "contact:read",
    "POST /api/v1/contacts": "contact:create",
    "PATCH /api/v1/contacts/{contact_id}": "contact:update",
    "DELETE /api/v1/contacts/{contact_id}": "contact:delete",
    "POST /api/v1/contacts/import": "contact:import",
    "GET /api/v1/deals": "deal:read",
    "PATCH /api/v1/deals/{deal_id}": "deal:update",
    "POST /api/v1/bulk/reassign-owner": "bulk:reassign_owner",
    "POST /api/v1/orgs/{org_id}/invites": "member:invite",
    "PUT /api/v1/teams/{team_id}/members/{user_id}": "member:assign",
}
```

```json
// policies/saleshub/data.json — матрица ролей (фрагмент)
{
  "roles": {
    "org_admin": {"level": 40, "permissions": {"deal:read": "org", "deal:update": "org", "member:assign": "org"}},
    "team_lead": {"level": 30, "permissions": {"deal:read": "team", "deal:update": "team", "member:assign": "team"}},
    "sales_rep": {"level": 20, "permissions": {"deal:read": "own", "deal:create": "team", "deal:update": "own"}},
    "viewer":    {"level": 10, "permissions": {"deal:read": "org", "report:read": "org"}},
    "member":    {"level": 0,  "permissions": {}}
  }
}
```

```rego
# policies/saleshub/authz.rego
package saleshub.authz

roles := data.saleshub.roles
action := data.saleshub.routes.actions[concat(" ", [input.method, input.route])]

# все «выдачи» на это действие: scope + где
grants contains {"scope": s, "org_id": m.org_id} if {
    some m in input.user.org_memberships
    s := roles[m.role].permissions[action]
}

grants contains {"scope": s, "org_id": m.org_id, "team_id": m.team_id} if {
    some m in input.user.team_memberships
    s := roles[m.role].permissions[action]
}

# главный ответ: можно ли действие и в каких областях
default decision := {"allow": false}

decision := {"allow": true, "action": action, "all": true} if input.user.is_superadmin

decision := {
    "allow": true,
    "action": action,
    "all": false,
    "org_ids": {g.org_id | some g in grants; g.scope == "org"},
    "team_ids": {g.team_id | some g in grants; g.scope == "team"},
    "own_org_ids": {g.org_id | some g in grants; g.scope == "own"},
} if {
    not input.user.is_superadmin
    count(grants) > 0
}

# Объекты здесь НЕ проверяются: области применяет Python (Grant),
# чтобы правило «своё / отдел / компания» жило в одном месте.

# анти-эскалация: отдельный вопрос только при назначении ролей
# input.target = {org_id, team_id}, input.payload = {role, user_id}
default my_level := 0   # нет роли в этой организации → уровень 0, назначать нельзя

my_level := max({roles[m.role].level |
    some m in array.concat(input.user.org_memberships, input.user.team_memberships)
    m.org_id == input.target.org_id
})

deny_reasons contains "role_above_own" if {
    not input.user.is_superadmin
    roles[input.payload.role].level >= my_level
}

deny_reasons contains "own_role" if {
    input.payload.user_id == input.user.id
}

default allow_assign := false

allow_assign if {
    action in {"member:assign", "member:invite"}
    decision.allow
    count(deny_reasons) == 0
}
```

```rego
# policies/saleshub/authz_test.rego — `opa test policies -v`
package saleshub.authz_test

import data.saleshub.authz

rep := {"id": "u1", "is_superadmin": false, "org_memberships": [],
    "team_memberships": [{"org_id": "o1", "team_id": "t1", "role": "sales_rep"}]}

lead := {"id": "u2", "is_superadmin": false, "org_memberships": [],
    "team_memberships": [{"org_id": "o1", "team_id": "t1", "role": "team_lead"}]}

assign := {"method": "PUT", "route": "/api/v1/teams/{team_id}/members/{user_id}",
    "target": {"org_id": "o1", "team_id": "t1"}}

test_rep_cannot_delete_contact if {
    not authz.decision.allow with input as {"method": "DELETE",
        "route": "/api/v1/contacts/{contact_id}", "user": rep}
}

test_rep_gets_only_own_scope if {
    d := authz.decision with input as {"method": "PATCH",
        "route": "/api/v1/deals/{deal_id}", "user": rep}
    d.allow
    d.own_org_ids == {"o1"}
    count(d.team_ids) == 0
}

test_lead_can_assign_rep if {
    authz.allow_assign with input as object.union(assign,
        {"user": lead, "payload": {"role": "sales_rep", "user_id": "u5"}})
}

test_lead_cannot_assign_lead if {
    not authz.allow_assign with input as object.union(assign,
        {"user": lead, "payload": {"role": "team_lead", "user_id": "u5"}})
}
```

### Сторона FastAPI

```python
# authz/opa.py
class OpaClient:
    async def query(self, rule: str, input: dict) -> Any:
        r = await self.http.post(f"{self.url}/v1/data/{rule}", json={"input": input}, timeout=0.5)
        r.raise_for_status()
        return r.json().get("result")

# authz/deps.py
async def authorize(request: Request,
                    user: CurrentUser = Depends(current_user),
                    ctx: AuthContext = Depends(get_auth_context),   # членства из кэша
                    opa: OpaClient = Depends(get_opa)) -> Grant:
    inp = {"method": request.method,
           "route": request.scope["route"].path,     # /api/v1/deals/{deal_id}
           "user": ctx.to_opa()}
    try:
        d = await opa.query("saleshub/authz/decision", inp)
    except httpx.HTTPError:
        raise ServiceUnavailable("authz")           # OPA недоступен → никого не пускаем
    if not d or not d["allow"]:
        raise Forbidden(action=(d or {}).get("action"))
    return Grant.from_decision(d, inp, opa)

protected = APIRouter(dependencies=[Depends(authorize)])   # всё, кроме /auth и /health
```

```python
# authz/grant.py — одно правило «своё / отдел / компания» в трёх формах
@dataclass(frozen=True)
class Grant:
    action: str
    user_id: UUID
    is_all: bool
    org_ids: frozenset[UUID]
    team_ids: frozenset[UUID]
    own_org_ids: frozenset[UUID]

    def matches(self, obj) -> bool:                      # 1) объект в памяти
        if self.is_all:
            return True
        return (obj.org_id in self.org_ids
                or obj.team_id in self.team_ids
                or (obj.org_id in self.own_org_ids and obj.owner_id == self.user_id))

    def ensure_object(self, obj) -> None:
        if not self.matches(obj):
            raise NotFound()          # 404, а не 403: не раскрываем, что объект существует

    def sql_filter(self, m):                             # 2) список и bulk → WHERE
        if self.is_all:
            return true()
        conds = []
        if self.org_ids:
            conds.append(m.org_id.in_(self.org_ids))
        if self.team_ids:
            conds.append(m.team_id.in_(self.team_ids))
        if self.own_org_ids:
            conds.append(and_(m.org_id.in_(self.own_org_ids), m.owner_id == self.user_id))
        return or_(*conds) if conds else false()

    def es_filter(self) -> dict: ...                     # 3) поиск → фильтр Elasticsearch (раздел 17)
```

**Почему списки фильтрует SQL, а не OPA.** Спросить OPA про каждый из 5 млн контактов невозможно. OPA отдаёт области доступа, бэкенд превращает их в `WHERE`, а индексы `(org_id, owner_id)`, `(team_id, …)` делают запрос быстрым. Тот же фильтр обязательно применяется в bulk-операциях: job сохраняет его в `bulk_jobs.params`, воркер добавляет к каждому чанку — иначе руководитель отдела мог бы массово переназначить чужих клиентов. Продвинутый вариант — partial evaluation в OPA (`/v1/compile`), которая сама возвращает условия для SQL; это эксперимент этапа 10.

### Кто выдаёт роли

1. Супер-админ создаётся командой при деплое (раздел 4). Только он создаёт организации и приглашает в них первого `org_admin`. Самостоятельной регистрации нет: пользователь появляется в системе только через приглашение.
2. `org_admin` создаёт отделы, приглашает людей (`member:invite`), назначает роли в организации и отделах.
3. `team_lead` назначает `sales_rep` в своём отделе (`member:assign` со scope `team`).
4. Анти-эскалация — в Rego (`deny_reasons`): нельзя выдать роль не ниже своей и нельзя менять роль себе. Причина отказа возвращается в теле 403.
5. Изменение членства → сброс кэша членств пользователя в Redis + запись в `audit_log`. Токен перевыпускать не нужно: роли не лежат в JWT.
6. Изменить матрицу (что может `sales_rep`) — PR в `data.json` + `opa test` в CI + деплой.

### Как OPA развёрнут

- **Сайдкар** в поде `api`: `opa run --server --addr=127.0.0.1:8181 /policies`. Запрос по localhost — около миллисекунды.
- Политики попадают в под через ConfigMap (Helm собирает `policies/**`). Позже (этап CD) — бандлом: CI делает `opa build` и публикует bundle в GHCR как OCI-артефакт, OPA сам его подтягивает.
- В compose — сервис `opa` с примонтированной папкой и `--watch`: правка `.rego` применяется без перезапуска. В Tilt — то же через `sync`.
- **Fail closed:** OPA недоступен → 503, алерт `OpaUnavailable` (раздел 8).
- **Decision logs:** OPA пишет каждое решение (вход + результат) в stdout → Loki → Grafana: видно, кому и почему отказали.

### Права в Swagger и тест покрытия

- При старте приложение читает `data.saleshub.routes.actions` из OPA и дописывает в описание каждой операции «Требуется: `deal:update`».
- `test_routes_match_policy`: множество маршрутов защищённого роутера должно совпадать с ключами `actions` в `routes.rego`. Новый эндпоинт без правила или устаревшее правило без эндпоинта — CI падает.

### Нужен ли Auth0

Нет. Auth0 — провайдер входа (пароли, MFA, SSO, соцсети); всё это делает свой модуль из раздела 4. Row-level доступ (own / team / org) Auth0 не решает в любом случае — это задача OPA и бэкенда. Попробовать Auth0 можно на этапе 10: проверка токена уже идёт через JWKS, меняются только `ISSUER`, `AUDIENCE` и `JWKS_URL`.

## 6. Большие объёмы данных и фоновая обработка

Массовая операция — это асинхронная job: API проверяет права, считает объём, создаёт запись `bulk_jobs` и сразу отвечает `202 + job_id`. Воркер режет выборку на чанки по диапазонам id и обновляет каждый чанк одним set-based SQL в своей транзакции. В очередь уходит одна команда, а SQL выполняется пачками — не одной транзакцией на миллион строк и не по одной строке.

> Система воркеров (очереди, ретраи, масштабирование, мониторинг, async внутри Celery) описана отдельным модулем в разделе 15. Здесь зафиксирован только общий подход к объёму данных.

### Сценарии CRM

| Сценарий | Объём | Техника |
| --- | --- | --- |
| Импорт контактов из CSV с дедупликацией по email | 500 тыс. строк | файл в MinIO → `COPY` в `contacts_staging` → валидация SQL-ом → `INSERT … ON CONFLICT (org_id, email) WHERE deleted_at IS NULL DO UPDATE`; битые строки → `import_errors` |
| Менеджер уволился: передать всех его клиентов | до 100 тыс. контактов + сделки + открытые активности | чанки `UPDATE … SET owner_id = :new WHERE owner_id = :old AND id BETWEEN …` по трём таблицам |
| Закрыть просроченные сделки как `lost` | десятки тысяч | частичный индекс `(org_id, expected_close_date) WHERE status = 'open'` + чанки |
| Добавить тег сегменту | до 1 млн контактов | `UPDATE … SET tags = array_append(tags, :t) WHERE NOT tags @> ARRAY[:t]` — идемпотентно |
| Экспорт сегмента в CSV | до 5 млн | серверный курсор + `StreamingResponse`; для больших — файл в MinIO и ссылка |
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
2. API: `authorize()` → OPA: `bulk:reassign_owner` → проверка, что новый ответственный состоит в том же отделе → фильтр `owner_id = :from` AND `grant.sql_filter()` → `count`, `min(id)`, `max(id)` по каждой таблице → `bulk_jobs(status=pending, total=…)` → `uow.commit()` → задача `bulk.plan` → `202 {job_id}`.
3. `bulk.plan`: режет диапазоны id по 10 000 и запускает группу задач `bulk.chunk` с финальной `bulk.finalize`.
4. `bulk.chunk(job_id, table, lo, hi)`: одна транзакция: `UPDATE … SET owner_id = :to, version = version + 1 WHERE id BETWEEN lo AND hi AND owner_id = :from AND <scope>`; `processed += rowcount`; событие в outbox.
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

1. `RequestIdMiddleware` — берёт `X-Request-ID` или генерирует, кладёт в `contextvars` и в ответ.
2. `AccessLogMiddleware` — метод, путь-шаблон (`/deals/{deal_id}`, а не реальный id), статус, длительность, user\_id.
3. `MetricsMiddleware` — гистограмма латентности и счётчик запросов по шаблону пути и статусу (раздел 8).
4. `CORSMiddleware` — для фронта.
5. (опц.) `GZipMiddleware`, лимит размера тела.

Механика лучше на чистом ASGI (а не `BaseHTTPMiddleware`) — заодно разобраться, почему `BaseHTTPMiddleware` плохо дружит со стримингом.

### Ошибки

```python
class AppError(Exception):
    status = 500
    code = "internal"
    def __init__(self, message: str = "", **details): ...

class NotFound(AppError):        status, code = 404, "not_found"
class Forbidden(AppError):       status, code = 403, "forbidden"
class Unauthorized(AppError):    status, code = 401, "auth.invalid_token"
class Conflict(AppError):        status, code = 409, "conflict"          # version mismatch, дубль
class ValidationFailed(AppError): status, code = 422, "validation"
class RateLimited(AppError):     status, code = 429, "rate_limited"
class ServiceUnavailable(AppError): status, code = 503, "unavailable"   # OPA, SMTP
```

Сервисы и репозитории бросают только `AppError`-наследников, ничего не знают про HTTP-коды фреймворка. Обработчики:

| Исключение | Ответ | Лог |
| --- | --- | --- |
| `AppError` | его status + тело ниже | warning (4xx) |
| `RequestValidationError` | 422, `code=validation`, поля в `details` | info |
| `IntegrityError` (unique) | 409 `conflict` | warning |
| любое другое | 500 `internal`, без стектрейса наружу | error + traceback |

Формат тела (по мотивам RFC 9457 problem+json):

```json
{
  "type": "https://saleshub.local/errors/not_found",
  "code": "not_found",
  "title": "Deal not found",
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

`/health/live` — процесс жив (для liveness). `/health/ready` — есть соединение с Postgres и Redis (для readiness, без него pod не получает трафик).

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

- `env.py` асинхронный (`async_engine_from_config` + `run_sync`), URL из той же `Settings`, что и приложение.
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
| unit | `Grant.ensure_object`, `sql_filter` (компиляция SQL), сервисы с `FakeUnitOfWork`, анти-эскалация, нарезка чанков, маппинг ошибок | pytest, pytest-asyncio, polyfactory | секунды |
| integration | репозитории на настоящем Postgres, API через `httpx.AsyncClient(transport=ASGITransport(app))`, миграции, Celery-задачи в eager-режиме и с реальным воркером | testcontainers (postgres, redis), factory-boy | десятки секунд |
| e2e (smoke) | логин → создать контакт → передать клиентов (bulk) → дождаться job | pytest против `tilt`/kind-стенда | минуты, в CD |
| нагрузка (опц.) | список контактов и bulk на 5 млн | locust / k6 | вручную |
| policy (Rego) | каждая роль × действие × свой/чужой объект, анти-эскалация | opa test --coverage, Regal (линтер) | миллисекунды |
| auth flow | приглашение → письмо → accept → login → refresh → повтор старого refresh (отзыв семьи) → reset | integration + Mailpit API (чтение писем из теста), freezegun для сроков | секунды |

### Изоляция БД в integration-тестах

- Один контейнер Postgres на сессию pytest, `alembic upgrade head` один раз.
- Каждый тест — во внешней транзакции с `SAVEPOINT` (`join_transaction_mode="create_savepoint"`), откат после теста: быстро и без очистки таблиц.
- Сессия подменяется через `app.dependency_overrides[get_session]`.
- Тесты bulk-операций, которые коммитят в нескольких транзакциях (воркер), используют `TRUNCATE` в фикстуре — отдельный маркер `@pytest.mark.committing`.

### Матрица прав (генерируется)

```python
CASES = build_matrix(app.routes, ROLE_MATRIX)  # из routes.rego + data.json, против живого OPA

@pytest.mark.parametrize("role,route,target,expected", CASES)
async def test_access(role, route, target, expected, client_as):
    # target: own | same_team | same_org | other_org
    resp = await client_as(role).request(route.method, route.url_for(target))
    assert resp.status_code == expected   # 2xx / 403 / 404
```

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
| Deployment telegram-bot | 1 реплика, со своим OPA-сайдкаром (раздел 16) |

Полезно один раз пройти руками: `kubectl get pods -w` во время выкатки, `kubectl logs -f`, `kubectl describe pod` для упавшего пода, `kubectl port-forward`, `helm upgrade --atomic` и откат `helm rollback`.

### Секреты: Sealed Secrets с первого дня

Секреты проекта: пароль Postgres, приватный RS256-ключ для JWT, пароль супер-админа, SMTP, токен Telegram-бота для алертов, ключи MinIO.

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
watch_file('./policies')                     # правка .rego → ConfigMap обновится
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
| `core` | api + OPA, воркеры, beat, outbox-relay, telegram-bot, Postgres, Redis, MinIO, Mailpit, Sealed Secrets, pgAdmin, RedisInsight, Flower | всегда |
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
| Redis | RedisInsight | `redis.saleshub.localhost` · 5540 | ключи кэша (права, членства, rate limit), очереди Celery, память |
| Celery | Flower | `flower.saleshub.localhost` · 5555 | воркеры, задачи, ретраи, длина очередей |
| Kafka (Redpanda) | Redpanda Console | `kafka.saleshub.localhost` · 8080 | топики, сообщения, consumer groups и их lag |
| Prometheus | встроенный UI Prometheus | `prometheus.saleshub.localhost` · 9090 | метрики, PromQL, targets, активные правила |
| Дашборды | Grafana | `grafana.saleshub.localhost` · 3000 | дашборды из раздела 8, логи (Loki) |
| Алерты | Alertmanager UI | `alerts.saleshub.localhost` · 9093 | сработавшие алерты, тишина (silences) |
| Почта | Mailpit | `mail.saleshub.localhost` · 8025 | письма приглашений и сброса пароля |
| Файлы | MinIO Console | `minio.saleshub.localhost` · 9001 | файлы импорта и экспорта |
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
| `integration` | services: postgres, redis; `alembic upgrade head`; `pytest tests/integration` | lint |
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
| Файлы | MinIO | S3 / GCS | только endpoint и ключи: клиент один (S3 API) |
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
- Файлы — только через S3 API (`aioboto3`), а не через специфичный клиент MinIO.
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
| Список контактов (TanStack Table, keyset-подгрузка, виртуальный скролл, поиск) | scope-фильтр, trigram-поиск на 5 млн строк |
| Карточка контакта: сделки, активности, заметки | связи, оптимистическая блокировка (`version`) |
| Импорт CSV: загрузка → прогресс → отчёт об ошибках | job, `import_errors`, дедупликация |
| Массовые действия: выбрать сегмент → передать другому менеджеру / добавить тег | bulk job, предпросмотр количества, прогресс по SSE |
| Корзина | мягкое удаление и восстановление |
| Отчёты: воронка, сумма сделок по менеджерам | materialized view, права `report:read` |
| Команда: отделы, участники, приглашения | назначение ролей, анти-эскалация (403 с причиной видна в UI) |

Кнопки прячутся по `GET /auth/me → permissions`, но это только UX: решение всегда принимает бэкенд. В Kubernetes фронт — отдельный Deployment с nginx и статикой, в Tilt — `vite dev` как `local_resource` с проксированием `/api`.

## 14. Дорожная карта и открытые вопросы

Двенадцать этапов (0–11), каждый разбит на шаги размером от нескольких часов до дня и заканчивается работающим результатом и тестами; к Kubernetes переходим только после того, как всё работает в compose. Этапы 0–8 — ядро (всё из исходной задачи), 9–11 — расширения по желанию.

**Как работать по плану:**

- Один шаг — одна ветка и один PR в `main`; после каждого шага CI зелёный.
- Отмечай шаг галочкой прямо здесь, когда он смержен.
- Не переходи к следующему этапу, пока не выполнено «Готово, когда».
- В скобках у шага — раздел документа, где описаны детали.
- Застрял на шаге дольше дня — разбей его на два и запиши здесь.

> **Ядро — этапы 0–8.** Закрывает все исходные цели: вход и права (JWT, роли, OPA, own / team / org), большие объёмы (bulk, Celery, beat, индексы), таблицы с индексами для экспериментов, Kubernetes и Tilt, тесты, CI, Alembic, логи, метрики и алерты. Сюда же входят Telegram-бот и валюты — они недорогие и опираются на то, что уже построено.

### Этап 0 — Подготовка

- [ ] 0.1 Установить: Docker, uv, Python 3.13, k3d, kubectl, helm, tilt, opa, kubeseal, gh CLI.
- [ ] 0.2 Создать публичный репозиторий `saleshub` на GitHub: README, лицензия, `.gitignore`, `.editorconfig`.
- [ ] 0.3 Экспортировать этот документ в `docs/DESIGN.md`.
- [ ] 0.4 pre-commit: ruff, ruff-format, mypy, gitleaks (поиск секретов).
- [ ] 0.5 В настройках GitHub включить secret scanning, push protection, Dependabot, защиту ветки `main`.

**Готово, когда:** `pre-commit run --all-files` проходит, первый коммит в `main`.

### Этап 1 — Скелет приложения

- [ ] 1.1 `pyproject.toml` через uv, структура папок (раздел 2).
- [ ] 1.2 `Settings` на pydantic-settings, профили dev/test/prod, `.env.example`.
- [ ] 1.3 `create_app()`, роутер `/api/v1`, `/health/live`.
- [ ] 1.4 structlog (JSON), `RequestIdMiddleware`, access-лог (раздел 7).
- [ ] 1.5 Иерархия `AppError`, обработчики, формат ошибки, тексты на украинском (раздел 7).
- [ ] 1.6 Dockerfile (multi-stage) и docker compose: api, Postgres, Redis, pgAdmin, RedisInsight, Mailpit, MinIO.
- [ ] 1.7 Async engine, сессия на запрос, `Base` с naming convention, `/health/ready`.
- [ ] 1.8 Alembic с async `env.py`, первая пустая миграция, `make migrate`.
- [ ] 1.9 pytest: `conftest`, testcontainers Postgres, первый тест health.
- [ ] 1.10 Минимальный `ci.yml`: lint + unit + integration.

**Готово, когда:** `make up` поднимает всё, все UI открываются (раздел 11), `/health/ready` отвечает 200, CI зелёный.

### Этап 2 — Вход по почте (раздел 4)

- [ ] 2.1 Модели `users`, `refresh_tokens`, `email_tokens` + миграция.
- [ ] 2.2 Хеширование argon2, правила пароля.
- [ ] 2.3 Ключи RS256, выпуск и проверка JWT, `/.well-known/jwks.json`.
- [ ] 2.4 Команда `create-superadmin` — первый пользователь, под которым можно войти.
- [ ] 2.5 `login`, зависимость `current_user`, кнопка Authorize в Swagger.
- [ ] 2.6 `refresh` с ротацией и защитой от повторного использования.
- [ ] 2.7 `logout`, `logout-all`, `sessions`, удаление сессии.
- [ ] 2.8 `EmailSender` (пока синхронно в Mailpit), шаблоны писем на Jinja2.
- [ ] 2.9 Приглашения: супер-админ приглашает по email → письмо → `invite/accept` задаёт пароль и подтверждает email. Организация и роль в приглашении — на этапе 3.
- [ ] 2.10 `password/forgot`, `password/reset`, `password/change`.
- [ ] 2.11 Rate limit на Redis для логина, `forgot` и приглашений.
- [ ] 2.12 Интеграционный тест всего сценария: письма читаются через API Mailpit, сроки — через freezegun.

**Готово, когда:** сценарий проверки из раздела 4 проходит руками в Swagger и автотестом.

### Этап 3 — Организации и права на OPA (раздел 5)

- [ ] 3.1 Модели `organizations` (валюта, часовой пояс), `teams`, `org_memberships`, `team_memberships` + миграция.
- [ ] 3.2 OPA в compose; `routes.rego`, `data.json`, `authz.rego`; первые `opa test`.
- [ ] 3.3 `OpaClient`, `AuthContext` (членства + кэш в Redis), `authorize()`, `Grant`.
- [ ] 3.4 Защищённый роутер и тест `test_routes_match_policy`.
- [ ] 3.5 Эндпоинты организаций (супер-админ) и отделов (org admin).
- [ ] 3.6 Приглашения в организацию с ролью и отделом, проверка allow\_assign: создание, письмо, `invite/accept`.
- [ ] 3.7 Назначение ролей, анти-эскалация в Rego, причина отказа в 403.
- [ ] 3.8 `audit_log` (пока без партиций) и сброс кэша членств при изменениях.
- [ ] 3.9 «Требуется: …» в описании операций Swagger.
- [ ] 3.10 Тест-матрица «роль × эндпоинт × свой/чужой» + `opa test --coverage` в CI.

**Готово, когда:** супер-админ создаёт организацию → приглашает org admin → тот создаёт отдел и приглашает менеджера; каждый видит и меняет только то, что разрешено матрицей.

### Этап 4 — CRM-домен и объём (раздел 3)

- [ ] 4.1 `companies` и `contacts` с мягким удалением и `version`; общий фильтр `deleted_at IS NULL` в репозитории.
- [ ] 4.2 CRUD компаний и контактов с `Grant.sql_filter` / `ensure_object`, keyset-пагинация.
- [ ] 4.3 Воронки, этапы, `deals`, эндпоинт канбана.
- [ ] 4.4 `fx_rates`, `FxService` (НБУ, кэш, резерв), суммы сделок в USD, параметр `?currency=`.
- [ ] 4.5 `activities` с партициями по месяцам и `notes`.
- [ ] 4.6 Корзина: удаление, восстановление, список удалённых.
- [ ] 4.7 Отчёты: materialized view и эндпоинт.
- [ ] 4.8 `seed_big.sql` (5 млн контактов, 1 млн сделок, 20 млн активностей), `make seed-big`.
- [ ] 4.9 Индексы из раздела 3: замеры «без / с» → `docs/experiments.md`.
- [ ] 4.10 Тесты: «список не возвращает чужого», конвертер валют, корзина.

**Готово, когда:** на сгенерированных данных все списки и отчёты идут по индексам (в `EXPLAIN` нет Seq Scan по большим таблицам), замеры записаны.

### Этап 5 — Воркеры, bulk и Telegram-бот (разделы 6, 15, 16)

- [ ] 5.1 `celery_app`, очереди, event loop на процесс, `BaseTask`, передача `request_id`; воркер, beat и Flower в compose.
- [ ] 5.2 Письма через очередь `notifications` + `sent_notifications` (без дублей).
- [ ] 5.3 `outbox_events` и процесс relay (`LISTEN/NOTIFY` + опрос).
- [ ] 5.4 `bulk_jobs`, `plan` / `chunk` / `finalize`, эндпоинт передачи клиентов, прогресс по SSE.
- [ ] 5.5 Отмена job, `reap_stuck`, `failed_tasks` и перезапуск.
- [ ] 5.6 Импорт CSV: загрузка в MinIO → `COPY` → merge → `import_errors`.
- [ ] 5.7 Закрытие просроченных сделок, теги, потоковый экспорт в CSV.
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
- [ ] 7.4 Сабчарты Postgres, Redis, MinIO, Mailpit, pgAdmin, RedisInsight с флагами `enabled`.
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
- [ ] 9.5 Search API, `Grant.es_filter()`, тест «менеджер не находит чужих клиентов».
- [ ] 9.6 `cd.yml`: сборка → GHCR → коммит нового тега в `deploy/envs/local`.
- [ ] 9.7 Argo CD в группе `cd`, приложение на `envs/local`; проверить `git push` → выкатка → `git revert` → откат.
- [ ] 9.8 (опционально) Self-hosted runner и push-деплой — для сравнения.

**Готово, когда:** `git push` в `main` сам приводит к новой версии в локальном кластере, а поиск находит контакт с опечаткой через секунду после его создания.

### Этап 10 — Эксперименты (по желанию, в любом порядке)

- [ ] 10.1 OPA partial evaluation вместо ручного `sql_filter`.
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
| Домен | мульти-тенантная CRM: организации → отделы → компании, контакты, сделки, активности |
| Кто создаёт организации | только супер-админ; дальше всё через приглашения |
| Удаление | мягкое (корзина 30 дней) для компаний, контактов, сделок; жёсткое для остального |
| 2FA | не делаем |
| Версии | Python 3.13, PostgreSQL 18 |
| Аутентификация | своя, по почте: access 1 ч, refresh 30 дней с ротацией |
| Движок прав | OPA + Rego, матрица ролей в git |
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
| Проверка прав | один запрос к OPA (действие + области); области применяет Grant в Python; анти-эскалация — в Rego |

### Открытые вопросы

- [x] Уведомления — email и Telegram-бот (раздел 16).
- [x] Поиск — Elasticsearch (раздел 17).
- [ ] Новых открытых вопросов нет; следующий шаг — этап 1 дорожной карты.

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
- **Запуск job:** заголовок `Idempotency-Key` → уникальный индекс `bulk_jobs(created_by, idempotency_key)`.
- **Одна тяжёлая job на организацию:** частичный уникальный индекс `bulk_jobs(org_id, kind) WHERE status IN ('pending', 'running')` — второй запуск получит 409.
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

Настройки: таблица `notification_settings(user_id, event_type, channel, enabled)` и эндпоинты `GET/PUT /api/v1/me/notifications`. «Тихие часы» (например, 22:00–08:00) — несрочное откладывается до утра.

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

Бот вызывает те же сервисы, что и API, от имени привязанного пользователя: права проверяет OPA, видны только свои данные. Поэтому в поде бота тот же OPA-сайдкар, что и у api.

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

Поиск обязан видеть ровно то же, что и списки. `Grant` из раздела 5 получает метод `es_filter()` — тот же scope, только в виде фильтра Elasticsearch:

```python
def es_filter(self) -> dict:
    if self.is_all:
        return {"match_all": {}}
    should = []
    if self.org_ids:
        should.append({"terms": {"org_id": list(self.org_ids)}})
    if self.team_ids:
        should.append({"terms": {"team_id": list(self.team_ids)}})
    if self.own_org_ids:
        should.append({"bool": {"filter": [
            {"terms": {"org_id": list(self.own_org_ids)}},
            {"term": {"owner_id": str(self.user_id)}}]}})
    return {"bool": {"should": should, "minimum_should_match": 1}} if should else {"match_none": {}}
```

Фильтр `deleted: false` и scope добавляются в каждый запрос в одном месте — в `SearchRepository`, вручную в эндпоинтах их не пишут. Отдельный тест проверяет, что `sales_rep` не находит чужих клиентов ни в списке, ни в поиске.

### API

| Метод | Путь | Что делает |
| --- | --- | --- |
| GET | `/api/v1/search/contacts?q=…&status=…&tags=…` | поиск с опечатками, фасеты по статусу, тегам, ответственному |
| GET | `/api/v1/search/suggest?q=…` | подсказки при вводе: первые 10 имён и компаний |
| POST | `/api/v1/admin/search/reindex` | полная переиндексация (только супер-админ), это bulk job |

### Запуск и ресурсы

- Локально — один узел Elasticsearch с памятью JVM 2 ГБ плюс Kibana: около 3–4 ГБ, на ноутбуке с 32 ГБ помещается рядом со всем остальным.
- Kibana — UI: смотреть индексы и документы, отлаживать запросы в Dev Tools (добавляется в таблицу UI, раздел 11).
- Для украинского языка — плагин морфологии `analysis-ukrainian`, ставится при сборке образа.
- В облаке — Elastic Cloud или Amazon OpenSearch Service; переключается адресом в values, как остальные внешние сервисы.
- Метрики — elasticsearch-exporter; алерты: кластер `red`, отставание индексатора (consumer lag > 10 000 сообщений).
