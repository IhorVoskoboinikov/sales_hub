"""Chaos-тест массовой операции (этап 5.10; разделы 6, 14 и 15 docs/DESIGN.md).

Запуск: make chaos (против стенда make up). Вопрос, на который отвечает тест: если посреди
передачи клиентов уволенного менеджера убить воркер, завершится ли job и не посчитается ли
какой-то чанк дважды. Unit- и integration-тесты проверяют то же на фикстуре celery_worker, но не
настоящий kill -9: только он показывает, что делают Celery (acks_late, task_reject_on_worker_lost),
Redis (visibility_timeout) и Postgres с транзакцией процесса, который умер на середине UPDATE.

Данные — свои: скрипт создаёт организацию chaos-<id>, отдел, org_admin (вход через API тем же
паролем, что задаёт hash_password приложения) и на каждый сценарий — своего org_admin-автора job и
двух менеджеров (sales_rep): прежнему ответственному generate_series даёт --contacts контактов (по
умолчанию 100 000). Seed-данные не трогаются. В конце всё созданное удаляется — и при ошибке, и по
Ctrl+C / SIGTERM: контакты, job и их чанки, строки журнала и failed_tasks, outbox организации и
sent_notifications её людей, люди, отдел, организация, письма в Mailpit; потом VACUUM (ANALYZE)
contacts.

Сценарии (--scenario a | b | all):

- A — kill -9 одного процесса пула worker-bulk. PID — из celery inspect (stats — процессы пула,
  active — какой из них выполняет какой чанк), kill — через docker compose exec. Главный процесс
  Celery видит, что процесс пула умер (WorkerLostError), и из-за task_reject_on_worker_lost
  возвращает сообщение в очередь (reject с requeue) — чанк сразу выполняется заново.
- B — docker compose kill -s KILL worker-bulk и docker compose up -d worker-bulk. Убиты все
  процессы, сообщения выполнявшихся чанков остались неподтверждёнными (unacked в Redis). Их
  вернёт Redis через visibility_timeout (20 мин), а bulk.reap_stuck переотправит pending-чанки
  раньше — через 5 минут без прогресса (BULK_REAP_STALL_SECONDS) и при пустой очереди bulk.
  После завершения job скрипт ждёт и повторную выдачу исходных сообщений (--redelivery-wait):
  копия чанка, пришедшая через 20 минут, не должна ничего изменить.

Как попасть в середину чанка. Чанк в 10 000 строк длится ~1 с, вся job — несколько секунд:
пока скрипт дождётся processed > 30 % и спросит celery inspect (~2 с), job бы уже закончилась.
Поэтому до запуска job скрипт держит в своей транзакции блокировку (SELECT … FOR UPDATE) одной
строки в середине каждого чанка начиная с floor(kill_at · N) + 1 (--hold, по умолчанию включено;
при kill_at 0,3 и 10 чанках — с 4-го: первые 40 % проходят). Следующие чанки доходят в своём
UPDATE до этой строки и ждут: у каждого процесса пула открыта транзакция, где половина строк
чанка уже изменена. Ровно тогда процесс (или весь воркер) убивается, и сразу после этого
блокировка отпускается. Ждать дольше soft_time_limit чанка (60 с) нельзя — чанк упал бы по
таймауту, поэтому блокировка отпускается и по сторожу через 40 с. Число чанков сверяется с
--chunk-size: если BULK_CHUNK_SIZE стенда другой, блокировки легли бы не туда — это промах.
--no-hold — без блокировок: kill по времени, сценарий может промахнуться (job успеет закончиться).

Что kill попал в чанк, проверяется, а не предполагается: celery inspect отвечает ~5 с и может
устареть. Прямо перед kill: строки этих чанков ещё pending (не закоммичены), а запросы воркера
стоят в pg_stat_activity за соединением блокировки (pg_blocking_pids). В B после kill
неподтверждённые сообщения читаются из hash unacked — убиты ровно они. После прогона по логам:
убитый в A чанк выполнен заново тем же сообщением (bulk_chunk_done того же task_id после kill,
не bulk_chunk_already_finished). Иначе — промах, код выхода 1.

Что считается доказательством «ни один чанк не посчитан дважды»:

- processed == числу созданных контактов (не только processed == total: чанк ставит
  total = GREATEST(total, processed), и дважды посчитанный чанк поднял бы оба числа вместе);
- сумма bulk_job_chunks.rows == processed, и у каждого чанка rows == реальному числу строк его
  диапазона id у нового ответственного;
- у всех контактов version == 2: создан с 1, каждая строка изменена ровно одним UPDATE (повторный
  UPDATE той же строки дал бы 3; его не пускает условие owner_id = :from, тест это проверяет);
- у прежнего ответственного 0 контактов, у нового — все; result.rows и одна строка журнала
  bulk.reassign_owner — итог записан один раз;
- нет строк failed_tasks этих задач, и в Redis (unacked, очередь bulk) не осталось их сообщений;
- в B — после повторной выдачи исходных сообщений все счётчики те же, что в момент done;
- в логах ровно один bulk_chunk_done на чанк, с rows как в базе (лог пишется до коммита, поэтому
  строки убитой задачи до kill не считаются: её транзакция откатилась), убитые чанки выполнены
  после kill; повторный UPDATE 0 строк счётчики бы не изменил, а второй bulk_chunk_done виден;
- в B у каждой копии исходного сообщения — bulk_chunk_already_finished и ни одного
  bulk_chunk_done: копия не выполняла чанк, а не просто ничего не изменила.

Уведомления (приёмка этапа 5, шаг 5.15). Автор job — свой org_admin на каждый сценарий, получатель
— новый менеджер; оба с подтверждённым email и настройками по умолчанию. После финала (в B — уже
после повторной выдачи исходных сообщений: копия чанка ещё раз вызывает finalize) по HTTP API
Mailpit стенда: автору ровно одно письмо с итогом, получателю — ровно одно «вам передано N
клієнтів»; в базе одно событие bulk_job.completed этой job и по строке sent_notifications на
письмо. Telegram — только если у стенда задан TELEGRAM_BOT_TOKEN и указаны два чата
(--telegram-chats): Bot API настоящий, сообщения не перехватить, поэтому проверяется строка
sent_notifications канала telegram у каждого и что привязка цела (403 / «chat not found» её бы
удалили). Иначе скрипт пишет, почему Telegram пропущен. Письма этих адресов в конце удаляются из
Mailpit вместе с остальными данными.

Таймлайн — из трёх источников: действия скрипта, строки базы (started_at, finished_at чанков и
job, рост reaps) и логи контейнеров (docker compose logs: выдача сообщений, bulk_chunk_done,
bulk_chunk_already_finished, bulk_job_reaped, гибель процесса пула, откат транзакции убитого
процесса в логе Postgres). Результаты — в docs/experiments.md.
"""

import argparse
import asyncio
import json
import math
import re
import signal
import sys
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # чтобы импортировать app

from app.core.config import get_settings
from app.core.security import hash_password
from app.texts import uk

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ("docker", "compose", "-f", str(ROOT / "deploy/compose/docker-compose.yml"))
CELERY = "celery -A app.workers.celery_app:celery_app"
APP_NAME = "chaos"  # application_name соединений скрипта
PASSWORD = "chaos-password-1"
EMAIL_DOMAIN = "chaos.example"
CONCURRENCY = 4  # -c у worker-bulk в compose: столько чанков выполняется одновременно
HOLD_WATCHDOG_SECONDS = 40  # меньше soft_time_limit чанка (60 с)
ANSI = re.compile(r"\x1b\[[0-9;]*m")
FINAL = ("done", "partial_failed", "failed", "cancelled")
NOTIFY_WAIT_SECONDS = 60  # от финала job до писем: relay → notify → relay → send_email
NOTIFY_SETTLE_SECONDS = 10  # после писем: лишнее письмо, если бы оно было, успело бы прийти
SUMMARY_SUBJECT = uk.EMAIL_SUBJECTS["bulk_job_completed"]
TRANSFER_SUBJECT = uk.EMAIL_SUBJECTS["bulk_reassigned"]


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def now() -> datetime:
    return datetime.now(UTC)


def short(task_id: str) -> str:
    return task_id[:8]


# --- команды стенда: docker compose, celery inspect, redis-cli


async def run(*cmd: str, check: bool = True) -> str:
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[-4:])}: код {proc.returncode}: {err.decode()[-500:]}")
    return out.decode()


async def compose(*args: str, check: bool = True) -> str:
    return await run(*COMPOSE, *args, check=check)


async def running_services() -> set[str]:
    return set((await compose("ps", "--status", "running", "--services")).split())


async def inspect(*commands: str) -> list[Any]:
    """celery inspect <команда> у worker-bulk (адресат — bulk@$HOSTNAME, как в healthcheck):
    ответ этого воркера по каждой команде. Несколько команд — в одном docker compose exec."""
    script = "; ".join(f"{CELERY} inspect {c} -d bulk@$HOSTNAME --json" for c in commands)
    out = await compose("exec", "-T", "worker-bulk", "sh", "-c", script)
    replies = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
    if len(replies) != len(commands):
        raise RuntimeError(f"celery inspect {commands}: нет ответа воркера: {out[-500:]}")
    return [next(iter(reply.values())) for reply in replies]


async def redis_text(*args: str) -> str:
    """redis-cli в контейнере redis, db 1 — брокер Celery (порт на хосте не нужен)."""
    return await compose("exec", "-T", "redis", "redis-cli", "-n", "1", "--raw", *args)


async def stack_has_telegram_token() -> bool:
    """Задан ли TELEGRAM_BOT_TOKEN у воркеров стенда (compose берёт его из deploy/compose/.env),
    а не у этого процесса: отправляет worker, у скрипта свои настройки."""
    out = await compose(
        "exec", "-T", "worker", "sh", "-c", '[ -n "$TELEGRAM_BOT_TOKEN" ] && echo yes || echo no'
    )
    return out.strip() == "yes"


async def broker_messages() -> str:
    """Все сообщения брокера, где может лежать задача чанка: неподтверждённые (hash unacked) и
    ждущие в очереди bulk."""
    return await redis_text("HVALS", "unacked") + await redis_text("LRANGE", "bulk", "0", "-1")


# --- данные теста


@dataclass
class Org:
    id: uuid.UUID
    slug: str
    team_id: uuid.UUID
    admin_id: uuid.UUID
    admin_email: str
    users: list[uuid.UUID] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)  # адреса людей — их письма в Mailpit
    jobs: list[uuid.UUID] = field(default_factory=list)


async def create_org(conn: asyncpg.Connection, run_id: str) -> Org:
    """Организация, отдел и org_admin с известным паролем (хэш — тем же hash_password, что у
    приложения: вход идёт через настоящий POST /auth/login)."""
    org = Org(
        id=uuid.uuid4(),
        slug=f"chaos-{run_id}",
        team_id=uuid.uuid4(),
        admin_id=uuid.uuid4(),
        admin_email=f"chaos-{run_id}-admin@{EMAIL_DOMAIN}",
    )
    password_hash = await hash_password(PASSWORD)
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)",
            org.id,
            f"Chaos {run_id}",
            org.slug,
        )
        await conn.execute(
            "INSERT INTO teams (id, org_id, name) VALUES ($1, $2, 'Chaos')", org.team_id, org.id
        )
        await conn.execute(
            "INSERT INTO users (id, email, password_hash, org_id, role, email_verified_at) "
            "VALUES ($1, $2, $3, $4, 'org_admin', now())",
            org.admin_id,
            org.admin_email,
            password_hash,
            org.id,
        )
    org.users.append(org.admin_id)
    org.emails.append(org.admin_email)
    return org


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    email: str


async def create_member(
    conn: asyncpg.Connection, org: Org, name: str, role: str = "sales_rep"
) -> Person:
    """Человек организации с тем же паролем, что у org_admin (вход через API), подтверждённым
    email и настройками уведомлений по умолчанию (строк notification_settings нет) — письма
    notifier ему уходят. Менеджер — в отделе, org_admin — без отдела."""
    person = Person(uuid.uuid4(), f"{org.slug}-{name}@{EMAIL_DOMAIN}")
    await conn.execute(
        "INSERT INTO users (id, email, password_hash, org_id, team_id, role, email_verified_at) "
        "SELECT $1, $2, password_hash, org_id, $3, $4, now() FROM users WHERE id = $5",
        person.id,
        person.email,
        org.team_id if role == "sales_rep" else None,
        role,
        org.admin_id,
    )
    org.users.append(person.id)
    org.emails.append(person.email)
    return person


async def create_contacts(conn: asyncpg.Connection, org: Org, owner: uuid.UUID, n: int) -> None:
    """n живых контактов менеджера одним INSERT … SELECT FROM generate_series: id идут подряд
    в конце таблицы, version = 1."""
    await conn.execute(
        """
        INSERT INTO contacts (org_id, team_id, owner_id, first_name, last_name, email, source)
        SELECT $1, $2, $3, 'Хаос', 'Клієнт ' || g, 'chaos' || g || '@' || $4, 'chaos'
        FROM generate_series(1, $5) g
        """,
        org.id,
        org.team_id,
        owner,
        f"{owner}.{EMAIL_DOMAIN}",
        n,
    )


async def cleanup(conn: asyncpg.Connection, mail: httpx.AsyncClient, org: Org) -> None:
    """Удалить всё, что создал скрипт. Порядок — по FK: job (чанки — каскадом), контакты, люди
    (refresh-токены, привязки Telegram, настройки уведомлений — каскадом), отдел, организация.
    Строки журнала и failed_tasks — по org_id, авторам и id job; outbox (события и команды
    notifier) — по org_id; sent_notifications внешнего ключа на users нет — по людям. Письма
    адресов скрипта — из Mailpit."""
    jobs = [str(j) for j in org.jobs]
    async with conn.transaction():
        failed = await conn.execute(
            "DELETE FROM failed_tasks WHERE args->>0 = ANY($1::text[]) OR user_id = ANY($2)",
            jobs,
            org.users,
        )
        audit = await conn.execute(
            "DELETE FROM audit_log WHERE org_id = $1 OR actor_id = ANY($2)", org.id, org.users
        )
        outbox = await conn.execute("DELETE FROM outbox_events WHERE org_id = $1", org.id)
        sent = await conn.execute(
            "DELETE FROM sent_notifications WHERE user_id = ANY($1)", org.users
        )
        await conn.execute("DELETE FROM bulk_jobs WHERE org_id = $1", org.id)
        contacts = await conn.execute("DELETE FROM contacts WHERE org_id = $1", org.id)
        await conn.execute("DELETE FROM users WHERE org_id = $1", org.id)
        await conn.execute("DELETE FROM teams WHERE org_id = $1", org.id)
        await conn.execute("DELETE FROM organizations WHERE id = $1", org.id)
    log(
        f"очистка: {contacts}, audit_log {audit.split()[-1]}, failed_tasks {failed.split()[-1]},"
        f" outbox_events {outbox.split()[-1]}, sent_notifications {sent.split()[-1]}"
    )
    try:
        ids = [m["ID"] for email in org.emails for m in await mailbox(mail, email)]
        if ids:
            (await mail.request("DELETE", "/messages", json={"IDs": ids})).raise_for_status()
        log(f"очистка: писем в Mailpit {len(ids)}")
    except httpx.HTTPError as error:  # стенд без Mailpit не повод оставить данные в базе
        log(f"очистка: Mailpit недоступен ({error!r}) — письма остались")
    started = time.perf_counter()
    await conn.execute("VACUUM (ANALYZE) contacts, bulk_jobs, bulk_job_chunks, users")
    log(f"очистка: VACUUM (ANALYZE) contacts — {time.perf_counter() - started:.0f} с")


# --- блокировка, которая держит чанки на середине


class Hold:
    """Своя транзакция с SELECT … FOR UPDATE по одной строке в середине каждого чанка начиная с
    first_held: UPDATE чанка доходит до неё и ждёт (см. docstring модуля)."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.conn: asyncpg.Connection | None = None
        self.tx: Any = None
        self.ids: list[int] = []
        self.pid = 0  # backend Postgres этой транзакции — его ждут запросы чанков
        self.watchdog: asyncio.Task[None] | None = None

    async def acquire(self, ids: list[int]) -> None:
        self.ids = ids
        self.conn = await asyncpg.connect(
            self.dsn, server_settings={"application_name": f"{APP_NAME}_hold"}
        )
        self.pid = self.conn.get_server_pid()
        self.tx = self.conn.transaction()
        await self.tx.start()
        await self.conn.execute("SELECT id FROM contacts WHERE id = ANY($1) FOR UPDATE", ids)

    def start_watchdog(self) -> None:
        """Чанки уже ждут: отпустить не позже чем через HOLD_WATCHDOG_SECONDS, что бы ни было."""

        async def guard() -> None:
            await asyncio.sleep(HOLD_WATCHDOG_SECONDS)
            log(f"сторож: блокировка держится {HOLD_WATCHDOG_SECONDS} с — отпускаю")
            await self.release(from_watchdog=True)

        self.watchdog = asyncio.create_task(guard())

    async def release(self, *, from_watchdog: bool = False) -> None:
        if self.watchdog is not None and not from_watchdog:
            self.watchdog.cancel()
        self.watchdog = None
        if self.conn is None:
            return
        conn, self.conn = self.conn, None
        try:
            await self.tx.rollback()
        finally:
            await conn.close()


async def hold_ids(
    conn: asyncpg.Connection, owner: uuid.UUID, chunk_size: int, first_chunk: int
) -> list[int]:
    """id строки в середине каждого чанка с номером ≥ first_chunk. Границы чанков plan строит по
    row_number() OVER (ORDER BY id) выборки (раздел 6, шаг 3) — та же нумерация здесь."""
    rows = await conn.fetch(
        """
        SELECT id FROM (
            SELECT id, row_number() OVER (ORDER BY id) AS rn
            FROM contacts WHERE owner_id = $1 AND deleted_at IS NULL
        ) s
        WHERE (rn - 1) % $2 = $2 / 2 AND (rn - 1) / $2 >= $3
        ORDER BY id
        """,
        owner,
        chunk_size,
        first_chunk,
    )
    return [r["id"] for r in rows]


# --- API


async def login(api: httpx.AsyncClient, email: str) -> dict[str, str]:
    response = await api.post("/api/v1/auth/login", data={"username": email, "password": PASSWORD})
    response.raise_for_status()
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def start_job(
    api: httpx.AsyncClient, author: Person, from_id: uuid.UUID, to_id: uuid.UUID
) -> uuid.UUID:
    headers = await login(api, author.email)
    response = await api.post(
        "/api/v1/bulk/reassign-owner",
        json={"from_user_id": str(from_id), "to_user_id": str(to_id), "include": ["contacts"]},
        headers={**headers, "Idempotency-Key": f"chaos-{uuid.uuid4()}"},
    )
    if response.status_code != 202:
        raise RuntimeError(f"POST /bulk/reassign-owner: {response.status_code} {response.text}")
    return uuid.UUID(response.json()["id"])


async def mailbox(mail: httpx.AsyncClient, to: str) -> list[dict[str, Any]]:
    """Письма Mailpit стенда для адреса to (HTTP API, от новых к старым): ID, Subject, Created —
    без текста."""
    response = await mail.get("/search", params={"query": f'to:"{to}"', "limit": 50})
    response.raise_for_status()
    found: list[dict[str, Any]] = response.json()["messages"]
    return found


async def letter_text(mail: httpx.AsyncClient, message_id: str) -> str:
    response = await mail.get(f"/message/{message_id}")
    response.raise_for_status()
    return str(response.json()["Text"])


# --- сценарий


@dataclass
class Event:
    at: datetime
    source: str  # скрипт, база, логи: имя сервиса
    text: str
    tag: str = ""  # для сводки: kill, redelivered, rerun_done, reaped, restored, …


@dataclass
class ChunkLog:
    """Строка лога воркера о чанке этой job: bulk_chunk_done или bulk_chunk_already_finished."""

    at: datetime
    name: str
    task_id: str
    no: int
    rows: int | None


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


@dataclass
class Outcome:
    scenario: str
    job_id: uuid.UUID | None = None
    author: Person | None = None  # кто запустил job: ему — письмо с итогом
    recipient: Person | None = None  # новый ответственный: ему — «вам передано N клієнтів»
    created: datetime | None = None
    events: list[Event] = field(default_factory=list)
    checks: list[Check] = field(default_factory=list)
    summary: list[tuple[str, str]] = field(default_factory=list)
    chunk_logs: list[ChunkLog] = field(default_factory=list)
    # B: лог убитого контейнера worker-bulk до up -d — см. kill_logs.
    killed_logs: str = ""
    missed: bool = False

    def miss(self, text: str) -> None:
        self.missed = True
        self.event(f"{text} — промах")

    def event(self, text: str, tag: str = "", source: str = "скрипт") -> None:
        self.events.append(Event(now(), source, text, tag))
        log(f"{self.scenario}: {text}")

    def check(self, name: str, ok: bool, detail: str) -> None:
        self.checks.append(Check(name, ok, detail))


async def job_row(conn: asyncpg.Connection, job_id: uuid.UUID) -> asyncpg.Record:
    row = await conn.fetchrow(
        "SELECT status, total, processed, failed_rows, chunks_total, failed_chunks, error, reaps,"
        " result, created_at, started_at, updated_at, finished_at,"
        " (SELECT count(*) FROM bulk_job_chunks WHERE job_id = $1 AND status = 'pending')"
        " AS pending FROM bulk_jobs WHERE id = $1",
        job_id,
    )
    assert row is not None
    return row


async def wait_for(
    probe: Callable[[], Awaitable[bool]], seconds: float, what: str, step: float = 0.2
) -> None:
    deadline = time.monotonic() + seconds
    while not await probe():
        if time.monotonic() > deadline:
            raise TimeoutError(f"не дождался: {what} за {seconds:.0f} с")
        await asyncio.sleep(step)


@dataclass
class Ctx:
    args: argparse.Namespace
    dsn: str
    conn: asyncpg.Connection
    api: httpx.AsyncClient
    mail: httpx.AsyncClient  # HTTP API Mailpit стенда
    org: Org
    hold: Hold | None = None
    # Чаты для автора и получателя, если Telegram проверяется; иначе — почему нет.
    telegram_chats: tuple[int, int] | None = None
    telegram_skip: str = ""


async def run_scenario(ctx: Ctx, scenario: str) -> Outcome:
    args, conn, org = ctx.args, ctx.conn, ctx.org
    out = Outcome(scenario)
    # Свой автор на сценарий: письма автору сценария A и B не смешиваются в одном ящике.
    out.author = await create_member(conn, org, f"{scenario}-author", "org_admin")
    from_id = (await create_member(conn, org, f"{scenario}-from")).id
    out.recipient = await create_member(conn, org, f"{scenario}-to")
    to_id = out.recipient.id
    if ctx.telegram_chats is not None:
        await link_telegram(ctx, out, ctx.telegram_chats)
    started = time.perf_counter()
    await create_contacts(conn, org, from_id, args.contacts)
    log(f"{scenario}: {args.contacts} контактов — {time.perf_counter() - started:.1f} с")

    chunks_expected = math.ceil(args.contacts / args.chunk_size)
    # Первый чанк, который держит блокировка: до него должно пройти больше kill_at строк.
    first_held = min(math.floor(args.kill_at * chunks_expected) + 1, chunks_expected - 1)
    if args.hold:
        ctx.hold = Hold(ctx.dsn)
        await ctx.hold.acquire(await hold_ids(conn, from_id, args.chunk_size, first_held))
        out.event(f"блокировка строк в чанках {first_held}…{chunks_expected - 1}")

    job_id = await start_job(ctx.api, out.author, from_id, to_id)
    org.jobs.append(job_id)
    out.job_id = job_id
    out.event(f"POST /bulk/reassign-owner → 202, job {job_id}", "post")

    async def planned() -> bool:
        return bool((await job_row(conn, job_id))["status"] != "pending")

    await wait_for(planned, 180, "plan (pending дольше 2 мин подберёт reaper)")
    job = await job_row(conn, job_id)
    out.created = job["created_at"]
    held: set[int] = set()  # чанки, в диапазоне которых есть строка под блокировкой
    if args.hold:
        assert ctx.hold is not None
        ctx.hold.start_watchdog()
        held = {
            r["no"]
            for r in await conn.fetch(
                "SELECT no FROM bulk_job_chunks c WHERE job_id = $1 AND EXISTS"
                " (SELECT 1 FROM unnest($2::bigint[]) h WHERE h BETWEEN c.lo AND c.hi)",
                job_id,
                ctx.hold.ids,
            )
        }
        uncovered = sorted(set(range(first_held, job["chunks_total"])) - held)
        if job["chunks_total"] != chunks_expected or uncovered:
            out.miss(
                f"чанков {job['chunks_total']} (ждали {chunks_expected}), без блокировки:"
                f" {uncovered} — BULK_CHUNK_SIZE стенда не {args.chunk_size}?"
            )
            await ctx.hold.release()  # блокировки легли не туда: kill без смысла
            return await finish(ctx, out, from_id, to_id, killed={})

    threshold = args.kill_at * job["total"]

    async def past_threshold() -> bool:
        row = await job_row(conn, job_id)
        return row["processed"] > threshold or row["status"] in FINAL

    await wait_for(past_threshold, 120, f"processed > {threshold:.0f}")
    job = await job_row(conn, job_id)
    if job["status"] in FINAL:
        out.miss(f"job закончилась раньше kill ({job['status']}), нужен --hold")
        return await finish(ctx, out, from_id, to_id, killed={})
    out.event(
        f"processed {job['processed']} из {job['total']} (> {args.kill_at:.0%}),"
        f" чанков {job['chunks_total']}",
        "threshold",
    )

    # Какие процессы пула выполняют чанки этой job. С блокировкой ждём, пока все CONCURRENCY
    # процессов не возьмут свой чанк (или чанков не останется).
    inflight: dict[str, int] = {}  # task_id → номер чанка: выполняются и ждут в процессе
    active: list[dict[str, Any]] = []
    stats: dict[str, Any] = {}
    deadline = time.monotonic() + 15
    want = min(CONCURRENCY, job["pending"]) if args.hold else 1
    while time.monotonic() < deadline:
        stats, active_all, reserved = await inspect("stats", "active", "reserved")
        active = [
            t for t in active_all if t["name"] == "bulk.chunk" and t["args"][0] == str(job_id)
        ]
        inflight = {
            t["id"]: int(t["args"][1])
            for t in [*active_all, *reserved]
            if t["name"] == "bulk.chunk" and t["args"][0] == str(job_id)
        }
        if len(active) >= want or (await job_row(conn, job_id))["status"] in FINAL:
            break
    if args.hold:  # убивать есть смысл только чанк, который держит блокировка
        active = [t for t in active if int(t["args"][1]) in held]
    if not active:
        out.miss("ни один чанк job не выполняется")
        if ctx.hold is not None:
            await ctx.hold.release()
        return await finish(ctx, out, from_id, to_id, killed={})
    pool = stats["pool"]["processes"]
    busy = ", ".join(f"чанк {int(t['args'][1])} — pid {t['worker_pid']}" for t in active)
    out.event(f"inspect: главный pid {stats['pid']}, пул {pool}; в работе: {busy}")
    # Ответ inspect мог устареть (~5 с на три команды): перед kill — что чанки ещё в транзакции.
    await confirm_in_transaction(ctx, out, job_id, sorted(int(t["args"][1]) for t in active))

    if scenario == "A":
        target = min(active, key=lambda t: int(t["args"][1]))
        no, pid = int(target["args"][1]), target["worker_pid"]
        killed = {target["id"]: no}
        await compose("exec", "-T", "worker-bulk", "sh", "-c", f"kill -9 {pid}")
        out.event(f"kill -9 pid {pid} (чанк {no}, задача {short(target['id'])})", "kill")
    else:
        await compose("kill", "-s", "KILL", "worker-bulk")
        out.event("docker compose kill -s KILL worker-bulk", "kill")
        # Воркер мёртв — hash unacked больше не меняется: убиты ровно эти сообщения.
        messages = await broker_messages()
        killed = {tid: no for tid, no in inflight.items() if tid in messages}
        chunks = ", ".join(
            f"{no} ({short(tid)})" for tid, no in sorted(killed.items(), key=lambda x: x[1])
        )
        out.event(f"неподтверждённые сообщения чанков в Redis: {chunks or 'нет'}")
        acked = sorted(int(t["args"][1]) for t in active if t["id"] not in killed)
        if acked:
            out.miss(f"чанки {acked} подтверждены до kill")
    if ctx.hold is not None:
        await ctx.hold.release()
        out.event("блокировка отпущена")
    if scenario == "A":
        (stats,) = await inspect("stats")
        out.event(f"пул после kill: {stats['pool']['processes']}")
    else:
        out.killed_logs = await kill_logs(out)  # после блокировки: она не ждёт лишнего
        await asyncio.sleep(args.down_seconds)
        await compose("up", "-d", "--wait", "worker-bulk")
        out.event("docker compose up -d --wait worker-bulk — воркер healthy", "worker_up")
    return await finish(ctx, out, from_id, to_id, killed)


async def confirm_in_transaction(
    ctx: Ctx, out: Outcome, job_id: uuid.UUID, numbers: list[int]
) -> None:
    """Перед kill: строки чанков ещё pending — их транзакции не закоммичены, и (с блокировкой)
    столько же запросов воркера ждут соединение блокировки — они в UPDATE, на середине чанка.
    Пока блокировка держится, закоммитить эти чанки нельзя: проверка не устареет до kill."""
    statuses = {
        r["no"]: r["status"]
        for r in await ctx.conn.fetch(
            "SELECT no, status FROM bulk_job_chunks WHERE job_id = $1 AND no = ANY($2)",
            job_id,
            numbers,
        )
    }
    finished = sorted(no for no in numbers if statuses.get(no) != "pending")
    if finished:
        out.miss(f"перед kill: чанки {finished} уже не pending ({statuses})")
        return
    if ctx.hold is None:
        out.event(f"перед kill: чанки {numbers} pending")
        return
    waiting = await ctx.conn.fetchval(
        "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
        ctx.hold.pid,
    )
    if waiting < len(numbers):
        out.miss(f"перед kill: блокировку ждут {waiting} запросов, чанков в работе {len(numbers)}")
        return
    out.event(f"перед kill: чанки {numbers} pending, блокировку ждут {waiting} запроса воркера")


async def finish(
    ctx: Ctx, out: Outcome, from_id: uuid.UUID, to_id: uuid.UUID, killed: dict[str, int]
) -> Outcome:
    """Дождаться финала job, проверить, в B — дождаться повторной выдачи исходных сообщений и
    проверить ещё раз; проверить логи чанков; собрать таймлайн. killed — task_id → номер чанка
    убитых сообщений."""
    args, conn = ctx.args, ctx.conn
    assert out.job_id is not None
    job_id = out.job_id
    deadline = time.monotonic() + args.timeout
    reaps, last_report = 0, 0.0
    while True:
        job = await job_row(conn, job_id)
        if job["reaps"] > reaps:
            reaps = job["reaps"]
            out.events.append(
                Event(job["updated_at"], "база", f"reaps = {reaps}: reaper переотправил чанки")
            )
        if job["status"] in FINAL:
            break
        if time.monotonic() - last_report > 30:
            last_report = time.monotonic()
            log(
                f"{out.scenario}: {job['status']}, processed {job['processed']}/{job['total']},"
                f" pending-чанков {job['pending']}, reaps {job['reaps']}"
            )
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job_id} не завершилась за {args.timeout} с")
        await asyncio.sleep(1)
    out.events.append(Event(job["started_at"], "база", "plan: job running, чанки в очереди"))
    out.events.append(Event(job["finished_at"], "база", f"job {job['status']}", "job_done"))
    for chunk in await conn.fetch(
        "SELECT no, rows, finished_at FROM bulk_job_chunks WHERE job_id = $1 ORDER BY no", job_id
    ):
        if chunk["finished_at"] is not None:
            text = f"чанк {chunk['no']} done, rows {chunk['rows']}"
            out.events.append(Event(chunk["finished_at"], "база", text))
    log(f"{out.scenario}: job {job['status']} — проверки")
    snapshot = await verify(conn, out, job_id, from_id, to_id, args.contacts)

    redelivered = out.scenario == "B" and bool(killed) and args.redelivery_wait > 0
    if redelivered:
        await wait_redelivery(out, killed, args.redelivery_wait)
        again = await verify(conn, Outcome("B"), job_id, from_id, to_id, args.contacts)
        out.check(
            "повторная выдача ничего не изменила",
            again == snapshot,
            "счётчики job и чанков, строки и version — те же"
            if again == snapshot
            else f"было {snapshot}, стало {again}",
        )
    left = [tid for tid in killed if tid in await broker_messages()]
    out.check(
        "в Redis нет сообщений убитых чанков",
        not left,
        "unacked и очередь bulk пусты по этим задачам" if not left else f"остались: {left}",
    )
    await api_check(ctx, out, job_id)
    # В B — уже после повторной выдачи: копии чанков ещё раз вызвали finalize.
    await check_notifications(ctx, out, job_id)
    await collect_logs(out, job_id, killed)
    chunk_rows = {no: rows for no, _, rows in snapshot[1]}
    check_chunk_logs(out, killed, chunk_rows, copies=redelivered)
    out.events.sort(key=lambda e: e.at)
    summarize(out)
    return out


async def verify(
    conn: asyncpg.Connection,
    out: Outcome,
    job_id: uuid.UUID,
    from_id: uuid.UUID,
    to_id: uuid.UUID,
    n: int,
) -> tuple[Any, ...]:
    """Проверки «done и не посчитано дважды» (см. docstring модуля). Возвращает снимок счётчиков
    — для сравнения после повторной выдачи сообщений."""
    job = await job_row(conn, job_id)
    out.check("job done", job["status"] == "done", f"status {job['status']}, error {job['error']}")
    out.check(
        "processed == total == создано",
        job["processed"] == job["total"] == n,
        f"processed {job['processed']}, total {job['total']}, создано {n}",
    )
    out.check(
        "без упавших чанков и строк",
        job["failed_chunks"] == 0 and job["failed_rows"] == 0,
        f"failed_chunks {job['failed_chunks']}, failed_rows {job['failed_rows']}",
    )
    chunks = await conn.fetch(
        """
        SELECT c.no, c.status, c.rows,
               (SELECT count(*) FROM contacts t
                WHERE t.id BETWEEN c.lo AND c.hi AND t.owner_id = $2) AS actual
        FROM bulk_job_chunks c WHERE c.job_id = $1 ORDER BY c.no
        """,
        job_id,
        to_id,
    )
    rows_sum = sum(c["rows"] or 0 for c in chunks)
    out.check(
        "все чанки done, их столько, сколько chunks_total",
        len(chunks) == job["chunks_total"] and all(c["status"] == "done" for c in chunks),
        f"{len(chunks)} чанков: {sorted({c['status'] for c in chunks})}",
    )
    out.check(
        "сумма rows чанков == processed",
        rows_sum == job["processed"],
        f"сумма rows {rows_sum}, processed {job['processed']}",
    )
    mismatched = [c["no"] for c in chunks if c["rows"] != c["actual"]]
    out.check(
        "rows каждого чанка == реальные строки диапазона у нового",
        not mismatched,
        "совпадают у всех" if not mismatched else f"не совпадают: чанки {mismatched}",
    )
    owners = await conn.fetchrow(
        "SELECT count(*) FILTER (WHERE owner_id = $1) AS old,"
        " count(*) FILTER (WHERE owner_id = $2) AS new FROM contacts WHERE owner_id IN ($1, $2)",
        from_id,
        to_id,
    )
    out.check(
        "у старого 0, у нового все",
        owners["old"] == 0 and owners["new"] == n,
        f"старый {owners['old']}, новый {owners['new']}",
    )
    versions = {
        r["version"]: r["n"]
        for r in await conn.fetch(
            "SELECT version, count(*) AS n FROM contacts WHERE owner_id IN ($1, $2)"
            " GROUP BY version ORDER BY version",
            from_id,
            to_id,
        )
    }
    out.check(
        "каждая строка изменена ровно один раз (version 1 → 2)",
        versions == {2: n},
        f"version → строк: {versions}",
    )
    result = json.loads(job["result"]) if isinstance(job["result"], str) else job["result"]
    audit = await conn.fetch(
        "SELECT changes FROM audit_log WHERE entity_type = 'bulk_job' AND entity_id = $1",
        str(job_id),
    )
    audit_rows = [json.loads(a["changes"])["rows"] for a in audit]
    out.check(
        "итог записан один раз: result и одна строка журнала",
        (result or {}).get("rows") == {"contacts": n} and audit_rows == [{"contacts": n}],
        f"result {result}, строк журнала {len(audit)}: {audit_rows}",
    )
    failed = await conn.fetchval(
        "SELECT count(*) FROM failed_tasks WHERE args->>0 = $1", str(job_id)
    )
    out.check("нет строк failed_tasks", failed == 0, f"строк {failed}")
    return (
        tuple(job[k] for k in ("status", "total", "processed", "failed_chunks", "reaps")),
        tuple((c["no"], c["status"], c["rows"]) for c in chunks),
        owners["old"],
        owners["new"],
        tuple(sorted(versions.items())),
        len(audit),
    )


async def wait_redelivery(out: Outcome, killed: dict[str, int], wait: int) -> None:
    """B: исходные сообщения убитых чанков лежат в hash unacked. Через visibility_timeout
    kombu (restore_visible раз в ~100 с) вернёт их в очередь bulk, воркер выполнит копию —
    claim не найдёт pending-строку — и подтвердит. Ждём, пока их нет ни там, ни там."""
    kill = next(e.at for e in out.events if e.tag == "kill")
    deadline = kill + timedelta(seconds=wait)
    log(f"B: жду повторной выдачи {len(killed)} исходных сообщений (до {deadline:%H:%M:%S} UTC)")
    last = 0.0
    while True:
        messages = await broker_messages()
        left = [tid for tid in killed if tid in messages]
        if not left:
            # Время — момента, когда это заметил опрос (раз в 5 с); сами копии — по логам.
            out.event("опрос: исходных сообщений в Redis больше нет")
            return
        if now() > deadline:
            out.event(f"не дождался повторной выдачи за {wait} с: в Redis {len(left)}")
            return
        if time.monotonic() - last > 60:
            last = time.monotonic()
            log(f"B: в Redis ещё {len(left)} исходных сообщений")
        await asyncio.sleep(5)


async def api_check(ctx: Ctx, out: Outcome, job_id: uuid.UUID) -> None:
    """Тот же итог через GET /bulk-jobs/{id} — как его видит пользователь."""
    assert out.author is not None
    headers = await login(ctx.api, out.author.email)
    response = await ctx.api.get(f"/api/v1/bulk-jobs/{job_id}", headers=headers)
    body = response.json()
    ok = response.status_code == 200 and body["status"] == "done"
    ok = ok and body["processed"] == body["total"] == ctx.args.contacts
    out.check(
        "GET /bulk-jobs/{id}",
        ok,
        f"{response.status_code}: {body.get('status')},"
        f" {body.get('processed')}/{body.get('total')}",
    )


async def link_telegram(ctx: Ctx, out: Outcome, chats: tuple[int, int]) -> None:
    """Привязать автора и получателя сценария к чатам --telegram-chats. chat_id уникален, поэтому
    привязки людей прошлого сценария к этим чатам снимаются. Чат, привязанный к человеку не из
    организации скрипта (например, к своему аккаунту на стенде), не трогаем — ошибка."""
    assert out.author is not None and out.recipient is not None
    taken = await ctx.conn.fetchval(
        "SELECT count(*) FROM telegram_links WHERE chat_id = ANY($1::bigint[])"
        " AND user_id <> ALL($2::uuid[])",
        list(chats),
        ctx.org.users,
    )
    if taken:
        raise RuntimeError(
            "--telegram-chats: чат уже привязан к пользователю стенда — отвяжите его (/stop)"
        )
    async with ctx.conn.transaction():
        await ctx.conn.execute("DELETE FROM telegram_links WHERE chat_id = ANY($1)", list(chats))
        for person, chat in zip((out.author, out.recipient), chats, strict=True):
            await ctx.conn.execute(
                "INSERT INTO telegram_links (user_id, chat_id) VALUES ($1, $2)", person.id, chat
            )
    out.event(f"Telegram: автор → чат {chats[0]}, получатель → чат {chats[1]}")


async def check_notifications(ctx: Ctx, out: Outcome, job_id: uuid.UUID) -> None:
    """Уведомления bulk_job.completed (шаг 5.15, см. docstring модуля): ровно одно письмо автору
    (итог) и одно получателю («вам передано N клієнтів») в Mailpit стенда, одно событие этой job,
    по строке sent_notifications на письмо; Telegram — если проверяется (ctx.telegram_chats)."""
    assert out.author is not None and out.recipient is not None
    n = ctx.args.contacts
    expected = {
        "автору": (out.author, SUMMARY_SUBJECT, f"Оброблено записів: {n} з {n}."),
        "получателю": (
            out.recipient,
            TRANSFER_SUBJECT,
            f"Вам передано клієнтів: {n} (контакти — {n}).",
        ),
    }

    async def arrived() -> bool:
        for person, _, _ in expected.values():
            if not await mailbox(ctx.mail, person.email):
                return False
        return True

    async def published() -> bool:
        left = await ctx.conn.fetchval(
            "SELECT count(*) FROM outbox_events WHERE org_id = $1 AND published_at IS NULL",
            ctx.org.id,
        )
        return not left

    try:
        await wait_for(arrived, NOTIFY_WAIT_SECONDS, "письма автору и получателю", step=1)
        await wait_for(published, NOTIFY_WAIT_SECONDS, "публикация outbox организации", step=1)
    except TimeoutError as error:
        log(f"{out.scenario}: {error}")  # проверки ниже покажут, чего не хватает
    # Команды notifier опубликованы; пауза — лишнее письмо, если бы оно было, успело бы прийти.
    await asyncio.sleep(NOTIFY_SETTLE_SECONDS)

    for whom, (person, subject, line) in expected.items():
        letters = await mailbox(ctx.mail, person.email)
        for letter in letters:
            at = datetime.fromisoformat(letter["Created"])
            out.events.append(
                Event(at, "mailpit", f"письмо {whom}: «{letter['Subject']}»", "email")
            )
        ok = len(letters) == 1 and letters[0]["Subject"] == subject
        ok = ok and line in await letter_text(ctx.mail, letters[0]["ID"])
        subjects = ", ".join(f"«{letter['Subject']}»" for letter in letters) or "—"
        out.check(
            f"ровно одно письмо {whom}",
            ok,
            f"{person.email}: писем {len(letters)} ({subjects})"
            + ("" if ok else f", ждали одно «{subject}» со строкой «{line}»"),
        )

    prefix = f"bulk_job.completed:job:{job_id}:"
    events = await ctx.conn.fetchval(
        "SELECT count(*) FROM outbox_events"
        " WHERE event_type = 'bulk_job.completed' AND payload->>'job_id' = $1",
        str(job_id),
    )
    commands = await ctx.conn.fetchval(
        "SELECT count(*) FROM outbox_events WHERE event_type IN ('email.send', 'telegram.send')"
        " AND starts_with(payload->>'dedup_key', $1)",
        prefix,
    )
    sent = {
        r["dedup_key"].removeprefix(prefix)
        for r in await ctx.conn.fetch(
            "SELECT dedup_key FROM sent_notifications WHERE starts_with(dedup_key, $1)", prefix
        )
    }
    people = (out.author, out.recipient)
    emails = {f"{p.id}:email" for p in people}
    out.check(
        "одно событие bulk_job.completed, по ключу дедупа на письмо",
        events == 1 and {key for key in sent if key.endswith(":email")} == emails,
        f"событий {events}, команд notifier {commands}, sent_notifications {len(sent)}",
    )
    if ctx.telegram_chats is None:
        return
    telegrams = {f"{p.id}:telegram" for p in people}
    links = await ctx.conn.fetchval(
        "SELECT count(*) FROM telegram_links WHERE user_id = ANY($1)", [p.id for p in people]
    )
    failed = await ctx.conn.fetchval(
        "SELECT count(*) FROM failed_tasks WHERE task_name = 'notifications.send_telegram'"
        " AND failed_at >= $1",
        out.created,
    )
    sent_telegram = {key for key in sent if key.endswith(":telegram")}
    out.check(
        "Telegram: по сообщению автору и получателю",
        sent_telegram == telegrams and links == 2 and failed == 0,
        f"sent_notifications telegram {len(sent_telegram)}, привязок {links},"
        f" failed_tasks send_telegram {failed}",
    )


def parse_line(line: str) -> tuple[str, datetime, str] | None:
    """«worker-bulk-1  | 2026-10-26T21:17:26.965643896Z текст» → (сервис, время, текст)."""
    service, sep, rest = ANSI.sub("", line).partition(" | ")
    stamp, _, text = rest.partition(" ")
    if not sep or not stamp.endswith("Z"):
        return None
    head, _, frac = stamp[:-1].partition(".")
    at = datetime.fromisoformat(f"{head}.{(frac + '000000')[:6]}+00:00")
    return service.strip().rsplit("-", 1)[0], at, text.strip()


def logs_since(out: Outcome) -> str:
    assert out.created is not None
    return (out.created - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")


async def kill_logs(out: Outcome) -> str:
    """B: лог убитого worker-bulk, пока контейнер ещё есть. Если конфигурация compose поменялась
    после его создания (новая переменная окружения), up -d создаст контейнер заново, и docker
    compose logs покажет только новый — строки чанков до kill пропали бы (так и было в первом
    прогоне 27.10.2026: «нет bulk_chunk_done у чанков 0–3»)."""
    return await compose(
        "logs", "--no-color", "--timestamps", "--since", logs_since(out), "worker-bulk"
    )


async def collect_logs(out: Outcome, job_id: uuid.UUID, killed: dict[str, int]) -> None:
    """События из логов: выдача сообщений убитых чанков (Task bulk.chunk[id] received), строки
    этой job (bulk_chunk_done, bulk_chunk_already_finished, bulk_job_reaped…), гибель процесса
    пула и откат транзакции убитого процесса в Postgres. Строки о чанках — ещё и в
    out.chunk_logs, для check_chunk_logs."""
    text = await compose(
        "logs", "--no-color", "--timestamps", "--since", logs_since(out),
        "worker-bulk", "worker", "postgres",
    )  # fmt: skip
    # Лог убитого контейнера (B) плюс текущие; тот же контейнер даёт те же строки — без повторов.
    lines = list(dict.fromkeys([*out.killed_logs.splitlines(), *text.splitlines()]))
    kill = next((e.at for e in out.events if e.tag == "kill"), None)
    seen: dict[str, int] = {}
    for line in lines:
        parsed = parse_line(line)
        if parsed is None:
            continue
        service, at, body = parsed
        fields = dict(re.findall(r"(\w+)=(\S+)", body))
        if service == "postgres":
            if "unexpected EOF" in body or "could not receive data" in body:
                out.events.append(Event(at, service, body[:110], "pg_abort"))
            continue
        received = re.search(r"Task bulk\.chunk\[([0-9a-f-]+)\] received", body)
        if received and received.group(1) in killed:
            tid = received.group(1)
            seen[tid] = seen.get(tid, 0) + 1
            again = seen[tid] > 1 or (kill is not None and at > kill)
            text_ = f"чанк {killed[tid]}: сообщение {short(tid)} выдано" + (
                " повторно" if again else ""
            )
            out.events.append(Event(at, service, text_, "redelivered" if again else ""))
            continue
        if "exited with" in body or "Task handler raised error" in body:  # гибель процесса пула
            out.events.append(Event(at, service, body.split(" ts=")[0][:110]))
            continue
        if fields.get("job_id") != str(job_id):
            continue
        name = body.split("]", 1)[-1].split()[0] if "]" in body else body.split()[0]
        tid = fields.get("task_id", "")
        after = kill is not None and at > kill
        if name in ("bulk_chunk_done", "bulk_chunk_already_finished"):
            no = int(fields["no"])
            rows = int(fields["rows"]) if "rows" in fields else None
            out.chunk_logs.append(ChunkLog(at, name, tid, no, rows))
        if name == "bulk_chunk_done":
            rerun = after and no in killed.values()
            text_ = f"bulk_chunk_done чанк {no}, rows {rows}, задача {short(tid)}"
            out.events.append(Event(at, service, text_, "rerun_done" if rerun else ""))
        elif name == "bulk_chunk_already_finished":
            copy = after and tid in killed
            text_ = f"bulk_chunk_already_finished чанк {no}, задача {short(tid)}"
            out.events.append(Event(at, service, text_, "copy" if copy else ""))
        elif name == "bulk_job_reaped":
            text_ = f"bulk_job_reaped reaps={fields.get('reaps')} chunks={fields.get('chunks')}"
            out.events.append(Event(at, service, text_, "reaped"))
        elif name in ("bulk_planned", "bulk_job_finished"):
            out.events.append(Event(at, service, body.split(" ts=")[0][:110]))


def check_chunk_logs(
    out: Outcome, killed: dict[str, int], chunk_rows: dict[int, int | None], *, copies: bool
) -> None:
    """Проверки по логам — то, чего не видно в счётчиках (см. docstring модуля).

    Повторный UPDATE того же чанка изменил бы 0 строк (условие owner_id = :from): processed,
    rows и version остались бы прежними, а второй bulk_chunk_done (rows 0) виден только в логе.
    bulk_chunk_done пишется до коммита чанка: строка убитой задачи до kill — транзакция, которую
    откатил Postgres, она не считается."""
    kill = next((e.at for e in out.events if e.tag == "kill"), None)

    def before_kill(entry: ChunkLog) -> bool:
        return kill is not None and entry.at < kill

    done = [
        e
        for e in out.chunk_logs
        if e.name == "bulk_chunk_done" and not (e.task_id in killed and before_kill(e))
    ]
    per_chunk = Counter(e.no for e in done)
    twice = sorted(no for no, n in per_chunk.items() if n > 1)
    absent = sorted(set(chunk_rows) - set(per_chunk))
    wrong = sorted({e.no for e in done if e.rows != chunk_rows.get(e.no)})
    out.check(
        "в логах ровно один bulk_chunk_done на чанк, rows как в базе",
        not (twice or absent or wrong),
        f"{len(done)} строк на {len(chunk_rows)} чанков"
        if not (twice or absent or wrong)
        else f"дважды: {twice}, нет: {absent}, rows не те: {wrong}",
    )
    if not killed:
        return
    late = sorted(e.no for e in done if e.no in killed.values() and before_kill(e))
    out.check(
        "убитые чанки выполнены после kill",
        not late,
        f"чанки {sorted(killed.values())}" if not late else f"выполнены до kill: {late}",
    )
    if out.scenario == "A":
        ((tid, no),) = killed.items()
        rerun = [e for e in done if e.task_id == tid and not before_kill(e)]
        already = [e for e in out.chunk_logs if e.name != "bulk_chunk_done" and e.task_id == tid]
        out.check(
            "убитый чанк выполнен заново тем же сообщением",
            bool(rerun) and not already,
            f"чанк {no}, задача {short(tid)}: bulk_chunk_done после kill {len(rerun)},"
            f" bulk_chunk_already_finished {len(already)}",
        )
        if already:  # kill попал между коммитом чанка и подтверждением сообщения
            out.miss(f"чанк {no} был уже закоммичен, когда убили процесс")
    elif copies:
        bad = [
            short(tid)
            for tid in killed
            if not any(e.name != "bulk_chunk_done" and e.task_id == tid for e in out.chunk_logs)
            or any(e.name == "bulk_chunk_done" and e.task_id == tid for e in done)
        ]
        out.check(
            "копии исходных сообщений: bulk_chunk_already_finished, чанк не выполняли",
            not bad,
            f"у всех {len(killed)} копий" if not bad else f"не так у задач: {bad}",
        )


def summarize(out: Outcome) -> None:
    """Сводка по времени: от kill до повторной выдачи, повторного выполнения, финала job."""

    def first(tag: str) -> datetime | None:
        return next((e.at for e in out.events if e.tag == tag), None)

    def last(tag: str) -> datetime | None:
        return next((e.at for e in reversed(out.events) if e.tag == tag), None)

    post, kill, done = first("post"), first("kill"), first("job_done")
    rows: list[tuple[str, datetime | None, datetime | None]] = [
        ("POST → job done (вся job)", post, done),
        ("kill → job done (восстановление)", kill, done),
        ("job done → письма автору и получателю", done, last("email")),
    ]
    if out.scenario == "A":
        rows += [
            ("kill → сообщение выдано повторно", kill, first("redelivered")),
            ("kill → убитый чанк выполнен заново", kill, last("rerun_done")),
        ]
    else:
        rows += [
            ("kill → воркер снова healthy", kill, first("worker_up")),
            ("kill → reaper переотправил чанки", kill, first("reaped")),
            ("kill → переотправленные чанки выполнены", kill, last("rerun_done")),
            ("kill → Redis выдал исходные сообщения", kill, first("redelivered")),
            ("kill → копии: bulk_chunk_already_finished", kill, last("copy")),
        ]
    for name, start, end in rows:
        value = f"{(end - start).total_seconds():.1f} с" if start and end else "—"
        out.summary.append((name, value))


def print_outcome(out: Outcome) -> None:
    print(f"\n=== Сценарий {out.scenario}: job {out.job_id}")
    base = out.created or (out.events[0].at if out.events else now())
    print("\nТаймлайн (время UTC, t — от создания job):")
    for e in out.events:
        t = (e.at - base).total_seconds()
        print(f"  {e.at:%H:%M:%S.%f}"[:-3] + f"  t={t:8.1f} с  [{e.source:<11}] {e.text}")
    print("\nСводка:")
    for name, value in out.summary:
        print(f"  {name:<48} {value}")
    print("\nПроверки:")
    for c in out.checks:
        print(f"  [{'OK' if c.ok else 'FAIL'}] {c.name}: {c.detail}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--scenario", choices=["a", "b", "all"], default="all")
    parser.add_argument("--contacts", type=int, default=100_000, help="контактов у менеджера")
    parser.add_argument(
        "--kill-at", type=float, default=0.3, help="доля processed, после которой kill (0.3)"
    )
    parser.add_argument(
        "--hold",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="держать чанки после kill-at на блокировке строки, чтобы kill попал в середину",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=10_000, help="BULK_CHUNK_SIZE стенда (compose)"
    )
    parser.add_argument(
        "--down-seconds", type=float, default=5, help="B: пауза между kill и up -d worker-bulk"
    )
    parser.add_argument(
        "--redelivery-wait",
        type=int,
        default=1500,
        help="B: сколько секунд от kill ждать повторной выдачи исходных сообщений (0 — не ждать)",
    )
    parser.add_argument("--timeout", type=int, default=1800, help="ждать финала job, с")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument(
        "--mailpit", default="http://localhost:8025/api/v1", help="HTTP API Mailpit стенда"
    )
    parser.add_argument(
        "--telegram-chats",
        help="два chat_id через запятую — автору и получателю (бот стенда должен мочь писать"
        " в оба чата): без них Telegram не проверяется",
    )
    return parser.parse_args()


async def telegram_plan(args: argparse.Namespace) -> tuple[tuple[int, int] | None, str]:
    """Проверять ли Telegram: (чаты автора и получателя, "") или (None, почему нет)."""
    if not await stack_has_telegram_token():
        return None, (
            "у стенда не задан TELEGRAM_BOT_TOKEN (deploy/compose/.env): send_telegram пропускает"
            " сообщения (telegram_skipped), проверять нечего"
        )
    if not args.telegram_chats:
        return None, (
            "токен задан, но нет --telegram-chats: Bot API настоящий, а придуманному chat_id"
            " он ответит «chat not found» — нужны два чата, куда бот может писать"
        )
    chats = [int(chat) for chat in args.telegram_chats.split(",")]
    if len(chats) != 2 or chats[0] == chats[1]:
        sys.exit("--telegram-chats: нужны два разных chat_id через запятую")
    return (chats[0], chats[1]), ""


async def main() -> int:
    args = parse_args()
    current = asyncio.current_task()
    assert current is not None
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, current.cancel)

    missing = {
        "api",
        "worker",
        "worker-bulk",
        "beat",
        "redis",
        "postgres",
        "outbox-relay",  # без него bulk_job.completed не дойдёт до notify — писем не будет
        "mailpit",
    } - await running_services()
    if missing:
        sys.exit(f"стенд не запущен ({', '.join(sorted(missing))}): сначала make up")
    queued = int((await redis_text("LLEN", "bulk")).strip() or 0)
    if queued:
        log(
            f"в очереди bulk уже {queued} сообщений:"
            " reaper отложит переотправку, пока она не опустеет"
        )

    dsn = get_settings().database_url.replace("postgresql+asyncpg", "postgresql")
    conn = await asyncpg.connect(dsn, server_settings={"application_name": APP_NAME})
    api = httpx.AsyncClient(base_url=args.api, timeout=30)
    mail = httpx.AsyncClient(base_url=args.mailpit, timeout=30)
    chats, telegram_skip = await telegram_plan(args)
    log(f"Telegram: чаты {chats}" if chats else f"Telegram не проверяется: {telegram_skip}")
    run_id = uuid.uuid4().hex[:8]
    org = await create_org(conn, run_id)
    log(f"организация {org.slug}, вход {org.admin_email} / {PASSWORD}")
    ctx = Ctx(
        args=args,
        dsn=dsn,
        conn=conn,
        api=api,
        mail=mail,
        org=org,
        telegram_chats=chats,
        telegram_skip=telegram_skip,
    )
    outcomes: list[Outcome] = []
    started = time.perf_counter()
    try:
        for scenario in ("A", "B") if args.scenario == "all" else (args.scenario.upper(),):
            outcomes.append(await run_scenario(ctx, scenario))
    finally:
        # Сначала то, без чего стенд остался бы сломанным: блокировка и убитый worker-bulk.
        if ctx.hold is not None:
            await ctx.hold.release()
        if "worker-bulk" not in await running_services():
            log("worker-bulk не запущен — поднимаю")
            await compose("up", "-d", "--wait", "worker-bulk")
        await cleanup(conn, mail, org)
        await conn.close()
        await api.aclose()
        await mail.aclose()
        for out in outcomes:
            print_outcome(out)
        if ctx.telegram_skip:
            print(f"\nTelegram не проверялся: {ctx.telegram_skip}")
        print(f"\nВсего: {(time.perf_counter() - started) / 60:.1f} мин")
    failed = [c for out in outcomes for c in out.checks if not c.ok]
    missed = [out.scenario for out in outcomes if out.missed]
    if missed:
        print(f"Промах (kill не попал в чанк): {', '.join(missed)}")
    return 1 if failed or missed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
