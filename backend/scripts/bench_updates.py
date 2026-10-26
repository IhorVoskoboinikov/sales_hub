"""Замеры способов массового UPDATE (этап 5.9, раздел 6 docs/DESIGN.md).

Запуск: make bench-updates (после make seed-big). Рабочая таблица contacts не меняется — скрипт её
только читает. Перед каждым замером он заново копирует её в bench_contacts (LIKE contacts
INCLUDING ALL — те же колонки, CHECK и все девять индексов, включая два GIN): каждый замер
начинается с одного и того же физического состояния — только что загруженная копия, страницы
заполнены плотно, после VACUUM (ANALYZE), её страницы в кэше (shared_buffers и page cache ОС).
Без этого каждый замер переписывал и раздувал бы копию для следующего, и цифры зависели бы от
порядка способов. Цена — ~1,5 мин на копию. Индексы строятся после загрузки (так быстрее), FK
owner_id → users добавляется (LIKE его не копирует, а при смене ответственного Postgres проверяет
его на каждой строке), автоочистка копии выключена. В конце копия удаляется — и при ошибке, и по
Ctrl+C.

Каждый замер меняет то же, что передача клиентов (bulk.chunk reassign_owner): owner_id, team_id,
version + 1, updated_at — и, как она, только живые строки (deleted_at IS NULL). Новое значение —
каждый раз другой пользователь: одинаковое значение Postgres тоже переписал бы, но без смены ключей
индексов, и это был бы другой замер.

Способы:

- U1 — ORM в цикле, только на --orm-rows строках (по умолчанию первые 10 000), время на 1 и 5 млн —
  экстраполяция. U1a: объекты одним SELECT, правка в цикле, один commit (SQLAlchemy шлёт UPDATE
  на каждую строку: version_id_col и eager_defaults требуют RETURNING и проверки rowcount).
  U1b: строка за строкой get → правка → commit, как вызов сервиса одной записи в цикле. Если
  пробник поменял строку между SELECT и UPDATE, U1a падает StaleDataError с откатом всей пачки —
  тогда она замеряется ещё раз без пробника. Для сравнения на тех же строках — U3 одним чанком:
  база для «во сколько раз ORM медленнее» при том же объёме и тех же полных образах страниц.
- U2 — один UPDATE … WHERE id BETWEEN на весь объём, одна транзакция.
- U3 — чанки id BETWEEN по --sizes id (1 000 / 10 000 / 50 000), commit после каждого — тот же
  BulkRepository.reassign, что выполняет воркер.
- U4 — UPDATE … FROM unnest(:ids, :owners, :teams) пачками по --batch строк: своё значение на
  строку (чётные id — одному пользователю, нечётные — другому).
- U5 — COPY во временную staging-таблицу и один UPDATE … FROM staging. Если планировщик выбрал
  Hash Join, U5 замеряется ещё раз с enable_hashjoin = off (Merge Join, строки в порядке id) —
  проверка, не от порядка ли строк растёт WAL.
- U6 — SQLAlchemy update(Model) со списком dict-ов (bulk UPDATE по первичному ключу, executemany)
  пачками по --batch строк; модель без version_id_col (почему — BenchContactPlain), поэтому
  version не меняется: в этом режиме пишутся только готовые значения.
- U7 — границы чанков у менеджера: по строкам (BulkRepository.chunk_bounds, как bulk.plan) против
  равных диапазонов id — шагом --chunk id и тем же числом чанков, что по строкам. Два менеджера:
  самый крупный sales_rep seed-данных (его записи лежат в диапазоне id своей организации) и
  синтетический — каждый --spread-every-й контакт таблицы (записи разбросаны по всем id); его
  копия строится сразу с этим ответственным, без UPDATE, — строки остаются на своих страницах.
- HOT — на последнем 1 млн id, чанками по --chunk: SET source (колонка без индекса — HOT, если
  на странице есть место) против SET owner_id, team_id (ключи индексов — не HOT); на копии с
  fillfactor 100 (как contacts после загрузки) и на копии, загруженной с fillfactor 90, — между
  ними разница только в fillfactor.

Объёмы (--volumes): 1m — первые 1 млн id (id <= p1M), 5m — вся таблица (5,47 млн); --repeat
повторяет U2–U6 несколько раз, каждый раз сдвигая порядок способов, — для оценки разброса.

Метрики каждого замера: время, строк в секунду, WAL (pg_wal_lsn_diff до и после; перед замером —
CHECKPOINT), полные образы страниц (wal_fpi из pg_stat_wal) и число чекпоинтов за замер
(pg_stat_checkpointer), пик размера pg_wal (pg_ls_waldir раз в секунду), n_tup_hot_upd и
n_dead_tup из pg_stat_user_tables, самый долгий чанк (верхняя граница ожидания ручной правки) и
«пробник» — параллельная сессия, которая каждые --probe-ms мс правит одну случайную живую строку
из тех, что массовая операция меняет прямо сейчас (текущего чанка или пачки; у U1, U2 и U5 — всего
сегмента): SELECT … FOR NO KEY UPDATE NOWAIT, и если строка занята — обычный UPDATE, который ждёт.
Так видно, сколько замеров действительно ждали блокировку и сколько: ожидание p50 / p99 / max —
только по ним, задержка остальных (сброс WAL при COMMIT, диск) — отдельно. После замера —
VACUUM (ANALYZE) и размер копии; их тоже печатает таблица. Результаты — в docs/experiments.md.
"""

import argparse
import asyncio
import contextlib
import json
import math
import random
import signal
import statistics
import sys
import time
import uuid
from array import array
from bisect import bisect_left, bisect_right
from collections.abc import Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any

import asyncpg
from sqlalchemy import (
    ClauseElement,
    ColumnElement,
    MetaData,
    Table,
    and_,
    event,
    func,
    select,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.sql.compiler import SQLCompiler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # чтобы импортировать app

from app.core.config import get_settings
from app.models import Contact
from app.repositories.bulk import BulkRepository

BENCH = "bench_contacts"
STAGING = "bench_staging"
APP_NAME = "bench_updates"  # application_name соединений замера: по нему их добивает очистка
ALL_METHODS = ("U7", "HOT", "U1", "U2", "U3", "U4", "U5", "U6")
MILLION = 1_000_000
MB = 2**20

# Та же таблица, что Contact, но под именем bench_contacts — с теми же version_id_col и
# eager_defaults, чтобы ORM в U1 и U6 вёл себя как с настоящими контактами. В её MetaData —
# копии всех таблиц приложения: unit of work сортирует таблицы по FK и ищет organizations, users…
bench_metadata = MetaData()
for _table in Contact.metadata.sorted_tables:
    _table.to_metadata(bench_metadata)
bench_table: Table = Contact.__table__.to_metadata(bench_metadata, name=BENCH)  # type: ignore[attr-defined]


class BenchBase(DeclarativeBase):
    pass


class BenchContact(BenchBase):
    __table__ = bench_table
    # Колонки берутся из bench_table; аннотации — только для типов в коде замеров.
    id: Mapped[int]
    owner_id: Mapped[uuid.UUID]
    team_id: Mapped[uuid.UUID]
    deleted_at: Mapped[datetime | None]
    __mapper_args__ = {"version_id_col": bench_table.c.version, "eager_defaults": True}  # noqa: RUF012


class PlainBase(DeclarativeBase):
    pass


class BenchContactPlain(PlainBase):
    """Та же копия без version_id_col — для U6. Bulk UPDATE по первичному ключу с version_id_col
    берёт version из dict как условие WHERE version = :v и при любой чужой правке строки пачки
    (здесь — пробника) падает StaleDataError с откатом всей пачки; а version + 1 выражением в
    этом режиме не записать — только готовые значения."""

    __table__ = bench_table
    id: Mapped[int]


@dataclass(frozen=True)
class CopySpec:
    """Какой строить копию: fillfactor таблицы и «разбросанный» менеджер (owner, team, every) —
    ответственный каждой every-й строки прямо при загрузке."""

    fillfactor: int = 100
    spread: tuple[uuid.UUID, uuid.UUID, int] | None = None


@dataclass
class Sample:
    ms: float
    waited: bool  # строка была занята массовой операцией (NOWAIT не взял блокировку)


@dataclass
class Run:
    method: str
    variant: str
    volume: str
    rows: int
    seconds: float
    wal: int
    fpi: int
    checkpoints: int
    wal_peak: int
    hot: int
    dead: int
    probe: list[Sample]
    longest: float
    vacuum: float
    size: int
    details: str = ""


@dataclass
class Stats:
    lsn: str
    fpi: int
    checkpoints: int
    hot: int
    dead: int


class Focus:
    """Строки, которые массовая операция меняет прямо сейчас: пробник правит случайную из них.
    ids — отсортированные id сегмента; aim(lo, hi) — начался чанк [lo, hi]. Заодно считает
    самый долгий чанк (от aim до следующего aim или конца работы, вместе с COMMIT)."""

    def __init__(self, ids: Sequence[int]) -> None:
        self.ids = ids
        self.lo, self.hi = 0, len(ids)  # до первого чанка — весь сегмент
        self.started: float | None = None
        self.longest = 0.0

    def aim(self, lo: int, hi: int) -> None:
        self.finish()
        self.started = time.perf_counter()
        self.lo, self.hi = bisect_left(self.ids, lo), bisect_right(self.ids, hi)

    def finish(self) -> None:
        if self.started is not None:
            self.longest = max(self.longest, time.perf_counter() - self.started)
            self.started = None

    def pick(self) -> int | None:
        return self.ids[random.randrange(self.lo, self.hi)] if self.lo < self.hi else None


@dataclass
class Ctx:
    dsn: str
    admin: asyncpg.Connection
    bulk: asyncpg.Connection
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    args: argparse.Namespace
    columns: list[str] = field(default_factory=list)
    min_id: int = 0
    max_id: int = 0
    targets: list[tuple[uuid.UUID, uuid.UUID]] = field(default_factory=list)
    spare: tuple[uuid.UUID, uuid.UUID] | None = None
    focus: Focus = field(default_factory=lambda: Focus([]))
    turn: int = 0
    builds: dict[int, str] = field(default_factory=dict)
    runs: list[Run] = field(default_factory=list)

    def target(self) -> tuple[uuid.UUID, uuid.UUID]:
        """Следующий (owner_id, team_id): у каждого замера — новый, ключи индексов меняются."""
        self.turn += 1
        return self.targets[self.turn % len(self.targets)]


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def rowcount(status: str) -> int:
    """'UPDATE 10000' → 10000."""
    return int(status.rsplit(" ", 1)[1])


def percentile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


async def connect(dsn: str, name: str = APP_NAME) -> asyncpg.Connection:
    return await asyncpg.connect(dsn, server_settings={"application_name": name})


def walk(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from walk(child)


async def plan(conn: asyncpg.Connection, sql: str, *args: Any) -> str:
    """Узлы плана (EXPLAIN без ANALYZE) в одну строку: «Update → Nested Loop → …»."""
    raw = await conn.fetchval(f"EXPLAIN (FORMAT JSON) {sql}", *args)
    nodes = walk(json.loads(raw)[0]["Plan"])
    return " → ".join(
        n["Node Type"] + (f" {n['Index Name']}" if "Index Name" in n else "") for n in nodes
    )


async def plan_query(ctx: Ctx, statement: ClauseElement) -> str:
    """План запроса SQLAlchemy: компилируем диалектом движка (asyncpg: $1, $2…) и передаём
    значения."""
    compiled = statement.compile(
        dialect=ctx.engine.dialect, compile_kwargs={"render_postcompile": True}
    )
    assert isinstance(compiled, SQLCompiler)
    values = [compiled.params[name] for name in compiled.positiontup or []]
    conn = ctx.bulk
    return await plan(conn, str(compiled), *values)


async def flush_stats(ctx: Ctx) -> None:
    """Счётчики pg_stat_* backend отдаёт не сразу (раз в секунду и реже) — просим сбросить их в
    конце транзакции на всех соединениях, которые писали в копию."""
    await ctx.bulk.execute("SELECT pg_stat_force_next_flush()")
    async with ctx.engine.connect() as conn:
        await conn.exec_driver_sql("SELECT pg_stat_force_next_flush()")


async def table_stats(admin: asyncpg.Connection) -> Stats:
    await admin.execute("SELECT pg_stat_clear_snapshot()")
    row = await admin.fetchrow(
        "SELECT pg_current_wal_lsn()::text AS lsn, (SELECT wal_fpi FROM pg_stat_wal) AS fpi,"
        " (SELECT num_done FROM pg_stat_checkpointer) AS checkpoints, n_tup_hot_upd, n_dead_tup"
        " FROM pg_stat_user_tables WHERE relname = $1",
        BENCH,
    )
    assert row is not None
    return Stats(
        row["lsn"], row["fpi"], row["checkpoints"], row["n_tup_hot_upd"], row["n_dead_tup"]
    )


async def vacuum(ctx: Ctx) -> float:
    started = time.perf_counter()
    await ctx.admin.execute(f"VACUUM (ANALYZE) {BENCH}")
    return time.perf_counter() - started


class Probe:
    """Ручная правка карточки во время массовой операции: каждые interval секунд — одна строка из
    тех, что меняются прямо сейчас (Focus), в своей транзакции. Сначала SELECT … FOR NO KEY UPDATE
    NOWAIT: если строка занята массовой операцией — замер «ждал», и обычный UPDATE ждёт её COMMIT;
    если свободна — UPDATE сразу. Задержка — всё время правки."""

    def __init__(self, dsn: str, focus: Focus, interval: float) -> None:
        self.dsn = dsn
        self.focus = focus
        self.interval = interval
        self.samples: list[Sample] = []
        self.ready = asyncio.Event()
        self.stop = asyncio.Event()

    async def run(self) -> None:
        conn = await connect(self.dsn)
        try:
            lock = await conn.prepare(
                f"SELECT 1 FROM {BENCH} WHERE id = $1 FOR NO KEY UPDATE NOWAIT"
            )
            edit = await conn.prepare(
                f"UPDATE {BENCH} SET version = version + 1, updated_at = now() WHERE id = $1"
            )
            self.ready.set()
            loop = asyncio.get_running_loop()
            next_at = loop.time()
            while not self.stop.is_set():
                row_id = self.focus.pick()
                if row_id is not None:
                    started = time.perf_counter()
                    waited = False
                    transaction = conn.transaction()
                    await transaction.start()
                    try:
                        await lock.fetch(row_id)
                    except asyncpg.LockNotAvailableError:
                        await transaction.rollback()
                        waited = True
                        await edit.fetch(row_id)  # вне транзакции: ждёт и сразу COMMIT
                    else:
                        await edit.fetch(row_id)
                        await transaction.commit()
                    self.samples.append(Sample((time.perf_counter() - started) * 1000, waited))
                next_at = max(next_at + self.interval, loop.time())
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), timeout=next_at - loop.time())
            await conn.execute("SELECT pg_stat_force_next_flush()")
        finally:
            self.ready.set()
            await conn.close()


class WalPeak:
    """Пик размера pg_wal за замер: раз в секунду сумма файлов pg_ls_waldir()."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.peak = 0
        self.ready = asyncio.Event()
        self.stop = asyncio.Event()

    async def run(self) -> None:
        conn = await connect(self.dsn)
        try:
            self.ready.set()
            while not self.stop.is_set():
                self.peak = max(
                    self.peak, await conn.fetchval("SELECT sum(size) FROM pg_ls_waldir()")
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), timeout=1)
        finally:
            self.ready.set()
            await conn.close()


async def measure(
    ctx: Ctx,
    method: str,
    variant: str,
    volume: str,
    ids: Sequence[int],
    work: Callable[[], Awaitable[tuple[int, str]]],
    *,
    probe: bool = True,
    spec: CopySpec | None = None,
) -> Run:
    """Один замер: свежая копия → CHECKPOINT → счётчики → работа под пробником → счётчики →
    VACUUM (ANALYZE) и размер копии. ids — отсортированные живые id сегмента (для пробника)."""
    title = f"{method} {variant} {volume}"
    log(f"{title}: свежая копия")
    await build_copy(ctx, spec or CopySpec())
    log(f"{title}: старт")
    await ctx.admin.execute("CHECKPOINT")
    before = await table_stats(ctx.admin)
    ctx.focus = Focus(ids)
    wal_peak = WalPeak(ctx.dsn)
    prober = Probe(ctx.dsn, ctx.focus, ctx.args.probe_ms / 1000)
    tasks = [asyncio.create_task(wal_peak.run())]
    if probe:
        tasks.append(asyncio.create_task(prober.run()))
    await asyncio.gather(wal_peak.ready.wait(), prober.ready.wait() if probe else asyncio.sleep(0))
    started = time.perf_counter()
    try:
        rows, details = await work()
        seconds = time.perf_counter() - started
        ctx.focus.finish()
    finally:
        prober.stop.set()
        wal_peak.stop.set()
        await asyncio.gather(*tasks)
    await flush_stats(ctx)
    await asyncio.sleep(0.5)
    after = await table_stats(ctx.admin)
    wal = int(
        await ctx.admin.fetchval(
            "SELECT pg_wal_lsn_diff($1::text::pg_lsn, $2::text::pg_lsn)", after.lsn, before.lsn
        )
    )
    vacuum_s = await vacuum(ctx)
    size = await ctx.admin.fetchval(f"SELECT pg_total_relation_size('{BENCH}')")
    run = Run(
        method,
        variant,
        volume,
        rows,
        seconds,
        wal,
        after.fpi - before.fpi,
        after.checkpoints - before.checkpoints,
        wal_peak.peak,
        after.hot - before.hot,
        after.dead - before.dead,
        prober.samples,
        ctx.focus.longest,
        vacuum_s,
        size,
        details,
    )
    ctx.runs.append(run)
    log(f"{title}: {rows} строк за {seconds:.1f} с, WAL {wal / MB:.0f} МБ")
    return run


# --- копия ---


async def prepare(ctx: Ctx) -> None:
    """Всё, что берётся из contacts один раз: колонки, границы id, кому передаём."""
    admin = ctx.admin
    ctx.columns = [
        r["attname"]
        for r in await admin.fetch(
            "SELECT attname FROM pg_attribute WHERE attrelid = 'contacts'::regclass"
            " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
        )
    ]
    ctx.min_id = await admin.fetchval("SELECT min(id) FROM contacts")
    ctx.max_id = await id_at(ctx, ctx.args.copy_rows - 1) if ctx.args.copy_rows else 0
    ctx.max_id = ctx.max_id or await admin.fetchval("SELECT max(id) FROM contacts")
    # Кому передаём: пользователи с отделом — каждый замер новому. spare — человек без живых
    # контактов: ответственный «разбросанного» менеджера U7, у него не должно быть своих строк.
    # Таких с отделом в seed почти нет (админы и наблюдатели без отдела), поэтому отдел ему даём
    # любой: FK на team_id у контактов нет, а для замера важна только смена значения.
    # (deleted_at IS NULL — условие частичного индекса по owner_id: без него это 2 000 проходов
    # по таблице.)
    rows = await admin.fetch(
        "SELECT u.id, COALESCE(u.team_id, (SELECT min(t.id::text)::uuid FROM teams t)) AS team_id,"
        " u.team_id IS NOT NULL AS has_team, NOT EXISTS (SELECT 1 FROM contacts c"
        " WHERE c.owner_id = u.id AND c.deleted_at IS NULL) AS free"
        " FROM users u ORDER BY u.id"
    )
    ctx.targets = [(r["id"], r["team_id"]) for r in rows if r["has_team"]]
    ctx.spare = next(((r["id"], r["team_id"]) for r in rows if r["free"]), None)
    n_tup_upd = await admin.fetchval(
        "SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = 'contacts'"
    )
    print(f"contacts: id {ctx.min_id}..{ctx.max_id}, n_tup_upd до прогона {n_tup_upd}")


async def build_copy(ctx: Ctx, spec: CopySpec) -> None:
    """Пересоздать bench_contacts: строки в порядке id, потом индексы, FK и VACUUM (ANALYZE)."""
    admin = ctx.admin
    await admin.execute(f"DROP TABLE IF EXISTS {BENCH}")
    started = time.perf_counter()
    await admin.execute(f"CREATE TABLE {BENCH} (LIKE contacts INCLUDING ALL EXCLUDING INDEXES)")
    await admin.execute(
        f"ALTER TABLE {BENCH} SET (autovacuum_enabled = false, fillfactor = {spec.fillfactor})"
    )
    columns = ", ".join(ctx.columns)
    args: list[Any] = [ctx.max_id]
    select_list = columns
    if spec.spread:
        owner, team, every = spec.spread
        args += [owner, team, every]
        select_list = ", ".join(
            f"CASE WHEN id % $4 = 0 THEN ${2 if c == 'owner_id' else 3} ELSE {c} END"
            if c in ("owner_id", "team_id")
            else c
            for c in ctx.columns
        )
    await admin.execute(
        f"INSERT INTO {BENCH} ({columns}) SELECT {select_list} FROM contacts"
        " WHERE id <= $1 ORDER BY id",
        *args,
    )
    copied = time.perf_counter() - started
    indexes = await admin.fetch(
        "SELECT c.relname AS name, pg_get_indexdef(i.indexrelid) AS ddl, i.indisprimary"
        " FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
        " WHERE i.indrelid = 'contacts'::regclass ORDER BY i.indisprimary DESC, c.relname"
    )
    for index in indexes:
        name = f"bench_{index['name']}"
        ddl = index["ddl"].replace(
            f"INDEX {index['name']} ON public.contacts", f"INDEX {name} ON public.{BENCH}"
        )
        await admin.execute(ddl)
        if index["indisprimary"]:
            await admin.execute(f"ALTER TABLE {BENCH} ADD PRIMARY KEY USING INDEX {name}")
    await admin.execute(
        f"ALTER TABLE {BENCH} ADD CONSTRAINT fk_{BENCH}_owner_id"
        " FOREIGN KEY (owner_id) REFERENCES users (id) NOT VALID"
    )
    indexed = time.perf_counter() - started - copied
    vacuumed = await vacuum(ctx)
    size = await admin.fetchval(f"SELECT pg_total_relation_size('{BENCH}')")
    line = (
        f"fillfactor {spec.fillfactor}: строки {copied:.0f} с, {len(indexes)} индексов"
        f" {indexed:.0f} с, VACUUM {vacuumed:.0f} с, размер {size / MB:.0f} МБ"
    )
    log(f"копия {line}")
    ctx.builds.setdefault(spec.fillfactor, line)


async def drop_copy(dsn: str) -> None:
    """Удалить копию в любом случае: сначала добить свои соединения (вдруг UPDATE ещё идёт)."""
    conn = await connect(dsn, f"{APP_NAME}_cleanup")
    try:
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
            APP_NAME,
        )
        await conn.execute(f"DROP TABLE IF EXISTS {BENCH}")
        log(f"{BENCH} удалена")
        n_tup_upd = await conn.fetchval(
            "SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = 'contacts'"
        )
        print(f"contacts: n_tup_upd после прогона {n_tup_upd}")
    finally:
        await conn.close()


async def id_at(ctx: Ctx, offset: int) -> int:
    """id строки номер offset + 1 по порядку id (граница объёма: id <= p1M)."""
    value = await ctx.admin.fetchval(
        "SELECT id FROM contacts ORDER BY id OFFSET $1 LIMIT 1", offset
    )
    return int(value) if value is not None else ctx.max_id


async def fetch_ids(ctx: Ctx, where: str, *args: Any) -> array[int]:
    """Живые id сегмента по порядку — из contacts (копия — её точная копия). Это и строки, которые
    «пришли из приложения» для U4–U6, и выбор пробника. Чтение в замер не входит."""
    ids = array("q")
    async with ctx.bulk.transaction():
        cursor = ctx.bulk.cursor(
            f"SELECT id FROM contacts WHERE deleted_at IS NULL AND id <= {ctx.max_id}"
            f" AND {where} ORDER BY id",
            *args,
            prefetch=50_000,
        )
        async for row in cursor:
            ids.append(row[0])
    return ids


# --- способы ---


def live() -> ColumnElement[bool]:
    """Как в reassign_owner: корзина не передаётся."""
    return BenchContact.deleted_at.is_(None)


async def chunked_reassign(ctx: Ctx, bounds: Sequence[tuple[int, int]]) -> list[int]:
    """Чанки как в bulk.chunk: BulkRepository.reassign и commit после каждого."""
    owner, team = ctx.target()
    counts = []
    async with ctx.sessions() as session:
        repo = BulkRepository(session)
        for lo, hi in bounds:
            ctx.focus.aim(lo, hi)
            counts.append(await repo.reassign(BenchContact, lo, hi, live(), owner, team))
            await session.commit()
    return counts


def equal_ranges(lo: int, hi: int, step: int) -> list[tuple[int, int]]:
    return [(a, min(a + step - 1, hi)) for a in range(lo, hi + 1, step)]


async def u1(ctx: Ctx) -> None:
    n = ctx.args.orm_rows
    lo, hi = ctx.min_id, await id_at(ctx, n - 1)
    ids = await fetch_ids(ctx, "id BETWEEN $1 AND $2", lo, hi)
    statements = 0

    def count(*_: Any) -> None:
        nonlocal statements
        statements += 1

    event.listen(ctx.engine.sync_engine, "before_cursor_execute", count)
    try:

        async def batch() -> tuple[int, str]:
            """Все объекты в памяти, один commit. Правка пробника между SELECT и UPDATE —
            StaleDataError (version_id_col) и откат всей пачки: это тоже результат замера."""
            nonlocal statements
            statements = 0
            ctx.focus.aim(lo, hi)
            owner, team = ctx.target()
            async with ctx.sessions() as session:
                query = select(BenchContact).where(BenchContact.id.between(lo, hi), live())
                contacts = (await session.scalars(query)).all()
                for contact in contacts:
                    contact.owner_id = owner
                    contact.team_id = team
                try:
                    await session.commit()
                except StaleDataError as e:
                    await session.rollback()
                    return 0, f"запросов {statements}; StaleDataError, откат всех строк: {e}"
            return len(contacts), f"запросов {statements}"

        async def one_by_one() -> tuple[int, str]:
            nonlocal statements
            statements = 0
            ctx.focus.aim(lo, hi)
            owner, team = ctx.target()
            done = conflicts = 0
            async with ctx.sessions() as session:
                for contact_id in ids:
                    contact = await session.get(BenchContact, contact_id)
                    if contact is None:
                        continue
                    contact.owner_id = owner
                    contact.team_id = team
                    try:
                        await session.commit()
                        done += 1
                    except StaleDataError:
                        await session.rollback()  # в API это 409 version_conflict
                        conflicts += 1
            return done, f"запросов {statements}, конфликтов версии {conflicts}"

        async def one_chunk() -> tuple[int, str]:
            counts = await chunked_reassign(ctx, [(lo, hi)])
            return counts[0], "для сравнения с U1 на тех же строках"

        volume = f"{n // 1000}k"
        run = await measure(ctx, "U1a", "объекты, правка в цикле, один commit", volume, ids, batch)
        if run.rows == 0:  # пробник сорвал пачку — время без него
            await measure(ctx, "U1a", "то же без пробника", volume, ids, batch, probe=False)
        await measure(ctx, "U1b", "get → правка → commit на строку", volume, ids, one_by_one)
        await measure(ctx, "U3", f"один чанк на {n} строк", volume, ids, one_chunk)
    finally:
        event.remove(ctx.engine.sync_engine, "before_cursor_execute", count)


def volume_jobs(
    ctx: Ctx, methods: set[str], volume: str, hi: int, ids: array[int]
) -> list[Callable[[], Awaitable[Any]]]:
    """Замеры U2–U6 одного объёма — список, чтобы --repeat мог менять их порядок."""
    lo = ctx.min_id
    batch = ctx.args.batch
    jobs: list[Callable[[], Awaitable[Any]]] = []
    run = partial(measure, ctx)

    def values() -> tuple[list[uuid.UUID], list[uuid.UUID]]:
        (owner_a, team_a), (owner_b, team_b) = ctx.target(), ctx.target()
        return [owner_a, owner_b], [team_a, team_b]

    if "U2" in methods:

        async def single() -> tuple[int, str]:
            counts = await chunked_reassign(ctx, [(lo, hi)])
            return counts[0], ""

        jobs.append(partial(run, "U2", "один UPDATE … WHERE", volume, ids, single))

    if "U3" in methods:
        for size in ctx.args.sizes:

            async def chunks(size: int = size) -> tuple[int, str]:
                counts = await chunked_reassign(ctx, equal_ranges(lo, hi, size))
                return sum(counts), f"чанков {len(counts)}"

            jobs.append(partial(run, "U3", f"чанки по {size}", volume, ids, chunks))

    if "U4" in methods:

        async def unnest() -> tuple[int, str]:
            owners, teams = values()
            sql = (
                f"UPDATE {BENCH} c SET owner_id = v.owner_id, team_id = v.team_id,"
                " version = c.version + 1, updated_at = now()"
                " FROM unnest($1::bigint[], $2::uuid[], $3::uuid[]) AS v(id, owner_id, team_id)"
                " WHERE c.id = v.id"
            )
            done = 0
            nodes = ""
            for start in range(0, len(ids), batch):
                part = ids[start : start + batch].tolist()
                params = (part, [owners[i & 1] for i in part], [teams[i & 1] for i in part])
                if not nodes:
                    nodes = await plan(ctx.bulk, sql, *params)
                ctx.focus.aim(part[0], part[-1])
                done += rowcount(await ctx.bulk.execute(sql, *params))
            return done, f"пачек {math.ceil(len(ids) / batch)}; план: {nodes}"

        jobs.append(
            partial(run, "U4", f"UPDATE FROM unnest, пачки по {batch}", volume, ids, unnest)
        )

    if "U5" in methods:

        async def staging(hashjoin: bool = True) -> tuple[int, str]:
            owners, teams = values()
            ctx.focus.aim(lo, hi)
            async with ctx.bulk.transaction():
                if not hashjoin:
                    await ctx.bulk.execute("SET LOCAL enable_hashjoin = off")
                await ctx.bulk.execute(
                    f"CREATE TEMP TABLE {STAGING} (id bigint, owner_id uuid, team_id uuid)"
                    " ON COMMIT DROP"
                )
                copy_started = time.perf_counter()
                await ctx.bulk.copy_records_to_table(
                    STAGING,
                    records=((i, owners[i & 1], teams[i & 1]) for i in ids),
                    columns=["id", "owner_id", "team_id"],
                )
                await ctx.bulk.execute(f"ANALYZE {STAGING}")
                copy_s = time.perf_counter() - copy_started
                sql = (
                    f"UPDATE {BENCH} c SET owner_id = s.owner_id, team_id = s.team_id,"
                    " version = c.version + 1, updated_at = now()"
                    f" FROM {STAGING} s WHERE c.id = s.id"
                )
                nodes = await plan(ctx.bulk, sql)
                update_started = time.perf_counter()
                status = await ctx.bulk.execute(sql)
                update_s = time.perf_counter() - update_started
            return rowcount(status), f"COPY {copy_s:.1f} с, UPDATE {update_s:.1f} с; план: {nodes}"

        async def staging_both() -> None:
            first = await run("U5", "COPY в staging + UPDATE FROM", volume, ids, staging)
            if "Hash Join" in first.details:
                variant = "то же, enable_hashjoin = off"
                await run("U5", variant, volume, ids, partial(staging, hashjoin=False))

        jobs.append(staging_both)

    if "U6" in methods:

        async def orm_bulk() -> tuple[int, str]:
            (owner_a, team_a), (owner_b, team_b) = ctx.target(), ctx.target()
            owners, teams = (owner_a, owner_b), (team_a, team_b)
            async with ctx.sessions() as session:
                for start in range(0, len(ids), batch):
                    end = min(start + batch, len(ids))
                    ctx.focus.aim(ids[start], ids[end - 1])
                    await session.execute(
                        update(BenchContactPlain),
                        [
                            {
                                "id": ids[k],
                                "owner_id": owners[ids[k] & 1],
                                "team_id": teams[ids[k] & 1],
                            }
                            for k in range(start, end)
                        ],
                    )
                    await session.commit()
            return len(ids), f"пачек {math.ceil(len(ids) / batch)}"

        variant = f"update(Model) + dict-ы, пачки по {batch}"
        jobs.append(partial(run, "U6", variant, volume, ids, orm_bulk))

    return jobs


def owner_where(owner_id: uuid.UUID) -> ColumnElement[bool]:
    """Выборка reassign_owner без фильтра Access: живые записи прежнего ответственного."""
    return and_(BenchContact.owner_id == owner_id, live())


async def move_owner(
    ctx: Ctx, bounds_kind: str, from_owner: uuid.UUID, segment_rows: int
) -> tuple[int, str]:
    """Один прогон U7: нарезка (входит во время) и чанки BulkRepository.reassign с commit.

    bounds_kind: rows — по строкам (chunk_bounds, как bulk.plan); step — равные диапазоны по
    --chunk id; count — равные диапазоны, но столько же чанков, сколько по строкам."""
    size = ctx.args.chunk
    where = owner_where(from_owner)
    to = ctx.target()
    bounds_query: Any = (
        BulkRepository.chunk_bounds_query(BenchContact, where, size)
        if bounds_kind == "rows"
        else select(func.min(BenchContact.id), func.max(BenchContact.id)).where(where)
    )
    nodes = await plan_query(ctx, bounds_query)  # EXPLAIN без ANALYZE — доли мс
    planned = time.perf_counter()
    async with ctx.sessions() as session:
        if bounds_kind == "rows":
            bounds, _ = await BulkRepository(session).chunk_bounds(BenchContact, where, size)
        else:
            lo, hi = (await session.execute(bounds_query)).one()
            if bounds_kind == "step":
                step = size
            else:
                step = math.ceil((hi - lo + 1) / math.ceil(segment_rows / size))
            bounds = equal_ranges(lo, hi, step)
        await session.commit()
    plan_ms = (time.perf_counter() - planned) * 1000
    counts: list[int] = []
    async with ctx.sessions() as session:
        repo = BulkRepository(session)
        for lo, hi in bounds:
            ctx.focus.aim(lo, hi)
            counts.append(await repo.reassign(BenchContact, lo, hi, where, *to))
            await session.commit()
    empty = sum(1 for c in counts if c == 0)
    details = (
        f"чанков {len(counts)}, пустых {empty}, строк в чанке"
        f" {min(counts)} / {int(statistics.median(counts))} / {max(counts)}"
        f" (min / медиана / max), нарезка {plan_ms:.0f} мс; план нарезки: {nodes}"
    )
    return sum(counts), details


async def u7(ctx: Ctx) -> None:
    """Границы чанков: по строкам (как bulk.plan) против равных диапазонов id. Каждый вариант —
    на свежей копии, от одного и того же менеджера."""
    heavy = await ctx.admin.fetchval(
        "SELECT c.owner_id FROM contacts c JOIN users u ON u.id = c.owner_id"
        " JOIN organizations o ON o.id = u.org_id"
        " WHERE c.deleted_at IS NULL AND c.id <= $1 AND u.role = 'sales_rep'"
        " AND o.slug LIKE 'seed-%' GROUP BY c.owner_id ORDER BY count(*) DESC LIMIT 1",
        ctx.max_id,
    )
    if heavy is None or ctx.spare is None:
        sys.exit("нет seed-данных: сначала make seed-big")
    variants = {
        "rows": "по строкам (chunk_bounds)",
        "step": f"равные диапазоны по {ctx.args.chunk} id",
        "count": "равные диапазоны, чанков как по строкам",
    }

    async def series(title: str, owner: uuid.UUID, ids: array[int], spec: CopySpec) -> None:
        volume = f"{title}: {len(ids)} строк, id {ids[0]}..{ids[-1]}"
        for kind, label in variants.items():
            work = partial(move_owner, ctx, kind, owner, len(ids))
            await measure(ctx, "U7", label, volume, ids, work, spec=spec)

    ids = await fetch_ids(ctx, "owner_id = $1", heavy)
    await series("крупнейший sales_rep", heavy, ids, CopySpec())

    # Синтетический менеджер: каждый N-й контакт таблицы — записи разбросаны по всем id. Копия
    # строится сразу с ним (CopySpec.spread), чтобы строки остались на своих страницах.
    every = ctx.args.spread_every
    owner, team = ctx.spare
    ids = await fetch_ids(ctx, "id % $1 = 0", every)
    await series(f"каждый {every}-й контакт", owner, ids, CopySpec(spread=(owner, team, every)))


async def hot(ctx: Ctx) -> None:
    """SET source (HOT, если есть место на странице) против SET owner_id (не HOT) — на копии с
    fillfactor 100 и на копии, загруженной с fillfactor 90."""
    size = ctx.args.chunk
    hi = ctx.max_id
    lo = await id_at(ctx, max(0, ctx.max_id - ctx.min_id + 1 - MILLION))
    ids = await fetch_ids(ctx, "id BETWEEN $1 AND $2", lo, hi)
    bounds = equal_ranges(lo, hi, size)
    volume = "последний 1 млн id"

    async def chunks(set_clause: str, *values: Any) -> int:
        sql = (
            f"UPDATE {BENCH} SET {set_clause}, version = version + 1, updated_at = now()"
            " WHERE id BETWEEN $1 AND $2 AND deleted_at IS NULL"
        )
        done = 0
        for a, b in bounds:
            ctx.focus.aim(a, b)
            done += rowcount(await ctx.bulk.execute(sql, a, b, *values))
        return done

    async def set_source() -> tuple[int, str]:
        ctx.turn += 1
        return await chunks("source = $3", f"bench-{ctx.turn}"), ""

    async def set_owner() -> tuple[int, str]:
        owner, team = ctx.target()
        return await chunks("owner_id = $3, team_id = $4", owner, team), ""

    for fillfactor in (100, 90):
        spec = CopySpec(fillfactor=fillfactor)
        for label, work in (("SET source", set_source), ("SET owner_id, team_id", set_owner)):
            variant = f"{label}, fillfactor {fillfactor}"
            await measure(ctx, "HOT", variant, volume, ids, work, spec=spec)


# --- вывод ---


def probe_cells(samples: list[Sample]) -> tuple[str, str, str]:
    """(замеров / ждали, ожидание p50 / p99 / max, без ожидания p99 / max)."""
    if not samples:
        return "—", "—", "—"
    waits = [s.ms for s in samples if s.waited]
    free = [s.ms for s in samples if not s.waited]
    counts = f"{len(samples)} / {len(waits)}"
    waited = (
        f"{percentile(waits, 0.5):.0f} / {percentile(waits, 0.99):.0f} / {max(waits):.0f}"
        if waits
        else "—"
    )
    rest = f"{percentile(free, 0.99):.0f} / {max(free):.0f}" if free else "—"
    return counts, waited, rest


def print_results(ctx: Ctx) -> None:
    print()
    for line in ctx.builds.values():
        print(f"Копия {BENCH}, {line}")
    print()
    print(
        "| Способ | Вариант | Объём | Строк | Время, с | Строк/с | WAL, МБ | FPI, тыс. | Чекпоинтов"
        " | Пик pg_wal, МБ | HOT | Мёртвых | Пробник: замеров / ждали"
        " | Ожидание p50 / p99 / max, мс | Без ожидания p99 / max, мс | Самый долгий чанк, мс"
        " | VACUUM, с | Копия после, МБ | Подробности |"
    )
    print(
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
        " ---: | ---: | ---: | ---: | ---: | --- |"
    )
    for r in ctx.runs:
        speed = r.rows / r.seconds if r.seconds else 0
        details = r.details
        if r.method.startswith("U1") and speed:
            total = ctx.max_id - ctx.min_id + 1
            details += (
                f"; на 1 млн ≈ {MILLION / speed / 60:.1f} мин,"
                f" на {total / MILLION:.2f} млн ≈ {total / speed / 60:.1f} мин"
            )
        counts, waited, rest = probe_cells(r.probe)
        print(
            f"| {r.method} | {r.variant} | {r.volume} | {r.rows} | {r.seconds:.1f} | {speed:.0f}"
            f" | {r.wal / MB:.0f} | {r.fpi / 1000:.0f} | {r.checkpoints} | {r.wal_peak / MB:.0f}"
            f" | {r.hot} | {r.dead} | {counts} | {waited} | {rest} | {r.longest * 1000:.0f}"
            f" | {r.vacuum:.1f} | {r.size / MB:.0f} | {details} |"
        )
    # Повторы (--repeat): разброс времени и WAL по одинаковым замерам.
    groups: dict[tuple[str, str, str], list[Run]] = {}
    for r in ctx.runs:
        groups.setdefault((r.method, r.variant, r.volume.split(" #")[0]), []).append(r)
    repeated = {k: v for k, v in groups.items() if len(v) > 1}
    if repeated:
        print(
            "\n| Способ | Вариант | Объём | Повторов | Время, с: min – max | WAL, МБ: min – max |"
        )
        print("| --- | --- | --- | ---: | ---: | ---: |")
        for (method, variant, volume), runs in repeated.items():
            times = [r.seconds for r in runs]
            wals = [r.wal / MB for r in runs]
            print(
                f"| {method} | {variant} | {volume} | {len(runs)} | {min(times):.1f} –"
                f" {max(times):.1f} | {min(wals):.0f} – {max(wals):.0f} |"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--methods",
        default=",".join(ALL_METHODS),
        help=f"какие замеры, через запятую (по умолчанию все: {','.join(ALL_METHODS)})",
    )
    parser.add_argument("--volumes", default="1m,5m", help="объёмы для U2–U6: 1m, 5m")
    parser.add_argument("--repeat", type=int, default=1, help="сколько раз повторить U2–U6")
    parser.add_argument("--sizes", default="1000,10000,50000", help="размеры чанков U3")
    parser.add_argument("--batch", type=int, default=10_000, help="пачка U4 и U6")
    parser.add_argument("--chunk", type=int, default=10_000, help="чанк U7 и HOT")
    parser.add_argument("--orm-rows", type=int, default=10_000, help="строк для U1")
    parser.add_argument("--spread-every", type=int, default=50, help="U7: каждый N-й контакт")
    parser.add_argument("--probe-ms", type=int, default=100, help="интервал пробника, мс")
    parser.add_argument(
        "--copy-rows", type=int, default=0, help="копировать только N первых строк (проверка)"
    )
    args = parser.parse_args()
    args.methods = {m.strip().upper() for m in args.methods.split(",") if m.strip()}
    unknown = args.methods - set(ALL_METHODS)
    if unknown:
        parser.error(f"неизвестные замеры: {', '.join(sorted(unknown))}")
    args.volumes = [v.strip().lower() for v in args.volumes.split(",") if v.strip()]
    if set(args.volumes) - {"1m", "5m"}:
        parser.error("объёмы — только 1m и 5m")
    args.sizes = [int(s) for s in args.sizes.split(",")]
    return args


async def main() -> int:
    args = parse_args()
    current = asyncio.current_task()
    assert current is not None
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, current.cancel)

    settings = get_settings()
    dsn = settings.database_url.replace("postgresql+asyncpg", "postgresql")
    admin = await connect(dsn, f"{APP_NAME}_admin")
    if not await admin.fetchval(
        "SELECT EXISTS (SELECT 1 FROM organizations WHERE slug LIKE 'seed-%')"
    ):
        await admin.close()
        sys.exit("нет seed-данных: сначала make seed-big")
    engine = create_async_engine(
        settings.database_url,
        pool_size=1,
        max_overflow=0,
        connect_args={"server_settings": {"application_name": APP_NAME}},
    )
    ctx = Ctx(
        dsn=dsn,
        admin=admin,
        bulk=await connect(dsn),
        engine=engine,
        sessions=async_sessionmaker(engine),
        args=args,
    )
    started = time.perf_counter()
    try:
        await prepare(ctx)
        # Порядок не важен: каждый замер начинается со свежей копии.
        if "U7" in args.methods:
            await u7(ctx)
        if "HOT" in args.methods:
            await hot(ctx)
        if "U1" in args.methods:
            await u1(ctx)
        for volume in args.volumes:
            hi = ctx.max_id if volume == "5m" else await id_at(ctx, MILLION - 1)
            if not args.methods & {"U2", "U3", "U4", "U5", "U6"}:
                break
            log(f"{volume}: читаем id сегмента")
            ids = await fetch_ids(ctx, "id BETWEEN $1 AND $2", ctx.min_id, hi)
            for rep in range(args.repeat):
                label = f"{volume} #{rep + 1}" if args.repeat > 1 else volume
                jobs = volume_jobs(ctx, args.methods, label, hi, ids)
                shift = rep % len(jobs)  # каждый повтор — другой порядок способов
                for job in jobs[shift:] + jobs[:shift]:
                    await job()
    finally:
        # Сначала копия: повторная отмена (второй SIGTERM) на закрытии соединений не должна
        # оставить 2–7 ГБ на диске.
        await drop_copy(dsn)
        await ctx.bulk.close()
        await engine.dispose()
        await admin.close()
        print_results(ctx)
        print(f"\nВсего: {(time.perf_counter() - started) / 60:.1f} мин")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
