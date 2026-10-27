"""Импорт контактов из CSV (раздел 6): staging, ошибки строк и set-based SQL шагов задачи.

Шаги задачи imports.import_contacts — parse, stage и merge — пишут только через этот
репозиторий: COPY в contacts_staging и import_errors (parse), UPDATE по staging (stage) и чанки
INSERT … ON CONFLICT в contacts (merge). Строки одного шага — одна транзакция сервиса, поэтому
повтор шага после падения начинается с чистого листа. COPY идёт мимо курсора SQLAlchemy, а BEGIN
адаптер asyncpg шлёт только перед запросом курсора, — поэтому parse открывает транзакцию
запросом (clear_parse), и _driver это проверяет: иначе каждый COPY коммитился бы сам.

email в staging — text (asyncpg не кодирует citext в COPY): дубли в файле и сопоставление с
результатом merge — по lower(email). citext сравнивает так же, поэтому «дубль в файле» и
«конфликт с контактом» — одно и то же понятие.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

from sqlalchemy import (
    Boolean,
    ColumnElement,
    Table,
    Uuid,
    and_,
    case,
    delete,
    exists,
    func,
    literal,
    literal_column,
    select,
    union_all,
    update,
)
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.authz.access import Access
from app.models import ROLES_WITH_TEAM, Contact, ContactStaging, ImportRowError, User
from app.repositories.pagination import Key, Page, PageResult, fetch_page, paginate

# Ошибки строк — по порядку файла: keyset по row_no внутри job (PK job_id, row_no).
ERROR_KEYS = (Key(ImportRowError.row_no),)

STAGING = cast(Table, ContactStaging.__table__)
ERRORS = cast(Table, ImportRowError.__table__)
CONTACTS = cast(Table, Contact.__table__)
USERS = cast(Table, User.__table__)

# Порядок колонок COPY — тот же, что у кортежей сервиса (StagingRow.record, RowError.record).
STAGING_COLUMNS = (
    "job_id",
    "row_no",
    "email",
    "first_name",
    "last_name",
    "phone",
    "status",
    "source",
    "tags",
    "owner_email",
)
ERROR_COLUMNS = ("job_id", "row_no", "code", "field", "raw")

# Коды ошибок строк шагов stage и merge (parse — в сервисе).
DUPLICATE_IN_FILE = "duplicate_in_file"
OWNER_INVALID = "owner_invalid"
OWNER_OUT_OF_SCOPE = "owner_out_of_scope"
DUPLICATE_OUTSIDE_SCOPE = "duplicate_outside_scope"
MAX_TAGS = 20  # как TagList в schemas/crm.py

# Теги существующего контакта и новые из файла — без повторов, в порядке «сначала свои», не
# больше 20: лишние новые отбрасываются, строка ошибкой не считается (раздел 6). contacts и
# excluded в ON CONFLICT DO UPDATE — старая строка и предложенная вставка.
MERGED_TAGS = (
    "ARRAY(SELECT t FROM unnest(contacts.tags || excluded.tags) WITH ORDINALITY AS u(t, n) "
    f"GROUP BY t ORDER BY min(n) LIMIT {MAX_TAGS})::varchar(64)[]"
)


def _text(value: str) -> ColumnElement[Any]:
    """Строковая константа литералом, а не параметром: у jsonb_build_object аргументы типа
    "any", и тип параметра asyncpg Postgres вывести не смог бы."""
    return literal_column("'" + value.replace("'", "''") + "'")


def staging_raw(columns: Any) -> ColumnElement[Any]:
    """raw ошибки шагов stage и merge — ячейки строки из staging (уже нормализованные; теги —
    снова строкой через запятую). Только данные файла: у duplicate_outside_scope — без данных
    существующего контакта, его автор импорта не видит."""
    return func.jsonb_strip_nulls(
        func.jsonb_build_object(
            _text("email"),
            columns.email,
            _text("first_name"),
            columns.first_name,
            _text("last_name"),
            columns.last_name,
            _text("phone"),
            columns.phone,
            _text("status"),
            columns.status,
            _text("source"),
            columns.source,
            _text("tags"),
            func.array_to_string(columns.tags, _text(",")),
            _text("owner_email"),
            columns.owner_email,
        )
    )


def as_citext(value: Any) -> ColumnElement[Any]:
    """text → citext явным CAST: иначе citext = text Postgres сравнил бы как text, с учётом
    регистра, и мимо уникального индекса users.email."""
    return sql_cast(value, CITEXT())


def can_own_sql() -> ColumnElement[bool]:
    """То же, что can_own() в services/crm.py, для WHERE: активный team_lead или sales_rep с
    отделом (организацию проверяет вызывающий)."""
    return and_(
        USERS.c.is_active.is_(True),
        USERS.c.role.in_(list(ROLES_WITH_TEAM)),
        USERS.c.team_id.is_not(None),
    )


@dataclass(frozen=True)
class MergeCounts:
    """Итог одного чанка merge."""

    created: int
    updated: int
    outside: list[int]  # row_no строк duplicate_outside_scope


class ImportErrorRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list(self, job_id: uuid.UUID, page: Page) -> PageResult[ImportRowError]:
        query = select(ImportRowError).where(ImportRowError.job_id == job_id)
        return await fetch_page(self.session, paginate(query, ERROR_KEYS, page), ERROR_KEYS, page)


class ImportRepository:
    """SQL шагов задачи импорта. Коммитит сервис: шаг или чанк — одна транзакция."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _driver(self) -> Any:
        """Соединение asyncpg транзакции сессии: copy_records_to_table есть только у него.
        COPY идёт в той же транзакции, что и остальные запросы шага. Адаптер asyncpg шлёт
        BEGIN лениво — перед первым запросом курсора, а COPY идёт мимо курсора: если запроса
        ещё не было, транзакцию открывает SELECT 1, иначе COPY закоммитился бы сам и откат
        шага его бы не отменил."""
        connection = await self.session.connection()
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        assert driver is not None  # соединение пула живое: сессия его только что выдала
        if not driver.is_in_transaction():
            await connection.execute(select(literal(1)))
        return driver

    # --- parse

    async def clear_parse(self, job_id: uuid.UUID) -> None:
        """Начало шага parse: строки staging и ошибки строк job, если что-то от прошлой копии
        всё же осталось, — повтор parse не упрётся в PK (job_id, row_no). Первый запрос шага:
        он же открывает транзакцию до COPY."""
        await self.session.execute(delete(ContactStaging).where(ContactStaging.job_id == job_id))
        await self.session.execute(delete(ImportRowError).where(ImportRowError.job_id == job_id))

    async def copy_staging(self, records: Sequence[tuple[Any, ...]]) -> None:
        """Проверенные строки файла — COPY (бинарный протокол asyncpg) в contacts_staging."""
        if records:
            driver = await self._driver()
            await driver.copy_records_to_table(
                "contacts_staging", records=records, columns=STAGING_COLUMNS
            )

    async def copy_errors(self, records: Sequence[tuple[Any, ...]]) -> None:
        """Ошибки строк — COPY в import_errors; raw — уже строка json.dumps (кодек jsonb
        asyncpg, который ставит SQLAlchemy, принимает текст)."""
        if records:
            driver = await self._driver()
            await driver.copy_records_to_table(
                "import_errors", records=records, columns=ERROR_COLUMNS
            )

    async def add_error(
        self, job_id: uuid.UUID, row_no: int, code: str, raw: dict[str, Any] | None = None
    ) -> None:
        """Одна ошибка строки (malformed_csv — у строки, на которой разбор остановился)."""
        await self.session.execute(
            pg_insert(ImportRowError)
            .values(job_id=job_id, row_no=row_no, code=code, raw=raw)
            .on_conflict_do_nothing()
        )

    # --- stage

    async def stage(self, job_id: uuid.UUID, access: Access, default_owner_id: uuid.UUID) -> int:
        """Проверки по файлу и базе одним проходом UPDATE-ов по staging (раздел 6, шаг stage):
        дубли email в файле (побеждает первая строка), ответственный из owner_email или
        default_owner_id. Ошибки — в import_errors. Возвращает число новых ошибок."""
        s = STAGING
        alive = and_(s.c.job_id == job_id, s.c.error.is_(None))
        # Дубли в файле: окно по lower(email) — один проход сортировки, а не сравнение каждой
        # строки с каждой (индекса по email у staging нет).
        ranked = (
            select(
                s.c.row_no,
                func.row_number()
                .over(partition_by=func.lower(s.c.email), order_by=s.c.row_no)
                .label("rn"),
            )
            .where(s.c.job_id == job_id)
            .subquery("ranked")
        )
        await self.session.execute(
            update(s)
            .where(alive, s.c.row_no == ranked.c.row_no, ranked.c.rn > 1)
            .values(error=DUPLICATE_IN_FILE)
        )
        # owner_email → активный team_lead / sales_rep организации (email у users — citext:
        # сравнение без учёта регистра и по уникальному индексу).
        with_owner = and_(alive, s.c.owner_email.is_not(None))
        await self.session.execute(
            update(s)
            .where(
                with_owner,
                USERS.c.email == as_citext(s.c.owner_email),
                USERS.c.org_id == access.org_id,
                can_own_sql(),
            )
            .values(owner_id=USERS.c.id)
        )
        await self.session.execute(
            update(s).where(with_owner, s.c.owner_id.is_(None)).values(error=OWNER_INVALID)
        )
        # Вне уровня того, кто запустил импорт, — тот же Access.sql_filter, что у записей: у
        # подзапроса те же колонки org_id, team_id, owner_id.
        owners = select(USERS.c.id.label("owner_id"), USERS.c.org_id, USERS.c.team_id).subquery(
            "owners"
        )
        await self.session.execute(
            update(s)
            .where(
                with_owner,
                s.c.owner_id == owners.c.owner_id,
                ~access.sql_filter(owners.c),
            )
            .values(error=OWNER_OUT_OF_SCOPE)
        )
        # Пустой owner_email — ответственный по умолчанию (его проверил API, а merge проверит
        # ещё раз под FOR SHARE).
        await self.session.execute(
            update(s).where(alive, s.c.owner_email.is_(None)).values(owner_id=default_owner_id)
        )
        failed = (
            select(
                s.c.job_id,
                s.c.row_no,
                s.c.error,
                case((s.c.error == DUPLICATE_IN_FILE, _text("email")), else_=_text("owner_email")),
                staging_raw(s.c),
            )
            .where(s.c.job_id == job_id, s.c.error.is_not(None))
            .order_by(s.c.row_no)
        )
        result = await self.session.execute(
            pg_insert(ERRORS)
            .from_select(list(ERROR_COLUMNS), failed)
            .on_conflict_do_nothing()
            .returning(ERRORS.c.row_no)
        )
        return len(result.all())

    # --- merge

    async def chunk_end(self, job_id: uuid.UUID, after: int, size: int) -> int | None:
        """Последний row_no следующего чанка: size ещё не перенесённых строк без ошибок после
        after. None — переносить больше нечего."""
        s = STAGING
        rows = (
            select(s.c.row_no)
            .where(
                s.c.job_id == job_id,
                s.c.row_no > after,
                s.c.merged.is_(False),
                s.c.error.is_(None),
            )
            .order_by(s.c.row_no)
            .limit(size)
            .subquery()
        )
        return cast(int | None, await self.session.scalar(select(func.max(rows.c.row_no))))

    @staticmethod
    def _chunk(job_id: uuid.UUID, lo: int, hi: int) -> ColumnElement[bool]:
        s = STAGING
        return and_(
            s.c.job_id == job_id,
            s.c.row_no.between(lo, hi),
            s.c.merged.is_(False),
            s.c.error.is_(None),
        )

    async def chunk_owners(self, job_id: uuid.UUID, lo: int, hi: int) -> list[uuid.UUID]:
        """Ответственные строк чанка — их сервис читает FOR SHARE и проверяет ещё раз."""
        s = STAGING
        return list(
            await self.session.scalars(
                select(s.c.owner_id).where(self._chunk(job_id, lo, hi)).distinct()
            )
        )

    async def lock_users(self, ids: Sequence[uuid.UUID]) -> list[User]:
        """Ответственные FOR SHARE, по порядку id (против дедлоков): пока чанк переносит строки,
        человека не отключат и не переведут — перевод подождёт и увидит его записи."""
        if not ids:
            return []
        return list(
            await self.session.scalars(
                select(User)
                .where(User.id.in_(ids))
                .order_by(User.id)
                .with_for_update(read=True)
                .execution_options(populate_existing=True)
            )
        )

    async def reject_owners(
        self, job_id: uuid.UUID, lo: int, hi: int, owner_ids: Sequence[uuid.UUID]
    ) -> int:
        """Строки чанка, чей ответственный к merge стал неактивным или вышел из уровня, —
        owner_invalid (строка staging перенесена — с ошибкой). Возвращает их число."""
        s = STAGING
        rejected = (
            update(s)
            .where(self._chunk(job_id, lo, hi), s.c.owner_id.in_(owner_ids))
            .values(error=OWNER_INVALID, merged=True)
            .returning(
                s.c.row_no,
                s.c.email,
                s.c.first_name,
                s.c.last_name,
                s.c.phone,
                s.c.status,
                s.c.source,
                s.c.tags,
                s.c.owner_email,
            )
            .cte("rejected")
        )
        errors = (
            pg_insert(ERRORS)
            .from_select(
                list(ERROR_COLUMNS),
                select(
                    literal(job_id, Uuid),
                    rejected.c.row_no,
                    _text(OWNER_INVALID),
                    case(
                        (rejected.c.owner_email.is_not(None), _text("owner_email")),
                        else_=None,
                    ),
                    staging_raw(rejected.c),
                ),
            )
            .on_conflict_do_nothing()
            .returning(ERRORS.c.row_no)
            .cte("errors")
        )
        count = await self.session.scalar(select(func.count()).select_from(errors))
        return int(count or 0)

    async def merge(
        self, job_id: uuid.UUID, lo: int, hi: int, org_id: uuid.UUID, access: Access
    ) -> MergeCounts:
        """Один statement (раздел 6, шаг merge): строки чанка → INSERT … ON CONFLICT (org_id,
        email) WHERE deleted_at IS NULL DO UPDATE … WHERE <access.sql_filter>; строки без
        результата (контакт с тем же email есть, но вне уровня автора) → import_errors
        duplicate_outside_scope. Отдел новой записи — из текущей строки ответственного
        (её чанк держит FOR SHARE).

        Вставок две — со статусом и без: пустая ячейка status не перезаписывает статус
        существующего контакта, а новый получает lead. В одной вставке их не различить:
        EXCLUDED.status — уже подставленное значение, а NULL в status не пропустит NOT NULL."""
        s = STAGING
        src = (
            select(
                s.c.row_no,
                s.c.email,
                s.c.first_name,
                s.c.last_name,
                s.c.phone,
                s.c.status,
                s.c.source,
                s.c.tags,
                s.c.owner_email,
                s.c.owner_id,
                USERS.c.team_id,
            )
            .join(USERS, USERS.c.id == s.c.owner_id)
            .where(self._chunk(job_id, lo, hi))
            .cte("src")
        )
        with_status = self._upsert(src, org_id, access, status_given=True).cte("with_status")
        without_status = self._upsert(src, org_id, access, status_given=False).cte("without_status")
        done = union_all(
            select(with_status.c.email, with_status.c.inserted),
            select(without_status.c.email, without_status.c.inserted),
        ).cte("done")
        # Нет результата — ON CONFLICT нашёл контакт, но WHERE уровня его не пустил. RETURNING
        # отдаёт email контакта (регистр может отличаться от файла) — сравнение по lower().
        merged = exists().where(func.lower(done.c.email) == func.lower(src.c.email))
        outside = (
            pg_insert(ERRORS)
            .from_select(
                list(ERROR_COLUMNS),
                select(
                    literal(job_id, Uuid),
                    src.c.row_no,
                    _text(DUPLICATE_OUTSIDE_SCOPE),
                    _text("email"),
                    staging_raw(src.c),
                ).where(~merged),
            )
            .on_conflict_do_nothing()
            .returning(ERRORS.c.row_no)
            .cte("outside")
        )
        inserted = done.c.inserted.is_(True)
        row = (
            await self.session.execute(
                select(
                    select(func.count())
                    .select_from(done)
                    .where(inserted)
                    .scalar_subquery()
                    .label("created"),
                    select(func.count())
                    .select_from(done)
                    .where(~inserted)
                    .scalar_subquery()
                    .label("updated"),
                    select(func.array_agg(outside.c.row_no)).scalar_subquery().label("outside"),
                )
            )
        ).one()
        return MergeCounts(int(row.created), int(row.updated), sorted(row.outside or []))

    @staticmethod
    def _upsert(src: Any, org_id: uuid.UUID, access: Access, *, status_given: bool) -> Any:
        """INSERT … ON CONFLICT для строк чанка со статусом или без (см. merge). Обновление
        существующего контакта (раздел 6, «Обновление существующего контакта»): непустая
        ячейка перезаписывает поле, пустая не трогает; first_name перезаписывается всегда;
        теги объединяются (MERGED_TAGS); ответственный и отдел не меняются; version + 1."""
        c = CONTACTS
        status = src.c.status if status_given else _text("lead")
        rows = (
            select(
                literal(org_id, Uuid),
                src.c.team_id,
                src.c.owner_id,
                src.c.first_name,
                func.coalesce(src.c.last_name, _text("")),
                src.c.email,
                src.c.phone,
                status,
                src.c.source,
                func.coalesce(src.c.tags, literal_column("'{}'::varchar(64)[]")),
            )
            .where(src.c.status.is_not(None) if status_given else src.c.status.is_(None))
            .order_by(src.c.row_no)  # строки контактов блокируются в постоянном порядке
        )
        insert = pg_insert(c).from_select(
            [
                "org_id",
                "team_id",
                "owner_id",
                "first_name",
                "last_name",
                "email",
                "phone",
                "status",
                "source",
                "tags",
            ],
            rows,
        )
        new = insert.excluded
        values: dict[str, Any] = {
            "first_name": new.first_name,
            # В staging пустая ячейка — NULL, во вставке она стала '': '' и значит «пусто».
            "last_name": case((new.last_name != _text(""), new.last_name), else_=c.c.last_name),
            "phone": func.coalesce(new.phone, c.c.phone),
            "source": func.coalesce(new.source, c.c.source),
            "tags": literal_column(MERGED_TAGS),
            "version": c.c.version + 1,
            "updated_at": func.now(),
        }
        if status_given:
            values["status"] = new.status
        return insert.on_conflict_do_update(
            index_elements=[c.c.org_id, c.c.email],
            index_where=c.c.deleted_at.is_(None),
            set_=values,
            where=access.sql_filter(c.c),
        ).returning(c.c.email, literal_column("xmax = 0", Boolean).label("inserted"))

    async def mark_merged(
        self, job_id: uuid.UUID, lo: int, hi: int, outside: Sequence[int]
    ) -> None:
        """Строки чанка перенесены: повтор задачи их не возьмёт. У строк без результата —
        ошибка duplicate_outside_scope (как в import_errors)."""
        s = STAGING
        error = (
            case((s.c.row_no.in_(outside), _text(DUPLICATE_OUTSIDE_SCOPE)), else_=None)
            if outside
            else None
        )
        await self.session.execute(
            update(s).where(self._chunk(job_id, lo, hi)).values(merged=True, error=error)
        )

    # --- финал

    async def delete_staging(self, job_id: uuid.UUID) -> None:
        """Строки staging job — в транзакции её финального перехода (любого)."""
        await self.session.execute(delete(ContactStaging).where(ContactStaging.job_id == job_id))
