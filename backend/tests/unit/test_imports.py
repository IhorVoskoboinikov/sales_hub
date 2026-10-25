"""Заголовок CSV импорта и строка файла (раздел 6, «Поток: импорт контактов из CSV»): чистые
функции, без базы. Разбор файла задачей импорта — CsvRows и parse_row: номера строк, пропуск
пустых, коды ошибок строк, битый CSV."""

import codecs
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from app.errors import ValidationFailed
from app.models import BulkJob
from app.schemas.crm import ContactImportRow
from app.services.bulk import request_hash
from app.services.imports import (
    CELL_MAX_CHARS,
    FIELD_MAX,
    HEADER_BYTES,
    CsvRows,
    MalformedCsv,
    RowError,
    StagingRow,
    import_request_hash,
    parse_row,
    read_header,
)


def header_error(head: bytes, *, whole_file: bool = True) -> ValidationFailed:
    with pytest.raises(ValidationFailed) as caught:
        read_header(head, whole_file=whole_file)
    return caught.value


def test_comma_header_any_case_with_unknown_columns() -> None:
    head = b"ID,Email , First_Name,tags,created_at\r\n1,a@b.c,I,,\r\n"

    header = read_header(head, whole_file=True)

    assert header.delimiter == ","
    assert header.columns == ("id", "email", "first_name", "tags", "created_at")


def test_semicolon_wins_when_it_gives_more_columns() -> None:
    # Excel с украинской локалью: «;», а запятая — внутри тегов.
    header = read_header(b'email;first_name;tags\na@b.c;I;"vip,b2b"\n', whole_file=True)

    assert header.delimiter == ";"
    assert header.columns == ("email", "first_name", "tags")


def test_bom_and_quoted_names_are_accepted() -> None:
    header = read_header(codecs.BOM_UTF8 + b'"email","first_name"', whole_file=True)

    assert header.columns == ("email", "first_name")


@pytest.mark.parametrize("head", [b"", b"\n\n", codecs.BOM_UTF8, b"  \r\n "])
def test_empty_file(head: bytes) -> None:
    assert header_error(head).code == "import.empty_file"


def test_blank_first_line_before_data_is_invalid_header() -> None:
    assert header_error(b"\nemail,first_name\n").code == "import.invalid_header"


def test_header_not_in_utf8() -> None:
    error = header_error("email,first_name,прізвище\n".encode("cp1251"))

    assert error.code == "import.invalid_encoding"


def test_broken_bytes_after_header_are_left_to_the_task() -> None:
    # Ошибки кодировки в строках — это ошибки строк (import_errors), а не всего файла.
    header = read_header(b"email,first_name\n\xff\xfe,\xc0\n", whole_file=True)

    assert header.columns == ("email", "first_name")


def test_required_columns_are_reported() -> None:
    error = header_error(b"email,last_name\n")

    assert error.code == "import.invalid_header"
    assert error.details == {"missing": ["first_name"], "duplicates": []}


def test_known_column_twice_is_invalid_unknown_twice_is_ignored() -> None:
    error = header_error(b"email,first_name,EMAIL,note,note\n")

    assert error.details == {"missing": [], "duplicates": ["email"]}


def test_header_longer_than_buffer_is_invalid() -> None:
    head = b"email,first_name," + b"x" * HEADER_BYTES

    error = header_error(head[:HEADER_BYTES], whole_file=False)

    assert (error.code, error.details) == ("import.invalid_header", {"reason": "too_long"})


def test_unclosed_quote_in_header_is_invalid() -> None:
    error = header_error(b'"email,first_name\n')

    assert (error.code, error.details) == ("import.invalid_header", {"reason": "malformed"})


def test_import_hash_depends_only_on_kind() -> None:
    # Ключ проверяется до приёма файла: тот же ключ — та же job, какой бы файл ни пришёл.
    assert import_request_hash() == import_request_hash()
    assert import_request_hash() != request_hash("import_contacts", {})


def test_row_cells_are_normalized() -> None:
    row = ContactImportRow.model_validate(
        {
            "email": " Ivan@Example.com ",
            "first_name": "Іван",
            "last_name": "",
            "phone": "'+380671234567",  # апостроф из нашей выгрузки (CSV-инъекция)
            "status": "",
            "tags": "vip, b2b,vip,, ",
            "owner_email": "",
        }
    )

    assert row.email == "Ivan@example.com"
    assert (row.last_name, row.status, row.owner_email) == (None, None, None)
    assert row.phone == "+380671234567"
    assert row.tags == ["vip", "b2b"]


def test_row_requires_email_and_uses_contact_field_rules() -> None:
    with pytest.raises(ValidationError) as caught:
        ContactImportRow.model_validate(
            {"email": "", "first_name": "x" * 101, "tags": ",".join(f"t{i}" for i in range(21))}
        )

    assert {err["loc"][0] for err in caught.value.errors()} == {"email", "first_name", "tags"}


def test_import_info_is_read_from_params() -> None:
    job = BulkJob(
        kind="import_contacts",
        params={"delimiter": ";", "columns": ["id", "email", "first_name", "", "note"]},
    )

    assert job.import_info == {
        "delimiter": ";",
        "columns": ["email", "first_name"],
        "ignored_columns": ["id", "note"],
    }
    assert BulkJob(kind="reassign_owner", params={}).import_info is None


# --- разбор файла задачей импорта

COLUMNS = ("id", "email", "first_name", "last_name", "status", "tags", "owner_email")


def write_csv(tmp_path: Path, content: bytes) -> Path:
    path = tmp_path / "import.csv"
    path.write_bytes(content)
    return path


def read_all(path: Path, columns: tuple[str, ...] = COLUMNS, delimiter: str = ",") -> list[Any]:
    rows = CsvRows(path, delimiter, columns)
    try:
        return rows.batch(1000)
    finally:
        rows.close()


def test_valid_row_is_normalized_for_staging() -> None:
    job_id = uuid.uuid4()
    cells = ["7", " Ivan@Example.com ", "'=Іван", "", "customer", "vip, b2b,vip", ""]

    row = parse_row(5, cells, COLUMNS)

    assert isinstance(row, StagingRow)
    assert row.record(job_id) == (
        job_id,
        5,
        "Ivan@example.com",  # домен в нижнем регистре — как у POST /contacts
        "=Іван",
        None,  # пустая ячейка — NULL: у существующего контакта поле не меняется
        None,
        "customer",
        None,
        ["vip", "b2b"],
        None,
    )


def test_status_and_tags_may_be_empty() -> None:
    row = parse_row(2, ["", "a@b.c", "A", "", "", ",", ""], COLUMNS)

    assert isinstance(row, StagingRow)
    record = row.record(uuid.uuid4())
    assert (record[6], record[8]) == (None, None)  # status не указан, тегов нет


@pytest.mark.parametrize(
    ("cells", "code", "field"),
    [
        (["1", "a@b.c", "A"], "invalid_columns", None),
        (["1", "a@b.c", "A", "", "", "", "", "extra"], "invalid_columns", None),
        (["1", "a@b.c", "Ів\ufffdн", "", "", "", ""], "invalid_encoding", "first_name"),
        (["1", "  ", "A", "", "", "", ""], "email_required", "email"),
        (["1", "not-an-email", "A", "", "", "", ""], "validation", "email"),
        (["1", "a@b.c", "", "", "", "", ""], "validation", "first_name"),
        (["1", "a@b.c", "A", "x" * 101, "", "", ""], "validation", "last_name"),
        (["1", "a@b.c", "A", "", "vip", "", ""], "validation", "status"),
        (
            ["1", "a@b.c", "A", "", "", ",".join(f"t{i}" for i in range(21)), ""],
            "validation",
            "tags",
        ),
        (["1", "a@b.c", "A", "", "", "", "boss"], "validation", "owner_email"),
    ],
)
def test_row_errors(cells: list[str], code: str, field: str | None) -> None:
    row = parse_row(3, cells, COLUMNS)

    assert isinstance(row, RowError)
    assert (row.row_no, row.code, row.field) == (3, code, field)
    # raw — ячейки известных колонок, как в файле (id не входит).
    assert row.raw == {c: v for c, v in zip(COLUMNS, cells, strict=False) if c != "id"}


def test_nul_is_invalid_encoding_and_replaced_in_raw() -> None:
    # Postgres не хранит NUL ни в text, ни в jsonb: строка — ошибка, а raw — без него.
    row = parse_row(3, ["1", "a@b.c", "A\x00B", "", "", "", ""], COLUMNS)
    short = parse_row(4, ["1", "a@b.c", "\x00"], COLUMNS)

    assert isinstance(row, RowError) and isinstance(short, RowError)
    assert (row.code, row.field, row.raw["first_name"]) == ("invalid_encoding", "first_name", "A�B")
    assert (short.code, short.raw) == ("invalid_columns", {"email": "a@b.c", "first_name": "�"})


def test_broken_bytes_in_unknown_column_do_not_spoil_row() -> None:
    # id — незнакомая колонка: её значения не хранятся, поэтому и не проверяются.
    broken = parse_row(3, ["\ufffd", "a@b.c", "A", "", "", "", ""], COLUMNS)
    nul = parse_row(4, ["1\x002", "a@b.c", "A", "", "", "", ""], COLUMNS)

    assert isinstance(broken, StagingRow) and isinstance(nul, StagingRow)
    assert broken.row.first_name == "A"


def test_error_record_has_json_raw() -> None:
    job_id = uuid.uuid4()
    error = RowError(4, "validation", "email", {"email": "Іван"})

    record = error.record(job_id)

    assert record[:4] == (job_id, 4, "validation", "email")
    assert json.loads(record[4]) == {"email": "Іван"}
    assert RowError(9, "malformed_csv", None, None).record(job_id)[4] is None


def test_rows_skip_header_and_empty_lines_and_count_file_lines(tmp_path: Path) -> None:
    content = (
        "id,email,first_name,last_name,status,tags,owner_email\r\n"
        "1,a@b.c,A,,,,\r\n"
        "\r\n"
        ",,,,,,\r\n"  # Excel сохраняет пустые строки таблицы так
        '2,b@b.c,"Ім\'я\nз переносом",,,,\r\n'
        "3,c@b.c,C,,,,\r\n"
    )
    path = write_csv(tmp_path, codecs.BOM_UTF8 + content.encode())

    rows = read_all(path)

    # Номер строки файла, с которой начинается запись: заголовок — 1, многострочная — 5.
    assert [r.row_no for r in rows] == [2, 5, 7]
    assert all(isinstance(r, StagingRow) for r in rows)
    assert rows[1].row.first_name == "Ім'я\nз переносом"


def test_rows_come_in_batches(tmp_path: Path) -> None:
    lines = ["email,first_name", *(f"u{i}@b.c,U" for i in range(5))]
    rows = CsvRows(write_csv(tmp_path, "\n".join(lines).encode()), ",", ("email", "first_name"))
    try:
        sizes = [len(rows.batch(2)) for _ in range(4)]
    finally:
        rows.close()

    assert sizes == [2, 2, 1, 0]


def test_bytes_not_in_utf8_spoil_only_their_row(tmp_path: Path) -> None:
    path = write_csv(tmp_path, b"email;first_name\na@b.c;\xc4\xe5\xed\nb@b.c;B\n")

    rows = read_all(path, ("email", "first_name"), ";")

    assert isinstance(rows[0], RowError) and rows[0].code == "invalid_encoding"
    assert isinstance(rows[1], StagingRow)


def test_unclosed_quote_is_malformed_with_row_number(tmp_path: Path) -> None:
    path = write_csv(tmp_path, b'email,first_name\na@b.c,A\nb@b.c,"B\nc@b.c,C\n')

    with pytest.raises(MalformedCsv) as caught:
        read_all(path, ("email", "first_name"))

    assert caught.value.row_no == 3


def test_field_longer_than_limit_is_malformed(tmp_path: Path) -> None:
    long = "x" * (FIELD_MAX + 1)
    path = write_csv(tmp_path, f"email,first_name\na@b.c,A\nb@b.c,{long}\n".encode())

    with pytest.raises(MalformedCsv) as caught:
        read_all(path, ("email", "first_name"))

    assert caught.value.row_no == 3


def test_quoted_field_of_doubled_quotes_fits_record_limit(tmp_path: Path) -> None:
    # Самая длинная допустимая ячейка в файле: FIELD_MAX кавычек, каждая сдвоена.
    quotes = '"' + '""' * FIELD_MAX + '"'
    path = write_csv(tmp_path, f"email,first_name\r\na@b.c,{quotes}\r\n".encode())

    [row] = read_all(path, ("email", "first_name"))

    assert isinstance(row, RowError) and row.field == "first_name"  # длиннее 100 — validation


def test_record_wider_than_limit_is_malformed_without_reading_it(tmp_path: Path) -> None:
    # Одна строка из одних разделителей — без предела csv.reader построил бы список на каждую
    # ячейку; запись длиннее CELL_MAX_CHARS на колонку — malformed_csv с её номером.
    wide = "," * (2 * CELL_MAX_CHARS + 1)
    path = write_csv(tmp_path, f"email,first_name\na@b.c,A\n{wide}\nc@b.c,C\n".encode())

    with pytest.raises(MalformedCsv) as caught:
        read_all(path, ("email", "first_name"))

    assert caught.value.row_no == 3


def test_record_limit_counts_all_lines_of_multiline_record(tmp_path: Path) -> None:
    # Короткие строки файла, но одна запись: ячейки в кавычках с переводом строки внутри.
    cell = ',"x\ny"'
    cells = cell * (2 * CELL_MAX_CHARS // len(cell) + 1)
    path = write_csv(tmp_path, f"email,first_name\na@b.c{cells}\nc@b.c,C\n".encode())

    with pytest.raises(MalformedCsv) as caught:
        read_all(path, ("email", "first_name"))

    assert caught.value.row_no == 2
