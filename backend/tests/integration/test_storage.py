"""Хранилище файлов на настоящем RustFS (разделы 11 и 12): тот же образ, что в compose.

Проверяем то, ради чего у S3FileStorage свои настройки: path-style адреса и контрольные суммы
when_required (иначе RustFS отверг бы PUT), потоковое скачивание больше одного куска,
идемпотентные delete и ensure_bucket, недоступное хранилище — ServiceUnavailable.
"""

import os
import tempfile
import uuid
from pathlib import Path

import pytest
from pydantic import SecretStr

from app.cli import ensure_bucket_command
from app.core.config import Settings
from app.errors import NotFound, ServiceUnavailable
from app.storage import S3FileStorage, s3
from tests.conftest import CLOSED_PORT
from tests.integration.conftest import RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RustFs, s3_storage


def spooled(data: bytes) -> tempfile.SpooledTemporaryFile[bytes]:
    """Как файл формы Starlette: в памяти до 1 МиБ, дальше — на диске."""
    file = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)  # noqa: SIM115 — закрывает тест
    file.write(data)
    file.seek(0)
    return file


async def test_put_download_delete(storage: S3FileStorage, tmp_path: Path) -> None:
    key = f"imports/{uuid.uuid4()}/{uuid.uuid4()}.csv"
    data = os.urandom(3 * s3.DOWNLOAD_CHUNK_BYTES + 17)  # скачивается несколькими кусками
    target = tmp_path / "import.csv"

    with spooled(data) as file:
        await storage.put(key, file, len(data), "text/csv")
    await storage.download_to(key, target)

    assert target.read_bytes() == data
    await storage.delete(key)
    with pytest.raises(NotFound):
        await storage.download_to(key, target)
    await storage.delete(key)  # уже нет — не ошибка


async def test_download_of_missing_file_is_not_found(
    storage: S3FileStorage, tmp_path: Path
) -> None:
    with pytest.raises(NotFound) as caught:
        await storage.download_to("imports/none.csv", tmp_path / "x.csv")

    assert caught.value.details == {"entity": "file", "key": "imports/none.csv"}


async def test_ensure_bucket_is_idempotent(rustfs: RustFs) -> None:
    fresh = s3_storage(rustfs, bucket=f"b-{uuid.uuid4().hex[:12]}")
    try:
        await fresh.ensure_bucket()  # создаёт
        await fresh.ensure_bucket()  # уже есть — ничего
        with spooled(b"email,first_name\n") as file:
            await fresh.put("k.csv", file, 17, "text/csv")  # бакет рабочий
    finally:
        await fresh.aclose()


async def test_unreachable_storage_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3, "ENSURE_BUCKET_SECONDS", 0.0)  # не ждать 30 с, как migrate
    down = S3FileStorage(
        endpoint_url=f"http://127.0.0.1:{CLOSED_PORT}",
        access_key="x",
        secret_key="y",
        bucket="saleshub",
        region="us-east-1",
    )
    try:
        with pytest.raises(ServiceUnavailable) as ensure:
            await down.ensure_bucket()
        with spooled(b"x") as file, pytest.raises(ServiceUnavailable) as put:
            await down.put("k.csv", file, 1, "text/csv")
    finally:
        await down.aclose()

    assert ensure.value.details == put.value.details == {"service": "storage"}


async def test_ensure_bucket_command(rustfs: RustFs, settings: Settings) -> None:
    configured = settings.model_copy(
        update={
            "s3_endpoint_url": rustfs.endpoint_url,
            "s3_access_key": RUSTFS_ACCESS_KEY,
            "s3_secret_key": SecretStr(RUSTFS_SECRET_KEY),
            "s3_bucket": f"cli-{uuid.uuid4().hex[:12]}",
        }
    )

    assert await ensure_bucket_command(configured) == 0
    assert await ensure_bucket_command(configured) == 0  # повторный запуск migrate
