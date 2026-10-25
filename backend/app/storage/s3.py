"""Хранилище файлов: порт FileStorage и его реализация на S3 API (aiobotocore).

Клиент один на процесс (он держит пул соединений aiohttp) и живёт в Resources. Создаётся лениво —
при первом обращении, уже внутри работающего event loop: Resources.create синхронный, а у
воркера loop свой на процесс (раздел 15). Закрывается в aclose() через AsyncExitStack — так же,
как его открыл бы `async with session.create_client(...)`.

Для RustFS и других S3-совместимых хранилищ (раздел 12): path-style адреса
(http://rustfs:9000/saleshub/<key>, а не http://saleshub.rustfs:9000/<key> — такого имени в
compose нет) и контрольные суммы только там, где их требует API (when_required): новые botocore
по умолчанию считают CRC32 каждого PUT, а часть хранилищ его не понимает.

Ошибки: временный сбой (нет соединения, таймаут, обрыв, 5xx, SlowDown) после ретраев botocore —
ServiceUnavailable: API ответит 503, а BaseTask воркера повторит задачу. Остальные (нет доступа,
нет бакета) — ошибка настройки, они уходят как есть и дают 500.
"""

import asyncio
from contextlib import AsyncExitStack
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, NoReturn, Protocol, cast

import aiohttp
import botocore.exceptions
import structlog
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

from app.core.config import Settings
from app.errors import NotFound, ServiceUnavailable

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client

logger = structlog.get_logger("storage")

CONNECT_TIMEOUT_SECONDS = 5
# Пауза в чтении ответа, а не время всего запроса: файл импорта до 100 МиБ идёт потоком.
READ_TIMEOUT_SECONDS = 60
# Режим standard: повтор с паузой на обрыв, таймаут, 5xx и SlowDown. Файл-тело botocore перед
# повтором перематывает сам (seek), поэтому оно должно поддерживать seek.
MAX_ATTEMPTS = 3
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
# ensure-bucket запускается сразу после старта compose: RustFS может ещё подниматься.
ENSURE_BUCKET_SECONDS = 30.0
ENSURE_BUCKET_PAUSE_SECONDS = 1.0

# Бакет уже есть: наш (повторный запуск) или занят (у S3 имена бакетов глобальные).
BUCKET_EXISTS = frozenset({"BucketAlreadyOwnedByYou", "BucketAlreadyExists"})
NOT_FOUND = frozenset({"NoSuchKey", "404"})
BUCKET_MISSING = frozenset({"NoSuchBucket", "404"})  # HEAD без тела: код — просто статус
RETRYABLE_CODES = frozenset({"SlowDown", "RequestTimeout", "ServiceUnavailable", "InternalError"})


class FileStorage(Protocol):
    """Файлы по ключу (imports/{org_id}/{uuid}.csv). В тестах API — фейк в памяти."""

    async def put(self, key: str, fileobj: BinaryIO, size: int, content_type: str) -> None:
        """Загрузить size байт из fileobj (с текущей позиции, файл должен поддерживать seek)."""
        ...

    async def download_to(self, key: str, path: Path) -> None:
        """Скачать файл в path (перезаписывает). Нет файла — NotFound."""
        ...

    async def delete(self, key: str) -> None:
        """Удалить файл. Нет файла — не ошибка."""
        ...

    async def ensure_bucket(self) -> None:
        """Создать бакет, если его нет (идемпотентно): RustFS сам бакеты не создаёт."""
        ...

    async def aclose(self) -> None: ...


def is_transient(exc: Exception) -> bool:
    """Временный ли сбой хранилища: после паузы запрос может пройти."""
    if isinstance(exc, ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        return status >= 500 or exc.response.get("Error", {}).get("Code") in RETRYABLE_CODES
    return isinstance(
        exc,
        botocore.exceptions.ConnectionError  # нет соединения, таймаут подключения
        | botocore.exceptions.HTTPClientError  # обрыв, таймаут чтения
        | aiohttp.ClientError  # обрыв посреди потока тела ответа
        | TimeoutError,
    )


def error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def reraise(exc: Exception, operation: str, key: str) -> NoReturn:
    """Временный сбой — ServiceUnavailable (503 в API, повтор задачи в воркере), остальное —
    как есть."""
    if is_transient(exc):
        logger.warning("storage_unavailable", operation=operation, key=key, error=repr(exc))
        raise ServiceUnavailable(service="storage") from exc
    raise exc


class S3FileStorage:
    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str,
    ) -> None:
        self.bucket = bucket
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._stack = AsyncExitStack()
        self._client: S3Client | None = None
        self._lock = asyncio.Lock()  # два первых запроса одновременно не создадут два клиента

    @classmethod
    def from_settings(cls, settings: Settings) -> "S3FileStorage":
        return cls(
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key.get_secret_value(),
            bucket=settings.s3_bucket,
            region=settings.s3_region,
        )

    async def _s3(self) -> "S3Client":
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    creator = get_session().create_client(
                        "s3",
                        endpoint_url=self._endpoint_url,
                        region_name=self._region,
                        aws_access_key_id=self._access_key,
                        aws_secret_access_key=self._secret_key,
                        config=AioConfig(
                            s3={"addressing_style": "path"},
                            request_checksum_calculation="when_required",
                            response_checksum_validation="when_required",
                            connect_timeout=CONNECT_TIMEOUT_SECONDS,
                            read_timeout=READ_TIMEOUT_SECONDS,
                            retries={"max_attempts": MAX_ATTEMPTS, "mode": "standard"},
                        ),
                    )
                    client = await self._stack.enter_async_context(creator)
                    self._client = cast("S3Client", client)
        return self._client

    async def aclose(self) -> None:
        await self._stack.aclose()
        self._client = None

    async def put(self, key: str, fileobj: BinaryIO, size: int, content_type: str) -> None:
        s3 = await self._s3()
        try:
            await s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=fileobj,
                ContentLength=size,
                ContentType=content_type,
            )
        except Exception as exc:
            reraise(exc, "put", key)
        logger.info("file_stored", key=key, size=size)

    async def download_to(self, key: str, path: Path) -> None:
        s3 = await self._s3()
        try:
            response = await s3.get_object(Bucket=self.bucket, Key=key)
            async with response["Body"] as stream:
                # Запись на диск — в потоке: файл импорта до 100 МиБ не держит event loop.
                out = await asyncio.to_thread(path.open, "wb")
                try:
                    while chunk := await stream.read(DOWNLOAD_CHUNK_BYTES):
                        await asyncio.to_thread(out.write, chunk)
                finally:
                    await asyncio.to_thread(out.close)
        except Exception as exc:
            if isinstance(exc, ClientError) and error_code(exc) in NOT_FOUND:
                raise NotFound(entity="file", key=key) from exc
            reraise(exc, "download", key)

    async def delete(self, key: str) -> None:
        s3 = await self._s3()
        try:
            await s3.delete_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            reraise(exc, "delete", key)

    async def ensure_bucket(self) -> None:
        """Создать бакет, если его нет. Пока хранилище не отвечает (RustFS стартует), повторять
        до ENSURE_BUCKET_SECONDS, потом — ServiceUnavailable."""
        s3 = await self._s3()
        deadline = asyncio.get_running_loop().time() + ENSURE_BUCKET_SECONDS
        while True:
            try:
                created = await self._create_bucket(s3)
            except Exception as exc:
                if not is_transient(exc) or asyncio.get_running_loop().time() >= deadline:
                    reraise(exc, "ensure_bucket", self.bucket)
                logger.info("storage_not_ready", bucket=self.bucket, error=repr(exc))
                await asyncio.sleep(ENSURE_BUCKET_PAUSE_SECONDS)
                continue
            logger.info("bucket_created" if created else "bucket_exists", bucket=self.bucket)
            return

    async def _create_bucket(self, s3: "S3Client") -> bool:
        """True — бакет создан сейчас, False — уже был."""
        try:
            await s3.head_bucket(Bucket=self.bucket)
            return False
        except ClientError as exc:
            if error_code(exc) not in BUCKET_MISSING:
                raise
        try:
            if self._region == "us-east-1":
                # Регион по умолчанию: LocationConstraint для него S3 не принимает.
                await s3.create_bucket(Bucket=self.bucket)
            else:
                await s3.create_bucket(
                    Bucket=self.bucket,
                    CreateBucketConfiguration={"LocationConstraint": self._region},  # type: ignore[typeddict-item]
                )
        except ClientError as exc:
            if error_code(exc) in BUCKET_EXISTS:  # создал параллельный запуск
                return False
            raise
        return True
