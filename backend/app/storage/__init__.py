"""Файлы: только через S3 API (раздел 12) — локально RustFS, в облаке S3 или GCS."""

from app.storage.s3 import FileStorage, S3FileStorage

__all__ = ["FileStorage", "S3FileStorage"]
