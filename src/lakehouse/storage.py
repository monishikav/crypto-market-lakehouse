"""Tiny storage abstraction: the same ingestion code writes to ./data or to S3.

Jobs only ever say "write these bytes to key X in layer Y". Whether that becomes a
local file or an S3 object is decided once, by STORAGE_MODE in .env. This is how real
pipelines stay testable locally and deployable to the cloud without code changes.

Local writes are *atomic*: bytes go to a temp file first, then os.replace() swaps it into
place. A crash mid-write therefore never leaves a half-written file that a later run
would mistake for a finished one - which is what makes "skip if exists" safe.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

from lakehouse.config import Settings


class Storage(Protocol):
    def exists(self, key: str) -> bool: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def read_bytes(self, key: str) -> bytes: ...
    def uri(self, key: str) -> str: ...


class LocalStorage:
    def __init__(self, root: Path | str):
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def write_bytes(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)  # atomic on the same filesystem (Windows + Linux)

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def uri(self, key: str) -> str:
        return self._path(key).as_posix()


class S3Storage:
    """S3 PUTs are already atomic: an object is either fully there or not at all."""

    def __init__(self, bucket: str, prefix: str, region: str):
        import boto3

        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.client = boto3.client("s3", region_name=region)

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def write_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def read_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._key(key)}"


def get_storage(settings: Settings, layer: str) -> Storage:
    if settings.storage_mode == "s3":
        return S3Storage(settings.s3_bucket, layer, settings.aws_region)
    return LocalStorage(settings.layer_path(layer))
