"""File store used where the old pipeline opened S3.

OC_ETL_LOCAL_ROOT points at a directory. Keys are paths under that
directory. s3://bucket/key is stored as bucket/key inside the root, so a
dummy run never calls AWS.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dagster import resource


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _local_path(self, object_key: str) -> Path:
        # s3://bucket/key and bucket/key both land under the same root.
        stripped = object_key.removeprefix("s3://").lstrip("/")
        destination = self.root / stripped
        if not destination.resolve().is_relative_to(self.root.resolve()):
            message = f"Object key escapes the local store: {object_key}"
            raise ValueError(message)
        return destination

    def open(self, object_key: str, mode: str):
        destination = self._local_path(object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        return destination.open(mode)

    def write_text(self, object_key: str, text: str) -> None:
        destination = self._local_path(object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")

    def read_text(self, object_key: str) -> str:
        return self._local_path(object_key).read_text(encoding="utf-8")

    def list_keys(self, prefix: str) -> list[str]:
        """Keys relative to the store root, under prefix."""
        folder = self._local_path(prefix)
        if not folder.exists():
            return []
        keys = []
        for file_path in folder.rglob("*"):
            if file_path.is_file():
                keys.append(str(file_path.relative_to(self.root)))
        return keys


def local_store_root() -> Path:
    configured = os.getenv("OC_ETL_LOCAL_ROOT")
    if configured:
        return Path(configured)
    return Path.cwd() / ".local_store"


def make_s3_resource():
    """Return a local store. Live S3 is intentionally not required for a run."""

    @resource
    def object_store_resource(_context: Any) -> LocalObjectStore:
        return LocalObjectStore(local_store_root())

    return object_store_resource
