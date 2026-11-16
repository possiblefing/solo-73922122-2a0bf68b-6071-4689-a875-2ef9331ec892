"""Durable storage of original uploaded CSV files.

The raw bytes are written to a content-addressed, nested directory
(``ab/cd/<sha256>.csv``) *before* any database row representing the batch is
inserted.  A successful return therefore means the original artifact has
survived on disk and its SHA-256 is known, which is what makes retry
re-use safe: identical bytes always land at the same path.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

from .config import settings


@dataclass(frozen=True)
class StoredArtifact:
    sha256: str
    storage_path: str
    size_bytes: int


def _target_path(sha: str) -> str:
    return os.path.join(settings.storage_dir, sha[:2], sha[2:4], f"{sha}.csv")


def persist_original(raw: bytes) -> StoredArtifact:
    sha = hashlib.sha256(raw).hexdigest()
    path = _target_path(sha)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        # Temp file + fsync + atomic rename: a crash never leaves a torn file
        # at the final path.
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "wb") as fh:
            fh.write(raw)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    # fsync the directory so the rename itself is durable.
    dir_fd = os.open(os.path.dirname(path), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return StoredArtifact(sha256=sha, storage_path=path, size_bytes=len(raw))
