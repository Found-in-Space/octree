from __future__ import annotations

import base64
import hashlib
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

MiB = 1024 * 1024
MIN_PART_SIZE = 5 * MiB
MAX_PARTS = 10_000

ChecksumMode = Literal["none", "crc32c", "sha256"]


@dataclass(frozen=True)
class S3Target:
    bucket: str
    key: str


def parse_s3_uri(uri: str) -> S3Target:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {uri}")
    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError(f"Invalid S3 URI (missing key): {uri}")
    return S3Target(bucket=parsed.netloc, key=key)


def choose_part_size_bytes(file_size: int, part_size_mib: int) -> int:
    if file_size <= 0:
        raise ValueError("file_size must be > 0")
    part_size = max(part_size_mib * MiB, MIN_PART_SIZE)
    while math.ceil(file_size / part_size) > MAX_PARTS:
        part_size *= 2
    return part_size


def part_count(file_size: int, part_size: int) -> int:
    return math.ceil(file_size / part_size)


def iter_part_plan(file_size: int, part_size: int) -> list[tuple[int, int, int]]:
    total = part_count(file_size, part_size)
    plan: list[tuple[int, int, int]] = []
    for idx in range(total):
        offset = idx * part_size
        length = min(part_size, file_size - offset)
        plan.append((idx + 1, offset, length))
    return plan


def now_iso() -> str:
    # UTC, second precision keeps state stable/readable.
    return (
        __import__("datetime")
        .datetime.now(tz=__import__("datetime").timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def detect_source(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def fingerprint_head_tail(path: Path, chunk_bytes: int = MiB) -> dict[str, str]:
    size = path.stat().st_size
    with path.open("rb") as f:
        head = f.read(min(chunk_bytes, size))
        if size > chunk_bytes:
            f.seek(max(0, size - chunk_bytes))
            tail = f.read(min(chunk_bytes, size))
        else:
            tail = head
    return {
        "mode": "size+mtime+headtail",
        "head_sha256": hashlib.sha256(head).hexdigest(),
        "tail_sha256": hashlib.sha256(tail).hexdigest(),
    }


def checksum_for_part(data: bytes, mode: ChecksumMode) -> tuple[str | None, str | None]:
    if mode == "none":
        return None, None
    if mode == "sha256":
        b64 = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        return "ChecksumSHA256", b64
    if mode == "crc32c":
        try:
            import google_crc32c
        except ImportError as exc:  # pragma: no cover - env dependent
            raise RuntimeError(
                "checksum mode 'crc32c' requires 'google-crc32c' dependency"
            ) from exc
        value = google_crc32c.value(data).to_bytes(4, byteorder="big", signed=False)
        b64 = base64.b64encode(value).decode("ascii")
        return "ChecksumCRC32C", b64
    raise ValueError(f"Unsupported checksum mode: {mode}")


def default_state_path(local_file: Path) -> Path:
    return local_file.with_name(f".{local_file.name}.s3mpu.json")


def atomic_write_json(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)
