from __future__ import annotations

from pathlib import Path
from typing import Any


def verify_remote_object(s3: Any, *, bucket: str, key: str, local_path: Path) -> bool:
    response = s3.head_object(Bucket=bucket, Key=key)
    return int(response["ContentLength"]) == local_path.stat().st_size
