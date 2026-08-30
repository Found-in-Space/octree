from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from foundinspace.octree.s3_resume.util import (
    atomic_write_json,
    detect_source,
    fingerprint_head_tail,
    iter_part_plan,
    now_iso,
)


def load_state(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    atomic_write_json(path, json.dumps(state, indent=2, sort_keys=True) + "\n")


def make_new_state(
    *,
    source_path: Path,
    bucket: str,
    key: str,
    region: str | None,
    profile: str | None,
    part_size: int,
    checksum_algorithm: str,
    object_params: dict[str, Any],
) -> dict[str, Any]:
    size, mtime_ns = detect_source(source_path)
    parts = {
        str(part_number): {
            "offset": offset,
            "length": length,
            "status": "pending",
            "attempts": 0,
        }
        for part_number, offset, length in iter_part_plan(size, part_size)
    }
    created_at = now_iso()
    return {
        "version": 1,
        "created_at": created_at,
        "updated_at": created_at,
        "upload_state": "NEW",
        "source": {
            "path": str(source_path),
            "size": size,
            "mtime_ns": mtime_ns,
            "fingerprint": fingerprint_head_tail(source_path),
        },
        "target": {
            "bucket": bucket,
            "key": key,
            "region": region,
        },
        "aws": {"profile": profile},
        "multipart": {
            "upload_id": None,
            "part_size": part_size,
            "checksum_algorithm": checksum_algorithm.upper()
            if checksum_algorithm != "none"
            else "NONE",
            "initiated_at": None,
        },
        "object_params": object_params,
        "parts": parts,
        "complete": False,
    }


def validate_source_unchanged(
    state: dict[str, Any],
    *,
    force_source_changed: bool,
) -> None:
    source_path = Path(state["source"]["path"])
    if not source_path.exists():
        raise ValueError(f"source file is missing: {source_path}")
    size, mtime_ns = detect_source(source_path)
    if (
        size != state["source"]["size"] or mtime_ns != state["source"]["mtime_ns"]
    ) and not force_source_changed:
        raise ValueError("source file changed; use --force-source-changed to proceed")
    current_fp = fingerprint_head_tail(source_path)
    saved_fp = state["source"].get("fingerprint")
    if saved_fp and current_fp != saved_fp and not force_source_changed:
        raise ValueError(
            "source fingerprint changed; use --force-source-changed to proceed"
        )


def expected_parts(state: dict[str, Any]) -> list[int]:
    return sorted(int(p) for p in state["parts"])


def uploaded_parts_for_completion(state: dict[str, Any]) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    checksum_algorithm = state["multipart"].get("checksum_algorithm", "NONE")
    for part_number in expected_parts(state):
        part = state["parts"][str(part_number)]
        etag = part.get("etag")
        if not etag:
            raise ValueError(f"missing ETag for part {part_number}")
        completion_part = {"PartNumber": part_number, "ETag": etag}
        if checksum_algorithm != "NONE":
            checksum = part.get("checksum")
            if not checksum:
                raise ValueError(f"missing checksum for part {part_number}")
            completion_part[f"Checksum{checksum_algorithm}"] = checksum
        parts.append(completion_part)
    return parts


def merge_remote_parts(
    state: dict[str, Any], remote_parts: dict[int, dict[str, Any]]
) -> None:
    for part_number in expected_parts(state):
        key = str(part_number)
        local = state["parts"][key]
        remote = remote_parts.get(part_number)
        if remote:
            local["status"] = "uploaded"
            local["etag"] = remote["ETag"]
            if remote.get("Checksum"):
                local["checksum"] = remote["Checksum"]
        else:
            local.pop("etag", None)
            local.pop("checksum", None)
            local["status"] = "pending"
