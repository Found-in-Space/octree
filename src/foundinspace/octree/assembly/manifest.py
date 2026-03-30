from __future__ import annotations

import json
import os
from pathlib import Path

from ..config import WORLD_CENTER, WORLD_HALF_SIZE_PC
from .formats import (
    INDEX_FILE_HDR,
    INDEX_HEADER_SIZE,
    INDEX_RECORD,
    INDEX_VERSION,
    MANIFEST_FORMAT,
    PAYLOAD_CODEC,
)


def manifest_path(out_dir: Path, *, name: str = "manifest.json") -> Path:
    return out_dir / name


def _validate_index_payload_pair(
    out_dir: Path,
    index_rel: str,
    payload_rel: str,
    *,
    expected_magic: bytes,
) -> None:
    index_path = out_dir / index_rel
    payload_path = out_dir / payload_rel

    if not index_path.exists():
        raise ValueError(f"Index file missing: {index_path}")
    if not payload_path.exists():
        raise ValueError(f"Payload file missing: {payload_path}")

    payload_size = payload_path.stat().st_size

    with open(index_path, "rb") as f:
        hdr_data = f.read(INDEX_FILE_HDR.size)
        if len(hdr_data) < INDEX_FILE_HDR.size:
            raise ValueError(f"Index file too small for header: {index_path}")

        (
            magic,
            version,
            header_size,
            _level,
            _prefix_bits,
            _flags,
            record_size,
            _prefix,
            record_count,
        ) = INDEX_FILE_HDR.unpack(hdr_data)

        if magic != expected_magic:
            raise ValueError(f"Bad magic in {index_path}: {magic!r}")
        if version != INDEX_VERSION:
            raise ValueError(f"Bad version in {index_path}: {version}")
        if header_size != INDEX_HEADER_SIZE:
            raise ValueError(f"Bad header_size in {index_path}: {header_size}")
        if record_size != INDEX_RECORD.size:
            raise ValueError(f"Bad record_size in {index_path}: {record_size}")

        expected_bytes = record_count * INDEX_RECORD.size
        actual_bytes = index_path.stat().st_size - INDEX_HEADER_SIZE
        if actual_bytes != expected_bytes:
            raise ValueError(
                f"Record count mismatch in {index_path}: header says "
                f"{record_count}, file has room for "
                f"{actual_bytes // INDEX_RECORD.size}"
            )

        prev_node_id: int | None = None
        for i in range(record_count):
            rec_data = f.read(INDEX_RECORD.size)
            node_id, pay_off, pay_len, _star_count = INDEX_RECORD.unpack(rec_data)

            if prev_node_id is not None and node_id <= prev_node_id:
                raise ValueError(
                    f"Non-ascending node_id at record {i} in {index_path}: "
                    f"{node_id} <= {prev_node_id}"
                )
            if pay_off + pay_len > payload_size:
                raise ValueError(
                    f"Payload bounds exceeded at record {i} in {index_path}: "
                    f"offset {pay_off} + length {pay_len} > "
                    f"file size {payload_size}"
                )
            prev_node_id = node_id


def manifest_entries(manifest: dict) -> list[dict]:
    entries: list[dict] = []
    for level_entry in manifest.get("levels", []):
        level = int(level_entry["level"])
        for shard in level_entry.get("shards", []):
            entries.append(
                {
                    "level": level,
                    "prefix_bits": int(shard["prefix_bits"]),
                    "prefix": int(shard["prefix"]),
                    "index_path": str(shard["index_path"]),
                    "payload_path": str(shard["payload_path"]),
                    "record_count": int(shard["record_count"]),
                }
            )
    return entries


def read_manifest(out_dir: Path, *, name: str = "manifest.json") -> dict | None:
    path = manifest_path(out_dir, name=name)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def read_manifest_file(path: Path) -> dict:
    return json.loads(path.read_text())


def validate_shard(out_dir: Path, entry: dict, *, expected_magic: bytes) -> None:
    _validate_index_payload_pair(
        out_dir,
        str(entry["index_path"]),
        str(entry["payload_path"]),
        expected_magic=expected_magic,
    )


def write_manifest(
    out_dir: Path,
    max_level: int,
    shard_entries: list[dict],
    *,
    artifact_kind: str,
    index_magic: bytes,
    mag_limit: float,
    name: str = "manifest.json",
) -> Path:
    for entry in shard_entries:
        validate_shard(out_dir, entry, expected_magic=index_magic)

    levels_map: dict[int, list[dict]] = {}
    for entry in shard_entries:
        levels_map.setdefault(entry["level"], []).append(entry)

    levels = []
    for lvl in sorted(levels_map):
        shards = sorted(levels_map[lvl], key=lambda e: e["prefix"])
        level_shards = []
        for s in shards:
            level_shards.append(
                {
                    "prefix_bits": s["prefix_bits"],
                    "prefix": s["prefix"],
                    "index_path": s["index_path"],
                    "payload_path": s["payload_path"],
                    "record_count": s["record_count"],
                }
            )
        levels.append(
            {
                "level": lvl,
                "shards": level_shards,
            }
        )

    manifest = {
        "format": MANIFEST_FORMAT,
        "artifact_kind": artifact_kind,
        "index_magic": index_magic.decode("ascii"),
        "world_center": WORLD_CENTER.tolist(),
        "world_half_size_pc": WORLD_HALF_SIZE_PC,
        "max_level": max_level,
        "mag_limit": float(mag_limit),
        "payload_codec": PAYLOAD_CODEC,
        "index_header_struct": INDEX_FILE_HDR.format,
        "index_record_struct": INDEX_RECORD.format,
        "levels": levels,
    }

    out_manifest = manifest_path(out_dir, name=name)
    tmp_path = out_dir / f".{name}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    os.replace(tmp_path, out_manifest)
    return out_manifest
