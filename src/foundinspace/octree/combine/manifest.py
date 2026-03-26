from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ..assembly.formats import (
    INDEX_FILE_HDR,
    INDEX_RECORD,
    MANIFEST_FORMAT,
    PAYLOAD_CODEC,
)
from ..assembly.manifest import validate_shard
from ..assembly.types import ShardKey


@dataclass(frozen=True, slots=True)
class ShardEntry:
    key: ShardKey
    index_path: Path
    payload_path: Path
    record_count: int


@dataclass(frozen=True, slots=True)
class CombineManifest:
    manifest_path: Path
    root_dir: Path
    max_level: int
    world_center: tuple[float, float, float]
    world_half_size_pc: float
    mag_limit: float
    payload_codec: str
    shards: tuple[ShardEntry, ...]


def _parse_world_center(raw: object) -> tuple[float, float, float]:
    if not isinstance(raw, list) or len(raw) != 3:
        raise ValueError("Manifest world_center must be a 3-element list")
    return (float(raw[0]), float(raw[1]), float(raw[2]))


def manifest_has_meta(manifest_path: Path) -> bool:
    """True only when the manifest has shards AND every shard has meta paths."""
    raw = json.loads(manifest_path.read_text())
    has_any = False
    for level_entry in raw.get("levels", []):
        for shard in level_entry.get("shards", []):
            has_any = True
            if "meta_index_path" not in shard:
                return False
    return has_any


def read_combine_manifest(
    manifest_path: Path,
    *,
    payload_kind: str = "render",
) -> CombineManifest:
    raw = json.loads(manifest_path.read_text())
    got_format = str(raw.get("format", ""))
    if got_format != MANIFEST_FORMAT:
        raise ValueError(
            f"Unsupported manifest format: {got_format!r} != {MANIFEST_FORMAT!r}"
        )
    got_index_hdr = str(raw.get("index_header_struct", ""))
    if got_index_hdr != INDEX_FILE_HDR.format:
        raise ValueError(
            "Manifest index_header_struct mismatch: "
            f"{got_index_hdr!r} != {INDEX_FILE_HDR.format!r}"
        )
    got_index_rec = str(raw.get("index_record_struct", ""))
    if got_index_rec != INDEX_RECORD.format:
        raise ValueError(
            "Manifest index_record_struct mismatch: "
            f"{got_index_rec!r} != {INDEX_RECORD.format!r}"
        )
    root_dir = manifest_path.parent
    max_level = int(raw["max_level"])
    world_center = _parse_world_center(raw["world_center"])
    world_half_size_pc = float(raw["world_half_size_pc"])
    payload_codec = str(raw.get("payload_codec", ""))
    if payload_codec != PAYLOAD_CODEC:
        raise ValueError(
            f"Unsupported payload codec: {payload_codec!r} != {PAYLOAD_CODEC!r}"
        )
    if "mag_limit" not in raw:
        raise ValueError("Manifest is missing required field: mag_limit")
    mag_limit = float(raw["mag_limit"])

    shards: list[ShardEntry] = []
    for level_entry in raw.get("levels", []):
        level = int(level_entry["level"])
        for shard in level_entry.get("shards", []):
            entry_dict = {
                "level": level,
                "prefix_bits": int(shard["prefix_bits"]),
                "prefix": int(shard["prefix"]),
                "index_path": str(shard["index_path"]),
                "payload_path": str(shard["payload_path"]),
                "record_count": int(shard["record_count"]),
            }
            if "meta_index_path" in shard:
                entry_dict["meta_index_path"] = str(shard["meta_index_path"])
                entry_dict["meta_payload_path"] = str(shard["meta_payload_path"])
            validate_shard(root_dir, entry_dict)
            key = ShardKey(
                level=level,
                prefix_bits=entry_dict["prefix_bits"],
                prefix=entry_dict["prefix"],
            )
            if payload_kind == "meta":
                if "meta_index_path" not in entry_dict:
                    raise ValueError(
                        f"Shard at level {level} prefix={shard['prefix']} "
                        "missing meta paths for meta combine"
                    )
                idx_path = root_dir / entry_dict["meta_index_path"]
                pay_path = root_dir / entry_dict["meta_payload_path"]
            else:
                idx_path = root_dir / entry_dict["index_path"]
                pay_path = root_dir / entry_dict["payload_path"]
            shards.append(
                ShardEntry(
                    key=key,
                    index_path=idx_path,
                    payload_path=pay_path,
                    record_count=entry_dict["record_count"],
                )
            )

    shards.sort(key=lambda s: (s.key.level, s.key.prefix_bits, s.key.prefix))
    return CombineManifest(
        manifest_path=manifest_path,
        root_dir=root_dir,
        max_level=max_level,
        world_center=world_center,
        world_half_size_pc=world_half_size_pc,
        mag_limit=mag_limit,
        payload_codec=payload_codec,
        shards=tuple(shards),
    )
