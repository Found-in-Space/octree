from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.mag_levels import MagLevelConfig

from .stage00 import (
    STAGE_STATE_FORMAT,
    STAGE_STATE_NAME,
    TREE_MANIFEST_FORMAT,
    TREE_MANIFEST_NAME,
    _align_tables_to_union_schema,
    _atomic_write_json,
    _read_json,
    _safe_input_shard_id,
    _tree_identity,
)

STAGE01_FORMAT = "foundinspace.octree.stage01/v0"
STAGE01_GROUP_CHECKSUM_ALGORITHM = "arrow-ipc-sha256/v0"
STAGE01_SORT_KEY = "level,final_node_id,mag_abs,source,source_id"
TREE_DIR_NAME = "tree"
REPORT_NAME = "stage01-report.json"


@dataclass(frozen=True, slots=True)
class Stage01Config:
    stage00_output_dir: Path
    output_dir: Path
    mag_config: MagLevelConfig
    max_level: int
    bucket_size: int
    batch_size: int = 100_000
    fragment_target_rows: int = 100_000
    force: bool = False

    def validate(self) -> None:
        if not self.stage00_output_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.stage00_output_dir}")
        if self.max_level < 0:
            raise ValueError("max_level must be >= 0")
        if self.max_level > MORTON_BITS:
            raise ValueError(
                f"max_level ({self.max_level}) must be <= MORTON_BITS ({MORTON_BITS})"
            )
        if self.bucket_size <= 0:
            raise ValueError("bucket_size must be > 0")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if self.fragment_target_rows <= 0:
            raise ValueError("fragment_target_rows must be > 0")


def run_stage01(config: Stage01Config) -> Path:
    """Sort and compact Stage 00 groups into deterministic Stage 01 groups."""
    config.validate()
    manifest_path = config.stage00_output_dir / TREE_MANIFEST_NAME
    state_path = config.stage00_output_dir / STAGE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 state: {state_path}")

    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    _validate_stage00_identity(config, manifest, state)

    if config.force and config.output_dir.exists():
        shutil.rmtree(config.output_dir)
    if (
        not config.force
        and not state.get("stage01_groups")
        and config.output_dir.exists()
        and any(config.output_dir.iterdir())
    ):
        raise FileExistsError(
            f"Output directory is not empty: {config.output_dir}. Use --force to replace it."
        )
    config.output_dir.mkdir(parents=True, exist_ok=True)

    stage00_groups = {group["key"]: group for group in state.get("stage00_groups", [])}
    existing_stage01_groups = {
        group["key"]: group for group in state.get("stage01_groups", [])
    }
    if config.force:
        existing_stage01_groups = {}
    dirty = state.get("dirty", {})
    deleted_keys = sorted(str(key) for key in dirty.get("deleted_stage00_groups", []))
    if config.force or not existing_stage01_groups:
        target_keys = sorted(stage00_groups)
    else:
        target_keys = sorted(
            key for key in dirty.get("stage01_groups", []) if key in stage00_groups
        )

    dirty_stage03_nodes: set[str] = {str(v) for v in dirty.get("stage03_nodes", [])}
    deleted_count = 0
    for key in deleted_keys:
        old_group = existing_stage01_groups.pop(key, None)
        if old_group is None:
            continue
        _delete_stage01_group_files(config.output_dir, old_group)
        dirty_stage03_nodes.update(str(v) for v in old_group.get("final_nodes", []))
        deleted_count += 1

    processed = 0
    changed = 0
    unchanged = 0
    rows_written = 0
    files_written = 0
    next_stage01_groups = dict(existing_stage01_groups)

    for key in target_keys:
        stage00_group = stage00_groups[key]
        old_group = existing_stage01_groups.get(key)
        sorted_table = _sorted_stage00_group(config.stage00_output_dir, stage00_group)
        checksum = _stage01_group_checksum(sorted_table)
        final_nodes = _final_node_keys(sorted_table)
        new_files = _write_sorted_group(config, stage00_group, sorted_table)
        if old_group is not None:
            _delete_stage01_group_files(config.output_dir, old_group)
        _publish_group_files(new_files)

        group_changed = old_group is None or old_group.get("checksum") != checksum
        if group_changed:
            changed += 1
            dirty_stage03_nodes.update(final_nodes)
            if old_group is not None:
                dirty_stage03_nodes.update(
                    str(v) for v in old_group.get("final_nodes", [])
                )
        else:
            unchanged += 1

        next_stage01_groups[key] = _stage01_group_state(
            stage00_group,
            files=[
                path.relative_to(config.output_dir).as_posix() for path in new_files
            ],
            row_count=len(sorted_table),
            checksum=checksum,
            final_nodes=final_nodes,
        )
        processed += 1
        rows_written += len(sorted_table)
        files_written += len(new_files)

    state["stage01_groups"] = [
        next_stage01_groups[key] for key in sorted(next_stage01_groups)
    ]
    state["dirty"] = {
        "stage01_groups": [],
        "deleted_stage00_groups": [],
        "stage03_nodes": sorted(dirty_stage03_nodes, key=_final_node_sort_key),
    }
    _atomic_write_json(state_path, state)

    report = {
        "format": STAGE01_FORMAT,
        "stage00_output_dir": str(config.stage00_output_dir),
        "output_dir": str(config.output_dir),
        "tree_dir": str(config.output_dir / TREE_DIR_NAME),
        "batch_size": config.batch_size,
        "fragment_target_rows": config.fragment_target_rows,
        "sort_key": STAGE01_SORT_KEY,
        "group_checksum_algorithm": STAGE01_GROUP_CHECKSUM_ALGORITHM,
        "force": config.force,
        "processed_group_count": processed,
        "changed_group_count": changed,
        "unchanged_group_count": unchanged,
        "deleted_group_count": deleted_count,
        "current_group_count": len(state["stage01_groups"]),
        "output_files_written": files_written,
        "rows_written": rows_written,
        "dirty_stage03_node_count": len(dirty_stage03_nodes),
    }
    report_path = config.output_dir / REPORT_NAME
    _atomic_write_json(report_path, report)
    return report_path


def _validate_stage00_identity(
    config: Stage01Config,
    manifest: dict[str, Any],
    state: dict[str, Any],
) -> None:
    if manifest.get("format") != TREE_MANIFEST_FORMAT:
        raise ValueError(
            f"Unsupported Stage 00 tree manifest format: {manifest.get('format')!r}"
        )
    if state.get("format") != STAGE_STATE_FORMAT:
        raise ValueError(f"Unsupported Stage 00 state format: {state.get('format')!r}")
    expected = _tree_identity(
        _Stage00IdentityConfig(
            mag_config=config.mag_config,
            max_level=config.max_level,
            bucket_size=config.bucket_size,
        )
    )
    existing = manifest.get("tree_identity")
    if existing != expected:
        raise ValueError(
            "Existing Stage 00 tree identity does not match current project config"
        )
    if state.get("tree_identity") != existing:
        raise ValueError("Stage 00 state identity does not match tree manifest")


@dataclass(frozen=True, slots=True)
class _Stage00IdentityConfig:
    mag_config: MagLevelConfig
    max_level: int
    bucket_size: int


def _sorted_stage00_group(stage00_output_dir: Path, group: dict[str, Any]) -> pa.Table:
    files = [stage00_output_dir / rel_path for rel_path in group.get("files", [])]
    if not files:
        return pa.table({})
    for path in files:
        if not path.is_file():
            raise FileNotFoundError(f"Missing Stage 00 group fragment: {path}")
    tables = [pq.read_table(path) for path in files]
    table = _align_tables_to_union_schema(tables)
    if len(table) == 0:
        return table
    final_node_ids = _final_node_id_array(table)
    sort_table = table.append_column(
        "_stage01_final_node_id",
        pa.array(final_node_ids, type=pa.uint64()),
    )
    sorted_with_helper = sort_table.take(
        pc.sort_indices(
            sort_table,
            sort_keys=[
                ("level", "ascending"),
                ("_stage01_final_node_id", "ascending"),
                ("mag_abs", "ascending"),
                ("source", "ascending"),
                ("source_id", "ascending"),
            ],
            null_placement="at_end",
        )
    )
    return sorted_with_helper.drop(["_stage01_final_node_id"])


def _final_node_id_array(table: pa.Table) -> np.ndarray:
    levels = np.asarray(table.column("level"), dtype=np.int32)
    morton_codes = np.asarray(table.column("morton_code"), dtype=np.uint64)
    out = np.zeros(len(table), dtype=np.uint64)
    for level in sorted(int(v) for v in np.unique(levels)):
        if level < 0 or level > MORTON_BITS:
            raise ValueError(f"Invalid row level {level}; expected 0..{MORTON_BITS}")
        indices = np.flatnonzero(levels == level)
        shift = 3 * (MORTON_BITS - level)
        out[indices] = morton_codes[indices] >> np.uint64(shift)
    return out


def _final_node_keys(table: pa.Table) -> list[str]:
    if len(table) == 0:
        return []
    levels = np.asarray(table.column("level"), dtype=np.int32)
    final_node_ids = _final_node_id_array(table)
    keys = {
        f"{int(level)}:{int(node_id)}"
        for level, node_id in zip(levels, final_node_ids, strict=True)
    }
    return sorted(keys, key=_final_node_sort_key)


def _final_node_sort_key(value: str) -> tuple[int, int]:
    level, node_id = str(value).split(":", 1)
    return int(level), int(node_id)


def _stage01_group_checksum(table: pa.Table) -> str:
    canonical = table.combine_chunks()
    sink = pa.BufferOutputStream()
    with pa_ipc.new_stream(sink, canonical.schema) as writer:
        writer.write_table(canonical)
    digest = hashlib.sha256(sink.getvalue()).hexdigest()
    return f"sha256:{digest}"


def _write_sorted_group(
    config: Stage01Config,
    group: dict[str, Any],
    table: pa.Table,
) -> list[Path]:
    path_octants = tuple(int(v) for v in group["path_octants"])
    directory = config.output_dir / TREE_DIR_NAME
    for octant in path_octants:
        directory = directory / f"o={octant}"
    directory.mkdir(parents=True, exist_ok=True)

    shard_id = _safe_input_shard_id(str(group["input_shard_id"]))
    kind = str(group["kind"])
    if kind not in {"pack", "lim"}:
        raise ValueError(f"Unsupported Stage 00 group kind: {kind!r}")
    paths: list[Path] = []
    offset = 0
    sequence = 1
    while offset < len(table) or (len(table) == 0 and sequence == 1):
        rows = min(config.fragment_target_rows, len(table) - offset)
        if rows <= 0 and len(table) > 0:
            break
        final_path = (
            directory / f"shard-{shard_id}-{kind}-sorted-{sequence:06d}.parquet"
        )
        tmp_path = _tmp_fragment_path(final_path)
        pq.write_table(table.slice(offset, rows), tmp_path, compression="zstd")
        paths.append(final_path)
        offset += rows
        sequence += 1
        if len(table) == 0:
            break
    return paths


def _tmp_fragment_path(final_path: Path) -> Path:
    return final_path.with_name(f".{final_path.name}.{os.getpid()}.tmp")


def _publish_group_files(final_paths: list[Path]) -> None:
    for final_path in final_paths:
        tmp_path = _tmp_fragment_path(final_path)
        os.replace(tmp_path, final_path)


def _delete_stage01_group_files(output_dir: Path, group: dict[str, Any]) -> None:
    for rel_file in group.get("files", []):
        path = output_dir / rel_file
        if path.exists():
            path.unlink()


def _stage01_group_state(
    stage00_group: dict[str, Any],
    *,
    files: list[str],
    row_count: int,
    checksum: str,
    final_nodes: list[str],
) -> dict[str, Any]:
    return {
        "key": stage00_group["key"],
        "node_path": stage00_group["node_path"],
        "path_octants": list(stage00_group["path_octants"]),
        "depth": stage00_group["depth"],
        "shard_id": stage00_group["input_shard_id"],
        "input_shard_id": stage00_group["input_shard_id"],
        "kind": stage00_group["kind"],
        "files": files,
        "file_count": len(files),
        "row_count": row_count,
        "checksum": checksum,
        "sorted_checksum": checksum,
        "final_nodes": final_nodes,
    }
