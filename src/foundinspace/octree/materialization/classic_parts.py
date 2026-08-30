from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import struct
import time
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..assembly.formats import (
    DEFAULT_FLAGS,
    IDENTIFIERS_ARTIFACT_KIND,
    IDENTIFIERS_INDEX_MAGIC,
    IDENTIFIERS_MANIFEST_NAME,
    INDEX_FILE_HDR,
    INDEX_HEADER_SIZE,
    INDEX_MAGIC,
    INDEX_RECORD,
    INDEX_VERSION,
    RENDER_ARTIFACT_KIND,
    RENDER_MANIFEST_NAME,
)
from ..assembly.identity_encoder import write_identity_arrays
from ..assembly.manifest import write_manifest
from ..assembly.types import CellKey, ShardKey
from ..assembly.writer import identifiers_shard_filenames, shard_filenames
from ..config import MORTON_BITS
from ..encoding.render import encode_render_records
from ..packing.lookup import FileHandleCache
from ..packing.manifest import read_packing_manifest
from ..sources.routing import _atomic_write_json
from ..terminal_packing import TerminalMap
from . import runs as shared_runs

CLASSIC_PARTS_ALGORITHM = "shared-terminal-parts-direct/v1"
CLASSIC_RENDER_PAYLOAD_NAME = "classic-render-dfs.payload"
CLASSIC_PART_PACK_COUNT = 256
CLASSIC_PACK_WORKERS = 8
CLASSIC_PARALLEL_MIN_ROWS = 10_000_000
CLASSIC_TASK_IN_MEMORY_MAX_ROWS = 250_000
CLASSIC_PARTS_STATE_NAME = "classic-parts-state.json"
_HELPER_COLUMNS = (
    "_classic_level",
    "_classic_node_id",
    "_classic_dfs_start",
    "_part_level",
    "_part_dfs_start",
    "_contributor",
    "_contributor_row",
)
_RAW_COLUMNS = (
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "mag_abs",
    "source",
    "source_id",
    "morton_code",
    "level",
)


@dataclass(frozen=True, slots=True)
class ClassicPartsResult:
    render_manifest_path: Path
    identifiers_manifest_path: Path
    row_count: int
    folded_row_count: int
    cell_count: int
    input_identity: str


@dataclass(frozen=True, order=True, slots=True)
class _TaskKey:
    dfs_start: int
    level: int
    node_id: int


@dataclass(slots=True)
class _ShardState:
    shard: ShardKey
    render_records_path: Path
    identifiers_records_path: Path
    identifiers_payload_path: Path
    count: int = 0
    last_node_id: int | None = None
    topology_digest: Any = field(default_factory=hashlib.sha256)


def materialize_classic_parts(
    *,
    groups: Sequence[Any],
    build_work_dir: Path,
    terminal_map_path: Path,
    plan: Any,
    input_identity: str,
) -> ClassicPartsResult:
    """Materialize STAR v1 directly from shared Preparation.

    The terminal map partitions work only. Rows retain their classic cells and
    are encoded from raw float64 positions into one global DFS payload.
    """
    terminal_map = TerminalMap(terminal_map_path)
    if terminal_map.max_level != int(plan.max_level):
        raise ValueError("Shared terminal map max_level does not match classic build")
    if terminal_map.waterline != int(plan.terminal_waterline):
        raise ValueError("Shared terminal map waterline does not match classic build")

    root = build_work_dir / "classic-parts"
    packs_root = root / "packs"
    artifacts_dir = root / "artifacts"
    packs_root.mkdir(parents=True, exist_ok=True)

    tasks: dict[_TaskKey, list[Any]] = {}
    for group in sorted(groups, key=lambda value: value.key):
        tasks.setdefault(_task_key(group, terminal_map, plan.max_level), []).append(
            group
        )

    tasks_by_pack: dict[int, list[_TaskKey]] = {}
    address_count = 1 << (3 * int(plan.max_level))
    for task_key in tasks:
        pack = min(
            CLASSIC_PART_PACK_COUNT - 1,
            (task_key.dfs_start * CLASSIC_PART_PACK_COUNT) // address_count,
        )
        tasks_by_pack.setdefault(pack, []).append(task_key)

    expected_rows = sum(int(group.row_count) for group in groups)
    materialization_started = time.monotonic()
    print(
        "Classic parts: starting "
        f"rows={expected_rows:,}, groups={len(groups):,}, "
        f"terminal_tasks={len(tasks):,}, spatial_packs={len(tasks_by_pack):,}",
        flush=True,
    )

    state_path = root / CLASSIC_PARTS_STATE_NAME
    state = _load_parts_state(state_path, input_identity=input_identity)
    completed = state["completed_packs"]
    active_packs = {str(pack) for pack in tasks_by_pack}
    completed = {
        key: value
        for key, value in completed.items()
        if key in active_packs
        and _completed_pack_is_valid(
            root,
            value,
            expected_identity=(
                str(value.get("identity", "")) if isinstance(value, dict) else ""
            ),
        )
    }
    state["completed_packs"] = completed
    processed_rows = 0
    ordered_packs = sorted(tasks_by_pack)
    pending: list[dict[str, Any]] = []
    for pack_index, pack in enumerate(ordered_packs, start=1):
        pack_tasks = sorted(tasks_by_pack[pack])
        pack_groups = [group for key in pack_tasks for group in tasks[key]]
        pack_rows = sum(int(group.row_count) for group in pack_groups)
        spill_tasks = sum(
            sum(int(group.row_count) for group in tasks[key])
            > CLASSIC_TASK_IN_MEMORY_MAX_ROWS
            for key in pack_tasks
        )
        pack_identity = _pack_identity(
            pack,
            pack_groups,
            terminal_map=terminal_map,
            plan=plan,
        )
        existing = completed.get(str(pack))
        if _completed_pack_is_valid(root, existing, expected_identity=pack_identity):
            processed_rows += int(existing["row_count"])
            print(
                "Classic parts: reused "
                f"pack={pack:03d} ({pack_index}/{len(ordered_packs)}), "
                f"rows={int(existing['row_count']):,}, "
                f"progress={processed_rows:,}/{expected_rows:,}",
                flush=True,
            )
            continue
        pending.append(
            {
                "pack": pack,
                "pack_index": pack_index,
                "pack_tasks": pack_tasks,
                "tasks": {key: tasks[key] for key in pack_tasks},
                "pack_rows": pack_rows,
                "spill_tasks": spill_tasks,
                "identity": pack_identity,
            }
        )

    def start_pack(spec: dict[str, Any]) -> float:
        print(
            "Classic parts: building "
            f"pack={int(spec['pack']):03d} "
            f"({int(spec['pack_index'])}/{len(ordered_packs)}), "
            f"rows={int(spec['pack_rows']):,}, "
            f"tasks={len(spec['pack_tasks']):,}, "
            f"spill_tasks={int(spec['spill_tasks']):,}",
            flush=True,
        )
        return time.monotonic()

    def record_pack(
        spec: dict[str, Any], result: dict[str, Any], *, pack_started: float
    ) -> None:
        nonlocal processed_rows
        pack = int(spec["pack"])
        completed[str(pack)] = result
        _atomic_write_json(state_path, state)
        processed_rows += int(result["row_count"])
        pack_elapsed = time.monotonic() - pack_started
        total_elapsed = time.monotonic() - materialization_started
        print(
            "Classic parts: completed "
            f"pack={pack:03d}, rows={int(result['row_count']):,}, "
            f"cells={int(result['cell_count']):,}, "
            f"elapsed={pack_elapsed:.1f}s, total_elapsed={total_elapsed:.1f}s, "
            f"progress={processed_rows:,}/{expected_rows:,}, "
            f"throughput={processed_rows / max(total_elapsed, 1e-9):,.0f} rows/s",
            flush=True,
        )

    use_parallel_packs = (
        expected_rows >= CLASSIC_PARALLEL_MIN_ROWS
        and len(pending) > 1
        and CLASSIC_PACK_WORKERS > 1
    )
    if use_parallel_packs:
        worker_count = min(CLASSIC_PACK_WORKERS, len(pending))
        print(
            f"Classic parts: using {worker_count} parallel spatial-pack workers",
            flush=True,
        )
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            futures: dict[Any, tuple[dict[str, Any], float]] = {}
            for spec in pending:
                pack_started = start_pack(spec)
                future = executor.submit(
                    _materialize_pack_from_path,
                    pack=int(spec["pack"]),
                    task_keys=spec["pack_tasks"],
                    tasks=spec["tasks"],
                    packs_root=packs_root,
                    terminal_map_path=terminal_map_path,
                    plan=plan,
                    identity=str(spec["identity"]),
                )
                futures[future] = (spec, pack_started)
            for future in as_completed(futures):
                spec, pack_started = futures[future]
                record_pack(spec, future.result(), pack_started=pack_started)
    else:
        for spec in pending:
            pack_started = start_pack(spec)
            result = _materialize_pack(
                pack=int(spec["pack"]),
                task_keys=spec["pack_tasks"],
                tasks=spec["tasks"],
                packs_root=packs_root,
                terminal_map=terminal_map,
                plan=plan,
                identity=str(spec["identity"]),
            )
            record_pack(spec, result, pack_started=pack_started)

    _prune_inactive_packs(root, completed)
    merge_started = time.monotonic()
    print(
        f"Classic parts: assembling {len(completed):,} completed spatial packs",
        flush=True,
    )
    render_manifest, identifiers_manifest, cell_count = _merge_completed_packs(
        root=root,
        artifacts_dir=artifacts_dir,
        completed=completed,
        plan=plan,
    )
    row_count = sum(int(value["row_count"]) for value in completed.values())
    folded_row_count = sum(
        int(value["folded_row_count"]) for value in completed.values()
    )
    part_count = sum(int(value["part_count"]) for value in completed.values())

    if row_count != expected_rows:
        raise ValueError(
            "Classic part materialization row count mismatch: "
            f"expected={expected_rows}, actual={row_count}"
        )
    total_elapsed = time.monotonic() - materialization_started
    print(
        "Classic parts: assembly complete "
        f"rows={row_count:,}, cells={cell_count:,}, parts={part_count:,}, "
        f"merge_elapsed={time.monotonic() - merge_started:.1f}s, "
        f"total_elapsed={total_elapsed:.1f}s, "
        f"throughput={row_count / max(total_elapsed, 1e-9):,.0f} rows/s",
        flush=True,
    )
    _atomic_write_json(
        artifacts_dir / "materialization-state.json",
        {
            "algorithm": CLASSIC_PARTS_ALGORITHM,
            "input_identity": input_identity,
            "terminal_map": str(terminal_map_path),
            "terminal_count": terminal_map.terminal_count,
            "part_count": part_count,
            "row_count": row_count,
            "folded_row_count": folded_row_count,
            "cell_count": cell_count,
        },
    )
    return ClassicPartsResult(
        render_manifest_path=render_manifest,
        identifiers_manifest_path=identifiers_manifest,
        row_count=row_count,
        folded_row_count=folded_row_count,
        cell_count=cell_count,
        input_identity=input_identity,
    )


def _load_parts_state(path: Path, *, input_identity: str) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        state = {}
    if state.get("algorithm") != CLASSIC_PARTS_ALGORITHM or not isinstance(
        state.get("completed_packs"), dict
    ):
        state = {
            "algorithm": CLASSIC_PARTS_ALGORITHM,
            "input_identity": input_identity,
            "completed_packs": {},
        }
    else:
        state["input_identity"] = input_identity
    return state


def _pack_identity(
    pack: int,
    groups: Sequence[Any],
    *,
    terminal_map: TerminalMap,
    plan: Any,
) -> str:
    value = {
        "algorithm": CLASSIC_PARTS_ALGORITHM,
        "pack": pack,
        "max_level": int(plan.max_level),
        "limiting_magnitude": float(plan.limiting_magnitude),
        "partition_from_level": int(plan.partition_from_level),
        "partition_prefix_bits": int(plan.partition_prefix_bits),
        "waterline": terminal_map.waterline,
        "terminal_map": _file_checksum(terminal_map.manifest_path),
        "groups": [
            (group.key, group.checksum, int(group.row_count))
            for group in sorted(groups, key=lambda item: item.key)
        ],
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _materialize_pack(
    *,
    pack: int,
    task_keys: Sequence[_TaskKey],
    tasks: dict[_TaskKey, list[Any]],
    packs_root: Path,
    terminal_map: TerminalMap,
    plan: Any,
    identity: str,
) -> dict[str, Any]:
    final_dir = packs_root / f"pack-{pack:04d}" / identity.removeprefix("sha256:")
    temporary = final_dir.with_name(f".{final_dir.name}.{uuid4().hex}.tmp")
    temporary.mkdir(parents=True)
    writer = _ClassicOutputWriter(temporary, plan=plan)
    row_count = 0
    folded_row_count = 0
    part_count = 0
    previous_part: tuple[int, int] | None = None
    try:
        for task_index, task_key in enumerate(task_keys):
            task_rows, task_folded, task_parts, previous_part = _materialize_task(
                tasks[task_key],
                writer=writer,
                terminal_map=terminal_map,
                plan=plan,
                spill_dir=temporary / f"task-{task_index:08d}",
                previous_part=previous_part,
            )
            row_count += task_rows
            folded_row_count += task_folded
            part_count += task_parts
        _render_manifest, _identifiers_manifest, cell_count = writer.close()
        _atomic_write_json(
            temporary / "pack-state.json",
            {
                "identity": identity,
                "row_count": row_count,
                "folded_row_count": folded_row_count,
                "part_count": part_count,
                "cell_count": cell_count,
            },
        )
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            shutil.rmtree(final_dir)
        temporary.replace(final_dir)
    except BaseException:
        writer.abort()
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {
        "identity": identity,
        "path": final_dir.relative_to(packs_root.parent).as_posix(),
        "row_count": row_count,
        "folded_row_count": folded_row_count,
        "part_count": part_count,
        "cell_count": cell_count,
    }


def _materialize_pack_from_path(
    *,
    pack: int,
    task_keys: Sequence[_TaskKey],
    tasks: dict[_TaskKey, list[Any]],
    packs_root: Path,
    terminal_map_path: Path,
    plan: Any,
    identity: str,
) -> dict[str, Any]:
    return _materialize_pack(
        pack=pack,
        task_keys=task_keys,
        tasks=tasks,
        packs_root=packs_root,
        terminal_map=TerminalMap(terminal_map_path),
        plan=plan,
        identity=identity,
    )


def _completed_pack_is_valid(
    root: Path,
    raw: Any,
    *,
    expected_identity: str,
) -> bool:
    if not isinstance(raw, dict) or raw.get("identity") != expected_identity:
        return False
    try:
        pack_dir = root / str(raw["path"])
        state = json.loads((pack_dir / "pack-state.json").read_text(encoding="utf-8"))
        if state.get("identity") != expected_identity:
            return False
        render = read_packing_manifest(
            pack_dir / RENDER_MANIFEST_NAME, deep_validation=False
        )
        identifiers = read_packing_manifest(
            pack_dir / IDENTIFIERS_MANIFEST_NAME, deep_validation=False
        )
        return (
            render.payload_layout == "global-dfs/v1"
            and identifiers.payload_layout == "per-shard-contiguous/v1"
            and sum(shard.record_count for shard in render.shards)
            == int(raw["cell_count"])
        )
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _prune_inactive_packs(root: Path, completed: dict[str, Any]) -> None:
    active = {(root / str(value["path"])).resolve() for value in completed.values()}
    packs_root = root / "packs"
    if not packs_root.is_dir():
        return
    for pack_root in packs_root.iterdir():
        if not pack_root.is_dir():
            continue
        for identity_dir in pack_root.iterdir():
            if identity_dir.is_dir() and identity_dir.resolve() not in active:
                shutil.rmtree(identity_dir)
        if not any(pack_root.iterdir()):
            pack_root.rmdir()


_INDEX_ARRAY_DTYPE = np.dtype(
    [
        ("node_id", "<u8"),
        ("payload_offset", "<u8"),
        ("payload_length", "<u4"),
        ("star_count", "<u4"),
    ]
)


def _merge_completed_packs(
    *,
    root: Path,
    artifacts_dir: Path,
    completed: dict[str, Any],
    plan: Any,
) -> tuple[Path, Path, int]:
    if artifacts_dir.exists():
        shutil.rmtree(artifacts_dir)
    artifacts_dir.mkdir(parents=True)
    render_path = artifacts_dir / CLASSIC_RENDER_PAYLOAD_NAME
    render_states: dict[ShardKey, _ShardState] = {}
    identifier_states: dict[ShardKey, _ShardState] = {}
    render_records = FileHandleCache(max(1, int(plan.max_open_files) // 2))
    identifier_records = FileHandleCache(max(1, int(plan.max_open_files) // 4))
    identifier_payloads = FileHandleCache(max(1, int(plan.max_open_files) // 4))
    try:
        with open(render_path, "wb") as render_output:
            for pack_number in sorted(completed, key=int):
                pack_dir = root / str(completed[pack_number]["path"])
                render_manifest = read_packing_manifest(
                    pack_dir / RENDER_MANIFEST_NAME, deep_validation=False
                )
                identifier_manifest = read_packing_manifest(
                    pack_dir / IDENTIFIERS_MANIFEST_NAME, deep_validation=False
                )
                render_payloads = {
                    shard.payload_path for shard in render_manifest.shards
                }
                if len(render_payloads) != 1:
                    raise ValueError("Classic part pack has multiple render payloads")
                render_base = render_output.tell()
                with open(next(iter(render_payloads)), "rb") as source:
                    shutil.copyfileobj(source, render_output, 8 << 20)
                for shard in render_manifest.shards:
                    state = _merged_state(render_states, shard.key, artifacts_dir)
                    _append_index_records(
                        shard.index_path,
                        render_records.open(state.render_records_path, "ab"),
                        state=state,
                        offset_base=render_base,
                    )

                for shard in identifier_manifest.shards:
                    state = _merged_state(identifier_states, shard.key, artifacts_dir)
                    payload_output = identifier_payloads.open(
                        state.identifiers_payload_path, "ab"
                    )
                    identifiers_base = payload_output.tell()
                    with open(shard.payload_path, "rb") as source:
                        shutil.copyfileobj(source, payload_output, 8 << 20)
                    _append_index_records(
                        shard.index_path,
                        identifier_records.open(state.identifiers_records_path, "ab"),
                        state=state,
                        offset_base=identifiers_base,
                    )
    finally:
        render_records.close_all()
        identifier_records.close_all()
        identifier_payloads.close_all()

    if set(render_states) != set(identifier_states):
        raise ValueError("Classic render and identifier packs have different shards")
    render_entries: list[dict[str, Any]] = []
    identifier_entries: list[dict[str, Any]] = []
    for shard in sorted(
        render_states, key=lambda key: (key.level, key.prefix_bits, key.prefix)
    ):
        render_state = render_states[shard]
        identifier_state = identifier_states[shard]
        if (
            render_state.count != identifier_state.count
            or render_state.topology_digest.digest()
            != identifier_state.topology_digest.digest()
        ):
            raise ValueError("Classic render and identifier pack topology differs")
        render_index_name, _unused = shard_filenames(shard)
        identifier_index_name, identifier_payload_name = identifiers_shard_filenames(
            shard
        )
        _publish_index(
            render_state.render_records_path,
            artifacts_dir / render_index_name,
            shard=shard,
            magic=INDEX_MAGIC,
            count=render_state.count,
        )
        _publish_index(
            identifier_state.identifiers_records_path,
            artifacts_dir / identifier_index_name,
            shard=shard,
            magic=IDENTIFIERS_INDEX_MAGIC,
            count=identifier_state.count,
        )
        topology_checksum = f"sha256:{render_state.topology_digest.hexdigest()}"
        render_entries.append(
            {
                "level": shard.level,
                "prefix_bits": shard.prefix_bits,
                "prefix": shard.prefix,
                "index_path": render_index_name,
                "payload_path": CLASSIC_RENDER_PAYLOAD_NAME,
                "record_count": render_state.count,
                "topology_checksum": topology_checksum,
            }
        )
        identifier_entries.append(
            {
                "level": shard.level,
                "prefix_bits": shard.prefix_bits,
                "prefix": shard.prefix,
                "index_path": identifier_index_name,
                "payload_path": identifier_payload_name,
                "record_count": identifier_state.count,
                "topology_checksum": topology_checksum,
            }
        )
    render_manifest = write_manifest(
        artifacts_dir,
        int(plan.max_level),
        render_entries,
        artifact_kind=RENDER_ARTIFACT_KIND,
        index_magic=INDEX_MAGIC,
        mag_limit=float(plan.limiting_magnitude),
        name=RENDER_MANIFEST_NAME,
        payload_layout="global-dfs/v1",
    )
    identifier_manifest = write_manifest(
        artifacts_dir,
        int(plan.max_level),
        identifier_entries,
        artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        mag_limit=float(plan.limiting_magnitude),
        name=IDENTIFIERS_MANIFEST_NAME,
        payload_layout="per-shard-contiguous/v1",
    )
    return (
        render_manifest,
        identifier_manifest,
        sum(state.count for state in render_states.values()),
    )


def _merged_state(
    states: dict[ShardKey, _ShardState],
    shard: ShardKey,
    root: Path,
) -> _ShardState:
    existing = states.get(shard)
    if existing is not None:
        return existing
    render_index_name, _unused = shard_filenames(shard)
    identifier_index_name, identifier_payload_name = identifiers_shard_filenames(shard)
    state = _ShardState(
        shard=shard,
        render_records_path=root / f".{render_index_name}.records",
        identifiers_records_path=root / f".{identifier_index_name}.records",
        identifiers_payload_path=root / identifier_payload_name,
    )
    states[shard] = state
    return state


def _append_index_records(
    source_path: Path,
    output,
    *,
    state: _ShardState,
    offset_base: int,
) -> None:
    with open(source_path, "rb") as source:
        raw_header = source.read(INDEX_FILE_HDR.size)
        if len(raw_header) != INDEX_FILE_HDR.size:
            raise ValueError(f"Truncated classic part index: {source_path}")
        header = INDEX_FILE_HDR.unpack(raw_header)
        remaining = int(header[-1])
        while remaining:
            take = min(remaining, 1_000_000)
            records = np.fromfile(source, dtype=_INDEX_ARRAY_DTYPE, count=take)
            if len(records) != take:
                raise ValueError(f"Truncated classic part records: {source_path}")
            first = int(records["node_id"][0])
            if state.last_node_id is not None and first <= state.last_node_id:
                raise ValueError("Classic part packs are not spatially ascending")
            if len(records) > 1 and np.any(
                records["node_id"][1:] <= records["node_id"][:-1]
            ):
                raise ValueError("Classic part index node IDs are not ascending")
            records = records.copy()
            records["payload_offset"] += np.uint64(offset_base)
            output.write(records.tobytes())
            nodes = np.ascontiguousarray(records["node_id"], dtype="<u8")
            state.topology_digest.update(nodes.tobytes())
            state.count += take
            state.last_node_id = int(records["node_id"][-1])
            remaining -= take
        if source.read(1):
            raise ValueError(f"Trailing classic part index bytes: {source_path}")


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while chunk := source.read(1 << 20):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _task_key(group: Any, terminal_map: TerminalMap, max_level: int) -> _TaskKey:
    path = tuple(int(value) for value in getattr(group, "path_octants", ()))
    depth = len(path)
    node_id = 0
    for octant in path:
        node_id = (node_id << 3) | octant
    level = min(depth, max_level)
    if depth > max_level:
        node_id >>= 3 * (depth - max_level)
    levels, nodes = terminal_map.remap(
        np.asarray([level], dtype=np.int16),
        np.asarray([node_id], dtype=np.uint64),
    )
    task_level = int(levels[0])
    task_node = int(nodes[0])
    return _TaskKey(
        dfs_start=task_node << (3 * (max_level - task_level)),
        level=task_level,
        node_id=task_node,
    )


def _materialize_task(
    groups: Sequence[Any],
    *,
    writer: _ClassicOutputWriter,
    terminal_map: TerminalMap,
    plan: Any,
    spill_dir: Path,
    previous_part: tuple[int, int] | None,
) -> tuple[int, int, int, tuple[int, int] | None]:
    expected_rows = sum(int(group.row_count) for group in groups)
    if expected_rows <= CLASSIC_TASK_IN_MEMORY_MAX_ROWS:
        table = _read_task(groups, batch_rows=plan.batch_rows)
        if len(table) == 0:
            return 0, 0, 0, previous_part
        ordered, folded = _order_task(
            table,
            terminal_map=terminal_map,
            max_level=plan.max_level,
        )
        row_count, part_count, previous_part = _write_ordered_task(
            writer,
            ordered,
            previous_part=previous_part,
        )
        return row_count, folded, part_count, previous_part
    return _materialize_task_external(
        groups,
        writer=writer,
        terminal_map=terminal_map,
        plan=plan,
        spill_dir=spill_dir,
        previous_part=previous_part,
        expected_rows=expected_rows,
    )


def _materialize_task_external(
    groups: Sequence[Any],
    *,
    writer: _ClassicOutputWriter,
    terminal_map: TerminalMap,
    plan: Any,
    spill_dir: Path,
    previous_part: tuple[int, int] | None,
    expected_rows: int,
) -> tuple[int, int, int, tuple[int, int] | None]:
    spill_dir.mkdir(parents=True)
    run_paths: list[Path] = []
    layout: shared_runs.SortedRunLayout | None = None
    folded_row_count = 0
    try:
        for index, batch in enumerate(
            _iter_task_batches(groups, batch_rows=plan.batch_rows)
        ):
            ordered, folded = _order_task(
                batch,
                terminal_map=terminal_map,
                max_level=plan.max_level,
            )
            folded_row_count += folded
            if layout is None:
                layout = _classic_run_layout(ordered.schema)
            elif not ordered.schema.equals(layout.schema, check_metadata=False):
                ordered = ordered.cast(layout.schema)
            path = spill_dir / f"run-{index:08d}.parquet"
            pq.write_table(
                ordered.replace_schema_metadata(None),
                path,
                compression="zstd",
                row_group_size=plan.batch_rows,
                write_statistics=False,
            )
            run_paths.append(path)
        if layout is None:
            if expected_rows:
                raise ValueError("Classic external task produced no sorted runs")
            return 0, 0, 0, previous_part

        final_runs = shared_runs.reduce_sorted_runs(
            run_paths,
            partition_dir=spill_dir / "merge",
            batch_size=plan.batch_rows,
            fan_in=plan.merge_fan_in,
            layout=layout,
        )
        merged = shared_runs.iter_merged_batches(
            final_runs,
            batch_size=plan.batch_rows,
            spill_dir=spill_dir / "overlap",
            layout=layout,
        )
        row_count = 0
        part_count = 0
        for (dfs_start, level), keyed_chunks in groupby(
            merged, key=lambda item: item[0]
        ):

            def tracked_chunks(chunks: Any = keyed_chunks) -> Any:
                nonlocal part_count, previous_part, row_count
                for _key, chunk in chunks:
                    added_parts, previous_part = _part_transitions(
                        chunk,
                        previous_part=previous_part,
                    )
                    part_count += added_parts
                    row_count += len(chunk)
                    yield chunk

            cell_level = int(level)
            cell_node = int(dfs_start) >> (3 * (int(plan.max_level) - cell_level))
            writer.write_cell_chunks(
                CellKey(level=cell_level, node_id=cell_node),
                tracked_chunks(),
            )
        if row_count != expected_rows:
            raise ValueError(
                "Classic external task row count mismatch: "
                f"expected={expected_rows}, actual={row_count}"
            )
        return row_count, folded_row_count, part_count, previous_part
    finally:
        shutil.rmtree(spill_dir, ignore_errors=True)


def _classic_run_layout(schema: pa.Schema) -> shared_runs.SortedRunLayout:
    return shared_runs.SortedRunLayout(
        schema=schema,
        cell_level_column="_classic_dfs_start",
        cell_node_column="_classic_level",
        overlap_sort_keys=(
            ("_part_dfs_start", "ascending"),
            ("_part_level", "ascending"),
            ("mag_abs", "ascending"),
            ("source", "ascending"),
            ("source_id", "ascending"),
            ("_contributor", "ascending"),
            ("_contributor_row", "ascending"),
        ),
        contributor_column="_classic_merge_contributor",
        contributor_row_column="_classic_merge_contributor_row",
    )


def _write_ordered_task(
    writer: _ClassicOutputWriter,
    ordered: pa.Table,
    *,
    previous_part: tuple[int, int] | None,
) -> tuple[int, int, tuple[int, int] | None]:
    part_count, previous_part = _part_transitions(
        ordered,
        previous_part=previous_part,
    )
    levels = np.asarray(ordered.column("_classic_level"), dtype=np.int16)
    nodes = np.asarray(ordered.column("_classic_node_id"), dtype=np.uint64)
    start = 0
    while start < len(ordered):
        end = start + 1
        while (
            end < len(ordered)
            and levels[end] == levels[start]
            and nodes[end] == nodes[start]
        ):
            end += 1
        writer.write_cell(
            CellKey(level=int(levels[start]), node_id=int(nodes[start])),
            ordered.slice(start, end - start),
        )
        start = end
    return len(ordered), part_count, previous_part


def _part_transitions(
    table: pa.Table,
    *,
    previous_part: tuple[int, int] | None,
) -> tuple[int, tuple[int, int] | None]:
    if len(table) == 0:
        return 0, previous_part
    levels = np.asarray(table.column("_part_level"), dtype=np.int16)
    starts = np.asarray(table.column("_part_dfs_start"), dtype=np.uint64)
    first = (int(levels[0]), int(starts[0]))
    count = int(previous_part != first)
    count += int(
        np.count_nonzero((levels[1:] != levels[:-1]) | (starts[1:] != starts[:-1]))
    )
    return count, (int(levels[-1]), int(starts[-1]))


def _read_task(groups: Sequence[Any], *, batch_rows: int) -> pa.Table:
    tables = list(_iter_task_batches(groups, batch_rows=batch_rows))
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def _iter_task_batches(groups: Sequence[Any], *, batch_rows: int) -> Any:
    for group in sorted(groups, key=lambda value: value.key):
        contributor_row = 0
        for path in group.files:
            schema = pq.read_schema(path)
            missing = sorted(set(_RAW_COLUMNS) - set(schema.names))
            if missing:
                raise ValueError(
                    "Classic materialization requires raw Preparation fields; "
                    f"{path} is missing {missing}"
                )
            columns = [*_RAW_COLUMNS]
            if "teff" in schema.names:
                columns.append("teff")
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=batch_rows, columns=columns):
                table = pa.Table.from_batches([batch])
                if "teff" not in table.column_names:
                    table = table.append_column(
                        "teff", pa.nulls(len(table), type=pa.float64())
                    )
                table = table.append_column(
                    "_contributor",
                    pa.array(
                        np.full(len(table), int(group.contributor_order), np.int32)
                    ),
                )
                table = table.append_column(
                    "_contributor_row",
                    pa.array(
                        np.arange(
                            contributor_row,
                            contributor_row + len(table),
                            dtype=np.int64,
                        )
                    ),
                )
                contributor_row += len(table)
                yield table


def _order_task(
    table: pa.Table,
    *,
    terminal_map: TerminalMap,
    max_level: int,
) -> tuple[pa.Table, int]:
    source_levels = _numpy_column(table, "level", np.int32, required=True)
    morton_codes = _numpy_column(table, "morton_code", np.uint64, required=True)
    final_levels = np.minimum(source_levels, max_level).astype(np.int16, copy=False)
    final_nodes = np.empty(len(table), dtype=np.uint64)
    for level_raw in np.unique(final_levels):
        level = int(level_raw)
        selected = np.flatnonzero(final_levels == level)
        final_nodes[selected] = morton_codes[selected] >> np.uint64(
            3 * (MORTON_BITS - level)
        )
    dfs_starts = np.left_shift(
        final_nodes,
        (3 * (max_level - final_levels.astype(np.int64))).astype(np.uint64),
    )
    part_levels, part_nodes = terminal_map.remap(final_levels, final_nodes)
    unresolved = (part_levels == final_levels) & (part_nodes == final_nodes)
    part_levels = np.where(unresolved, final_levels, part_levels).astype(np.int16)
    part_nodes = np.where(unresolved, final_nodes, part_nodes).astype(np.uint64)
    part_starts = np.left_shift(
        part_nodes,
        (3 * (max_level - part_levels.astype(np.int64))).astype(np.uint64),
    )
    augmented = table
    for name, values, arrow_type in (
        ("_classic_level", final_levels, pa.int16()),
        ("_classic_node_id", final_nodes, pa.uint64()),
        ("_classic_dfs_start", dfs_starts, pa.uint64()),
        ("_part_level", part_levels, pa.int16()),
        ("_part_dfs_start", part_starts, pa.uint64()),
    ):
        augmented = augmented.append_column(name, pa.array(values, type=arrow_type))
    indices = pc.sort_indices(
        augmented,
        sort_keys=[
            ("_part_dfs_start", "ascending"),
            ("_part_level", "ascending"),
            ("_classic_dfs_start", "ascending"),
            ("_classic_level", "ascending"),
            ("mag_abs", "ascending"),
            ("source", "ascending"),
            ("source_id", "ascending"),
            ("_contributor", "ascending"),
            ("_contributor_row", "ascending"),
        ],
        null_placement="at_end",
    )
    return augmented.take(indices), int(np.count_nonzero(source_levels > max_level))


class _ClassicOutputWriter:
    def __init__(self, root: Path, *, plan: Any) -> None:
        self.root = root
        self.plan = plan
        self.render_path = root / CLASSIC_RENDER_PAYLOAD_NAME
        self.render_fp = open(self.render_path, "wb")  # noqa: SIM115
        self.payload_files = FileHandleCache(max(1, int(plan.max_open_files) // 2))
        self.record_files = FileHandleCache(max(1, int(plan.max_open_files) // 2))
        self.states: dict[ShardKey, _ShardState] = {}
        self.last_dfs_key: tuple[int, int] | None = None
        self.closed = False

    def write_cell(self, key: CellKey, table: pa.Table) -> None:
        self.write_cell_chunks(key, (table,))

    def write_cell_chunks(self, key: CellKey, chunks: Any) -> None:
        dfs_key = (
            key.node_id << (3 * (int(self.plan.max_level) - key.level)),
            key.level,
        )
        if self.last_dfs_key is not None and dfs_key <= self.last_dfs_key:
            raise ValueError(
                f"Classic cells are not in DFS order: {dfs_key} <= {self.last_dfs_key}"
            )
        self.last_dfs_key = dfs_key
        shard = _shard_for_cell(key.level, key.node_id, self.plan)
        state = self._state(shard)
        if state.last_node_id is not None and key.node_id <= state.last_node_id:
            raise ValueError("Classic shard node IDs are not strictly ascending")

        render_offset = self.render_fp.tell()
        identifiers_fp = self.payload_files.open(state.identifiers_payload_path, "ab")
        identifiers_offset = identifiers_fp.tell()
        star_count = 0
        with (
            gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=self.render_fp,
                compresslevel=9,
                mtime=0,
            ) as render_gzip,
            gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=identifiers_fp,
                compresslevel=9,
                mtime=0,
            ) as identifiers_gzip,
        ):
            for table in chunks:
                if len(table) == 0:
                    continue
                levels = np.asarray(table.column("_classic_level"), dtype=np.int16)
                nodes = np.asarray(table.column("_classic_node_id"), dtype=np.uint64)
                if np.any(levels != key.level) or np.any(nodes != key.node_id):
                    raise ValueError(f"Classic cell chunks do not match {key}")
                render_gzip.write(_encode_render(table))
                write_identity_arrays(
                    identifiers_gzip,
                    table.column("source").combine_chunks(),
                    table.column("source_id").combine_chunks(),
                )
                star_count += len(table)
        if star_count == 0:
            raise ValueError(f"Classic cell {key} has no rows")
        render_length = self.render_fp.tell() - render_offset
        identifiers_length = identifiers_fp.tell() - identifiers_offset

        self.record_files.open(state.render_records_path, "ab").write(
            INDEX_RECORD.pack(key.node_id, render_offset, render_length, star_count)
        )
        self.record_files.open(state.identifiers_records_path, "ab").write(
            INDEX_RECORD.pack(
                key.node_id,
                identifiers_offset,
                identifiers_length,
                star_count,
            )
        )
        state.count += 1
        state.last_node_id = key.node_id
        state.topology_digest.update(struct.pack("<Q", key.node_id))

    def _state(self, shard: ShardKey) -> _ShardState:
        existing = self.states.get(shard)
        if existing is not None:
            return existing
        render_name, _unused_payload = shard_filenames(shard)
        identifiers_name, identifiers_payload_name = identifiers_shard_filenames(shard)
        state = _ShardState(
            shard=shard,
            render_records_path=self.root / f".{render_name}.records",
            identifiers_records_path=self.root / f".{identifiers_name}.records",
            identifiers_payload_path=self.root / identifiers_payload_name,
        )
        self.states[shard] = state
        return state

    def close(self) -> tuple[Path, Path, int]:
        if self.closed:
            raise ValueError("Classic output writer is already closed")
        self.closed = True
        self.render_fp.close()
        self.payload_files.close_all()
        self.record_files.close_all()
        render_entries: list[dict[str, Any]] = []
        identifier_entries: list[dict[str, Any]] = []
        for shard in sorted(
            self.states, key=lambda key: (key.level, key.prefix_bits, key.prefix)
        ):
            state = self.states[shard]
            render_index_name, _unused = shard_filenames(shard)
            identifiers_index_name, identifiers_payload_name = (
                identifiers_shard_filenames(shard)
            )
            _publish_index(
                state.render_records_path,
                self.root / render_index_name,
                shard=shard,
                magic=INDEX_MAGIC,
                count=state.count,
            )
            _publish_index(
                state.identifiers_records_path,
                self.root / identifiers_index_name,
                shard=shard,
                magic=IDENTIFIERS_INDEX_MAGIC,
                count=state.count,
            )
            topology_checksum = f"sha256:{state.topology_digest.hexdigest()}"
            render_entries.append(
                {
                    "level": shard.level,
                    "prefix_bits": shard.prefix_bits,
                    "prefix": shard.prefix,
                    "index_path": render_index_name,
                    "payload_path": CLASSIC_RENDER_PAYLOAD_NAME,
                    "record_count": state.count,
                    "topology_checksum": topology_checksum,
                }
            )
            identifier_entries.append(
                {
                    "level": shard.level,
                    "prefix_bits": shard.prefix_bits,
                    "prefix": shard.prefix,
                    "index_path": identifiers_index_name,
                    "payload_path": identifiers_payload_name,
                    "record_count": state.count,
                    "topology_checksum": topology_checksum,
                }
            )
        render_manifest = write_manifest(
            self.root,
            int(self.plan.max_level),
            render_entries,
            artifact_kind=RENDER_ARTIFACT_KIND,
            index_magic=INDEX_MAGIC,
            mag_limit=float(self.plan.limiting_magnitude),
            name=RENDER_MANIFEST_NAME,
            payload_layout="global-dfs/v1",
        )
        identifiers_manifest = write_manifest(
            self.root,
            int(self.plan.max_level),
            identifier_entries,
            artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
            index_magic=IDENTIFIERS_INDEX_MAGIC,
            mag_limit=float(self.plan.limiting_magnitude),
            name=IDENTIFIERS_MANIFEST_NAME,
            payload_layout="per-shard-contiguous/v1",
        )
        return (
            render_manifest,
            identifiers_manifest,
            sum(state.count for state in self.states.values()),
        )

    def abort(self) -> None:
        if not self.render_fp.closed:
            self.render_fp.close()
        self.payload_files.close_all()
        self.record_files.close_all()


def _publish_index(
    records_path: Path,
    output_path: Path,
    *,
    shard: ShardKey,
    magic: bytes,
    count: int,
) -> None:
    with open(output_path, "wb") as output:
        output.write(
            INDEX_FILE_HDR.pack(
                magic,
                INDEX_VERSION,
                INDEX_HEADER_SIZE,
                shard.level,
                shard.prefix_bits,
                DEFAULT_FLAGS,
                INDEX_RECORD.size,
                shard.prefix,
                count,
            )
        )
        with open(records_path, "rb") as records:
            shutil.copyfileobj(records, output, 1 << 20)
    records_path.unlink()


def _encode_render(table: pa.Table) -> bytes:
    morton_codes = _numpy_column(table, "morton_code", np.uint64, required=True)
    positions = np.column_stack(
        [
            _numpy_column(table, name, np.float64, required=True)
            for name in ("x_icrs_pc", "y_icrs_pc", "z_icrs_pc")
        ]
    )
    encoded = encode_render_records(
        morton_codes=morton_codes,
        positions=positions,
        mag_abs=_numpy_column(table, "mag_abs", np.float64, required=False),
        teff=_numpy_column(table, "teff", np.float64, required=False),
        levels=_numpy_column(table, "_classic_level", np.int16, required=True),
        node_ids=_numpy_column(table, "_classic_node_id", np.uint64, required=True),
    )
    return encoded.tobytes()


def _numpy_column(
    table: pa.Table,
    name: str,
    dtype: Any,
    *,
    required: bool,
) -> np.ndarray:
    if name not in table.column_names:
        if required:
            raise ValueError(f"Classic materialization input is missing {name}")
        return np.full(len(table), np.nan, dtype=dtype)
    column = table.column(name).combine_chunks()
    if required and column.null_count:
        raise ValueError(f"Classic materialization input has null {name} values")
    values = column.to_numpy(zero_copy_only=False)
    if not required and column.null_count:
        values = np.asarray(values, dtype=np.float64)
        values[column.is_null().to_numpy(zero_copy_only=False)] = np.nan
    return np.asarray(values, dtype=dtype)


def _shard_for_cell(level: int, node_id: int, plan: Any) -> ShardKey:
    if (
        level == 0
        or level < int(plan.partition_from_level)
        or int(plan.partition_prefix_bits) == 0
    ):
        return ShardKey(level=level, prefix_bits=0, prefix=0)
    prefix_bits = min(int(plan.partition_prefix_bits), 3 * level)
    prefix = node_id >> (3 * level - prefix_bits)
    return ShardKey(level=level, prefix_bits=prefix_bits, prefix=prefix)
