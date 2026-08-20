from __future__ import annotations

import fcntl
import hashlib
import heapq
import json
import os
import shutil
import struct
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from ..assembly.formats import INDEX_FILE_HDR, INDEX_RECORD
from ..terminal_packing import (
    LOGICAL_HAS_PAYLOAD,
    LOGICAL_IS_TERMINAL,
    LOGICAL_TOPOLOGY_HEADER,
    LOGICAL_TOPOLOGY_MAGIC,
    LOGICAL_TOPOLOGY_RECORD,
    TerminalMap,
)
from .lookup import FileHandleCache, FixedRecordFile
from .manifest import PackingManifest
from .records import (
    DESCRIPTOR_SIZE,
    FRONTIER_REF_FMT,
    HAS_CHILDREN,
    HAS_PAYLOAD,
    HEADER_SIZE,
    IS_FRONTIER,
    IS_TERMINAL,
    RELOC_HEADER_FMT,
    RELOC_MAGIC,
    RELOC_RECORD_FMT,
    SHARD_HDR_FMT,
    SHARD_NODE_FMT,
    SHARD_NODE_V2_FMT,
    STAR_FORMAT_VERSION_V1,
    pack_brightest_level,
    pack_shard_header,
)

SKELETON_CACHE_FORMAT = "foundinspace.octree.index-skeleton-cache/v5"
SKELETON_COMPILER_VERSION = "authoritative-logical-topology/v5"

_LAYOUT_RECORD = struct.Struct("<QHBBB3x")
_RELOCATION_LAYOUT_RECORD = struct.Struct("<QQIIH6x")
_SKELETON_MAGIC = b"OSKL"
_SKELETON_HEADER = struct.Struct("<4sHHhHQI32s")
_SKELETON_RECORD = struct.Struct("<QHHBBBB")
_PACK_MAGIC = b"OSKP"
_PACK_HEADER = struct.Struct("<4sHHH2x32sQ")
_RAW_PLAN_RECORD = struct.Struct("<hHQIHHQI32s")
_PLAN_RECORD = struct.Struct("<hHQIHHQI32s32s")
_CHILD_OFFSET_RECORD = struct.Struct("<QQ")

_END_LINEAGE = -1


class IndexEmissionStrategy(str, Enum):
    FORWARD = "forward"
    TEMP_PWRITE_BATCHED = "temp-pwrite-batched"
    TEMP_PWRITE_PER_CHILD = "temp-pwrite-per-child"


@dataclass(frozen=True, slots=True)
class StreamingIndexResult:
    index_offset: int
    index_length: int
    skeleton_count: int


@dataclass(frozen=True, slots=True)
class _SkeletonPlanEntry:
    parent_level: int
    parent_node_id: int
    node_count: int
    bucket: int
    frontier_count: int
    pack_offset: int
    pack_length: int
    digest: bytes
    pack_digest: bytes


@dataclass(frozen=True, slots=True)
class _PendingChild:
    parent_shard_id: int
    parent_node_index: int


@dataclass(frozen=True, slots=True)
class _PreparedShard:
    entry: _SkeletonPlanEntry
    entry_nodes: tuple[int, int, int, int, int, int, int, int]
    packed_nodes: bytes
    frontier_rows: tuple[tuple[int, int, int, int], ...]


@dataclass(slots=True)
class _FrontierFrame:
    shard_id: int
    table_local_offset: int
    table: bytearray | None
    children: tuple[tuple[int, int, int, int], ...]
    next_child: int = 0


def write_streaming_index(
    manifest: PackingManifest,
    relocation_files: tuple[Path, ...],
    output_fp: BinaryIO,
    *,
    max_open_files: int,
    star_format_version: int,
    skeleton_pack_count: int,
    emission_strategy: IndexEmissionStrategy,
    cache_dir: Path,
) -> StreamingIndexResult:
    """Compile and write the final index with sequential bounded-memory joins."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    with _cache_lock(cache_dir):
        _clean_incomplete_trees(cache_dir)
        scratch = cache_dir / f".compile-{os.getpid()}-{uuid4().hex}.tmp"
        scratch.mkdir()
        try:
            topology_identity = _topology_identity(
                manifest,
                cache_dir=cache_dir,
                star_format_version=star_format_version,
                skeleton_pack_count=skeleton_pack_count,
            )
            plan_path = _load_topology_plan(cache_dir, identity=topology_identity)
            if plan_path is None:
                topology_runs = _build_topology_runs(
                    manifest,
                    scratch=scratch,
                    star_format_version=star_format_version,
                )
                layout_path = _merge_topology_layout(
                    topology_runs,
                    scratch=scratch,
                    max_open_files=max_open_files,
                )
                temporary_plan, skeleton_count = _compile_skeletons(
                    layout_path,
                    cache_dir=cache_dir,
                    scratch=scratch,
                    max_level=manifest.max_level,
                    star_format_version=star_format_version,
                    skeleton_pack_count=skeleton_pack_count,
                    max_open_files=max_open_files,
                )
                plan_path = _publish_topology_plan(
                    temporary_plan,
                    cache_dir=cache_dir,
                    identity=topology_identity,
                    skeleton_count=skeleton_count,
                )
            else:
                skeleton_count = sum(1 for _entry in _iter_plan(plan_path))
            relocation_layout = _merge_relocation_layout(
                relocation_files,
                scratch=scratch,
                max_open_files=max_open_files,
            )
            index_offset = output_fp.tell()
            if emission_strategy == IndexEmissionStrategy.FORWARD:
                child_offset_runs, expected_end = _build_child_offset_runs(
                    plan_path,
                    scratch=scratch,
                    index_offset=index_offset,
                    star_format_version=star_format_version,
                    max_open_files=max_open_files,
                )
                _emit_index(
                    output_fp,
                    plan_path=plan_path,
                    pack_root=cache_dir / "packs",
                    relocation_layout=relocation_layout,
                    child_offset_runs=child_offset_runs,
                    max_level=manifest.max_level,
                    star_format_version=star_format_version,
                    max_open_files=max_open_files,
                )
                if output_fp.tell() != expected_end:
                    raise ValueError(
                        "Streaming index size differs from the topology-plan prepass"
                    )
            else:
                _emit_index_via_temporary(
                    output_fp,
                    plan_path=plan_path,
                    pack_root=cache_dir / "packs",
                    relocation_layout=relocation_layout,
                    scratch=scratch,
                    index_offset=index_offset,
                    max_level=manifest.max_level,
                    star_format_version=star_format_version,
                    max_open_files=max_open_files,
                    batched=(
                        emission_strategy == IndexEmissionStrategy.TEMP_PWRITE_BATCHED
                    ),
                )
            _activate_and_prune_topology_plan(
                cache_dir=cache_dir,
                identity=topology_identity,
                plan_path=plan_path,
            )
            return StreamingIndexResult(
                index_offset=index_offset,
                index_length=output_fp.tell() - index_offset,
                skeleton_count=skeleton_count,
            )
        finally:
            shutil.rmtree(scratch, ignore_errors=True)


@contextmanager
def _cache_lock(cache_dir: Path) -> Iterator[None]:
    """Serialize cache cleanup, publication, reads, and active-plan pruning."""
    lock_path = cache_dir / ".cache.lock"
    with open(lock_path, "a+b") as lock_fp:
        fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)


def _clean_incomplete_trees(cache_dir: Path) -> None:
    for path in cache_dir.glob(".compile-*.tmp"):
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    pack_root = cache_dir / "packs"
    if pack_root.is_dir():
        for path in pack_root.glob(".*.tmp"):
            path.unlink(missing_ok=True)
    plans_root = cache_dir / "plans"
    if plans_root.is_dir():
        for path in plans_root.glob(".*.tmp"):
            shutil.rmtree(path, ignore_errors=True)


def _topology_identity(
    manifest: PackingManifest,
    *,
    cache_dir: Path,
    star_format_version: int,
    skeleton_pack_count: int,
) -> str:
    shards: list[dict[str, object]] = []
    terminal_policy: object = None
    if star_format_version != STAR_FORMAT_VERSION_V1 and manifest.terminal_map_path:
        raw = json.loads(manifest.terminal_map_path.read_text(encoding="utf-8"))
        terminal_policy = {
            "format": raw.get("format"),
            "topology_identity": raw.get("topology_identity"),
            "topology_levels": [
                {
                    "level": entry.get("level"),
                    "count": entry.get("count"),
                    "checksum": entry.get("checksum"),
                }
                for entry in raw.get("topology_levels", [])
            ],
        }
    else:
        for entry in manifest.shards:
            checksum = entry.topology_checksum or _legacy_topology_checksum(
                entry.index_path,
                magic=manifest.index_magic,
                cache_dir=cache_dir,
            )
            shards.append(
                {
                    "level": entry.key.level,
                    "prefix_bits": entry.key.prefix_bits,
                    "prefix": entry.key.prefix,
                    "record_count": entry.record_count,
                    "topology_checksum": checksum,
                }
            )
    policy, _digest = _skeleton_policy(
        max_level=manifest.max_level,
        star_format_version=star_format_version,
    )
    canonical = {
        "policy": policy,
        "terminal_policy": terminal_policy,
        "skeleton_pack_count": skeleton_pack_count,
        "shards": sorted(
            shards,
            key=lambda row: (row["level"], row["prefix_bits"], row["prefix"]),
        ),
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _legacy_topology_checksum(path: Path, *, magic: bytes, cache_dir: Path) -> str:
    stat = path.stat()
    physical = {
        "path": str(path.resolve()),
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    key = hashlib.sha256(
        json.dumps(physical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    checkpoint_dir = cache_dir / "legacy-topology-checksums"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_dir / f"{key}.json"
    try:
        raw = json.loads(checkpoint.read_text(encoding="utf-8"))
        checksum = str(raw["topology_checksum"])
        if raw.get("physical") == physical and checksum.startswith("sha256:"):
            return checksum
    except (KeyError, OSError, TypeError, ValueError):
        pass
    digest = hashlib.sha256()
    reader = FixedRecordFile(path, INDEX_FILE_HDR, INDEX_RECORD, magic)
    try:
        for record in reader.iter_records():
            digest.update(struct.pack("<Q", int(record[0])))
    finally:
        reader.close()
    checksum = f"sha256:{digest.hexdigest()}"
    temporary = checkpoint.with_name(f".{checkpoint.name}.{uuid4().hex}.tmp")
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(
            {"physical": physical, "topology_checksum": checksum},
            fp,
            sort_keys=True,
        )
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, checkpoint)
    return checksum


def _iter_own_nodes(manifest: PackingManifest, level: int) -> Iterator[int]:
    shards = [entry for entry in manifest.shards if entry.key.level == level]
    shards.sort(key=lambda entry: _spatial_shard_range(entry.key)[0])
    previous: int | None = None
    for entry in shards:
        reader = FixedRecordFile(
            entry.index_path,
            INDEX_FILE_HDR,
            INDEX_RECORD,
            manifest.index_magic,
        )
        try:
            if reader.header.level != level:
                raise ValueError(f"Intermediate level mismatch: {entry.index_path}")
            for record in reader.iter_records():
                node_id = int(record[0])
                if previous is not None and node_id <= previous:
                    raise ValueError(
                        f"Intermediate node stream is not ascending at level {level}"
                    )
                previous = node_id
                yield node_id
        finally:
            reader.close()


def _spatial_shard_range(key) -> tuple[int, int]:
    shift = 3 * int(key.level) - int(key.prefix_bits)
    lower = int(key.prefix) << shift
    upper = ((int(key.prefix) + 1) << shift) - 1 if shift else int(key.prefix)
    return lower, upper


def _build_topology_runs(
    manifest: PackingManifest,
    *,
    scratch: Path,
    star_format_version: int,
) -> dict[int, Path]:
    if (
        star_format_version != STAR_FORMAT_VERSION_V1
        and manifest.terminal_map_path is not None
    ):
        terminal_map = TerminalMap(manifest.terminal_map_path)
        return {
            level: path
            for level in terminal_map.topology_levels
            if (path := terminal_map.topology_path(level)) is not None
        }

    child_path: Path | None = None
    result: dict[int, Path] = {}
    for level in range(manifest.max_level, -1, -1):
        own = _peekable(_iter_own_nodes(manifest, level))
        parents = _peekable(
            _iter_parent_masks(child_path) if child_path is not None else iter(())
        )
        output = scratch / f"topology-{level:02d}.bin"
        count = 0
        with open(output, "wb") as fp:
            fp.write(
                LOGICAL_TOPOLOGY_HEADER.pack(
                    LOGICAL_TOPOLOGY_MAGIC,
                    1,
                    LOGICAL_TOPOLOGY_HEADER.size,
                    level,
                    0,
                )
            )
            buffer = bytearray()
            while own.value is not None or parents.value is not None:
                candidates = []
                if own.value is not None:
                    candidates.append(int(own.value))
                if parents.value is not None:
                    candidates.append(int(parents.value[0]))
                node_id = min(candidates)
                flags = 0
                child_mask = 0
                brightest_level: int | None = None
                if own.value == node_id:
                    flags |= LOGICAL_HAS_PAYLOAD
                    brightest_level = level
                    own.advance()
                if parents.value is not None and int(parents.value[0]) == node_id:
                    child_mask = int(parents.value[1])
                    child_brightest_level = int(parents.value[2])
                    brightest_level = (
                        child_brightest_level
                        if brightest_level is None
                        else min(brightest_level, child_brightest_level)
                    )
                    parents.advance()
                if brightest_level is None:
                    raise ValueError(
                        f"Topology node has no represented stars: ({level}, {node_id})"
                    )
                buffer.extend(
                    LOGICAL_TOPOLOGY_RECORD.pack(
                        node_id, child_mask, flags, brightest_level
                    )
                )
                if len(buffer) >= 1 << 20:
                    fp.write(buffer)
                    buffer.clear()
                count += 1
            if buffer:
                fp.write(buffer)
            fp.seek(0)
            fp.write(
                LOGICAL_TOPOLOGY_HEADER.pack(
                    LOGICAL_TOPOLOGY_MAGIC,
                    1,
                    LOGICAL_TOPOLOGY_HEADER.size,
                    level,
                    count,
                )
            )
            fp.flush()
            os.fsync(fp.fileno())
        if count:
            result[level] = output
            child_path = output
        else:
            output.unlink()
            child_path = None
    return result


class _Peekable:
    def __init__(self, values: Iterator):
        self._values = iter(values)
        self.value = next(self._values, None)

    def advance(self) -> None:
        self.value = next(self._values, None)


def _peekable(values: Iterator) -> _Peekable:
    return _Peekable(values)


def _iter_topology(path: Path) -> Iterator[tuple[int, int, int, int]]:
    with open(path, "rb") as fp:
        raw = fp.read(LOGICAL_TOPOLOGY_HEADER.size)
        if len(raw) != LOGICAL_TOPOLOGY_HEADER.size:
            raise ValueError(f"Truncated topology run: {path}")
        magic, version, header_size, _level, count = LOGICAL_TOPOLOGY_HEADER.unpack(raw)
        if magic != LOGICAL_TOPOLOGY_MAGIC or version != 1 or header_size != len(raw):
            raise ValueError(f"Invalid topology run: {path}")
        previous: int | None = None
        for _ in range(count):
            raw = fp.read(LOGICAL_TOPOLOGY_RECORD.size)
            if len(raw) != LOGICAL_TOPOLOGY_RECORD.size:
                raise ValueError(f"Truncated topology run: {path}")
            node_id, child_mask, flags, brightest_level = (
                LOGICAL_TOPOLOGY_RECORD.unpack(raw)
            )
            if previous is not None and node_id <= previous:
                raise ValueError(f"Non-ascending topology run: {path}")
            previous = int(node_id)
            yield int(node_id), int(child_mask), int(flags), int(brightest_level)
        if fp.read(1):
            raise ValueError(f"Trailing bytes in topology run: {path}")


def _iter_parent_masks(path: Path) -> Iterator[tuple[int, int, int]]:
    pending_parent: int | None = None
    child_mask = 0
    brightest_level = 0xFF
    for node_id, _children, _flags, child_brightest in _iter_topology(path):
        parent = node_id >> 3
        if pending_parent is not None and parent != pending_parent:
            yield pending_parent, child_mask, brightest_level
            child_mask = 0
            brightest_level = 0xFF
        pending_parent = parent
        child_mask |= 1 << (node_id & 0x7)
        brightest_level = min(brightest_level, child_brightest)
    if pending_parent is not None:
        yield pending_parent, child_mask, brightest_level


def _parent_key(level: int, node_id: int) -> tuple[int, int]:
    parent_level = 5 * (level // 5) - 1
    if parent_level < 0:
        return -1, 0
    return parent_level, node_id >> (3 * (level - parent_level))


def _local_key(level: int, node_id: int) -> tuple[int, int]:
    parent_level, _parent_node = _parent_key(level, node_id)
    local_depth = level - parent_level
    local_mask = (1 << (3 * local_depth)) - 1
    return local_depth, node_id & local_mask


def _layout_key(level: int, node_id: int) -> tuple[int, ...]:
    parent_level, _parent_node = _parent_key(level, node_id)
    lineage: list[int] = []
    for boundary in range(4, parent_level + 1, 5):
        lineage.append(node_id >> (3 * (level - boundary)))
    local_depth, local_path = _local_key(level, node_id)
    return (*lineage, _END_LINEAGE, local_depth, local_path)


def _iter_level_layout(
    level: int, path: Path
) -> Iterator[tuple[int, int, int, int, int]]:
    for node_id, child_mask, flags, brightest_level in _iter_topology(path):
        yield node_id, level, child_mask, flags, brightest_level


def _iter_layout_file(path: Path) -> Iterator[tuple[int, int, int, int, int]]:
    with open(path, "rb") as fp:
        while raw := fp.read(_LAYOUT_RECORD.size):
            if len(raw) != _LAYOUT_RECORD.size:
                raise ValueError(f"Truncated layout run: {path}")
            node_id, level, child_mask, flags, brightest_level = _LAYOUT_RECORD.unpack(
                raw
            )
            yield (
                int(node_id),
                int(level),
                int(child_mask),
                int(flags),
                int(brightest_level),
            )


def _merge_sorted_sources(
    sources: Sequence[Iterator[tuple]],
    output: Path,
    *,
    record_struct: struct.Struct,
    key_fn,
) -> None:
    heap: list[tuple[tuple[int, ...], int, tuple, Iterator[tuple]]] = []
    for sequence, source in enumerate(sources):
        iterator = iter(source)
        value = next(iterator, None)
        if value is not None:
            heapq.heappush(heap, (key_fn(value), sequence, value, iterator))
    with open(output, "wb") as fp:
        buffer = bytearray()
        while heap:
            _key, sequence, value, iterator = heapq.heappop(heap)
            buffer.extend(record_struct.pack(*value))
            if len(buffer) >= 1 << 20:
                fp.write(buffer)
                buffer.clear()
            following = next(iterator, None)
            if following is not None:
                heapq.heappush(heap, (key_fn(following), sequence, following, iterator))
        if buffer:
            fp.write(buffer)
        fp.flush()
        os.fsync(fp.fileno())


def _bounded_reduce(
    sources: list[Iterator[tuple]],
    *,
    scratch: Path,
    prefix: str,
    record_struct: struct.Struct,
    key_fn,
    read_fn,
    max_open_files: int,
) -> Path:
    fan_in = max(2, max_open_files)
    paths: list[Path] = []
    sequence = 0
    for start in range(0, len(sources), fan_in):
        sequence += 1
        path = scratch / f"{prefix}-0-{sequence:04d}.bin"
        _merge_sorted_sources(
            sources[start : start + fan_in],
            path,
            record_struct=record_struct,
            key_fn=key_fn,
        )
        paths.append(path)
    tier = 0
    while len(paths) > 1:
        tier += 1
        reduced: list[Path] = []
        for start in range(0, len(paths), fan_in):
            chunk = paths[start : start + fan_in]
            if len(chunk) == 1:
                reduced.append(chunk[0])
                continue
            sequence += 1
            output = scratch / f"{prefix}-{tier}-{sequence:04d}.bin"
            _merge_sorted_sources(
                [read_fn(path) for path in chunk],
                output,
                record_struct=record_struct,
                key_fn=key_fn,
            )
            for path in chunk:
                path.unlink()
            reduced.append(output)
        paths = reduced
    if not paths:
        empty = scratch / f"{prefix}-empty.bin"
        empty.touch()
        return empty
    return paths[0]


def _merge_topology_layout(
    topology_runs: dict[int, Path], *, scratch: Path, max_open_files: int
) -> Path:
    return _bounded_reduce(
        [
            _iter_level_layout(level, path)
            for level, path in sorted(topology_runs.items())
        ],
        scratch=scratch,
        prefix="layout",
        record_struct=_LAYOUT_RECORD,
        key_fn=lambda row: _layout_key(int(row[1]), int(row[0])),
        read_fn=_iter_layout_file,
        max_open_files=max_open_files,
    )


def _skeleton_policy(
    *,
    max_level: int,
    star_format_version: int,
) -> tuple[dict[str, object], bytes]:
    policy = {
        "format": SKELETON_CACHE_FORMAT,
        "compiler": SKELETON_COMPILER_VERSION,
        "levels_per_shard": 5,
        "max_level": max_level,
        "star_format_version": star_format_version,
        "record_struct": _SKELETON_RECORD.format,
    }
    encoded = json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()
    return policy, hashlib.sha256(encoded).digest()


def _compile_skeletons(
    layout_path: Path,
    *,
    cache_dir: Path,
    scratch: Path,
    max_level: int,
    star_format_version: int,
    skeleton_pack_count: int,
    max_open_files: int,
) -> tuple[Path, int]:
    _policy, policy_digest = _skeleton_policy(
        max_level=max_level,
        star_format_version=star_format_version,
    )
    pack_root = cache_dir / "packs"
    pack_root.mkdir(parents=True, exist_ok=True)
    staging_root = scratch / "pack-staging"
    staging_root.mkdir()
    raw_plan_path = scratch / "skeleton-plan.raw"
    plan_path = scratch / "skeleton-plan.bin"
    skeleton_count = 0
    bucket_counts: dict[int, int] = {}
    bucket_offsets: dict[int, int] = {}
    pack_files = FileHandleCache(max(1, max_open_files))
    current_key: tuple[int, int] | None = None
    records: list[tuple[int, int, int, int, int, int, int]] = []

    def flush(plan_fp: BinaryIO) -> None:
        nonlocal records, current_key, skeleton_count
        if current_key is None:
            return
        parent_level, parent_node_id = current_key
        header = _SKELETON_HEADER.pack(
            _SKELETON_MAGIC,
            1,
            _SKELETON_HEADER.size,
            parent_level,
            0,
            parent_node_id,
            len(records),
            policy_digest,
        )
        body = b"".join(_SKELETON_RECORD.pack(*record) for record in records)
        data = header + body
        digest = hashlib.sha256(data).digest()
        frontier_count = sum(record[3] == 5 for record in records)
        bucket = _skeleton_bucket(
            parent_level, parent_node_id, bucket_count=skeleton_pack_count
        )
        pack_path = staging_root / f"bucket-{bucket:04d}.pack"
        offset = bucket_offsets.get(bucket)
        if offset is None:
            with open(pack_path, "wb") as fp:
                fp.write(
                    _PACK_HEADER.pack(
                        _PACK_MAGIC,
                        1,
                        _PACK_HEADER.size,
                        bucket,
                        policy_digest,
                        0,
                    )
                )
            offset = _PACK_HEADER.size
        pack_files.open(pack_path, "ab").write(data)
        bucket_offsets[bucket] = offset + len(data)
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        plan_fp.write(
            _RAW_PLAN_RECORD.pack(
                parent_level,
                0,
                parent_node_id,
                len(records),
                bucket,
                frontier_count,
                offset,
                len(data),
                digest,
            )
        )
        skeleton_count += 1
        records = []

    try:
        with open(raw_plan_path, "wb") as plan_fp:
            for (
                node_id,
                level,
                child_mask,
                logical_flags,
                brightest_level,
            ) in _iter_layout_file(layout_path):
                key = _parent_key(level, node_id)
                if current_key != key:
                    flush(plan_fp)
                    current_key = key
                local_depth, local_path = _local_key(level, node_id)
                flags = 0
                if logical_flags & LOGICAL_HAS_PAYLOAD:
                    flags |= HAS_PAYLOAD
                if child_mask:
                    flags |= HAS_CHILDREN
                if local_depth == 5:
                    flags |= IS_FRONTIER
                if (
                    star_format_version != STAR_FORMAT_VERSION_V1
                    and logical_flags & LOGICAL_IS_TERMINAL
                ):
                    if not logical_flags & LOGICAL_HAS_PAYLOAD:
                        raise ValueError(
                            f"Terminal node has no payload: ({level}, {node_id})"
                        )
                    if child_mask:
                        raise ValueError(
                            f"Terminal node retains descendants: ({level}, {node_id})"
                        )
                    flags |= IS_TERMINAL
                packed_brightest_level = 0
                if star_format_version != STAR_FORMAT_VERSION_V1:
                    packed_brightest_level = pack_brightest_level(
                        level=level,
                        brightest_level=brightest_level,
                    )
                records.append(
                    (
                        node_id,
                        local_path,
                        level,
                        local_depth,
                        child_mask,
                        flags,
                        packed_brightest_level,
                    )
                )
            flush(plan_fp)
            plan_fp.flush()
            os.fsync(plan_fp.fileno())
    finally:
        pack_files.close_all()

    pack_digests: dict[int, bytes] = {}
    for bucket, count in sorted(bucket_counts.items()):
        staged = staging_root / f"bucket-{bucket:04d}.pack"
        with open(staged, "r+b") as fp:
            fp.seek(0)
            fp.write(
                _PACK_HEADER.pack(
                    _PACK_MAGIC,
                    1,
                    _PACK_HEADER.size,
                    bucket,
                    policy_digest,
                    count,
                )
            )
            fp.flush()
            os.fsync(fp.fileno())
        pack_digest = bytes.fromhex(_file_checksum(staged).removeprefix("sha256:"))
        target = pack_root / f"{pack_digest.hex()}.pack"
        expected_checksum = f"sha256:{pack_digest.hex()}"
        if not target.is_file() or _file_checksum(target) != expected_checksum:
            temporary = pack_root / f".{pack_digest.hex()}.{uuid4().hex}.tmp"
            os.link(staged, temporary)
            os.replace(temporary, target)
        pack_digests[bucket] = pack_digest

    with open(plan_path, "wb") as plan_fp:
        buffer = bytearray()
        for entry in _iter_raw_plan(raw_plan_path):
            pack_digest = pack_digests[entry.bucket]
            buffer.extend(
                _PLAN_RECORD.pack(
                    entry.parent_level,
                    0,
                    entry.parent_node_id,
                    entry.node_count,
                    entry.bucket,
                    entry.frontier_count,
                    entry.pack_offset,
                    entry.pack_length,
                    entry.digest,
                    pack_digest,
                )
            )
            if len(buffer) >= 1 << 20:
                plan_fp.write(buffer)
                buffer.clear()
        if buffer:
            plan_fp.write(buffer)
        plan_fp.flush()
        os.fsync(plan_fp.fileno())
    return plan_path, skeleton_count


def _skeleton_bucket(
    parent_level: int, parent_node_id: int, *, bucket_count: int
) -> int:
    if parent_level < 4:
        return 0
    level_four_ancestor = parent_node_id >> (3 * (parent_level - 4))
    return min(
        bucket_count - 1,
        (level_four_ancestor * bucket_count) // (1 << 12),
    )


def _iter_raw_plan(path: Path) -> Iterator[_SkeletonPlanEntry]:
    with open(path, "rb") as fp:
        while raw := fp.read(_RAW_PLAN_RECORD.size):
            if len(raw) != _RAW_PLAN_RECORD.size:
                raise ValueError(f"Truncated raw skeleton plan: {path}")
            (
                parent_level,
                _reserved,
                parent_node,
                node_count,
                bucket,
                frontier_count,
                pack_offset,
                pack_length,
                digest,
            ) = _RAW_PLAN_RECORD.unpack(raw)
            yield _SkeletonPlanEntry(
                parent_level,
                parent_node,
                node_count,
                bucket,
                frontier_count,
                pack_offset,
                pack_length,
                digest,
                b"",
            )


def _publish_topology_plan(
    temporary_plan: Path,
    *,
    cache_dir: Path,
    identity: str,
    skeleton_count: int,
) -> Path:
    directory = cache_dir / "plans" / identity.removeprefix("sha256:")
    existing = _load_topology_plan(cache_dir, identity=identity)
    if existing is not None:
        return existing
    if directory.exists():
        if directory.is_dir():
            shutil.rmtree(directory)
        else:
            directory.unlink()
    temporary = directory.with_name(f".{directory.name}.{uuid4().hex}.tmp")
    temporary.mkdir(parents=True)
    shutil.copyfile(temporary_plan, temporary / "plan.bin")
    plan_checksum = _file_checksum(temporary / "plan.bin")
    packs: dict[str, int] = {}
    for entry in _iter_plan(temporary / "plan.bin"):
        pack_name = entry.pack_digest.hex()
        if pack_name not in packs:
            packs[pack_name] = (
                (cache_dir / "packs" / f"{pack_name}.pack").stat().st_size
            )
    manifest = {
        "format": SKELETON_CACHE_FORMAT,
        "identity": identity,
        "skeleton_count": skeleton_count,
        "plan_size": (temporary / "plan.bin").stat().st_size,
        "plan_checksum": plan_checksum,
        "packs": [
            {"digest": digest, "size": size} for digest, size in sorted(packs.items())
        ],
    }
    with open(temporary / "manifest.json", "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    directory.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(temporary, directory)
    except OSError:
        if not directory.is_dir():
            raise
        shutil.rmtree(temporary)
    result = _load_topology_plan(cache_dir, identity=identity)
    if result is None:
        raise ValueError("Published topology skeleton plan is invalid")
    return result


def _load_topology_plan(cache_dir: Path, *, identity: str) -> Path | None:
    directory = cache_dir / "plans" / identity.removeprefix("sha256:")
    try:
        raw = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if raw.get("format") != SKELETON_CACHE_FORMAT:
            return None
        if raw.get("identity") != identity:
            return None
        expected_count = int(raw["skeleton_count"])
        plan_path = directory / "plan.bin"
        if plan_path.stat().st_size != int(raw["plan_size"]):
            return None
        if _file_checksum(plan_path) != str(raw["plan_checksum"]):
            return None
        packs: dict[bytes, int] = {}
        for pack in raw["packs"]:
            digest = bytes.fromhex(str(pack["digest"]))
            size = int(pack["size"])
            if not _validate_pack_metadata(
                cache_dir / "packs" / f"{digest.hex()}.pack",
                expected_size=size,
            ):
                return None
            packs[digest] = size
        count = 0
        for entry in _iter_plan(plan_path):
            pack_size = packs.get(entry.pack_digest)
            if (
                pack_size is None
                or entry.frontier_count > entry.node_count
                or entry.pack_offset < _PACK_HEADER.size
                or entry.pack_length
                != _SKELETON_HEADER.size + entry.node_count * _SKELETON_RECORD.size
                or entry.pack_offset + entry.pack_length > pack_size
            ):
                return None
            count += 1
        if count != expected_count:
            return None
        return plan_path
    except (KeyError, OSError, TypeError, ValueError):
        return None


def _activate_and_prune_topology_plan(
    cache_dir: Path, *, identity: str, plan_path: Path
) -> None:
    manifest_path = plan_path.parent / "manifest.json"
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    active_packs = {str(entry["digest"]) for entry in raw["packs"]}
    current = cache_dir / "current-plan.json"
    temporary = current.with_name(f".{current.name}.{uuid4().hex}.tmp")
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(
            {
                "format": SKELETON_CACHE_FORMAT,
                "identity": identity,
                "plan": plan_path.relative_to(cache_dir).as_posix(),
            },
            fp,
            sort_keys=True,
        )
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, current)

    plans_root = cache_dir / "plans"
    if plans_root.is_dir():
        for directory in plans_root.iterdir():
            if directory.is_dir() and directory != plan_path.parent:
                shutil.rmtree(directory)
    packs_root = cache_dir / "packs"
    if packs_root.is_dir():
        for path in packs_root.glob("*.pack"):
            if path.stem not in active_packs:
                path.unlink()


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(1 << 20):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _validate_pack_metadata(path: Path, *, expected_size: int) -> bool:
    try:
        if path.stat().st_size != expected_size:
            return False
        with open(path, "rb") as fp:
            header = fp.read(_PACK_HEADER.size)
        unpacked = _PACK_HEADER.unpack(header)
        return (
            unpacked[0] == _PACK_MAGIC
            and unpacked[1] == 1
            and unpacked[2] == _PACK_HEADER.size
            and unpacked[-1] > 0
        )
    except (OSError, struct.error):
        return False


def _iter_relocation_file(path: Path) -> Iterator[tuple[int, int, int, int, int]]:
    reader = FixedRecordFile(path, RELOC_HEADER_FMT, RELOC_RECORD_FMT, RELOC_MAGIC)
    try:
        level = reader.header.level
        for record in reader.iter_records():
            yield int(record[0]), int(record[1]), int(record[2]), int(record[3]), level
    finally:
        reader.close()


def _iter_relocation_layout(path: Path) -> Iterator[tuple[int, int, int, int, int]]:
    with open(path, "rb") as fp:
        while raw := fp.read(_RELOCATION_LAYOUT_RECORD.size):
            if len(raw) != _RELOCATION_LAYOUT_RECORD.size:
                raise ValueError(f"Truncated relocation layout: {path}")
            node_id, offset, length, count, level = _RELOCATION_LAYOUT_RECORD.unpack(
                raw
            )
            yield int(node_id), int(offset), int(length), int(count), int(level)


def _merge_relocation_layout(
    relocation_files: tuple[Path, ...], *, scratch: Path, max_open_files: int
) -> Path:
    return _bounded_reduce(
        [_iter_relocation_file(path) for path in relocation_files],
        scratch=scratch,
        prefix="relocation-layout",
        record_struct=_RELOCATION_LAYOUT_RECORD,
        key_fn=lambda row: _layout_key(int(row[4]), int(row[0])),
        read_fn=_iter_relocation_layout,
        max_open_files=max_open_files,
    )


def _iter_plan(path: Path) -> Iterator[_SkeletonPlanEntry]:
    with open(path, "rb") as fp:
        while raw := fp.read(_PLAN_RECORD.size):
            if len(raw) != _PLAN_RECORD.size:
                raise ValueError(f"Truncated skeleton plan: {path}")
            (
                parent_level,
                _reserved,
                parent_node,
                node_count,
                bucket,
                frontier_count,
                pack_offset,
                pack_length,
                digest,
                pack_digest,
            ) = _PLAN_RECORD.unpack(raw)
            yield _SkeletonPlanEntry(
                parent_level,
                parent_node,
                node_count,
                bucket,
                frontier_count,
                pack_offset,
                pack_length,
                digest,
                pack_digest,
            )


def _node_record_size(star_format_version: int) -> int:
    return (
        SHARD_NODE_FMT.size
        if star_format_version == STAR_FORMAT_VERSION_V1
        else SHARD_NODE_V2_FMT.size
    )


def _build_child_offset_runs(
    plan_path: Path,
    *,
    scratch: Path,
    index_offset: int,
    star_format_version: int,
    max_open_files: int,
) -> tuple[dict[int, Path], int]:
    """Prefix-sum shard sizes and publish ascending child offsets by boundary."""
    paths: dict[int, Path] = {}
    previous_nodes: dict[int, int] = {}
    files = FileHandleCache(max(1, max_open_files))
    offset = index_offset
    try:
        for entry in _iter_plan(plan_path):
            if entry.parent_level >= 0:
                previous = previous_nodes.get(entry.parent_level)
                if previous is not None and entry.parent_node_id <= previous:
                    raise ValueError(
                        "Skeleton plan is not spatially ascending at boundary "
                        f"{entry.parent_level}"
                    )
                previous_nodes[entry.parent_level] = entry.parent_node_id
                path = paths.setdefault(
                    entry.parent_level,
                    scratch / f"child-offset-{entry.parent_level:04d}.bin",
                )
                files.open(path, "ab").write(
                    _CHILD_OFFSET_RECORD.pack(entry.parent_node_id, offset)
                )
            offset += (
                SHARD_HDR_FMT.size
                + entry.node_count * _node_record_size(star_format_version)
                + entry.frontier_count * FRONTIER_REF_FMT.size
            )
    finally:
        files.close_all()
    return paths, offset


class _ChildOffsetStreams:
    """Bounded-FD sequential readers for child offsets partitioned by boundary."""

    def __init__(self, paths: dict[int, Path], *, max_open_files: int):
        self._paths = paths
        self._positions: dict[int, int] = {}
        self._files = FileHandleCache(max(1, max_open_files))

    def take(self, level: int, node_id: int) -> int:
        path = self._paths.get(level)
        if path is None:
            raise ValueError(f"Missing child-offset stream for ({level}, {node_id})")
        fp = self._files.open(path, "rb")
        fp.seek(self._positions.get(level, 0))
        raw = fp.read(_CHILD_OFFSET_RECORD.size)
        if len(raw) != _CHILD_OFFSET_RECORD.size:
            raise ValueError(f"Missing child offset for ({level}, {node_id})")
        self._positions[level] = fp.tell()
        actual_node_id, offset = _CHILD_OFFSET_RECORD.unpack(raw)
        if actual_node_id != node_id:
            raise ValueError(
                "Child-offset stream differs from topology skeleton: "
                f"expected ({level}, {node_id}), got ({level}, {actual_node_id})"
            )
        return int(offset)

    def ensure_exhausted(self) -> None:
        for level, path in self._paths.items():
            fp = self._files.open(path, "rb")
            fp.seek(self._positions.get(level, 0))
            first = fp.read(1)
            if first:
                raw = first + fp.read(_CHILD_OFFSET_RECORD.size - 1)
                if len(raw) != _CHILD_OFFSET_RECORD.size:
                    raise ValueError(f"Truncated child-offset stream: {path}")
                node_id, _offset = _CHILD_OFFSET_RECORD.unpack(raw)
                raise ValueError(
                    f"Unused child offset remains for ({level}, {node_id})"
                )

    def close(self) -> None:
        self._files.close_all()


def _read_skeleton(
    fp: BinaryIO, path: Path, expected: _SkeletonPlanEntry
) -> list[tuple[int, int, int, int, int, int, int]]:
    fp.seek(expected.pack_offset)
    data = fp.read(expected.pack_length)
    if len(data) != expected.pack_length:
        raise ValueError(f"Truncated skeleton in pack: {path}")
    if hashlib.sha256(data).digest() != expected.digest:
        raise ValueError(f"Skeleton digest mismatch in pack: {path}")
    (
        magic,
        version,
        header_size,
        parent_level,
        _reserved,
        parent_node,
        count,
        _policy,
    ) = _SKELETON_HEADER.unpack_from(data)
    if (
        magic != _SKELETON_MAGIC
        or version != 1
        or header_size != _SKELETON_HEADER.size
        or parent_level != expected.parent_level
        or parent_node != expected.parent_node_id
        or count != expected.node_count
    ):
        raise ValueError(f"Invalid skeleton header in pack: {path}")
    return [
        tuple(
            int(value)
            for value in _SKELETON_RECORD.unpack_from(
                data, _SKELETON_HEADER.size + index * _SKELETON_RECORD.size
            )
        )
        for index in range(count)
    ]


def _prepare_shard(
    entry: _SkeletonPlanEntry,
    nodes: list[tuple[int, int, int, int, int, int, int]],
    relocations: _Peekable,
    *,
    star_format_version: int,
) -> _PreparedShard:
    if not nodes:
        raise ValueError(
            f"Empty topology skeleton: {entry.parent_level}:{entry.parent_node_id}"
        )
    if len(nodes) > 0xFFFF:
        raise ValueError(f"node_count exceeds u16: {len(nodes)}")
    node_format = (
        SHARD_NODE_FMT
        if star_format_version == STAR_FORMAT_VERSION_V1
        else SHARD_NODE_V2_FMT
    )
    index_by_key = {
        (local_depth, local_path): index
        for index, (
            _node,
            local_path,
            _level,
            local_depth,
            _mask,
            _flags,
            _brightest_level,
        ) in enumerate(nodes, 1)
    }
    entry_nodes = [0] * 8
    frontier_rows: list[tuple[int, int, int, int]] = []
    packed_nodes = bytearray()
    for node_index, (
        node_id,
        local_path,
        level,
        local_depth,
        child_mask,
        flags,
        brightest_level,
    ) in enumerate(nodes, 1):
        if local_depth == 1:
            entry_nodes[local_path & 0x7] = node_index
        child_indices = []
        if local_depth < 5:
            for octant in range(8):
                if child_mask & (1 << octant):
                    child_indices.append(
                        index_by_key[(local_depth + 1, (local_path << 3) | octant)]
                    )
        first_child = child_indices[0] if child_indices else 0
        if child_indices and child_indices != list(
            range(first_child, first_child + len(child_indices))
        ):
            raise ValueError(f"In-shard children are not contiguous: {level}:{node_id}")
        if local_depth == 5:
            first_child = 0
            frontier_rows.append((node_index, level, node_id, child_mask))

        payload_offset = payload_length = star_count = 0
        if flags & HAS_PAYLOAD:
            if relocations.value is None:
                raise ValueError(
                    f"Missing relocation entry for payload node ({level}, {node_id})"
                )
            reloc_node, offset, length, count, reloc_level = relocations.value
            if (reloc_level, reloc_node) != (level, node_id):
                raise ValueError(
                    f"Missing relocation entry for payload node ({level}, {node_id})"
                )
            payload_offset, payload_length, star_count = offset, length, count
            relocations.advance()
        values = (
            first_child,
            local_path,
            child_mask,
            local_depth,
            flags,
            brightest_level,
            payload_offset,
            payload_length,
        )
        packed_nodes.extend(
            node_format.pack(*values)
            if star_format_version == STAR_FORMAT_VERSION_V1
            else node_format.pack(*values, star_count)
        )

    if len(frontier_rows) != entry.frontier_count:
        raise ValueError(
            "Skeleton frontier count differs from topology plan: "
            f"{entry.parent_level}:{entry.parent_node_id}"
        )
    return _PreparedShard(
        entry=entry,
        entry_nodes=tuple(entry_nodes),  # type: ignore[arg-type]
        packed_nodes=bytes(packed_nodes),
        frontier_rows=tuple(frontier_rows),
    )


def _decode_grid(level: int, node_id: int) -> tuple[int, int, int]:
    if level < 0:
        return 0, 0, 0
    x = y = z = 0
    for index in range(level):
        shift = 3 * (level - 1 - index)
        octant = (node_id >> shift) & 0x7
        x = (x << 1) | (octant & 1)
        y = (y << 1) | ((octant >> 1) & 1)
        z = (z << 1) | ((octant >> 2) & 1)
    return x, y, z


def _pack_prepared_shard_header(
    prepared: _PreparedShard,
    *,
    shard_offset: int,
    shard_id: int,
    parent_shard_id: int,
    parent_node_index: int,
    star_format_version: int,
) -> bytes:
    entry = prepared.entry
    node_table_offset = shard_offset + SHARD_HDR_FMT.size
    frontier_table_offset = node_table_offset + len(prepared.packed_nodes)
    parent_grid = _decode_grid(entry.parent_level, entry.parent_node_id)
    return pack_shard_header(
        shard_id=shard_id,
        parent_shard_id=parent_shard_id,
        parent_node_index=parent_node_index,
        node_count=entry.node_count,
        parent_global_depth=entry.parent_level,
        parent_grid_x=parent_grid[0],
        parent_grid_y=parent_grid[1],
        parent_grid_z=parent_grid[2],
        entry_nodes=prepared.entry_nodes,
        first_frontier_index=(
            prepared.frontier_rows[0][0] if prepared.frontier_rows else 0
        ),
        node_table_offset=node_table_offset,
        frontier_table_offset=frontier_table_offset,
        payload_base_offset=HEADER_SIZE + DESCRIPTOR_SIZE,
        version=star_format_version,
    )


def _emit_index(
    output_fp: BinaryIO,
    *,
    plan_path: Path,
    pack_root: Path,
    relocation_layout: Path,
    child_offset_runs: dict[int, Path],
    max_level: int,
    star_format_version: int,
    max_open_files: int,
) -> None:
    pack_files = FileHandleCache(max(1, max_open_files))
    child_offsets = _ChildOffsetStreams(
        child_offset_runs, max_open_files=max_open_files
    )
    try:
        _emit_index_with_cache(
            output_fp,
            plan_path=plan_path,
            pack_root=pack_root,
            relocation_layout=relocation_layout,
            child_offsets=child_offsets,
            max_level=max_level,
            star_format_version=star_format_version,
            pack_files=pack_files,
        )
    finally:
        pack_files.close_all()
        child_offsets.close()


def _emit_index_with_cache(
    output_fp: BinaryIO,
    *,
    plan_path: Path,
    pack_root: Path,
    relocation_layout: Path,
    child_offsets: _ChildOffsetStreams,
    max_level: int,
    star_format_version: int,
    pack_files: FileHandleCache,
) -> None:
    relocations = _peekable(_iter_relocation_layout(relocation_layout))
    pending: dict[tuple[int, int], _PendingChild] = {}
    next_shard_id = 1
    for entry in _iter_plan(plan_path):
        pack_path = pack_root / f"{entry.pack_digest.hex()}.pack"
        nodes = _read_skeleton(pack_files.open(pack_path, "rb"), pack_path, entry)
        prepared = _prepare_shard(
            entry,
            nodes,
            relocations,
            star_format_version=star_format_version,
        )
        shard_offset = output_fp.tell()
        if entry.parent_level < 0:
            parent_shard_id = 0
            parent_node_index = 0
        else:
            child_ref = pending.pop((entry.parent_level, entry.parent_node_id), None)
            if child_ref is None:
                raise ValueError(
                    "Missing parent frontier for skeleton "
                    f"{entry.parent_level}:{entry.parent_node_id}"
                )
            parent_shard_id = child_ref.parent_shard_id
            parent_node_index = child_ref.parent_node_index

        shard_id = next_shard_id
        next_shard_id += 1
        packed_frontiers = bytearray()
        for node_index, level, node_id, child_mask in prepared.frontier_rows:
            child_offset = 0
            if child_mask and level < max_level:
                child_offset = child_offsets.take(level, node_id)
                pending[(level, node_id)] = _PendingChild(
                    parent_shard_id=shard_id,
                    parent_node_index=node_index,
                )
            packed_frontiers.extend(FRONTIER_REF_FMT.pack(child_offset))

        header = _pack_prepared_shard_header(
            prepared,
            shard_offset=shard_offset,
            shard_id=shard_id,
            parent_shard_id=parent_shard_id,
            parent_node_index=parent_node_index,
            star_format_version=star_format_version,
        )
        output_fp.write(header + prepared.packed_nodes + packed_frontiers)
    if relocations.value is not None:
        node_id, _offset, _length, _count, level = relocations.value
        raise ValueError(
            f"Unexpected relocation entry for non-payload node ({level}, {node_id})"
        )
    if pending:
        level, node_id = next(iter(pending))
        raise ValueError(f"Missing child skeleton for frontier ({level}, {node_id})")
    child_offsets.ensure_exhausted()


def _pwrite_all(fd: int, data: bytes | bytearray, offset: int) -> None:
    view = memoryview(data)
    while view:
        written = os.pwrite(fd, view, offset)
        if written <= 0:
            raise OSError("pwrite made no progress")
        offset += written
        view = view[written:]


def _resolve_temporary_parent(
    stack: list[_FrontierFrame],
    entry: _SkeletonPlanEntry,
    *,
    shard_offset: int,
    index_fp: BinaryIO,
    batched: bool,
) -> tuple[int, int]:
    if entry.parent_level < 0:
        if stack:
            raise ValueError("Root skeleton appeared inside an unresolved subtree")
        return 0, 0
    if not stack:
        raise ValueError(
            "Missing parent frontier for skeleton "
            f"{entry.parent_level}:{entry.parent_node_id}"
        )
    frame = stack[-1]
    level, node_id, node_index, ordinal = frame.children[frame.next_child]
    if (level, node_id) != (entry.parent_level, entry.parent_node_id):
        raise ValueError(
            "Temporary-index DFS order differs from topology skeleton: "
            f"expected ({level}, {node_id}), got "
            f"({entry.parent_level}, {entry.parent_node_id})"
        )
    encoded_offset = FRONTIER_REF_FMT.pack(shard_offset)
    if batched:
        if frame.table is None:
            raise AssertionError("Batched frontier frame has no table buffer")
        start = ordinal * FRONTIER_REF_FMT.size
        frame.table[start : start + FRONTIER_REF_FMT.size] = encoded_offset
    else:
        index_fp.flush()
        _pwrite_all(
            index_fp.fileno(),
            encoded_offset,
            frame.table_local_offset + ordinal * FRONTIER_REF_FMT.size,
        )
    frame.next_child += 1
    if frame.next_child == len(frame.children):
        if batched:
            assert frame.table is not None
            index_fp.flush()
            _pwrite_all(index_fp.fileno(), frame.table, frame.table_local_offset)
        stack.pop()
    return frame.shard_id, node_index


def _emit_index_via_temporary(
    output_fp: BinaryIO,
    *,
    plan_path: Path,
    pack_root: Path,
    relocation_layout: Path,
    scratch: Path,
    index_offset: int,
    max_level: int,
    star_format_version: int,
    max_open_files: int,
    batched: bool,
) -> None:
    temporary_index = scratch / "completed-index.bin"
    pack_files = FileHandleCache(max(1, max_open_files))
    relocations = _peekable(_iter_relocation_layout(relocation_layout))
    stack: list[_FrontierFrame] = []
    next_shard_id = 1
    try:
        with open(temporary_index, "w+b") as index_fp:
            for entry in _iter_plan(plan_path):
                local_offset = index_fp.tell()
                shard_offset = index_offset + local_offset
                parent_shard_id, parent_node_index = _resolve_temporary_parent(
                    stack,
                    entry,
                    shard_offset=shard_offset,
                    index_fp=index_fp,
                    batched=batched,
                )
                pack_path = pack_root / f"{entry.pack_digest.hex()}.pack"
                nodes = _read_skeleton(
                    pack_files.open(pack_path, "rb"), pack_path, entry
                )
                prepared = _prepare_shard(
                    entry,
                    nodes,
                    relocations,
                    star_format_version=star_format_version,
                )
                shard_id = next_shard_id
                next_shard_id += 1
                header = _pack_prepared_shard_header(
                    prepared,
                    shard_offset=shard_offset,
                    shard_id=shard_id,
                    parent_shard_id=parent_shard_id,
                    parent_node_index=parent_node_index,
                    star_format_version=star_format_version,
                )
                table_size = len(prepared.frontier_rows) * FRONTIER_REF_FMT.size
                table_local_offset = (
                    local_offset + len(header) + len(prepared.packed_nodes)
                )
                index_fp.write(header + prepared.packed_nodes + b"\x00" * table_size)
                children = tuple(
                    (level, node_id, node_index, ordinal)
                    for ordinal, (node_index, level, node_id, child_mask) in enumerate(
                        prepared.frontier_rows
                    )
                    if child_mask and level < max_level
                )
                if children:
                    stack.append(
                        _FrontierFrame(
                            shard_id=shard_id,
                            table_local_offset=table_local_offset,
                            table=bytearray(table_size) if batched else None,
                            children=children,
                        )
                    )
            if stack:
                frame = stack[-1]
                level, node_id, _index, _ordinal = frame.children[frame.next_child]
                raise ValueError(
                    f"Missing child skeleton for frontier ({level}, {node_id})"
                )
            if relocations.value is not None:
                node_id, _offset, _length, _count, level = relocations.value
                raise ValueError(
                    "Unexpected relocation entry for non-payload node "
                    f"({level}, {node_id})"
                )
            index_fp.flush()
            temporary_size = index_fp.tell()
    finally:
        pack_files.close_all()

    with open(temporary_index, "rb") as source:
        while chunk := source.read(1 << 20):
            output_fp.write(chunk)
    if output_fp.tell() != index_offset + temporary_size:
        raise ValueError("Temporary index copy length differs from completed index")
