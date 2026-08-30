from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import numpy as np

from ..terminal_packing import TerminalMap
from .dfs import iter_cells_dfs
from .lookup import FileHandleCache, IntermediateLookup, RelocationLookup
from .manifest import read_packing_manifest
from .records import (
    DESCRIPTOR_SIZE,
    FRONTIER_REF_FMT,
    HAS_CHILDREN,
    HAS_PAYLOAD,
    HEADER_FMT,
    HEADER_SIZE,
    IS_FRONTIER,
    IS_TERMINAL,
    RELOC_HEADER_FMT,
    RELOC_HEADER_SIZE,
    RELOC_MAGIC,
    RELOC_RECORD_FMT,
    RELOC_RECORD_SIZE,
    SHARD_NODE_FMT,
    SHARD_NODE_V2_FMT,
    STAR_FORMAT_VERSION_V1,
    SUPPORTED_STAR_FORMAT_VERSIONS,
    PackedDescriptorFields,
    PackedHeaderFields,
    pack_descriptor,
    pack_shard_header,
    pack_top_level_header,
    validate_star_format_version,
)
from .streaming_index import IndexEmissionStrategy, write_streaming_index


@dataclass(frozen=True, slots=True)
class PackingPlan:
    max_open_files: int = 32
    lookup_cache_records: int = 65536
    retain_relocation_files: bool = False
    star_format_version: int = STAR_FORMAT_VERSION_V1
    cache_dir: Path | None = None
    skeleton_pack_count: int = 256
    index_emission_strategy: IndexEmissionStrategy = (
        IndexEmissionStrategy.TEMP_PWRITE_BATCHED
    )

    def validate(self) -> None:
        if self.max_open_files <= 0:
            raise ValueError("max_open_files must be > 0")
        if self.lookup_cache_records <= 0:
            raise ValueError("lookup_cache_records must be > 0")
        if self.skeleton_pack_count <= 0 or self.skeleton_pack_count > 4096:
            raise ValueError("skeleton_pack_count must be in 1..4096")
        IndexEmissionStrategy(self.index_emission_strategy)
        validate_star_format_version(self.star_format_version)


@dataclass(frozen=True, slots=True)
class PayloadPassResult:
    payload_end_offset: int
    relocation_files: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class IndexPassResult:
    index_offset: int
    index_length: int


@dataclass(frozen=True, slots=True)
class _ShardNode:
    global_level: int
    node_id: int
    local_depth: int
    local_path: int
    child_mask: int
    payload: tuple[int, int, int] | None
    children: tuple[_ShardNode, ...]
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class _ShardBuildResult:
    shard_offset: int
    shard_id: int


DEFAULT_PACKING_PLAN = PackingPlan()


def _format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    units = ("KiB", "MiB", "GiB", "TiB")
    value = float(n)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f} {unit}"
    return f"{value:.1f} PiB"


class _RelocAppender:
    def __init__(self, path: Path, *, level: int, prefix_bits: int, prefix: int):
        self.path = path
        self.level = level
        self.prefix_bits = prefix_bits
        self.prefix = prefix
        self.count = 0
        self._last_node_id: int | None = None
        with open(path, "wb") as fp:
            fp.write(
                RELOC_HEADER_FMT.pack(
                    RELOC_MAGIC,
                    1,
                    RELOC_HEADER_SIZE,
                    level,
                    prefix_bits,
                    0,
                    RELOC_RECORD_SIZE,
                    prefix,
                    0,
                )
            )

    def append(
        self,
        *,
        node_id: int,
        output_offset: int,
        payload_length: int,
        star_count: int,
        file_cache: FileHandleCache,
    ) -> None:
        if self._last_node_id is not None and node_id <= self._last_node_id:
            raise ValueError(
                f"Relocation node_id must be increasing: {node_id} <= {self._last_node_id}"
            )
        fp = file_cache.open(self.path, "ab")
        fp.write(
            RELOC_RECORD_FMT.pack(
                int(node_id),
                int(output_offset),
                int(payload_length),
                int(star_count),
            )
        )
        self.count += 1
        self._last_node_id = node_id

    def finalize(self) -> None:
        with open(self.path, "r+b") as fp:
            fp.seek(0)
            fp.write(
                RELOC_HEADER_FMT.pack(
                    RELOC_MAGIC,
                    1,
                    RELOC_HEADER_SIZE,
                    self.level,
                    self.prefix_bits,
                    0,
                    RELOC_RECORD_SIZE,
                    self.prefix,
                    self.count,
                )
            )


def pack_octree(
    manifest_path: Path,
    output_path: Path,
    *,
    plan: PackingPlan = DEFAULT_PACKING_PLAN,
    descriptor: PackedDescriptorFields | None = None,
) -> None:
    t0 = time.perf_counter()
    plan.validate()
    manifest = read_packing_manifest(manifest_path, deep_validation=False)
    if descriptor is None:
        descriptor = PackedDescriptorFields(artifact_kind=manifest.artifact_kind)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(
        (
            f"Packing: starting ({len(manifest.shards)} shard(s), "
            f"max_level={manifest.max_level}) -> {output_path}"
        ),
        flush=True,
    )

    with open(output_path, "wb") as out_fp:
        phase_a_start = time.perf_counter()
        print("Packing: Phase A (payload relocation) started.", flush=True)
        out_fp.write(
            pack_top_level_header(
                PackedHeaderFields(
                    world_center=manifest.world_center,
                    world_half_size_pc=manifest.world_half_size_pc,
                    max_level=manifest.max_level,
                    mag_limit=manifest.mag_limit,
                ),
                version=plan.star_format_version,
            )
        )
        out_fp.write(pack_descriptor(descriptor))
        payload_result = relocate_payloads_dfs(manifest_path, out_fp, plan=plan)
        print(
            (
                "Packing: Phase A complete "
                f"({len(payload_result.relocation_files)} relocation file(s), "
                f"payload_end_offset={payload_result.payload_end_offset}, "
                f"elapsed={time.perf_counter() - phase_a_start:.1f}s)."
            ),
            flush=True,
        )
        phase_b_start = time.perf_counter()
        print("Packing: Phase B (index write) started.", flush=True)
        index_result = write_final_shard_index(
            manifest_path,
            payload_result.relocation_files,
            out_fp,
            plan=plan,
        )
        print(
            (
                "Packing: Phase B complete "
                f"(index_offset={index_result.index_offset}, "
                f"index_length={index_result.index_length}, "
                f"elapsed={time.perf_counter() - phase_b_start:.1f}s)."
            ),
            flush=True,
        )

    phase_c_start = time.perf_counter()
    print("Packing: Phase C (header finalize + cleanup) started.", flush=True)
    finalize_octree_header(
        output_path,
        index_offset=index_result.index_offset,
        index_length=index_result.index_length,
        world_center=manifest.world_center,
        world_half_size_pc=manifest.world_half_size_pc,
    )
    print(
        f"Packing: header patched in {time.perf_counter() - phase_c_start:.1f}s.",
        flush=True,
    )

    if not plan.retain_relocation_files:
        for p in payload_result.relocation_files:
            p.unlink(missing_ok=True)
        print(
            f"Packing: removed {len(payload_result.relocation_files)} relocation file(s).",
            flush=True,
        )
    else:
        print(
            "Packing: retained relocation files (--retain-relocation-files).",
            flush=True,
        )

    print(
        f"Packing: done in {time.perf_counter() - t0:.1f}s.",
        flush=True,
    )


def relocate_payloads_dfs(
    manifest_path: Path,
    output_fp: BinaryIO,
    *,
    plan: PackingPlan,
) -> PayloadPassResult:
    manifest = read_packing_manifest(manifest_path, deep_validation=False)
    if manifest.payload_layout == "global-dfs/v1":
        return _relocate_global_dfs_payload(manifest, output_fp)
    shard_by_key = {
        (s.key.level, s.key.prefix_bits, s.key.prefix): s for s in manifest.shards
    }
    reloc_by_key: dict[tuple[int, int, int], _RelocAppender] = {}
    copied_cells = 0
    copied_bytes = 0
    progress_every_cells = 250_000
    progress_every_seconds = 2.0
    next_report_cell = progress_every_cells
    last_report_t = time.perf_counter()
    payload_files = FileHandleCache(plan.max_open_files)
    relocation_files = FileHandleCache(plan.max_open_files)

    try:
        for cell in iter_cells_dfs(
            manifest_path,
            max_open_files=plan.max_open_files,
        ):
            key = (cell.shard.level, cell.shard.prefix_bits, cell.shard.prefix)
            shard = shard_by_key[key]
            src_fp = payload_files.open(shard.payload_path, "rb")
            src_fp.seek(cell.payload_offset)
            output_offset = output_fp.tell()

            remaining = cell.payload_length
            while remaining > 0:
                chunk = src_fp.read(min(remaining, 1 << 20))
                if not chunk:
                    raise ValueError(
                        f"Unexpected EOF copying payload for node {cell.level}:{cell.node_id}"
                    )
                output_fp.write(chunk)
                remaining -= len(chunk)
            copied_cells += 1
            copied_bytes += int(cell.payload_length)
            now = time.perf_counter()
            if (
                copied_cells >= next_report_cell
                or now - last_report_t >= progress_every_seconds
            ):
                print(
                    (
                        "Packing: Phase A progress "
                        f"cells={copied_cells:,}, "
                        f"bytes={_format_bytes(copied_bytes)}, "
                        f"out_offset={output_fp.tell():,}"
                    ),
                    flush=True,
                )
                while copied_cells >= next_report_cell:
                    next_report_cell += progress_every_cells
                last_report_t = now

            reloc = reloc_by_key.get(key)
            if reloc is None:
                stem = shard.index_path.name
                reloc_path = manifest.root_dir / f"{stem}.reloc"
                reloc = _RelocAppender(
                    reloc_path,
                    level=cell.shard.level,
                    prefix_bits=cell.shard.prefix_bits,
                    prefix=cell.shard.prefix,
                )
                reloc_by_key[key] = reloc
            reloc.append(
                node_id=cell.node_id,
                output_offset=output_offset,
                payload_length=cell.payload_length,
                star_count=cell.star_count,
                file_cache=relocation_files,
            )
    finally:
        payload_files.close_all()
        relocation_files.close_all()

    relocation_paths: list[Path] = []
    for key in sorted(reloc_by_key):
        app = reloc_by_key[key]
        app.finalize()
        relocation_paths.append(app.path)

    print(
        (
            "Packing: Phase A final "
            f"cells={copied_cells:,}, "
            f"bytes={_format_bytes(copied_bytes)}, "
            f"out_offset={output_fp.tell():,}"
        ),
        flush=True,
    )

    return PayloadPassResult(
        payload_end_offset=output_fp.tell(),
        relocation_files=tuple(relocation_paths),
    )


_INDEX_DTYPE = np.dtype(
    [
        ("node_id", "<u8"),
        ("payload_offset", "<u8"),
        ("payload_length", "<u4"),
        ("star_count", "<u4"),
    ]
)


def _relocate_global_dfs_payload(
    manifest,
    output_fp: BinaryIO,
) -> PayloadPassResult:
    """Copy a direct classic payload once and translate offsets blockwise."""
    payload_paths = {shard.payload_path for shard in manifest.shards}
    if len(payload_paths) != 1:
        raise ValueError("global-dfs payload layout requires one shared payload file")
    payload_path = next(iter(payload_paths))
    payload_base = output_fp.tell()
    with open(payload_path, "rb") as source:
        shutil.copyfileobj(source, output_fp, 8 << 20)
    payload_size = payload_path.stat().st_size
    if output_fp.tell() != payload_base + payload_size:
        raise ValueError("Global DFS payload copy length mismatch")

    relocation_paths: list[Path] = []
    copied_cells = 0
    described_bytes = 0
    for shard in manifest.shards:
        reloc_path = manifest.root_dir / f"{shard.index_path.name}.reloc"
        with open(shard.index_path, "rb") as source, open(reloc_path, "wb") as target:
            header = source.read(RELOC_HEADER_SIZE)
            if len(header) != RELOC_HEADER_SIZE:
                raise ValueError(f"Truncated direct classic index: {shard.index_path}")
            target.write(
                RELOC_HEADER_FMT.pack(
                    RELOC_MAGIC,
                    1,
                    RELOC_HEADER_SIZE,
                    shard.key.level,
                    shard.key.prefix_bits,
                    0,
                    RELOC_RECORD_SIZE,
                    shard.key.prefix,
                    shard.record_count,
                )
            )
            remaining = shard.record_count
            while remaining:
                take = min(remaining, 1_000_000)
                records = np.fromfile(source, dtype=_INDEX_DTYPE, count=take)
                if len(records) != take:
                    raise ValueError(
                        f"Truncated direct classic records: {shard.index_path}"
                    )
                records = records.copy()
                records["payload_offset"] += np.uint64(payload_base)
                described_bytes += int(records["payload_length"].sum(dtype=np.uint64))
                target.write(records.tobytes())
                copied_cells += take
                remaining -= take
            if source.read(1):
                raise ValueError(
                    f"Trailing direct classic index bytes: {shard.index_path}"
                )
        relocation_paths.append(reloc_path)
    if described_bytes != payload_size:
        raise ValueError(
            "Global DFS payload directory does not cover the payload exactly: "
            f"described={described_bytes}, payload={payload_size}"
        )
    print(
        (
            "Packing: Phase A direct-part copy "
            f"cells={copied_cells:,}, bytes={_format_bytes(payload_size)}, "
            f"files=1, out_offset={output_fp.tell():,}"
        ),
        flush=True,
    )
    return PayloadPassResult(
        payload_end_offset=output_fp.tell(),
        relocation_files=tuple(relocation_paths),
    )


def _decode_grid(level: int, node_id: int) -> tuple[int, int, int]:
    if level < 0:
        return 0, 0, 0
    x = 0
    y = 0
    z = 0
    for i in range(level):
        shift = 3 * (level - 1 - i)
        octant = (node_id >> shift) & 0x7
        x = (x << 1) | (octant & 0x1)
        y = (y << 1) | ((octant >> 1) & 0x1)
        z = (z << 1) | ((octant >> 2) & 0x1)
    return x, y, z


def _build_node(
    existence: IntermediateLookup,
    relocation: RelocationLookup,
    terminal_map: TerminalMap | None,
    *,
    max_level: int,
    global_level: int,
    node_id: int,
    local_depth: int,
    local_path: int,
) -> _ShardNode | None:
    if not existence.descendant_exists(global_level, node_id, max_level):
        return None
    has_payload = existence.has_payload_node(global_level, node_id)
    payload_row = relocation.get_payload(global_level, node_id)
    if has_payload and payload_row is None:
        raise ValueError(
            "Missing relocation entry for payload-bearing node "
            f"({global_level}, {node_id})"
        )
    if not has_payload and payload_row is not None:
        raise ValueError(
            "Unexpected relocation entry for non-payload node "
            f"({global_level}, {node_id})"
        )

    child_mask = 0
    child_candidates: list[tuple[int, int, int, int]] = []
    if global_level < max_level:
        base = node_id << 3
        for octant in range(8):
            child_id = base | octant
            next_level = global_level + 1
            if existence.descendant_exists(next_level, child_id, max_level):
                child_mask |= 1 << octant
                child_candidates.append(
                    (next_level, child_id, local_depth + 1, (local_path << 3) | octant)
                )

    children: list[_ShardNode] = []
    if local_depth < 5:
        for next_level, child_id, child_depth, child_path in child_candidates:
            child = _build_node(
                existence,
                relocation,
                terminal_map,
                max_level=max_level,
                global_level=next_level,
                node_id=child_id,
                local_depth=child_depth,
                local_path=child_path,
            )
            if child is not None:
                children.append(child)

    terminal = terminal_map is not None and terminal_map.contains(global_level, node_id)
    if terminal and payload_row is None:
        raise ValueError(f"Terminal node has no payload: ({global_level}, {node_id})")
    if terminal and child_mask != 0:
        raise ValueError(
            f"Terminal node retains descendants: ({global_level}, {node_id})"
        )
    return _ShardNode(
        global_level=global_level,
        node_id=node_id,
        local_depth=local_depth,
        local_path=local_path,
        child_mask=child_mask,
        payload=payload_row,
        terminal=terminal,
        children=tuple(children),
    )


def _flatten_nodes(top_nodes: list[_ShardNode]) -> list[_ShardNode]:
    out: list[_ShardNode] = []

    def walk(n: _ShardNode) -> None:
        out.append(n)
        for child in n.children:
            walk(child)

    for node in top_nodes:
        walk(node)
    out.sort(key=lambda n: (n.local_depth, n.local_path))
    return out


def _build_shard_nodes(
    existence: IntermediateLookup,
    relocation: RelocationLookup,
    terminal_map: TerminalMap | None,
    *,
    max_level: int,
    parent_level: int,
    parent_node_id: int,
) -> list[_ShardNode]:
    top_nodes: list[_ShardNode] = []
    child_level = parent_level + 1
    if child_level > max_level:
        return top_nodes
    base = parent_node_id << 3
    for octant in range(8):
        node_id = base | octant
        node = _build_node(
            existence,
            relocation,
            terminal_map,
            max_level=max_level,
            global_level=child_level,
            node_id=node_id,
            local_depth=1,
            local_path=octant,
        )
        if node is not None:
            top_nodes.append(node)
    return _flatten_nodes(top_nodes)


def write_final_shard_index(
    manifest_path: Path,
    relocation_files: tuple[Path, ...],
    output_fp: BinaryIO,
    *,
    plan: PackingPlan,
) -> IndexPassResult:
    manifest = read_packing_manifest(manifest_path, deep_validation=False)
    result = write_streaming_index(
        manifest,
        relocation_files,
        output_fp,
        max_open_files=plan.max_open_files,
        star_format_version=plan.star_format_version,
        skeleton_pack_count=plan.skeleton_pack_count,
        emission_strategy=IndexEmissionStrategy(plan.index_emission_strategy),
        cache_dir=plan.cache_dir or (manifest.root_dir / ".packing-index-cache"),
    )
    return IndexPassResult(
        index_offset=result.index_offset,
        index_length=result.index_length,
    )


def _write_final_shard_index_legacy(
    manifest_path: Path,
    relocation_files: tuple[Path, ...],
    output_fp: BinaryIO,
    *,
    plan: PackingPlan,
) -> IndexPassResult:
    """Lookup-based writer retained only as a byte-compatibility test oracle."""
    if plan.star_format_version != STAR_FORMAT_VERSION_V1:
        raise ValueError("The legacy index test oracle only supports STAR v1")
    manifest = read_packing_manifest(manifest_path)
    existence = IntermediateLookup(manifest, max_open_files=plan.max_open_files)
    relocation = RelocationLookup(relocation_files, max_open_files=plan.max_open_files)
    writer = _IndexWriter(
        output_fp,
        existence,
        relocation,
        max_level=manifest.max_level,
        star_format_version=plan.star_format_version,
        terminal_map=(
            TerminalMap(manifest.terminal_map_path)
            if manifest.terminal_map_path is not None
            else None
        ),
    )
    try:
        index_offset = output_fp.tell()
        writer.write_root()
        index_length = output_fp.tell() - index_offset
        return IndexPassResult(index_offset=index_offset, index_length=index_length)
    finally:
        existence.close()
        relocation.close()


class _IndexWriter:
    def __init__(
        self,
        output_fp: BinaryIO,
        existence: IntermediateLookup,
        relocation: RelocationLookup,
        *,
        max_level: int,
        star_format_version: int,
        terminal_map: TerminalMap | None,
    ):
        self._out = output_fp
        self._existence = existence
        self._relocation = relocation
        self._max_level = max_level
        self._star_format_version = validate_star_format_version(star_format_version)
        self._terminal_map = terminal_map
        self._next_shard_id = 1

    def write_root(self) -> None:
        if not self._existence.descendant_exists(0, 0, self._max_level):
            return
        self._write_shard(
            parent_shard_id=0,
            parent_node_index=0,
            parent_level=-1,
            parent_node_id=0,
        )

    def _write_shard(
        self,
        *,
        parent_shard_id: int,
        parent_node_index: int,
        parent_level: int,
        parent_node_id: int,
    ) -> _ShardBuildResult:
        nodes = _build_shard_nodes(
            self._existence,
            self._relocation,
            self._terminal_map,
            max_level=self._max_level,
            parent_level=parent_level,
            parent_node_id=parent_node_id,
        )
        if not nodes:
            return _ShardBuildResult(shard_offset=0, shard_id=0)

        node_count = len(nodes)
        if node_count > 0xFFFF:
            raise ValueError(f"node_count exceeds u16: {node_count}")

        shard_id = self._next_shard_id
        self._next_shard_id += 1
        shard_offset = self._out.tell()
        self._out.write(b"\x00" * 80)
        node_table_offset = self._out.tell()

        index_by_key = {
            (n.local_depth, n.local_path): i + 1 for i, n in enumerate(nodes)
        }

        entry_nodes = [0] * 8
        frontier_indices: list[int] = []
        frontier_nodes: list[_ShardNode] = []

        for i, node in enumerate(nodes, start=1):
            if node.local_depth == 1:
                top_octant = node.local_path & 0x7
                entry_nodes[top_octant] = i

            in_shard_children = [c for c in node.children if c.local_depth <= 5]
            child_indices = [
                index_by_key[(c.local_depth, c.local_path)] for c in in_shard_children
            ]
            child_indices.sort()
            first_child = child_indices[0] if child_indices else 0
            if len(child_indices) > 1:
                for i in range(len(child_indices) - 1):
                    a = child_indices[i]
                    b = child_indices[i + 1]
                    if b != a + 1:
                        raise ValueError(
                            f"In-shard children not contiguous for local_path={node.local_path}"
                        )

            is_frontier = node.local_depth == 5
            if is_frontier:
                first_child = 0
                frontier_indices.append(i)
                frontier_nodes.append(node)

            if node.local_path > 0x7FFF:
                raise ValueError(f"local_path exceeds u15: {node.local_path}")

            flags = 0
            payload_offset = 0
            payload_length = 0
            star_count = 0
            if node.payload is not None:
                payload_offset, payload_length, star_count = node.payload
                flags |= HAS_PAYLOAD
            if node.child_mask != 0:
                flags |= HAS_CHILDREN
            if is_frontier:
                flags |= IS_FRONTIER
            if self._star_format_version != STAR_FORMAT_VERSION_V1 and node.terminal:
                flags |= IS_TERMINAL

            if self._star_format_version == STAR_FORMAT_VERSION_V1:
                packed_node = SHARD_NODE_FMT.pack(
                    first_child,
                    node.local_path,
                    node.child_mask,
                    node.local_depth,
                    flags,
                    0,
                    payload_offset,
                    payload_length,
                )
            else:
                packed_node = SHARD_NODE_V2_FMT.pack(
                    first_child,
                    node.local_path,
                    node.child_mask,
                    node.local_depth,
                    flags,
                    0,
                    payload_offset,
                    payload_length,
                    star_count,
                )
            self._out.write(packed_node)

        frontier_table_offset = self._out.tell()
        for _ in frontier_nodes:
            self._out.write(FRONTIER_REF_FMT.pack(0))

        for i, node in enumerate(frontier_nodes):
            ref = 0
            if node.child_mask != 0 and node.global_level < self._max_level:
                child = self._write_shard(
                    parent_shard_id=shard_id,
                    parent_node_index=frontier_indices[i],
                    parent_level=node.global_level,
                    parent_node_id=node.node_id,
                )
                ref = child.shard_offset
            patch_at = frontier_table_offset + i * FRONTIER_REF_FMT.size
            end = self._out.tell()
            self._out.seek(patch_at)
            self._out.write(FRONTIER_REF_FMT.pack(ref))
            self._out.seek(end)

        first_frontier_index = frontier_indices[0] if frontier_indices else 0
        parent_grid_x, parent_grid_y, parent_grid_z = _decode_grid(
            parent_level, parent_node_id
        )
        header = pack_shard_header(
            shard_id=shard_id,
            parent_shard_id=parent_shard_id,
            parent_node_index=parent_node_index,
            node_count=node_count,
            parent_global_depth=parent_level,
            parent_grid_x=parent_grid_x,
            parent_grid_y=parent_grid_y,
            parent_grid_z=parent_grid_z,
            entry_nodes=(
                entry_nodes[0],
                entry_nodes[1],
                entry_nodes[2],
                entry_nodes[3],
                entry_nodes[4],
                entry_nodes[5],
                entry_nodes[6],
                entry_nodes[7],
            ),
            first_frontier_index=first_frontier_index,
            node_table_offset=node_table_offset,
            frontier_table_offset=frontier_table_offset,
            payload_base_offset=HEADER_SIZE + DESCRIPTOR_SIZE,
            version=self._star_format_version,
        )

        end = self._out.tell()
        self._out.seek(shard_offset)
        self._out.write(header)
        self._out.seek(end)
        return _ShardBuildResult(shard_offset=shard_offset, shard_id=shard_id)


def finalize_octree_header(
    output_path: Path,
    *,
    index_offset: int,
    index_length: int,
    world_center: tuple[float, float, float],
    world_half_size_pc: float,
) -> None:
    with open(output_path, "r+b") as fp:
        old = fp.read(HEADER_SIZE)
        if len(old) != HEADER_SIZE:
            raise ValueError("Output file too small for header patch")
        (
            magic,
            version,
            _flags,
            _old_index_offset,
            _old_index_length,
            _old_cx,
            _old_cy,
            _old_cz,
            _old_half,
            payload_record_size,
            max_level,
            mag_limit,
            reserved,
        ) = HEADER_FMT.unpack(old)
        if magic != b"STAR" or version not in SUPPORTED_STAR_FORMAT_VERSIONS:
            raise ValueError("Output file has invalid header magic/version")
        cx, cy, cz = world_center
        patched = pack_top_level_header(
            PackedHeaderFields(
                world_center=(float(cx), float(cy), float(cz)),
                world_half_size_pc=world_half_size_pc,
                max_level=max_level,
                mag_limit=mag_limit,
                index_offset=index_offset,
                index_length=index_length,
            ),
            version=int(version),
        )
        # Keep currently reserved area and payload-record size/max level contract.
        patched = bytearray(patched)
        patched[40:42] = int(payload_record_size).to_bytes(2, "little")
        patched[42:44] = int(max_level).to_bytes(2, "little")
        patched[48:64] = reserved
        fp.seek(0)
        fp.write(bytes(patched))
