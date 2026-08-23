from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from foundinspace.octree.config import MORTON_BITS, WORLD_CENTER, WORLD_HALF_SIZE_PC
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.magnitudes import RENDER_MAG_CODEC_IDENTITY

from .semantic_checksum import (
    SEMANTIC_CHECKSUM_ALGORITHM,
    checksum_parquet_files,
)

ROUTING_GROUP_CHECKSUM_ALGORITHM = SEMANTIC_CHECKSUM_ALGORITHM
ROUTING_ROW_SCHEMA_VERSION = "routing-row-schema/v4"
ROUTING_SPLIT_POLICY = "lower-mag-limited-bucket/v0"
NATURAL_LEVEL_POLICY = "full-width/v1"
ROUTING_INPUT_MODE_PRE_ROUTED = "pre-routed"
ROUTING_INPUT_MODE_CARTESIAN = "cartesian"
ROUTING_INPUT_MODES = (
    ROUTING_INPUT_MODE_PRE_ROUTED,
    ROUTING_INPUT_MODE_CARTESIAN,
)
TREE_DIR_NAME = "tree"
REPORT_NAME = "routing-report.json"
TREE_MANIFEST_NAME = "tree-manifest.json"
PIPELINE_STATE_NAME = "pipeline-state.json"
ROUTING_TRANSACTION_NAME = ".routing-transaction.json"
ROUTING_CHECKSUM_DIR = ".routing-checksums"
ROUTING_PROGRESS_COUNTERS = (
    "fragment_files_written",
    "fragment_files_deleted_on_split",
    "fragment_files_deleted_on_compaction",
    "split_rewrites",
    "compaction_rewrites",
    "compaction_input_files",
    "compaction_output_files",
)
LOWER_MAG_LIMITED_MARKER = "_LOWER_MAG_LIMITED"
_FRAGMENT_RE = re.compile(
    r"^(?:shard-(?P<shard>.+?)|hp(?P<legacy_shard>.+?))"
    r"-(?P<kind>pack|lim)-(?P<seq>\d+)\.parquet$"
)
_ROUTING_COLUMN_TYPES = {
    "morton_code": pa.uint64(),
    "level": pa.int32(),
}
_CANONICAL_INPUT_COLUMN_TYPES = {
    "quality_flags": pa.uint16(),
}


@dataclass(frozen=True, slots=True)
class RoutingConfig:
    input_shards_dir: Path
    routed_dir: Path
    mag_config: MagLevelConfig
    bucket_rows: int = 1_000_000
    scan_batch_rows: int = 1_000_000
    fragment_target_rows: int = 100_000
    max_open_writers: int = 128
    compact_after_files: int = 64
    shard_ids: tuple[str, ...] = ()
    max_shards: int | None = None
    input_mode: str = ROUTING_INPUT_MODE_PRE_ROUTED
    force: bool = False
    replace_shards: bool = False

    def validate(self) -> None:
        if not self.input_shards_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.input_shards_dir}")
        if self.bucket_rows <= 0:
            raise ValueError("bucket_rows must be > 0")
        if self.scan_batch_rows <= 0:
            raise ValueError("scan_batch_rows must be > 0")
        if self.fragment_target_rows <= 0:
            raise ValueError("fragment_target_rows must be > 0")
        if self.max_open_writers <= 0:
            raise ValueError("max_open_writers must be > 0")
        if self.compact_after_files < 0:
            raise ValueError("compact_after_files must be >= 0")
        if self.max_shards is not None and self.max_shards <= 0:
            raise ValueError("max_shards must be > 0")
        if self.input_mode not in ROUTING_INPUT_MODES:
            raise ValueError(
                "routing input_mode must be one of "
                f"{list(ROUTING_INPUT_MODES)}, got {self.input_mode!r}"
            )
        if self.replace_shards:
            if not self.shard_ids:
                raise ValueError("--replace-shards requires one or more --shard")
            if self.force:
                raise ValueError("--replace-shards cannot be used with --force")
            if self.max_shards is not None:
                raise ValueError("--replace-shards cannot be used with --max-shards")


@dataclass(slots=True)
class _BucketNode:
    path_octants: tuple[int, ...]
    prefix: int
    directory: Path
    lower_mag_limited: bool = False
    row_count: int = 0
    next_sequence: int = 1
    split_count: int = 0
    current_files: set[Path] = field(default_factory=set)

    @property
    def depth(self) -> int:
        return len(self.path_octants)


@dataclass(frozen=True, slots=True)
class _WriterKey:
    path_octants: tuple[int, ...]
    input_shard_id: str
    kind: str


@dataclass(frozen=True, slots=True)
class _InputShard:
    shard_id: str
    parquet_files: tuple[Path, ...]


@dataclass(slots=True)
class _OpenFragmentWriter:
    key: _WriterKey
    node: _BucketNode
    path: Path
    schema: pa.Schema
    writer: pq.ParquetWriter
    rows: int = 0


class _RoutingTransaction:
    def __init__(self, output_dir: Path) -> None:
        self.routed_dir = output_dir
        self.transaction_id = uuid4().hex
        self.journal_path = output_dir / ROUTING_TRANSACTION_NAME
        self.created_files: list[str] = []
        self.obsolete_files: list[str] = []
        self._write_journal()

    def register_created(self, path: Path) -> None:
        rel_path = path.relative_to(self.routed_dir).as_posix()
        if rel_path not in self.created_files:
            self.created_files.append(rel_path)
            self._write_journal()

    def register_obsolete(self, path: Path) -> None:
        rel_path = path.relative_to(self.routed_dir).as_posix()
        if rel_path not in self.obsolete_files:
            self.obsolete_files.append(rel_path)
            self._write_journal()

    def preserve(self, path: Path) -> None:
        """Keep a previously published file after candidate comparison."""
        rel_path = path.relative_to(self.routed_dir).as_posix()
        if rel_path in self.obsolete_files:
            self.obsolete_files.remove(rel_path)
            self._write_journal()

    def commit_state(
        self,
        state_path: Path,
        state: dict[str, Any],
    ) -> None:
        state["builds"]["routing"]["last_committed_transaction"] = self.transaction_id
        _atomic_write_json(state_path, state)
        self._cleanup_obsolete()
        self.journal_path.unlink(missing_ok=True)

    def _cleanup_obsolete(self) -> None:
        for rel_path in self.obsolete_files:
            (self.routed_dir / rel_path).unlink(missing_ok=True)

    def _write_journal(self) -> None:
        _atomic_write_json(
            self.journal_path,
            {
                "transaction_id": self.transaction_id,
                "created_files": self.created_files,
                "obsolete_files": self.obsolete_files,
            },
        )


class _RoutingBuilder:
    def __init__(
        self,
        config: RoutingConfig,
        *,
        existing_state: dict[str, Any] | None = None,
        preserve_existing_topology: bool = False,
        transaction: _RoutingTransaction | None = None,
    ) -> None:
        self._config = config
        self._tree_dir = config.routed_dir / TREE_DIR_NAME
        self._nodes: dict[tuple[int, ...], _BucketNode] = {}
        self._preserve_existing_topology = preserve_existing_topology
        self._transaction = transaction
        self._open_writers: OrderedDict[_WriterKey, _OpenFragmentWriter] = OrderedDict()
        self._file_rows: dict[Path, int] = {}
        self._rows_in = 0
        self._rows_after_filter = 0
        self._rows_written = 0
        self._files_written = 0
        self._files_deleted_on_split = 0
        self._files_deleted_on_compaction = 0
        self._split_rewrites = 0
        self._compaction_rewrites = 0
        self._compaction_input_files = 0
        self._compaction_output_files = 0
        self._max_open_writers_seen = 0
        self._batches = 0
        if existing_state is not None:
            self._load_existing_state(existing_state)

    @property
    def rows_in(self) -> int:
        return self._rows_in

    @property
    def rows_after_filter(self) -> int:
        return self._rows_after_filter

    @property
    def input_batches(self) -> int:
        return self._batches

    def process_table(self, table: pa.Table, *, input_shard_id: str) -> None:
        if len(table) == 0:
            return
        self._batches += 1
        input_rows = len(table)
        self._rows_in += input_rows
        filtered = _apply_input_mode(table, self._config)
        _ensure_equal_row_count(
            before=input_rows,
            after=len(filtered),
            context=f"Routing input_mode {self._config.input_mode}",
        )
        self._rows_after_filter += len(filtered)
        normalized = _normalize_routing_input_schema(filtered)
        routed = _ensure_routing_columns(normalized)
        if self._config.input_mode == ROUTING_INPUT_MODE_PRE_ROUTED:
            _validate_pre_routed_levels(routed, self._config.mag_config)
        self._route_table(
            self._node_for_path(()),
            routed,
            input_shard_id=input_shard_id,
        )

    def finish(self, *, compact_shard_ids: set[str] | None = None) -> None:
        self._close_all_writers()
        self._compact_current_files(compact_shard_ids=compact_shard_ids)

    def report(
        self,
        *,
        processed_input_shards: list[str],
        input_files: int,
        groups: list[dict[str, Any]] | None = None,
        rows_in: int | None = None,
        rows_after_filter: int | None = None,
        input_batches: int | None = None,
    ) -> dict[str, Any]:
        by_depth: dict[int, dict[str, Any]] = {}
        for node in self._nodes.values():
            row = by_depth.setdefault(
                node.depth,
                {
                    "depth": node.depth,
                    "nodes": 0,
                    "lower_mag_limited_nodes": 0,
                    "packed_nodes": 0,
                    "current_files": 0,
                    "current_rows": 0,
                    "max_node_rows": 0,
                },
            )
            row["nodes"] += 1
            if node.lower_mag_limited:
                row["lower_mag_limited_nodes"] += 1
            else:
                row["packed_nodes"] += 1
            row["current_files"] += len(node.current_files)
            row["current_rows"] += node.row_count
            row["max_node_rows"] = max(row["max_node_rows"], node.row_count)

        current_files = sum(len(node.current_files) for node in self._nodes.values())
        current_rows = sum(node.row_count for node in self._nodes.values())
        lower_limited = sum(
            1 for node in self._nodes.values() if node.lower_mag_limited
        )
        return {
            "input_shards_dir": str(self._config.input_shards_dir),
            "routed_dir": str(self._config.routed_dir),
            "tree_dir": str(self._tree_dir),
            "bucket_rows": self._config.bucket_rows,
            "scan_batch_rows": self._config.scan_batch_rows,
            "fragment_target_rows": self._config.fragment_target_rows,
            "max_open_writers": self._config.max_open_writers,
            "compact_after_files": self._config.compact_after_files,
            "tree_manifest": TREE_MANIFEST_NAME,
            "pipeline_state": PIPELINE_STATE_NAME,
            "group_checksum_algorithm": ROUTING_GROUP_CHECKSUM_ALGORITHM,
            "replacement_mode": self._config.replace_shards,
            "processed_input_shards": processed_input_shards,
            "input_files": input_files,
            "input_batches": (
                self._batches if input_batches is None else int(input_batches)
            ),
            "input_mode": self._config.input_mode,
            "rows_in": self._rows_in if rows_in is None else int(rows_in),
            "rows_after_filter": (
                self._rows_after_filter
                if rows_after_filter is None
                else int(rows_after_filter)
            ),
            "rows_current": current_rows,
            "staging_nodes": len(self._nodes),
            "lower_mag_limited_nodes": lower_limited,
            "packed_nodes": len(self._nodes) - lower_limited,
            "current_fragment_files": current_files,
            "fragment_files_written": self._files_written,
            "fragment_files_deleted_on_split": self._files_deleted_on_split,
            "fragment_files_deleted_on_compaction": self._files_deleted_on_compaction,
            "split_rewrites": self._split_rewrites,
            "compaction_rewrites": self._compaction_rewrites,
            "compaction_input_files": self._compaction_input_files,
            "compaction_output_files": self._compaction_output_files,
            "max_open_writers_seen": self._max_open_writers_seen,
            "by_depth": [by_depth[d] for d in sorted(by_depth)],
            "groups": self._group_reports() if groups is None else groups,
        }

    def nodes_report(self) -> list[dict[str, Any]]:
        return [
            {
                "node_path": _node_path_label(node.path_octants),
                "path_octants": list(node.path_octants),
                "depth": node.depth,
                "row_count": node.row_count,
                "lower_mag_limited": node.lower_mag_limited,
            }
            for node in sorted(self._nodes.values(), key=lambda item: item.path_octants)
        ]

    def remove_input_shards(self, shard_ids: set[str], state: dict[str, Any]) -> None:
        for group in state.get("products", {}).get("routed_groups", []):
            if (
                str(group.get("input_shard_id", group.get("shard_id", "")))
                not in shard_ids
            ):
                continue
            path_octants = tuple(int(v) for v in group["path_octants"])
            node = self._node_for_path(path_octants)
            for rel_file in group.get("files", []):
                path = self._config.routed_dir / rel_file
                node.current_files.discard(path)
                self._mark_obsolete(path)
            node.row_count = max(0, node.row_count - int(group.get("row_count", 0)))

    def reuse_existing_group(
        self,
        *,
        candidate: dict[str, Any],
        existing: dict[str, Any],
    ) -> None:
        """Discard a replacement candidate and restore its immutable group."""
        if self._transaction is None:
            raise ValueError("Reusing a Routing group requires a transaction")
        if candidate["key"] != existing["key"]:
            raise ValueError("Cannot reuse a Routing group with a different key")

        candidate_rows = int(candidate["row_count"])
        existing_rows = int(existing["row_count"])
        _ensure_equal_row_count(
            before=candidate_rows,
            after=existing_rows,
            context=f"reusing Routing group {candidate['key']}",
        )
        node = self._node_for_path(tuple(int(v) for v in candidate["path_octants"]))
        for rel_file in candidate.get("files", []):
            path = self._config.routed_dir / str(rel_file)
            node.current_files.discard(path)
            self._mark_obsolete(path)

        old_file_rows = list(existing.get("file_row_counts", []))
        for index, rel_file in enumerate(existing.get("files", [])):
            path = self._config.routed_dir / str(rel_file)
            if not path.is_file():
                raise FileNotFoundError(
                    f"Missing published Routing group fragment: {path}"
                )
            self._transaction.preserve(path)
            node.current_files.add(path)
            if index < len(old_file_rows):
                self._file_rows[path] = int(old_file_rows[index])
            else:
                self._file_rows[path] = pq.read_metadata(path).num_rows

        node.row_count += existing_rows - candidate_rows

    def _load_existing_state(self, state: dict[str, Any]) -> None:
        for node_record in sorted(
            state.get("nodes", []),
            key=lambda row: tuple(int(v) for v in row["path_octants"]),
        ):
            node = self._node_for_path(
                tuple(int(v) for v in node_record["path_octants"])
            )
            node.row_count = int(node_record.get("row_count", 0))
            node.lower_mag_limited = bool(node_record.get("lower_mag_limited", False))

        for group in state.get("products", {}).get("routed_groups", []):
            node = self._node_for_path(tuple(int(v) for v in group["path_octants"]))
            file_rows = list(group.get("file_row_counts", []))
            for index, rel_file in enumerate(group.get("files", [])):
                path = self._config.routed_dir / rel_file
                node.current_files.add(path)
                if index < len(file_rows):
                    self._file_rows[path] = int(file_rows[index])
                else:
                    self._file_rows[path] = pq.read_metadata(path).num_rows

        for node in self._nodes.values():
            node.next_sequence = _next_fragment_sequence(node.current_files)

    def _group_reports(self, *, checksums: bool = True) -> list[dict[str, Any]]:
        groups: dict[tuple[tuple[int, ...], str, str], list[Path]] = {}
        for node in self._nodes.values():
            for path in sorted(node.current_files):
                groups.setdefault(
                    (
                        node.path_octants,
                        _input_shard_id_from_fragment(path),
                        _fragment_kind(path),
                    ),
                    [],
                ).append(path)

        rows: list[dict[str, Any]] = []
        for (path_octants, input_shard_id, kind), files in sorted(groups.items()):
            row_count = sum(self._file_rows[path] for path in files)
            row = {
                "key": _routing_group_key(path_octants, input_shard_id, kind),
                "node_path": _node_path_label(path_octants),
                "path_octants": list(path_octants),
                "depth": len(path_octants),
                "input_shard_id": input_shard_id,
                "kind": kind,
                "file_count": len(files),
                "row_count": row_count,
                "files": [
                    path.relative_to(self._config.routed_dir).as_posix()
                    for path in files
                ],
                "file_row_counts": [self._file_rows[path] for path in files],
            }
            if checksums:
                checksum, checksummed_rows = _routing_group_checksum(files)
                _ensure_equal_row_count(
                    before=row_count,
                    after=checksummed_rows,
                    context=f"checksumming group {row['key']}",
                )
                row["content_checksum"] = checksum
            rows.append(row)
        return rows

    def _node_for_path(self, path_octants: tuple[int, ...]) -> _BucketNode:
        existing = self._nodes.get(path_octants)
        if existing is not None:
            return existing

        prefix = 0
        directory = self._tree_dir
        for octant in path_octants:
            prefix = (prefix << 3) | int(octant)
            directory = directory / f"o={octant}"
        directory.mkdir(parents=True, exist_ok=True)
        node = _BucketNode(
            path_octants=path_octants, prefix=prefix, directory=directory
        )
        self._nodes[path_octants] = node
        return node

    def _route_table(
        self,
        node: _BucketNode,
        table: pa.Table,
        *,
        input_shard_id: str,
    ) -> None:
        if len(table) == 0:
            return

        levels = _level_array(table)
        if levels.min(initial=node.depth) < node.depth:
            raise ValueError(
                f"Encountered final level above staging node depth {node.depth}"
            )

        if node.lower_mag_limited:
            self._route_into_lower_limited_node(
                node,
                table,
                input_shard_id=input_shard_id,
            )
            return

        self._write_fragment(node, table, input_shard_id=input_shard_id, kind="pack")
        if (
            node.row_count >= self._config.bucket_rows
            and not self._preserve_existing_topology
        ):
            self._make_lower_mag_limited(node)

    def _route_into_lower_limited_node(
        self,
        node: _BucketNode,
        table: pa.Table,
        *,
        input_shard_id: str,
    ) -> None:
        levels = _level_array(table)
        resident_indices = np.flatnonzero(levels == node.depth)
        if len(resident_indices) > 0:
            self._write_fragment(
                node,
                _take_rows(table, resident_indices),
                input_shard_id=input_shard_id,
                kind="lim",
            )

        descendant_indices = np.flatnonzero(levels > node.depth)
        _ensure_equal_row_count(
            before=len(table),
            after=len(resident_indices) + len(descendant_indices),
            context=f"routing lower-mag-limited node {_node_path_label(node.path_octants)}",
        )
        if len(descendant_indices) == 0:
            return
        if node.depth >= MORTON_BITS:
            raise ValueError(
                f"Rows below Morton depth {node.depth} cannot be routed lower"
            )

        descendants = _take_rows(table, descendant_indices)
        routed_descendant_rows = 0
        for octant, child_table in _tables_by_child_octant(descendants, node.depth):
            routed_descendant_rows += len(child_table)
            child = self._node_for_path((*node.path_octants, int(octant)))
            self._route_table(child, child_table, input_shard_id=input_shard_id)
        _ensure_equal_row_count(
            before=len(descendants),
            after=routed_descendant_rows,
            context=f"routing descendants below {_node_path_label(node.path_octants)}",
        )

    def _make_lower_mag_limited(self, node: _BucketNode) -> None:
        if node.lower_mag_limited:
            return
        self._close_writers_for_node(node, kind="pack")
        node.lower_mag_limited = True
        node.split_count += 1
        self._split_rewrites += 1
        (node.directory / LOWER_MAG_LIMITED_MARKER).write_text(
            "lower-mag-limited\n",
            encoding="utf-8",
        )

        pack_files = sorted(
            p for p in node.current_files if _fragment_kind(p) == "pack"
        )
        rows_before_split = node.row_count
        rows_in_pack_files = 0
        node.current_files.clear()
        node.row_count = 0

        for path in pack_files:
            input_shard_id = _input_shard_id_from_fragment(path)
            table = pq.ParquetFile(path).read()
            rows_in_pack_files += len(table)
            self._mark_obsolete(path)
            self._files_deleted_on_split += 1
            self._route_into_lower_limited_node(
                node,
                table,
                input_shard_id=input_shard_id,
            )
        _ensure_equal_row_count(
            before=rows_before_split,
            after=rows_in_pack_files,
            context=f"reading split input for node {_node_path_label(node.path_octants)}",
        )

    def _write_fragment(
        self,
        node: _BucketNode,
        table: pa.Table,
        *,
        input_shard_id: str,
        kind: str,
    ) -> None:
        if kind not in {"pack", "lim"}:
            raise ValueError(f"Unsupported fragment kind: {kind!r}")
        if len(table) == 0:
            return
        key = _WriterKey(
            path_octants=node.path_octants,
            input_shard_id=_safe_input_shard_id(input_shard_id),
            kind=kind,
        )
        offset = 0
        while offset < len(table):
            writer = self._writer_for(node, key, table.schema)
            remaining_capacity = self._config.fragment_target_rows - writer.rows
            if remaining_capacity <= 0:
                self._close_writer(key)
                continue
            rows_to_write = min(remaining_capacity, len(table) - offset)
            chunk = table.slice(offset, rows_to_write)
            writer.writer.write_table(chunk)
            writer.rows += rows_to_write
            node.row_count += rows_to_write
            self._rows_written += rows_to_write
            offset += rows_to_write
            self._open_writers.move_to_end(key)
            if writer.rows >= self._config.fragment_target_rows:
                self._close_writer(key)

    def _writer_for(
        self,
        node: _BucketNode,
        key: _WriterKey,
        schema: pa.Schema,
    ) -> _OpenFragmentWriter:
        existing = self._open_writers.get(key)
        if existing is not None:
            if not existing.schema.equals(schema, check_metadata=False):
                raise ValueError(
                    "Routing group schema changed while writing "
                    f"{_routing_group_key(node.path_octants, key.input_shard_id, key.kind)}"
                )
            self._open_writers.move_to_end(key)
            return existing

        while len(self._open_writers) >= self._config.max_open_writers:
            old_key = next(iter(self._open_writers))
            self._close_writer(old_key)

        path = self._next_fragment_path(
            node,
            input_shard_id=key.input_shard_id,
            kind=key.kind,
        )
        if self._transaction is not None:
            self._transaction.register_created(path)
        writer = _OpenFragmentWriter(
            key=key,
            node=node,
            path=path,
            schema=schema,
            writer=pq.ParquetWriter(path, schema, compression="zstd"),
        )
        node.current_files.add(path)
        self._files_written += 1
        self._open_writers[key] = writer
        self._max_open_writers_seen = max(
            self._max_open_writers_seen,
            len(self._open_writers),
        )
        return writer

    def _next_fragment_path(
        self,
        node: _BucketNode,
        *,
        input_shard_id: str,
        kind: str,
    ) -> Path:
        safe_input_shard = _safe_input_shard_id(input_shard_id)
        path = (
            node.directory
            / f"shard-{safe_input_shard}-{kind}-{node.next_sequence:06d}.parquet"
        )
        node.next_sequence += 1
        return path

    def _close_writer(self, key: _WriterKey) -> None:
        writer = self._open_writers.pop(key, None)
        if writer is None:
            return
        writer.writer.close()
        self._file_rows[writer.path] = writer.rows

    def _close_writers_for_node(
        self,
        node: _BucketNode,
        *,
        kind: str | None = None,
    ) -> None:
        keys = [
            key
            for key in self._open_writers
            if key.path_octants == node.path_octants
            and (kind is None or key.kind == kind)
        ]
        for key in keys:
            self._close_writer(key)

    def _close_all_writers(self) -> None:
        for key in list(self._open_writers):
            self._close_writer(key)

    def _compact_current_files(self, *, compact_shard_ids: set[str] | None) -> None:
        threshold = self._config.compact_after_files
        if threshold == 0:
            return
        for node in sorted(self._nodes.values(), key=lambda n: n.path_octants):
            groups: dict[tuple[str, str], list[Path]] = {}
            for path in sorted(node.current_files):
                groups.setdefault(
                    (_input_shard_id_from_fragment(path), _fragment_kind(path)),
                    [],
                ).append(path)

            for (input_shard_id, kind), files in sorted(groups.items()):
                if (
                    compact_shard_ids is not None
                    and input_shard_id not in compact_shard_ids
                ):
                    continue
                if len(files) <= threshold:
                    continue
                self._compact_file_group(
                    node,
                    files,
                    input_shard_id=input_shard_id,
                    kind=kind,
                )

    def _compact_file_group(
        self,
        node: _BucketNode,
        files: list[Path],
        *,
        input_shard_id: str,
        kind: str,
    ) -> None:
        new_files: list[Path] = []
        buffered: list[pa.Table] = []
        buffered_rows = 0
        buffered_schema: pa.Schema | None = None
        input_rows = 0
        output_rows = 0

        def flush_buffer() -> None:
            nonlocal buffered, buffered_rows, buffered_schema, output_rows
            if not buffered:
                return
            compacted = pa.concat_tables(buffered, promote_options="none")
            path = self._next_fragment_path(
                node,
                input_shard_id=input_shard_id,
                kind=kind,
            )
            if self._transaction is not None:
                self._transaction.register_created(path)
            pq.write_table(compacted, path, compression="zstd")
            new_files.append(path)
            self._file_rows[path] = len(compacted)
            self._files_written += 1
            self._rows_written += len(compacted)
            output_rows += len(compacted)
            self._compaction_output_files += 1
            buffered = []
            buffered_rows = 0
            buffered_schema = None

        for path in files:
            table = pq.ParquetFile(path).read()
            input_rows += len(table)
            if buffered_schema is None:
                buffered_schema = table.schema
            elif not buffered_schema.equals(table.schema, check_metadata=False):
                raise ValueError(
                    "Cannot compact Routing group with schema drift: "
                    f"{path.relative_to(self._config.routed_dir).as_posix()}"
                )
            offset = 0
            while offset < len(table):
                if buffered_rows >= self._config.fragment_target_rows:
                    flush_buffer()
                rows_to_take = min(
                    self._config.fragment_target_rows - buffered_rows,
                    len(table) - offset,
                )
                buffered.append(table.slice(offset, rows_to_take))
                buffered_rows += rows_to_take
                offset += rows_to_take
        flush_buffer()
        _ensure_equal_row_count(
            before=input_rows,
            after=output_rows,
            context=f"compacting group {_routing_group_key(node.path_octants, input_shard_id, kind)}",
        )

        for path in files:
            self._mark_obsolete(path)
        node.current_files.difference_update(files)
        node.current_files.update(new_files)
        self._files_deleted_on_compaction += len(files)
        self._compaction_input_files += len(files)
        self._compaction_rewrites += 1

    def _mark_obsolete(self, path: Path) -> None:
        self._file_rows.pop(path, None)
        if self._transaction is not None:
            self._transaction.register_obsolete(path)
        else:
            path.unlink(missing_ok=True)


def route_contributions(config: RoutingConfig) -> Path:
    """Route input shards into adaptive contribution buckets.

    Pre-routed input must contain Morton code and natural-level columns. Cartesian
    input receives those columns before rows are assigned to contribution buckets.
    """
    config.validate()
    if config.replace_shards:
        selected_input_shards = _selected_input_shards(config)
        return _route_contributions_replacement(config, selected_input_shards)

    if config.force and config.routed_dir.exists():
        shutil.rmtree(config.routed_dir)
    config.routed_dir.mkdir(parents=True, exist_ok=True)

    selected_input_shards = _selected_input_shards(config)
    input_plan = [
        _input_shard_metadata(config.input_shards_dir, shard)
        for shard in selected_input_shards
        if shard.parquet_files
    ]
    manifest_path = config.routed_dir / TREE_MANIFEST_NAME
    state_path = config.routed_dir / PIPELINE_STATE_NAME
    if manifest_path.is_file() and state_path.is_file():
        state = _read_json(state_path)
        _recover_routing_transaction(config.routed_dir, state)
        state = _read_json(state_path)
        _validate_resumable_routing_state(
            config,
            state=state,
            manifest=_read_json(manifest_path),
            input_plan=input_plan,
        )
        _sync_topology_markers(config.routed_dir, state)
    elif any(config.routed_dir.iterdir()):
        raise FileExistsError(
            f"Output directory is not a resumable routing build: {config.routed_dir}. "
            "Use --force to replace it."
        )
    else:
        builder = _RoutingBuilder(config)
        manifest = _tree_manifest(config)
        state = _pipeline_state(
            config,
            builder=builder,
            input_shards=[],
            groups=[],
            dirty=_initial_dirty_state(),
        )
        state["builds"]["routing"] = {
            "status": "in_progress",
            "input_plan": input_plan,
            "completed_shards": [],
            "progress": {
                "rows_in": 0,
                "rows_after_filter": 0,
                "input_files": 0,
                "input_batches": 0,
            },
        }
        _atomic_write_json(manifest_path, manifest)
        _atomic_write_json(state_path, state)

    build_state = state["builds"]["routing"]
    completed_shards = {str(value) for value in build_state.get("completed_shards", [])}
    selected_by_id = {shard.shard_id: shard for shard in selected_input_shards}
    if build_state.get("status") == "in_progress":
        for shard_meta in input_plan:
            shard_id = str(shard_meta["shard_id"])
            if shard_id in completed_shards:
                continue
            shard = selected_by_id[shard_id]
            transaction = _RoutingTransaction(config.routed_dir)
            builder = _RoutingBuilder(
                config,
                existing_state=state,
                preserve_existing_topology=False,
                transaction=transaction,
            )
            try:
                _processed, input_files = _process_input_shards(
                    builder,
                    [shard],
                    scan_batch_rows=config.scan_batch_rows,
                )
                builder.finish(compact_shard_ids={shard_id})
            except Exception:
                builder._close_all_writers()
                raise
            groups = builder._group_reports(checksums=False)
            shard_report = builder.report(
                processed_input_shards=[shard_id],
                input_files=input_files,
                groups=groups,
            )
            progress = dict(build_state.get("progress", {}))
            progress["rows_in"] = int(progress.get("rows_in", 0)) + builder.rows_in
            progress["rows_after_filter"] = (
                int(progress.get("rows_after_filter", 0)) + builder.rows_after_filter
            )
            progress["input_files"] = int(progress.get("input_files", 0)) + input_files
            progress["input_batches"] = (
                int(progress.get("input_batches", 0)) + builder.input_batches
            )
            for counter in ROUTING_PROGRESS_COUNTERS:
                progress[counter] = int(progress.get(counter, 0)) + int(
                    shard_report[counter]
                )
            progress["max_open_writers_seen"] = max(
                int(progress.get("max_open_writers_seen", 0)),
                int(shard_report["max_open_writers_seen"]),
            )
            completed_shards.add(shard_id)
            state = _pipeline_state(
                config,
                builder=builder,
                input_shards=_replace_input_shard_metadata(
                    state.get("input_shards", []),
                    target_shards={shard_id},
                    replacements=[shard_meta],
                ),
                groups=groups,
                dirty=_initial_dirty_state(),
            )
            state["builds"]["routing"] = {
                "status": "in_progress",
                "input_plan": input_plan,
                "completed_shards": sorted(completed_shards),
                "progress": progress,
            }
            transaction.commit_state(state_path, state)
            build_state = state["builds"]["routing"]

        state["builds"]["routing"]["status"] = "checksumming"
        _atomic_write_json(state_path, state)

    if state["builds"]["routing"].get("status") == "checksumming":
        state = _checkpoint_routing_group_checksums(config, state)

    if state["builds"]["routing"].get("status") != "complete":
        raise ValueError("Routing did not reach a complete checkpoint")

    builder = _RoutingBuilder(config, existing_state=state)
    progress = state["builds"]["routing"]["progress"]
    report_groups = [
        _report_group(group) for group in state["products"]["routed_groups"]
    ]
    report = builder.report(
        processed_input_shards=[
            str(row["shard_id"]) for row in state.get("input_shards", [])
        ],
        input_files=int(progress["input_files"]),
        groups=report_groups,
        rows_in=int(progress["rows_in"]),
        rows_after_filter=int(progress["rows_after_filter"]),
        input_batches=int(progress["input_batches"]),
    )
    for counter in ROUTING_PROGRESS_COUNTERS:
        report[counter] = int(progress.get(counter, 0))
    report["max_open_writers_seen"] = int(progress.get("max_open_writers_seen", 0))
    report["replacement_mode"] = False
    report["changed_group_count"] = len(report["groups"])
    report["unchanged_group_count"] = 0
    report["deleted_group_count"] = 0
    _validate_routing_full_row_counts(report)
    report_path = config.routed_dir / REPORT_NAME
    _atomic_write_json(report_path, report)
    return report_path


def _route_contributions_replacement(
    config: RoutingConfig,
    selected_input_shards: list[_InputShard],
) -> Path:
    manifest_path = config.routed_dir / TREE_MANIFEST_NAME
    state_path = config.routed_dir / PIPELINE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Routing tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Routing state: {state_path}")

    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    _recover_routing_transaction(config.routed_dir, state)
    state = _read_json(state_path)
    expected_identity = _tree_identity(config)
    existing_identity = manifest.get("tree_identity")
    if existing_identity != expected_identity:
        raise ValueError(
            "Existing Routing tree identity does not match current project config; "
            "use --force for a full rebuild."
        )
    if state.get("tree_identity") != existing_identity:
        raise ValueError("Routing state identity does not match tree manifest")
    build_state = state.get("builds", {}).get("routing")
    if isinstance(build_state, dict) and build_state.get("status") != "complete":
        raise ValueError("Routing shard replacement requires a complete full build")

    target_shards = {shard.shard_id for shard in selected_input_shards}
    old_target_groups = {
        group["key"]: group
        for group in state.get("products", {}).get("routed_groups", [])
        if str(group.get("input_shard_id", group.get("shard_id", ""))) in target_shards
    }
    old_total_rows = _sum_group_rows(state.get("products", {}).get("routed_groups", []))
    old_target_rows = _sum_group_rows(old_target_groups.values())

    transaction = _RoutingTransaction(config.routed_dir)
    builder = _RoutingBuilder(
        config,
        existing_state=state,
        preserve_existing_topology=True,
        transaction=transaction,
    )
    builder.remove_input_shards(target_shards, state)
    try:
        processed_input_shards, input_files = _process_input_shards(
            builder,
            selected_input_shards,
            scan_batch_rows=config.scan_batch_rows,
        )
        builder.finish(compact_shard_ids=target_shards)
    except Exception:
        builder._close_all_writers()
        raise

    candidate_groups = _replacement_group_reports(
        config,
        builder=builder,
        published_groups=state.get("products", {}).get("routed_groups", []),
        target_shards=target_shards,
    )
    new_target_groups = {
        group["key"]: group
        for group in candidate_groups
        if group["input_shard_id"] in target_shards
    }
    changed_group_keys = [
        key
        for key, group in sorted(new_target_groups.items())
        if old_target_groups.get(key, {}).get("content_checksum")
        != group["content_checksum"]
    ]
    unchanged_group_keys = [
        key
        for key, group in sorted(new_target_groups.items())
        if old_target_groups.get(key, {}).get("content_checksum")
        == group["content_checksum"]
    ]
    deleted_group_keys = sorted(set(old_target_groups) - set(new_target_groups))

    for key in unchanged_group_keys:
        builder.reuse_existing_group(
            candidate=new_target_groups[key],
            existing=old_target_groups[key],
        )
    unchanged_group_key_set = set(unchanged_group_keys)
    groups = [
        _report_group(old_target_groups[group["key"]])
        if group["key"] in unchanged_group_key_set
        else group
        for group in candidate_groups
    ]
    report = builder.report(
        processed_input_shards=processed_input_shards,
        input_files=input_files,
        groups=groups,
    )
    report["replacement_mode"] = True
    _validate_routing_replacement_row_counts(
        report,
        old_total_rows=old_total_rows,
        old_target_rows=old_target_rows,
        new_target_rows=_sum_group_rows(new_target_groups.values()),
    )
    report["changed_group_count"] = len(changed_group_keys)
    report["unchanged_group_count"] = len(unchanged_group_keys)
    report["deleted_group_count"] = len(deleted_group_keys)

    input_shards = _replace_input_shard_metadata(
        state.get("input_shards", []),
        target_shards=target_shards,
        replacements=[
            _input_shard_metadata(config.input_shards_dir, shard)
            for shard in selected_input_shards
        ],
    )
    existing_dirty = state.get("dirty", {}).get("preparation", {})
    current_group_keys = {str(group["key"]) for group in report["groups"]}
    pending_preparation_groups = sorted(
        (
            {str(key) for key in existing_dirty.get("group_keys", [])}.union(
                changed_group_keys
            )
        )
        & current_group_keys
    )
    pending_deleted_routing_groups = sorted(
        (
            {
                str(key) for key in existing_dirty.get("deleted_routed_group_keys", [])
            }.union(deleted_group_keys)
        )
        - current_group_keys
    )
    next_state = _pipeline_state(
        config,
        builder=builder,
        input_shards=input_shards,
        groups=report["groups"],
        dirty={
            "preparation": {
                "all": bool(existing_dirty.get("all", False)),
                "group_keys": pending_preparation_groups,
                "deleted_routed_group_keys": pending_deleted_routing_groups,
            }
        },
        preparation_groups=list(state.get("products", {}).get("prepared_groups", [])),
    )
    build_state = dict(state.get("builds", {}).get("routing", {}))
    if build_state:
        build_state["input_plan"] = input_shards
        build_state["completed_shards"] = [str(row["shard_id"]) for row in input_shards]
        progress = dict(build_state.get("progress", {}))
        progress["rows_in"] = int(report["rows_current"])
        progress["rows_after_filter"] = int(report["rows_current"])
        progress["input_files"] = sum(
            len(row.get("source_files", [])) for row in input_shards
        )
        build_state["progress"] = progress
        next_state["builds"]["routing"] = build_state
    transaction.commit_state(state_path, next_state)
    _atomic_write_json(config.routed_dir / REPORT_NAME, report)
    return config.routed_dir / REPORT_NAME


def _process_input_shards(
    builder: _RoutingBuilder,
    input_shards: list[_InputShard],
    *,
    scan_batch_rows: int,
) -> tuple[list[str], int]:
    processed_input_shards: list[str] = []
    input_files = 0

    for input_shard in input_shards:
        if not input_shard.parquet_files:
            continue
        processed_input_shards.append(input_shard.shard_id)
        for src_file in input_shard.parquet_files:
            input_files += 1
            parquet_file = pq.ParquetFile(src_file)
            for batch in parquet_file.iter_batches(batch_size=scan_batch_rows):
                table = pa.Table.from_batches([batch])
                builder.process_table(table, input_shard_id=input_shard.shard_id)
    return processed_input_shards, input_files


def _replacement_group_reports(
    config: RoutingConfig,
    *,
    builder: _RoutingBuilder,
    published_groups: list[dict[str, Any]],
    target_shards: set[str],
) -> list[dict[str, Any]]:
    """Checksum replacement candidates while trusting untouched published groups."""
    published_by_key = {str(group["key"]): group for group in published_groups}
    reports: list[dict[str, Any]] = []
    for group in builder._group_reports(checksums=False):
        if group["input_shard_id"] not in target_shards:
            published = published_by_key.get(str(group["key"]))
            if published is None:
                raise ValueError(
                    "Routing replacement unexpectedly created a group for an "
                    f"untargeted shard: {group['key']}"
                )
            if group["files"] != published.get("files") or int(
                group["row_count"]
            ) != int(published.get("row_count", -1)):
                raise ValueError(
                    f"Routing replacement modified an untargeted group: {group['key']}"
                )
            reports.append(_report_group(published))
            continue

        paths = [config.routed_dir / str(value) for value in group.get("files", [])]
        checksum, row_count = _routing_group_checksum(paths)
        _ensure_equal_row_count(
            before=int(group["row_count"]),
            after=row_count,
            context=f"checksumming replacement group {group['key']}",
        )
        group["content_checksum"] = checksum
        reports.append(group)
    return reports


def _tree_manifest(config: RoutingConfig) -> dict[str, Any]:
    return {"tree_identity": _tree_identity(config)}


def _tree_identity(config: RoutingConfig) -> dict[str, Any]:
    return _tree_identity_values(
        limiting_magnitude=float(config.mag_config.v_mag),
        bucket_rows=config.bucket_rows,
        input_mode=config.input_mode,
    )


def _tree_identity_values(
    *,
    limiting_magnitude: float,
    bucket_rows: int,
    input_mode: str = ROUTING_INPUT_MODE_PRE_ROUTED,
) -> dict[str, Any]:
    return {
        "coordinate_frame": "icrs-cartesian-pc",
        "world_center": [float(v) for v in WORLD_CENTER.tolist()],
        "world_half_size_pc": float(WORLD_HALF_SIZE_PC),
        "morton_bits": MORTON_BITS,
        "limiting_magnitude": float(limiting_magnitude),
        "bucket_rows": bucket_rows,
        "input_mode": input_mode,
        "split_policy": ROUTING_SPLIT_POLICY,
        "natural_level_policy": NATURAL_LEVEL_POLICY,
        "render_magnitude_codec": RENDER_MAG_CODEC_IDENTITY,
        "row_schema_version": ROUTING_ROW_SCHEMA_VERSION,
        "group_checksum_algorithm": ROUTING_GROUP_CHECKSUM_ALGORITHM,
    }


def _pipeline_state(
    config: RoutingConfig,
    *,
    builder: _RoutingBuilder,
    input_shards: list[dict[str, Any]],
    groups: list[dict[str, Any]],
    dirty: dict[str, Any],
    preparation_groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state = {
        "tree_manifest": TREE_MANIFEST_NAME,
        "tree_identity": _tree_identity(config),
        "input_shards_dir": str(config.input_shards_dir),
        "routed_dir": str(config.routed_dir),
        "input_mode": config.input_mode,
        "input_shards": sorted(input_shards, key=lambda row: row["shard_id"]),
        "nodes": builder.nodes_report(),
        "products": {
            "routed_groups": [_state_group(group) for group in groups],
            "prepared_groups": preparation_groups or [],
        },
        "builds": {},
        "dirty": dirty,
    }
    return state


def _state_group(group: dict[str, Any]) -> dict[str, Any]:
    row = {
        "key": group["key"],
        "node_path": group["node_path"],
        "path_octants": list(group["path_octants"]),
        "depth": group["depth"],
        "shard_id": group["input_shard_id"],
        "input_shard_id": group["input_shard_id"],
        "kind": group["kind"],
        "files": list(group["files"]),
        "file_count": group["file_count"],
        "row_count": group["row_count"],
        "file_row_counts": list(group.get("file_row_counts", [])),
    }
    checksum = group.get("content_checksum")
    if checksum is not None:
        row["checksum"] = checksum
        row["content_checksum"] = checksum
    return row


def _report_group(group: dict[str, Any]) -> dict[str, Any]:
    checksum = group.get("content_checksum")
    if checksum is None:
        raise ValueError(f"Routing group is not checksummed: {group['key']}")
    return {
        "key": group["key"],
        "node_path": group["node_path"],
        "path_octants": list(group["path_octants"]),
        "depth": group["depth"],
        "input_shard_id": group["input_shard_id"],
        "kind": group["kind"],
        "file_count": group["file_count"],
        "row_count": group["row_count"],
        "content_checksum": checksum,
        "files": list(group["files"]),
        "file_row_counts": list(group.get("file_row_counts", [])),
    }


def _input_shard_metadata(input_shards_dir: Path, shard: _InputShard) -> dict[str, Any]:
    source_files: list[dict[str, Any]] = []
    for path in shard.parquet_files:
        stat = path.stat()
        source_files.append(
            {
                "path": path.relative_to(input_shards_dir).as_posix(),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return {
        "shard_id": shard.shard_id,
        "source_files": source_files,
    }


def _replace_input_shard_metadata(
    existing: list[dict[str, Any]],
    *,
    target_shards: set[str],
    replacements: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        row for row in existing if str(row.get("shard_id", "")) not in target_shards
    ]
    rows.extend(replacements)
    return sorted(rows, key=lambda row: row["shard_id"])


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _initial_dirty_state() -> dict[str, Any]:
    return {
        "preparation": {
            "all": True,
            "group_keys": [],
            "deleted_routed_group_keys": [],
        }
    }


def _validate_resumable_routing_state(
    config: RoutingConfig,
    *,
    state: dict[str, Any],
    manifest: dict[str, Any],
    input_plan: list[dict[str, Any]],
) -> None:
    expected_identity = _tree_identity(config)
    if manifest.get("tree_identity") != expected_identity:
        raise ValueError(
            "Existing Routing tree identity does not match current project config; "
            "rerun route with --force."
        )
    if state.get("tree_identity") != expected_identity:
        raise ValueError("Routing state identity does not match tree manifest")
    build = state.get("builds", {}).get("routing")
    if not isinstance(build, dict):
        raise FileExistsError(
            "Existing routing output has no resumable build state; "
            "use --force to replace it."
        )
    if build.get("status") not in {"in_progress", "checksumming", "complete"}:
        raise ValueError(f"Invalid Routing build status: {build.get('status')!r}")
    if build.get("input_plan") != input_plan:
        raise ValueError(
            "Routing resume input plan changed; restore the original inputs "
            "or use --force for a new build."
        )


def _recover_routing_transaction(
    output_dir: Path,
    state: dict[str, Any],
) -> None:
    journal_path = output_dir / ROUTING_TRANSACTION_NAME
    if not journal_path.is_file():
        return
    journal = _read_json(journal_path)
    transaction_id = str(journal.get("transaction_id", ""))
    committed_id = str(
        state.get("builds", {}).get("routing", {}).get("last_committed_transaction", "")
    )
    cleanup_key = (
        "obsolete_files" if transaction_id == committed_id else "created_files"
    )
    for rel_path in journal.get(cleanup_key, []):
        (output_dir / str(rel_path)).unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)


def _sync_topology_markers(output_dir: Path, state: dict[str, Any]) -> None:
    expected: set[Path] = set()
    tree_dir = output_dir / TREE_DIR_NAME
    for node in state.get("nodes", []):
        if not bool(node.get("lower_mag_limited", False)):
            continue
        directory = tree_dir
        for octant in node.get("path_octants", []):
            directory = directory / f"o={int(octant)}"
        expected.add(directory / LOWER_MAG_LIMITED_MARKER)
    if tree_dir.exists():
        for path in tree_dir.rglob(LOWER_MAG_LIMITED_MARKER):
            if path not in expected:
                path.unlink()
    for path in expected:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("lower-mag-limited\n", encoding="utf-8")


def _checkpoint_routing_group_checksums(
    config: RoutingConfig,
    state: dict[str, Any],
) -> dict[str, Any]:
    state_path = config.routed_dir / PIPELINE_STATE_NAME
    groups = list(state.get("products", {}).get("routed_groups", []))
    checkpointed = _load_routing_checksum_checkpoints(
        config.routed_dir,
        groups,
    )
    for group in groups:
        checkpoint_checksum = checkpointed.get(str(group["key"]))
        if checkpoint_checksum is not None:
            group["checksum"] = checkpoint_checksum
            group["content_checksum"] = checkpoint_checksum
        if group.get("content_checksum") is not None:
            continue
        paths = [config.routed_dir / str(value) for value in group.get("files", [])]
        checksum, row_count = _routing_group_checksum(paths)
        _ensure_equal_row_count(
            before=int(group["row_count"]),
            after=row_count,
            context=f"checksumming group {group['key']}",
        )
        group["checksum"] = checksum
        group["content_checksum"] = checksum
        _write_routing_checksum_checkpoint(
            config.routed_dir,
            group=group,
            checksum=checksum,
        )
    state["products"]["routed_groups"] = groups
    state["dirty"] = _initial_dirty_state()
    state["builds"]["routing"]["status"] = "complete"
    _atomic_write_json(state_path, state)
    shutil.rmtree(
        config.routed_dir / ROUTING_CHECKSUM_DIR,
        ignore_errors=True,
    )
    return state


def _routing_checksum_checkpoint_path(output_dir: Path, key: str) -> Path:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return output_dir / ROUTING_CHECKSUM_DIR / f"{digest}.json"


def _write_routing_checksum_checkpoint(
    output_dir: Path,
    *,
    group: dict[str, Any],
    checksum: str,
) -> None:
    _atomic_write_json(
        _routing_checksum_checkpoint_path(output_dir, str(group["key"])),
        {
            "key": group["key"],
            "files": list(group.get("files", [])),
            "row_count": int(group["row_count"]),
            "content_checksum": checksum,
        },
    )


def _load_routing_checksum_checkpoints(
    output_dir: Path,
    groups: list[dict[str, Any]],
) -> dict[str, str]:
    expected = {str(group["key"]): group for group in groups}
    checkpoint_dir = output_dir / ROUTING_CHECKSUM_DIR
    out: dict[str, str] = {}
    if not checkpoint_dir.is_dir():
        return out
    for path in sorted(checkpoint_dir.glob("*.json")):
        try:
            raw = _read_json(path)
            key = str(raw["key"])
            group = expected[key]
            if raw.get("files") != group.get("files"):
                raise ValueError("checksum checkpoint files changed")
            if int(raw.get("row_count", -1)) != int(group["row_count"]):
                raise ValueError("checksum checkpoint row count changed")
            checksum = str(raw["content_checksum"])
            if not checksum.startswith("sha256:"):
                raise ValueError("invalid checksum checkpoint digest")
            out[key] = checksum
        except (KeyError, OSError, TypeError, ValueError):
            path.unlink(missing_ok=True)
    return out


def _next_fragment_sequence(paths: set[Path]) -> int:
    max_sequence = 0
    for path in paths:
        match = _FRAGMENT_RE.match(path.name)
        if match is not None:
            max_sequence = max(max_sequence, int(match.group("seq")))
    return max_sequence + 1


def _selected_input_shards(config: RoutingConfig) -> list[_InputShard]:
    if config.shard_ids:
        shards: list[_InputShard] = []
        missing: list[str] = []
        for shard_id in config.shard_ids:
            direct = config.input_shards_dir / shard_id
            with_suffix = config.input_shards_dir / f"{shard_id}.parquet"
            if direct.is_dir():
                shards.append(
                    _InputShard(
                        shard_id=direct.name,
                        parquet_files=tuple(sorted(direct.glob("*.parquet"))),
                    )
                )
            elif direct.is_file() and direct.suffix == ".parquet":
                shards.append(
                    _InputShard(shard_id=direct.stem, parquet_files=(direct,))
                )
            elif with_suffix.is_file():
                shards.append(
                    _InputShard(
                        shard_id=with_suffix.stem,
                        parquet_files=(with_suffix,),
                    )
                )
            else:
                missing.append(str(direct))
        if missing:
            raise FileNotFoundError(
                f"Missing input shard directories or parquet shards: {missing}"
            )
    else:
        shards = []
        for path in sorted(config.input_shards_dir.iterdir()):
            if path.is_dir():
                parquet_files = tuple(sorted(path.glob("*.parquet")))
                if parquet_files:
                    shards.append(
                        _InputShard(
                            shard_id=path.name,
                            parquet_files=parquet_files,
                        )
                    )
            elif path.is_file() and path.suffix == ".parquet":
                shards.append(
                    _InputShard(
                        shard_id=path.stem,
                        parquet_files=(path,),
                    )
                )
    if config.max_shards is not None:
        shards = shards[: config.max_shards]
    return shards


def _ensure_equal_row_count(*, before: int, after: int, context: str) -> None:
    if before != after:
        raise ValueError(f"{context} changed row count: before={before}, after={after}")


def _sum_group_rows(groups: Any) -> int:
    return sum(int(group.get("row_count", 0)) for group in groups)


def _validate_routing_full_row_counts(report: dict[str, Any]) -> None:
    group_rows = _sum_group_rows(report.get("groups", []))
    _ensure_equal_row_count(
        before=int(report["rows_in"]),
        after=int(report["rows_after_filter"]),
        context="Routing input filtering",
    )
    _ensure_equal_row_count(
        before=int(report["rows_after_filter"]),
        after=int(report["rows_current"]),
        context="Routing staging",
    )
    _ensure_equal_row_count(
        before=int(report["rows_current"]),
        after=group_rows,
        context="Routing group accounting",
    )


def _validate_routing_replacement_row_counts(
    report: dict[str, Any],
    *,
    old_total_rows: int,
    old_target_rows: int,
    new_target_rows: int,
) -> None:
    _ensure_equal_row_count(
        before=int(report["rows_in"]),
        after=int(report["rows_after_filter"]),
        context="Routing replacement input filtering",
    )
    _ensure_equal_row_count(
        before=int(report["rows_after_filter"]),
        after=new_target_rows,
        context="Routing replacement target staging",
    )
    expected_current_rows = old_total_rows - old_target_rows + new_target_rows
    _ensure_equal_row_count(
        before=expected_current_rows,
        after=int(report["rows_current"]),
        context="Routing replacement total row accounting",
    )
    _ensure_equal_row_count(
        before=int(report["rows_current"]),
        after=_sum_group_rows(report.get("groups", [])),
        context="Routing replacement group accounting",
    )


def _apply_input_mode(table: pa.Table, config: RoutingConfig) -> pa.Table:
    if config.input_mode == ROUTING_INPUT_MODE_PRE_ROUTED:
        return table
    if config.input_mode == ROUTING_INPUT_MODE_CARTESIAN:
        from .routing_columns import _add_routing_columns

        return _add_routing_columns(
            table,
            mag_config=config.mag_config,
        )
    raise ValueError(f"Unsupported Routing input_mode: {config.input_mode!r}")


def _normalize_routing_input_schema(table: pa.Table) -> pa.Table:
    """Safely normalize canonical payload fields from compatible legacy inputs."""
    for name, expected_type in _CANONICAL_INPUT_COLUMN_TYPES.items():
        if name not in table.schema.names:
            continue
        index = table.schema.get_field_index(name)
        field = table.schema.field(index)
        if field.type.equals(expected_type) and field.nullable:
            continue
        try:
            column = table.column(index).cast(expected_type, safe=True)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError) as exc:
            raise ValueError(
                "Routing cannot safely normalize input column "
                f"{name} from {field.type} to {expected_type}"
            ) from exc
        table = table.set_column(
            index,
            pa.field(
                name,
                expected_type,
                nullable=True,
                metadata=field.metadata,
            ),
            column,
        )
    return table


def _ensure_routing_columns(table: pa.Table) -> pa.Table:
    names = set(table.schema.names)
    missing = set(_ROUTING_COLUMN_TYPES) - names
    if missing:
        raise ValueError(
            "Routing input is missing required routing columns "
            f"{sorted(_ROUTING_COLUMN_TYPES)}; missing columns {sorted(missing)}"
        )
    for name, expected_type in _ROUTING_COLUMN_TYPES.items():
        field = table.schema.field(name)
        if not field.type.equals(expected_type):
            raise ValueError(
                "Routing column has wrong type: "
                f"{name} must be {expected_type}, got {field.type}"
            )
        if table.column(name).null_count:
            raise ValueError(f"Routing column must not contain nulls: {name}")
    return table


def _validate_pre_routed_levels(
    table: pa.Table,
    mag_config: MagLevelConfig,
) -> None:
    if "mag_abs" not in table.schema.names:
        raise ValueError(
            "Pre-routed input requires mag_abs to validate natural levels"
        )
    from .routing_columns import _compute_level

    magnitudes = np.asarray(table.column("mag_abs"), dtype=np.float64)
    supplied = np.asarray(table.column("level"), dtype=np.int32)
    expected = _compute_level(magnitudes, mag_config)
    mismatches = np.flatnonzero(supplied != expected)
    if len(mismatches) == 0:
        return
    first = int(mismatches[0])
    raise ValueError(
        "Pre-routed natural levels do not match full-width/v1 assignment from "
        "the represented render magnitude: "
        f"mismatches={len(mismatches)}, first_row={first}, "
        f"supplied={int(supplied[first])}, expected={int(expected[first])}"
    )


def _routing_group_key(
    path_octants: tuple[int, ...],
    input_shard_id: str,
    kind: str,
) -> str:
    return f"{_node_path_label(path_octants)}|{input_shard_id}|{kind}"


def _node_path_label(path_octants: tuple[int, ...]) -> str:
    return "/".join(f"o={octant}" for octant in path_octants)


def _routing_group_checksum(paths: list[Path]) -> tuple[str, int]:
    return checksum_parquet_files(sorted(paths))


def _align_tables_to_union_schema(tables: list[pa.Table]) -> pa.Table:
    if not tables:
        return pa.table({})
    schema = tables[0].schema
    for table in tables[1:]:
        if not table.schema.equals(schema, check_metadata=False):
            raise ValueError(
                "Routing group fragments must have identical schemas; "
                "schema drift would require padding or dropping columns"
            )
    return pa.concat_tables(tables, promote_options="none").combine_chunks()


def _level_array(table: pa.Table) -> np.ndarray:
    levels = np.asarray(table.column("level"), dtype=np.int32)
    if len(levels) and (levels.min() < 0 or levels.max() > MORTON_BITS):
        raise ValueError(f"Routing level values must be in 0..{MORTON_BITS}")
    return levels


def _morton_array(table: pa.Table) -> np.ndarray:
    return np.asarray(table.column("morton_code"), dtype=np.uint64)


def _take_rows(table: pa.Table, indices: np.ndarray) -> pa.Table:
    return table.take(pa.array(indices.astype(np.int64, copy=False)))


def _tables_by_child_octant(
    table: pa.Table,
    parent_depth: int,
) -> list[tuple[int, pa.Table]]:
    shift = 3 * (MORTON_BITS - parent_depth - 1)
    octants = ((_morton_array(table) >> shift) & np.uint64(0x7)).astype(np.int8)
    out: list[tuple[int, pa.Table]] = []
    for octant in sorted(int(v) for v in np.unique(octants)):
        indices = np.flatnonzero(octants == octant)
        out.append((octant, _take_rows(table, indices)))
    return out


def _safe_input_shard_id(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("input shard id must not be empty")
    if "/" in text or "\\" in text:
        raise ValueError(f"input shard id must not contain path separators: {value!r}")
    return text


def _input_shard_id_from_fragment(path: Path) -> str:
    match = _FRAGMENT_RE.match(path.name)
    if match is None:
        raise ValueError(f"Invalid Routing fragment filename: {path.name}")
    return match.group("shard") or match.group("legacy_shard")


def _fragment_kind(path: Path) -> str:
    match = _FRAGMENT_RE.match(path.name)
    if match is None:
        raise ValueError(f"Invalid Routing fragment filename: {path.name}")
    return match.group("kind")
