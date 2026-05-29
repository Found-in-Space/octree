from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from foundinspace.octree.config import MORTON_BITS, WORLD_CENTER, WORLD_HALF_SIZE_PC
from foundinspace.octree.mag_levels import MagLevelConfig

from .add_shard_columns import _enrich_table

STAGE00_FORMAT = "foundinspace.octree.stage00/v0"
STAGE00_GROUP_CHECKSUM_ALGORITHM = "arrow-ipc-sha256/v0"
TREE_DIR_NAME = "tree"
REPORT_NAME = "stage00-report.json"
LOWER_MAG_LIMITED_MARKER = "_LOWER_MAG_LIMITED"
_FRAGMENT_RE = re.compile(r"^hp(?P<hp>.+?)-(?P<kind>pack|lim)-(?P<seq>\d+)\.parquet$")
_HEALPIX_COLUMN_CANDIDATES = ("healpix", "healpix_id", "hp")
_COLUMN_TYPES = {
    "source": pa.large_string(),
    "source_id": pa.large_string(),
    "x_icrs_pc": pa.float64(),
    "y_icrs_pc": pa.float64(),
    "z_icrs_pc": pa.float64(),
    "ra_deg": pa.float64(),
    "dec_deg": pa.float64(),
    "r_pc": pa.float64(),
    "mag_abs": pa.float64(),
    "teff": pa.float64(),
    "quality_flags": pa.int64(),
    "astrometry_quality": pa.float64(),
    "photometry_quality": pa.float64(),
    "morton_code": pa.uint64(),
    "render": pa.binary(16),
    "level": pa.int32(),
}


@dataclass(frozen=True, slots=True)
class Stage00Config:
    input_root: Path
    output_dir: Path
    mag_config: MagLevelConfig
    max_level: int
    bucket_size: int = 1_000_000
    batch_size: int = 1_000_000
    fragment_target_rows: int = 100_000
    max_open_writers: int = 128
    compact_after_files: int = 64
    healpix_ids: tuple[str, ...] = ()
    max_pixels: int | None = None
    force: bool = False

    def validate(self) -> None:
        if not self.input_root.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.input_root}")
        if self.bucket_size <= 0:
            raise ValueError("bucket_size must be > 0")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if self.fragment_target_rows <= 0:
            raise ValueError("fragment_target_rows must be > 0")
        if self.max_open_writers <= 0:
            raise ValueError("max_open_writers must be > 0")
        if self.compact_after_files < 0:
            raise ValueError("compact_after_files must be >= 0")
        if self.max_level < 0:
            raise ValueError("max_level must be >= 0")
        if self.max_level > MORTON_BITS:
            raise ValueError(
                f"max_level ({self.max_level}) must be <= MORTON_BITS ({MORTON_BITS})"
            )
        if self.max_pixels is not None and self.max_pixels <= 0:
            raise ValueError("max_pixels must be > 0")


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
    healpix_id: str
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


class _Stage00Builder:
    def __init__(self, config: Stage00Config) -> None:
        self._config = config
        self._tree_dir = config.output_dir / TREE_DIR_NAME
        self._nodes: dict[tuple[int, ...], _BucketNode] = {}
        self._open_writers: OrderedDict[_WriterKey, _OpenFragmentWriter] = OrderedDict()
        self._rows_in = 0
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

    @property
    def rows_in(self) -> int:
        return self._rows_in

    def process_table(self, table: pa.Table, *, healpix_id: str) -> None:
        if len(table) == 0:
            return
        self._batches += 1
        self._rows_in += len(table)
        enriched = _ensure_stage00_columns(table, self._config.mag_config)
        self._route_table(self._node_for_path(()), enriched, healpix_id=healpix_id)

    def finish(self) -> None:
        self._close_all_writers()
        self._compact_current_files()

    def report(
        self, *, processed_healpix: list[str], input_files: int
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
            "format": STAGE00_FORMAT,
            "input_root": str(self._config.input_root),
            "output_dir": str(self._config.output_dir),
            "tree_dir": str(self._tree_dir),
            "bucket_size": self._config.bucket_size,
            "batch_size": self._config.batch_size,
            "fragment_target_rows": self._config.fragment_target_rows,
            "max_open_writers": self._config.max_open_writers,
            "compact_after_files": self._config.compact_after_files,
            "max_level": self._config.max_level,
            "group_checksum_algorithm": STAGE00_GROUP_CHECKSUM_ALGORITHM,
            "processed_healpix": processed_healpix,
            "input_files": input_files,
            "input_batches": self._batches,
            "rows_in": self._rows_in,
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
            "groups": self._group_reports(),
        }

    def _group_reports(self) -> list[dict[str, Any]]:
        groups: dict[tuple[tuple[int, ...], str, str], list[Path]] = {}
        for node in self._nodes.values():
            for path in sorted(node.current_files):
                groups.setdefault(
                    (
                        node.path_octants,
                        _healpix_id_from_fragment(path),
                        _fragment_kind(path),
                    ),
                    [],
                ).append(path)

        rows: list[dict[str, Any]] = []
        for (path_octants, healpix_id, kind), files in sorted(groups.items()):
            checksum, row_count = _stage00_group_checksum(files)
            rows.append(
                {
                    "key": _stage00_group_key(path_octants, healpix_id, kind),
                    "node_path": _node_path_label(path_octants),
                    "path_octants": list(path_octants),
                    "depth": len(path_octants),
                    "input_shard_id": healpix_id,
                    "kind": kind,
                    "file_count": len(files),
                    "row_count": row_count,
                    "content_checksum": checksum,
                    "files": [
                        path.relative_to(self._config.output_dir).as_posix()
                        for path in files
                    ],
                }
            )
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
        healpix_id: str,
    ) -> None:
        if len(table) == 0:
            return

        levels = _level_array(table)
        if levels.min(initial=node.depth) < node.depth:
            raise ValueError(
                f"Encountered final level above staging node depth {node.depth}"
            )

        if node.lower_mag_limited:
            self._route_into_lower_limited_node(node, table, healpix_id=healpix_id)
            return

        self._write_fragment(node, table, healpix_id=healpix_id, kind="pack")
        if node.row_count >= self._config.bucket_size:
            self._make_lower_mag_limited(node)

    def _route_into_lower_limited_node(
        self,
        node: _BucketNode,
        table: pa.Table,
        *,
        healpix_id: str,
    ) -> None:
        levels = _level_array(table)
        resident_indices = np.flatnonzero(levels == node.depth)
        if len(resident_indices) > 0:
            self._write_fragment(
                node,
                _take_rows(table, resident_indices),
                healpix_id=healpix_id,
                kind="lim",
            )

        descendant_indices = np.flatnonzero(levels > node.depth)
        if len(descendant_indices) == 0:
            return
        if node.depth >= self._config.max_level:
            raise ValueError(
                f"Rows below max staging depth {node.depth} cannot be routed lower"
            )

        descendants = _take_rows(table, descendant_indices)
        for octant, child_table in _tables_by_child_octant(descendants, node.depth):
            child = self._node_for_path((*node.path_octants, int(octant)))
            self._route_table(child, child_table, healpix_id=healpix_id)

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
        node.current_files.clear()
        node.row_count = 0

        for path in pack_files:
            healpix_id = _healpix_id_from_fragment(path)
            table = pq.ParquetFile(path).read()
            path.unlink()
            self._files_deleted_on_split += 1
            self._route_into_lower_limited_node(node, table, healpix_id=healpix_id)

    def _write_fragment(
        self,
        node: _BucketNode,
        table: pa.Table,
        *,
        healpix_id: str,
        kind: str,
    ) -> None:
        if kind not in {"pack", "lim"}:
            raise ValueError(f"Unsupported fragment kind: {kind!r}")
        if len(table) == 0:
            return
        key = _WriterKey(
            path_octants=node.path_octants,
            healpix_id=_safe_healpix_id(healpix_id),
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
                self._close_writer(key)
            else:
                self._open_writers.move_to_end(key)
                return existing

        existing = self._open_writers.get(key)
        if existing is not None:
            self._open_writers.move_to_end(key)
            return existing

        while len(self._open_writers) >= self._config.max_open_writers:
            old_key = next(iter(self._open_writers))
            self._close_writer(old_key)

        path = self._next_fragment_path(node, healpix_id=key.healpix_id, kind=key.kind)
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
        healpix_id: str,
        kind: str,
    ) -> Path:
        safe_healpix = _safe_healpix_id(healpix_id)
        path = (
            node.directory / f"hp{safe_healpix}-{kind}-{node.next_sequence:06d}.parquet"
        )
        node.next_sequence += 1
        return path

    def _close_writer(self, key: _WriterKey) -> None:
        writer = self._open_writers.pop(key, None)
        if writer is None:
            return
        writer.writer.close()

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

    def _compact_current_files(self) -> None:
        threshold = self._config.compact_after_files
        if threshold == 0:
            return
        for node in sorted(self._nodes.values(), key=lambda n: n.path_octants):
            groups: dict[tuple[str, str], list[Path]] = {}
            for path in sorted(node.current_files):
                groups.setdefault(
                    (_healpix_id_from_fragment(path), _fragment_kind(path)),
                    [],
                ).append(path)

            for (healpix_id, kind), files in sorted(groups.items()):
                if len(files) <= threshold:
                    continue
                self._compact_file_group(
                    node,
                    files,
                    healpix_id=healpix_id,
                    kind=kind,
                )

    def _compact_file_group(
        self,
        node: _BucketNode,
        files: list[Path],
        *,
        healpix_id: str,
        kind: str,
    ) -> None:
        new_files: list[Path] = []
        buffered: list[pa.Table] = []
        buffered_rows = 0
        buffered_schema: pa.Schema | None = None

        def flush_buffer() -> None:
            nonlocal buffered, buffered_rows, buffered_schema
            if not buffered:
                return
            compacted = pa.concat_tables(buffered, promote_options="none")
            path = self._next_fragment_path(node, healpix_id=healpix_id, kind=kind)
            pq.write_table(compacted, path, compression="zstd")
            new_files.append(path)
            self._files_written += 1
            self._rows_written += len(compacted)
            self._compaction_output_files += 1
            buffered = []
            buffered_rows = 0
            buffered_schema = None

        for path in files:
            table = pq.ParquetFile(path).read()
            if buffered_schema is None:
                buffered_schema = table.schema
            elif not buffered_schema.equals(table.schema, check_metadata=False):
                flush_buffer()
                buffered_schema = table.schema
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

        for path in files:
            path.unlink()
        node.current_files.difference_update(files)
        node.current_files.update(new_files)
        self._files_deleted_on_compaction += len(files)
        self._compaction_input_files += len(files)
        self._compaction_rewrites += 1


def run_stage00(config: Stage00Config) -> Path:
    """Build the packed Stage 00 staging tree.

    Input may be raw merged HEALPix parquet or already enriched parquet. Rows are
    routed into adaptive staging buckets, with lower magnitude limiting enabled
    only after a bucket reaches the configured row cap.
    """
    config.validate()
    if config.force and config.output_dir.exists():
        shutil.rmtree(config.output_dir)
    if config.output_dir.exists() and any(config.output_dir.iterdir()):
        raise FileExistsError(
            f"Output directory is not empty: {config.output_dir}. Use --force to replace it."
        )
    config.output_dir.mkdir(parents=True, exist_ok=True)

    builder = _Stage00Builder(config)
    processed_healpix: list[str] = []
    input_files = 0

    for input_shard in _selected_input_shards(config):
        if not input_shard.parquet_files:
            continue
        processed_healpix.append(input_shard.shard_id)
        for src_file in input_shard.parquet_files:
            input_files += 1
            parquet_file = pq.ParquetFile(src_file)
            for batch in parquet_file.iter_batches(batch_size=config.batch_size):
                table = pa.Table.from_batches([batch])
                builder.process_table(table, healpix_id=input_shard.shard_id)

    builder.finish()
    report = builder.report(
        processed_healpix=processed_healpix,
        input_files=input_files,
    )
    report_path = config.output_dir / REPORT_NAME
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report_path


def _selected_input_shards(config: Stage00Config) -> list[_InputShard]:
    if config.healpix_ids:
        shards: list[_InputShard] = []
        missing: list[str] = []
        for shard_id in config.healpix_ids:
            direct = config.input_root / shard_id
            with_suffix = config.input_root / f"{shard_id}.parquet"
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
                f"Missing HEALPix input directories or parquet shards: {missing}"
            )
    else:
        shards = []
        for path in sorted(config.input_root.iterdir()):
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
    if config.max_pixels is not None:
        shards = shards[: config.max_pixels]
    return shards


def _ensure_stage00_columns(table: pa.Table, mag_config: MagLevelConfig) -> pa.Table:
    names = set(table.schema.names)
    required = {"morton_code", "render", "level", "mag_abs"}
    if required.issubset(names):
        return _normalize_stage00_schema(_drop_healpix_columns(table))

    raw_required = {"x_icrs_pc", "y_icrs_pc", "z_icrs_pc", "mag_abs"}
    missing = raw_required - names
    if missing:
        raise ValueError(
            "Input table must be Stage 00-enriched or raw merged parquet; "
            f"missing columns {sorted(missing)}"
        )
    enriched = _enrich_table(
        table,
        mag_config=mag_config,
        center=WORLD_CENTER.copy(),
        half_size=WORLD_HALF_SIZE_PC,
    )
    return _normalize_stage00_schema(_drop_healpix_columns(enriched))


def _drop_healpix_columns(table: pa.Table) -> pa.Table:
    drop = [name for name in _HEALPIX_COLUMN_CANDIDATES if name in table.schema.names]
    if not drop:
        return table
    return table.drop(drop)


def _normalize_stage00_schema(table: pa.Table) -> pa.Table:
    arrays: list[pa.ChunkedArray] = []
    fields: list[pa.Field] = []
    for source_field in table.schema:
        column = table.column(source_field.name)
        target_type = _COLUMN_TYPES.get(source_field.name)
        if target_type is not None and not source_field.type.equals(target_type):
            column = column.cast(target_type)
            fields.append(pa.field(source_field.name, target_type))
        else:
            fields.append(pa.field(source_field.name, source_field.type))
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _stage00_group_key(
    path_octants: tuple[int, ...],
    healpix_id: str,
    kind: str,
) -> str:
    return f"{_node_path_label(path_octants)}|{healpix_id}|{kind}"


def _node_path_label(path_octants: tuple[int, ...]) -> str:
    return "/".join(f"o={octant}" for octant in path_octants)


def _stage00_group_checksum(paths: list[Path]) -> tuple[str, int]:
    tables = [pq.read_table(path) for path in sorted(paths)]
    canonical = _align_tables_to_union_schema(tables)
    sink = pa.BufferOutputStream()
    with pa_ipc.new_stream(sink, canonical.schema) as writer:
        writer.write_table(canonical)
    digest = hashlib.sha256(sink.getvalue()).hexdigest()
    return f"sha256:{digest}", len(canonical)


def _align_tables_to_union_schema(tables: list[pa.Table]) -> pa.Table:
    fields: dict[str, pa.DataType] = {}
    field_order: list[str] = []
    for table in tables:
        for schema_field in table.schema:
            existing = fields.get(schema_field.name)
            if existing is None:
                fields[schema_field.name] = schema_field.type
                field_order.append(schema_field.name)
            elif not existing.equals(schema_field.type):
                raise ValueError(
                    "Cannot checksum Stage 00 group with conflicting field types: "
                    f"{schema_field.name} has both {existing} and {schema_field.type}"
                )

    schema = pa.schema(pa.field(name, fields[name]) for name in field_order)
    aligned: list[pa.Table] = []
    for table in tables:
        arrays: list[pa.ChunkedArray | pa.Array] = []
        for schema_field in schema:
            if schema_field.name in table.schema.names:
                arrays.append(table.column(schema_field.name))
            else:
                arrays.append(pa.nulls(len(table), type=schema_field.type))
        aligned.append(pa.Table.from_arrays(arrays, schema=schema))
    if not aligned:
        return pa.table({})
    return pa.concat_tables(aligned, promote_options="none").combine_chunks()


def _level_array(table: pa.Table) -> np.ndarray:
    return np.asarray(table.column("level"), dtype=np.int32)


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


def _safe_healpix_id(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("HEALPix id must not be empty")
    if "/" in text or "\\" in text:
        raise ValueError(f"HEALPix id must not contain path separators: {value!r}")
    return text


def _healpix_id_from_fragment(path: Path) -> str:
    match = _FRAGMENT_RE.match(path.name)
    if match is None:
        raise ValueError(f"Invalid Stage 00 fragment filename: {path.name}")
    return match.group("hp")


def _fragment_kind(path: Path) -> str:
    match = _FRAGMENT_RE.match(path.name)
    if match is None:
        raise ValueError(f"Invalid Stage 00 fragment filename: {path.name}")
    return match.group("kind")
