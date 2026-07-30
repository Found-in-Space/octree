from __future__ import annotations

import gzip
import json
import math
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import duckdb
import numpy as np
import pyarrow.parquet as pq

from .assembly.formats import (
    IDENTIFIERS_ARTIFACT_KIND,
    IDENTIFIERS_INDEX_MAGIC,
    IDENTIFIERS_MANIFEST_NAME,
    INDEX_MAGIC,
    RENDER_ARTIFACT_KIND,
    RENDER_MANIFEST_NAME,
)
from .assembly.identity_encoder import encode_identity_rows
from .assembly.manifest import write_manifest
from .assembly.types import CellKey, EncodedCell, ShardKey
from .assembly.writer import IntermediateShardWriter, identifiers_shard_filenames
from .combine import CombinePlan, combine_octree
from .combine.records import PackedDescriptorFields
from .config import DEFAULT_CLASSIC_MAX_LEVEL, MORTON_BITS
from .duckdb_util import configure_connection
from .encoding.render import encode_render_records
from .identifiers_order import combine_identifiers_order
from .sources.stage00 import (
    STAGE_STATE_FORMAT,
    STAGE_STATE_NAME,
    TREE_MANIFEST_FORMAT,
    TREE_MANIFEST_NAME,
)

CLASSIC_INTERMEDIATES_DIR_NAME = "classic-intermediates"
_RAW_RENDER_COLUMNS = {
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "mag_abs",
    "source",
    "source_id",
    "morton_code",
    "level",
}


@dataclass(frozen=True, slots=True)
class ClassicBuildConfig:
    stage00_output_dir: Path
    stage01_output_dir: Path
    output_path: Path
    identifiers_order_path: Path
    mag_limit: float
    max_level: int = DEFAULT_CLASSIC_MAX_LEVEL
    batch_size: int = 100_000
    max_open_files: int = 32
    retain_relocation_files: bool = False

    def validate(self) -> None:
        if not self.stage00_output_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.stage00_output_dir}")
        if not self.stage01_output_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.stage01_output_dir}")
        if self.max_level < 0 or self.max_level > MORTON_BITS:
            raise ValueError(f"max_level must be in 0..{MORTON_BITS}")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if self.max_open_files <= 0:
            raise ValueError("max_open_files must be > 0")
        if not math.isfinite(self.mag_limit):
            raise ValueError("mag_limit must be finite")


@dataclass(frozen=True, slots=True)
class ClassicMaterializationResult:
    render_manifest_path: Path
    identifiers_manifest_path: Path
    row_count: int
    folded_row_count: int
    cell_count: int


@dataclass(frozen=True, slots=True)
class ClassicBuildResult:
    output_path: Path
    identifiers_order_path: Path
    intermediates_dir: Path
    dataset_uuid: UUID
    identifiers_uuid: UUID
    row_count: int
    folded_row_count: int
    cell_count: int


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _tracked_stage01_files(
    stage00_output_dir: Path,
    stage01_output_dir: Path,
) -> list[Path]:
    manifest_path = stage00_output_dir / TREE_MANIFEST_NAME
    state_path = stage00_output_dir / STAGE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 state: {state_path}")

    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    if manifest.get("format") != TREE_MANIFEST_FORMAT:
        raise ValueError(
            f"Unsupported Stage 00 tree manifest format: {manifest.get('format')!r}"
        )
    if state.get("format") != STAGE_STATE_FORMAT:
        raise ValueError(f"Unsupported Stage 00 state format: {state.get('format')!r}")
    if state.get("tree_identity") != manifest.get("tree_identity"):
        raise ValueError("Stage 00 state identity does not match tree manifest")

    dirty = state.get("dirty", {})
    if dirty.get("stage01_groups"):
        raise ValueError("Classic build requires no dirty Stage 01 groups")
    if dirty.get("deleted_stage00_groups"):
        raise ValueError("Classic build requires no deleted Stage 00 groups")
    if "stage01_groups" not in state:
        raise ValueError("Classic build requires Stage 01 to run first")

    files: list[Path] = []
    seen: set[Path] = set()
    for group in sorted(state.get("stage01_groups", []), key=lambda row: row["key"]):
        for rel_path in group.get("files", []):
            path = stage01_output_dir / str(rel_path)
            if path in seen:
                raise ValueError(f"Duplicate Stage 01 group file in state: {path}")
            if not path.is_file():
                raise FileNotFoundError(f"Missing Stage 01 group file: {path}")
            seen.add(path)
            files.append(path)
    if not files:
        raise ValueError("Classic build found no Stage 01 parquet files")
    return files


def _duckdb_read_parquet_source(files: list[Path]) -> str:
    quoted = []
    for path in files:
        escaped = path.as_posix().replace("'", "''")
        quoted.append(f"'{escaped}'")
    if len(quoted) == 1:
        return quoted[0]
    return "[" + ", ".join(quoted) + "]"


def _validate_raw_stage01_schema(stage01_files: list[Path]) -> bool:
    has_teff = False
    for path in stage01_files:
        names = set(pq.read_schema(path).names)
        missing = sorted(_RAW_RENDER_COLUMNS - names)
        if missing:
            raise ValueError(
                "Classic materialization requires raw Stage 01 fields; "
                f"{path} is missing {missing}. Rebuild Stage 00 and Stage 01."
            )
        has_teff = has_teff or "teff" in names
    return has_teff


def materialize_classic_intermediates(
    *,
    stage01_files: list[Path],
    out_dir: Path,
    max_level: int,
    mag_limit: float,
    batch_size: int,
) -> ClassicMaterializationResult:
    if max_level < 0 or max_level > MORTON_BITS:
        raise ValueError(f"max_level must be in 0..{MORTON_BITS}")
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if not stage01_files:
        raise ValueError("Classic materialization requires Stage 01 parquet files")
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"Classic intermediate directory is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    has_teff = _validate_raw_stage01_schema(stage01_files)
    source = _duckdb_read_parquet_source(stage01_files)
    final_level_expr = f"CASE WHEN level > {max_level} THEN {max_level} ELSE level END"
    teff_expr = "teff" if has_teff else "NULL::DOUBLE AS teff"
    query = f"""
        WITH staged AS (
            SELECT
                {final_level_expr} AS final_level,
                level AS source_level,
                morton_code,
                x_icrs_pc,
                y_icrs_pc,
                z_icrs_pc,
                mag_abs,
                {teff_expr},
                source,
                source_id
            FROM read_parquet({source}, union_by_name = true)
        )
        SELECT
            final_level,
            (
                morton_code
                >> CAST((3 * ({MORTON_BITS} - final_level)) AS INTEGER)
            ) AS final_node_id,
            source_level,
            morton_code,
            x_icrs_pc,
            y_icrs_pc,
            z_icrs_pc,
            mag_abs,
            teff,
            source,
            source_id
        FROM staged
        ORDER BY
            final_level,
            final_node_id,
            mag_abs ASC NULLS LAST,
            source ASC NULLS LAST,
            source_id ASC NULLS LAST
    """

    render_entries: list[dict] = []
    identifiers_entries: list[dict] = []
    render_writer: IntermediateShardWriter | None = None
    identifiers_writer: IntermediateShardWriter | None = None
    writer_level: int | None = None
    current_key: tuple[int, int] | None = None
    current_renders = bytearray()
    current_identities: list[tuple[str, str]] = []
    row_count = 0
    folded_row_count = 0
    cell_count = 0

    def close_writers() -> None:
        nonlocal render_writer, identifiers_writer, writer_level
        if render_writer is None or identifiers_writer is None:
            return
        render_entry = render_writer.close()
        identifiers_entry = identifiers_writer.close()
        render_writer = None
        identifiers_writer = None
        writer_level = None
        if render_entry is None or identifiers_entry is None:
            raise ValueError("Classic render and identifiers shard presence mismatch")
        if render_entry["record_count"] != identifiers_entry["record_count"]:
            raise ValueError("Classic render and identifiers record counts differ")
        render_entries.append(render_entry)
        identifiers_entries.append(identifiers_entry)

    def writers_for_level(
        level: int,
    ) -> tuple[IntermediateShardWriter, IntermediateShardWriter]:
        nonlocal render_writer, identifiers_writer, writer_level
        if writer_level != level:
            close_writers()
            shard = ShardKey(level=level, prefix_bits=0, prefix=0)
            render_writer = IntermediateShardWriter(shard, out_dir)
            identifiers_writer = IntermediateShardWriter(
                shard,
                out_dir,
                index_magic=IDENTIFIERS_INDEX_MAGIC,
                filename_fn=identifiers_shard_filenames,
            )
            writer_level = level
        assert render_writer is not None
        assert identifiers_writer is not None
        return render_writer, identifiers_writer

    def flush_cell() -> None:
        nonlocal current_renders, current_identities, cell_count
        if current_key is None:
            return
        level, node_id = current_key
        active_render_writer, active_identifiers_writer = writers_for_level(level)
        active_render_writer.write_cell(
            EncodedCell(
                key=CellKey(level=level, node_id=node_id),
                payload=gzip.compress(bytes(current_renders), mtime=0),
                star_count=len(current_identities),
            )
        )
        active_identifiers_writer.write_cell(
            EncodedCell(
                key=CellKey(level=level, node_id=node_id),
                payload=encode_identity_rows(current_identities),
                star_count=len(current_identities),
            )
        )
        current_renders = bytearray()
        current_identities = []
        cell_count += 1

    con = duckdb.connect()
    configure_connection(con)
    try:
        con.execute(query)
        while True:
            rows = con.fetchmany(batch_size)
            if not rows:
                break
            parsed_rows: list[tuple[int, int, int, str, str]] = []
            positions = np.empty((len(rows), 3), dtype=np.float64)
            magnitudes = np.empty(len(rows), dtype=np.float64)
            temperatures = np.empty(len(rows), dtype=np.float64)
            final_levels = np.empty(len(rows), dtype=np.int32)
            morton_codes = np.empty(len(rows), dtype=np.uint64)
            for index, (
                final_level_raw,
                final_node_id_raw,
                source_level_raw,
                morton_code_raw,
                x_raw,
                y_raw,
                z_raw,
                mag_abs_raw,
                teff_raw,
                source_raw,
                source_id_raw,
            ) in enumerate(rows):
                required = {
                    "final_level": final_level_raw,
                    "final_node_id": final_node_id_raw,
                    "source_level": source_level_raw,
                    "morton_code": morton_code_raw,
                    "x_icrs_pc": x_raw,
                    "y_icrs_pc": y_raw,
                    "z_icrs_pc": z_raw,
                    "source": source_raw,
                    "source_id": source_id_raw,
                }
                missing = [name for name, value in required.items() if value is None]
                if missing:
                    raise ValueError(
                        f"Classic materialization input has null required fields: {missing}"
                    )

                final_level = int(final_level_raw)
                final_node_id = int(final_node_id_raw)
                source_level = int(source_level_raw)
                morton_code = int(morton_code_raw)
                if source_level < final_level:
                    raise ValueError(
                        f"Classic final level {final_level} exceeds source level "
                        f"{source_level}"
                    )
                parsed_rows.append(
                    (
                        final_level,
                        final_node_id,
                        source_level,
                        str(source_raw),
                        str(source_id_raw),
                    )
                )
                positions[index] = (float(x_raw), float(y_raw), float(z_raw))
                magnitudes[index] = (
                    np.nan if mag_abs_raw is None else float(mag_abs_raw)
                )
                temperatures[index] = np.nan if teff_raw is None else float(teff_raw)
                final_levels[index] = final_level
                morton_codes[index] = morton_code

            encoded_renders = encode_render_records(
                morton_codes=morton_codes,
                positions=positions,
                mag_abs=magnitudes,
                teff=temperatures,
                levels=final_levels,
            )
            for (
                final_level,
                final_node_id,
                source_level,
                source,
                source_id,
            ), render in zip(parsed_rows, encoded_renders, strict=True):
                key = (final_level, final_node_id)
                if current_key is not None and key < current_key:
                    raise ValueError(
                        f"Classic materialization rows are not ordered: {key} < {current_key}"
                    )
                if current_key is not None and key != current_key:
                    flush_cell()
                current_key = key

                if source_level > final_level:
                    folded_row_count += 1

                current_renders.extend(render.tobytes())
                current_identities.append((source, source_id))
                row_count += 1
        flush_cell()
        close_writers()
    except Exception:
        if render_writer is not None:
            render_writer.abort()
        if identifiers_writer is not None:
            identifiers_writer.abort()
        raise
    finally:
        con.close()

    render_manifest_path = write_manifest(
        out_dir,
        max_level,
        render_entries,
        artifact_kind=RENDER_ARTIFACT_KIND,
        index_magic=INDEX_MAGIC,
        mag_limit=mag_limit,
        name=RENDER_MANIFEST_NAME,
    )
    identifiers_manifest_path = write_manifest(
        out_dir,
        max_level,
        identifiers_entries,
        artifact_kind=IDENTIFIERS_ARTIFACT_KIND,
        index_magic=IDENTIFIERS_INDEX_MAGIC,
        mag_limit=mag_limit,
        name=IDENTIFIERS_MANIFEST_NAME,
    )
    return ClassicMaterializationResult(
        render_manifest_path=render_manifest_path,
        identifiers_manifest_path=identifiers_manifest_path,
        row_count=row_count,
        folded_row_count=folded_row_count,
        cell_count=cell_count,
    )


def _publish_intermediates(
    *,
    temporary_dir: Path,
    final_dir: Path,
) -> None:
    backup_dir = final_dir.with_name(f".{final_dir.name}.{uuid4().hex}.backup")
    moved_existing = False
    if final_dir.exists():
        os.replace(final_dir, backup_dir)
        moved_existing = True
    try:
        os.replace(temporary_dir, final_dir)
    except Exception:
        if moved_existing and not final_dir.exists():
            os.replace(backup_dir, final_dir)
        raise
    if moved_existing:
        shutil.rmtree(backup_dir)


def build_classic_artifacts(
    config: ClassicBuildConfig,
    *,
    dataset_uuid: UUID | None = None,
    identifiers_uuid: UUID | None = None,
) -> ClassicBuildResult:
    """Build the traditional magnitude-level octree from staged parquet groups."""
    config.validate()
    stage01_files = _tracked_stage01_files(
        config.stage00_output_dir,
        config.stage01_output_dir,
    )

    intermediates_dir = config.stage01_output_dir / CLASSIC_INTERMEDIATES_DIR_NAME
    temporary_intermediates_dir = intermediates_dir.with_name(
        f".{intermediates_dir.name}.{uuid4().hex}.tmp"
    )
    materialized = materialize_classic_intermediates(
        stage01_files=stage01_files,
        out_dir=temporary_intermediates_dir,
        max_level=config.max_level,
        mag_limit=config.mag_limit,
        batch_size=config.batch_size,
    )
    _publish_intermediates(
        temporary_dir=temporary_intermediates_dir,
        final_dir=intermediates_dir,
    )
    render_manifest_path = intermediates_dir / materialized.render_manifest_path.name
    identifiers_manifest_path = (
        intermediates_dir / materialized.identifiers_manifest_path.name
    )

    resolved_dataset_uuid = dataset_uuid or uuid4()
    resolved_identifiers_uuid = identifiers_uuid or uuid4()
    output_tmp = config.output_path.with_name(
        f".{config.output_path.name}.{os.getpid()}.tmp"
    )
    identifiers_tmp = config.identifiers_order_path.with_name(
        f".{config.identifiers_order_path.name}.{os.getpid()}.tmp"
    )
    output_tmp.unlink(missing_ok=True)
    identifiers_tmp.unlink(missing_ok=True)
    try:
        combine_octree(
            render_manifest_path,
            output_tmp,
            plan=CombinePlan(
                max_open_files=config.max_open_files,
                retain_relocation_files=config.retain_relocation_files,
            ),
            descriptor=PackedDescriptorFields(
                artifact_kind="render",
                dataset_uuid=resolved_dataset_uuid,
            ),
        )
        combine_identifiers_order(
            identifiers_manifest_path,
            identifiers_tmp,
            parent_dataset_uuid=resolved_dataset_uuid,
            artifact_uuid=resolved_identifiers_uuid,
        )
        config.identifiers_order_path.parent.mkdir(parents=True, exist_ok=True)
        config.output_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(identifiers_tmp, config.identifiers_order_path)
        os.replace(output_tmp, config.output_path)
    finally:
        output_tmp.unlink(missing_ok=True)
        identifiers_tmp.unlink(missing_ok=True)
        if temporary_intermediates_dir.exists():
            shutil.rmtree(temporary_intermediates_dir)

    return ClassicBuildResult(
        output_path=config.output_path,
        identifiers_order_path=config.identifiers_order_path,
        intermediates_dir=intermediates_dir,
        dataset_uuid=resolved_dataset_uuid,
        identifiers_uuid=resolved_identifiers_uuid,
        row_count=materialized.row_count,
        folded_row_count=materialized.folded_row_count,
        cell_count=materialized.cell_count,
    )
