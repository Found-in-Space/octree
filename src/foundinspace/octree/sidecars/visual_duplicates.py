"""Build the optional sparse visual-duplicate review sidecar."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import shutil
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO
from uuid import UUID, uuid4

import numpy as np
import pyarrow.parquet as pq

from ..assembly.formats import SIDECAR_ARTIFACT_KIND, SIDECAR_INDEX_MAGIC
from ..assembly.manifest import write_manifest
from ..assembly.plan import BuildPlan
from ..assembly.types import CellKey
from ..assembly.writer import (
    IntermediateShardWriter,
    belongs_to_shard,
    sidecar_shard_filenames,
)
from ..combine import CombinePlan, combine_octree
from ..combine.records import PackedDescriptorFields
from ..identifiers_order import (
    IDENTITY_UNCOMPRESSED_CELL_LIMIT_BYTES,
    IdentifiersOrderReader,
    IdentifiersOrderRecord,
)
from ..identifiers_order import (
    read_header as read_identifiers_order_header,
)
from ..reader import read_header

SIDECAR_KIND = "visual-duplicates"
REPORT_FORMAT = "foundinspace.octree.visual-duplicates-report/v1"
PAYLOAD_ENCODING = "gzip-json/sparse-ordinal-records/v1"
DEFAULT_SCAN_BATCH_BYTES = 32 * 1024 * 1024
DEFAULT_MAX_EVIDENCE_PAIRS = 1_000_000
DEFAULT_PROGRESS_INTERVAL_CELLS = 100_000
EVIDENCE_BATCH_ROWS = 16_384
_EVIDENCE_COLUMNS = (
    "gaia_source_id",
    "hip_source_id",
    "mapping_source",
    "number_of_neighbours",
    "angular_distance",
)
_SOURCE_PREFIXES = {
    "gaia": b"\x04\x00gaia",
    "hip": b"\x03\x00hip",
    "manual": b"\x06\x00manual",
}


@dataclass(frozen=True, slots=True)
class VisualDuplicatePair:
    gaia_source_id: str
    hip_source_id: str
    mapping_source: str
    number_of_neighbours: int
    angular_distance_arcsec: float

    @property
    def pair_id(self) -> str:
        return f"gaia:{self.gaia_source_id}|hip:{self.hip_source_id}"


@dataclass(frozen=True, slots=True)
class RenderedObjectLocation:
    level: int
    node_id: int
    ordinal: int

    def as_counterpart_ref(self) -> dict[str, object]:
        return {
            "level": self.level,
            "mortonCode": str(self.node_id),
            "ordinal": self.ordinal,
        }


@dataclass(frozen=True, slots=True)
class VisualDuplicatesScanProgress:
    scanned_cells: int
    total_cells: int
    scanned_stars: int
    found_endpoints: int
    expected_endpoints: int


@dataclass(frozen=True, slots=True)
class VisualDuplicatesBuildConfig:
    render_octree_path: Path
    identifiers_order_path: Path
    evidence_path: Path
    output_path: Path
    work_dir: Path
    report_path: Path
    deep_shard_from_level: int
    deep_prefix_bits: int
    max_open_files: int
    scan_batch_bytes: int = DEFAULT_SCAN_BATCH_BYTES
    max_cell_payload_bytes: int = IDENTITY_UNCOMPRESSED_CELL_LIMIT_BYTES
    max_evidence_pairs: int = DEFAULT_MAX_EVIDENCE_PAIRS
    progress_interval_cells: int = DEFAULT_PROGRESS_INTERVAL_CELLS
    force: bool = False
    progress: Callable[[VisualDuplicatesScanProgress], None] | None = None

    def validate(self) -> None:
        if self.deep_shard_from_level < 0:
            raise ValueError("deep_shard_from_level must be >= 0")
        if self.deep_prefix_bits < 0:
            raise ValueError("deep_prefix_bits must be >= 0")
        if self.max_open_files <= 0:
            raise ValueError("max_open_files must be > 0")
        if self.scan_batch_bytes <= 0:
            raise ValueError("scan_batch_bytes must be > 0")
        if self.max_cell_payload_bytes <= 0:
            raise ValueError("max_cell_payload_bytes must be > 0")
        if self.max_evidence_pairs <= 0:
            raise ValueError("max_evidence_pairs must be > 0")
        if self.progress_interval_cells <= 0:
            raise ValueError("progress_interval_cells must be > 0")


@dataclass(frozen=True, slots=True)
class VisualDuplicatesBuildResult:
    output_path: Path
    report_path: Path
    work_manifest_path: Path
    parent_dataset_uuid: UUID
    sidecar_uuid: UUID
    evidence_pair_count: int
    rendered_endpoint_count: int
    payload_cell_count: int


@dataclass(frozen=True, slots=True)
class _BufferedIdentityCell:
    record: IdentifiersOrderRecord
    payload: bytes


def build_visual_duplicates_sidecar(
    config: VisualDuplicatesBuildConfig,
) -> VisualDuplicatesBuildResult:
    """Build a sparse review sidecar from a one-to-one display-pair map."""
    config.validate()
    render_path = Path(config.render_octree_path).expanduser().resolve()
    order_path = Path(config.identifiers_order_path).expanduser().resolve()
    evidence_path = Path(config.evidence_path).expanduser().resolve()
    output_path = Path(config.output_path).expanduser().resolve()
    work_dir = Path(config.work_dir).expanduser().resolve()
    report_path = Path(config.report_path).expanduser().resolve()

    for path, label in (
        (render_path, "Render octree"),
        (order_path, "Identifiers/order artifact"),
        (evidence_path, "Visual-duplicate evidence"),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label} not found: {path}")
    if len({output_path, report_path, work_dir}) != 3:
        raise ValueError("Output, report, and work paths must be distinct")

    render_header = read_header(render_path)
    if render_header.artifact_kind != "render" or render_header.dataset_uuid is None:
        raise ValueError(
            "Visual-duplicate sidecars require a render octree with dataset_uuid metadata"
        )
    order_header = read_identifiers_order_header(order_path)
    if order_header.parent_dataset_uuid != render_header.dataset_uuid:
        raise ValueError(
            "Identifiers/order artifact does not match render octree dataset_uuid"
        )

    pairs, endpoints, mapping_source_counts = _load_evidence(
        evidence_path,
        max_pairs=config.max_evidence_pairs,
    )
    _prepare_outputs(
        output_path=output_path,
        report_path=report_path,
        work_dir=work_dir,
        force=config.force,
    )

    locations, scanned_cells, scanned_stars = _locate_rendered_endpoints(
        order_path,
        endpoints=endpoints,
        scan_batch_bytes=config.scan_batch_bytes,
        max_cell_payload_bytes=config.max_cell_payload_bytes,
        progress_interval_cells=config.progress_interval_cells,
        progress=config.progress,
    )
    entries_by_cell, coverage = _build_sparse_entries(pairs, locations)
    if not entries_by_cell:
        raise ValueError(
            "No visual-duplicate evidence endpoints occur in the render octree"
        )

    plan = BuildPlan(
        max_level=render_header.max_level,
        deep_shard_from_level=config.deep_shard_from_level,
        deep_prefix_bits=config.deep_prefix_bits,
        batch_size=1,
        mag_limit=render_header.mag_limit,
    )
    plan.validate()
    work_manifest_path = _write_sparse_intermediates(
        work_dir,
        plan=plan,
        entries_by_cell=entries_by_cell,
    )

    sidecar_uuid = uuid4()
    temporary_output = output_path.with_name(f".{output_path.name}.tmp")
    temporary_output.unlink(missing_ok=True)
    try:
        combine_octree(
            work_manifest_path,
            temporary_output,
            plan=CombinePlan(max_open_files=config.max_open_files),
            descriptor=PackedDescriptorFields(
                artifact_kind="sidecar",
                parent_dataset_uuid=render_header.dataset_uuid,
                sidecar_uuid=sidecar_uuid,
                sidecar_kind=SIDECAR_KIND,
            ),
        )
        os.replace(temporary_output, output_path)
    finally:
        temporary_output.unlink(missing_ok=True)

    report = {
        "format": REPORT_FORMAT,
        "sidecar_kind": SIDECAR_KIND,
        "payload_encoding": PAYLOAD_ENCODING,
        "record_schema": {
            "ordinal": "uint32",
            "pair_id": "string",
            "role": "gaia|hip",
            "identity": "{source:string,source_id:string}",
            "counterpart_identity": "{source:string,source_id:string}",
            "counterpart_ref": "{level:uint16,mortonCode:string,ordinal:uint32}|null",
            "mapping_source": "string",
            "number_of_neighbours": "int16",
            "angular_distance_arcsec": "float32",
        },
        "render_octree_path": str(render_path),
        "identifiers_order_path": str(order_path),
        "identifiers_order_artifact_uuid": str(order_header.artifact_uuid),
        "evidence_path": str(evidence_path),
        "evidence_sha256": _sha256_file(evidence_path),
        "output_path": str(output_path),
        "output_sha256": _sha256_file(output_path),
        "parent_dataset_uuid": str(render_header.dataset_uuid),
        "sidecar_uuid": str(sidecar_uuid),
        "evidence_pair_count": len(pairs),
        "expected_endpoint_count": len(endpoints),
        "render_cells_scanned": scanned_cells,
        "render_stars_scanned": scanned_stars,
        "payload_cell_count": len(entries_by_cell),
        "mapping_source_counts": dict(sorted(mapping_source_counts.items())),
        "coverage": coverage,
    }
    _write_json_atomic(report_path, report)
    return VisualDuplicatesBuildResult(
        output_path=output_path,
        report_path=report_path,
        work_manifest_path=work_manifest_path,
        parent_dataset_uuid=render_header.dataset_uuid,
        sidecar_uuid=sidecar_uuid,
        evidence_pair_count=len(pairs),
        rendered_endpoint_count=len(locations),
        payload_cell_count=len(entries_by_cell),
    )


def _load_evidence(
    path: Path,
    *,
    max_pairs: int,
) -> tuple[
    list[VisualDuplicatePair],
    dict[tuple[str, str], VisualDuplicatePair],
    Counter[str],
]:
    parquet = pq.ParquetFile(path)
    missing = sorted(set(_EVIDENCE_COLUMNS) - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(
            "Visual-duplicate evidence is missing required column(s): "
            + ", ".join(missing)
        )
    if parquet.metadata.num_rows > max_pairs:
        raise ValueError(
            f"Visual-duplicate evidence has {parquet.metadata.num_rows:,} rows; "
            f"the configured bound is {max_pairs:,}"
        )

    pairs: list[VisualDuplicatePair] = []
    endpoints: dict[tuple[str, str], VisualDuplicatePair] = {}
    mapping_source_counts: Counter[str] = Counter()
    for batch in parquet.iter_batches(
        batch_size=EVIDENCE_BATCH_ROWS,
        columns=list(_EVIDENCE_COLUMNS),
        use_threads=False,
    ):
        for row in batch.to_pylist():
            gaia_id = _positive_integral_id(row["gaia_source_id"], "gaia_source_id")
            hip_id = _positive_integral_id(row["hip_source_id"], "hip_source_id")
            mapping_source = str(row["mapping_source"] or "").strip()
            if not mapping_source:
                raise ValueError("mapping_source must not be empty")
            neighbours = row["number_of_neighbours"]
            if isinstance(neighbours, bool) or not isinstance(neighbours, int):
                raise ValueError("number_of_neighbours must be an integer")
            angular_distance = float(row["angular_distance"])
            if not math.isfinite(angular_distance) or angular_distance < 0:
                raise ValueError("angular_distance must be finite and >= 0")
            pair = VisualDuplicatePair(
                gaia_source_id=gaia_id,
                hip_source_id=hip_id,
                mapping_source=mapping_source,
                number_of_neighbours=int(neighbours),
                angular_distance_arcsec=angular_distance,
            )
            for endpoint in (("gaia", gaia_id), ("hip", hip_id)):
                previous = endpoints.get(endpoint)
                if previous is not None:
                    raise ValueError(
                        "Visual-duplicate evidence is not one-to-one: "
                        f"{endpoint[0]}:{endpoint[1]} occurs in both "
                        f"{previous.pair_id} and {pair.pair_id}"
                    )
                endpoints[endpoint] = pair
            pairs.append(pair)
            mapping_source_counts[mapping_source] += 1

    if not pairs:
        raise ValueError("Visual-duplicate evidence contains no pairs")
    return pairs, endpoints, mapping_source_counts


def _positive_integral_id(value: object, name: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if normalized <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return str(normalized)


def _prepare_outputs(
    *,
    output_path: Path,
    report_path: Path,
    work_dir: Path,
    force: bool,
) -> None:
    existing_files = [path for path in (output_path, report_path) if path.exists()]
    if existing_files and not force:
        raise FileExistsError(str(existing_files[0]))
    if work_dir.exists():
        if any(work_dir.iterdir()) and not force:
            raise FileExistsError(str(work_dir))
        shutil.rmtree(work_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)


def _locate_rendered_endpoints(
    order_path: Path,
    *,
    endpoints: dict[tuple[str, str], VisualDuplicatePair],
    scan_batch_bytes: int,
    max_cell_payload_bytes: int,
    progress_interval_cells: int,
    progress: Callable[[VisualDuplicatesScanProgress], None] | None,
) -> tuple[dict[tuple[str, str], RenderedObjectLocation], int, int]:
    candidate_ids = {
        "gaia": _candidate_ids_by_length(endpoints, source="gaia"),
        "hip": _candidate_ids_by_length(endpoints, source="hip"),
    }
    locations: dict[tuple[str, str], RenderedObjectLocation] = {}
    buffered: list[_BufferedIdentityCell] = []
    buffered_bytes = 0
    scanned_cells = 0
    scanned_stars = 0
    next_progress_cell = progress_interval_cells

    with IdentifiersOrderReader(order_path) as reader:
        total_cells = reader.header.record_count
        for record, payload in reader.iter_cell_identity_payloads(
            max_uncompressed_bytes=max_cell_payload_bytes
        ):
            if buffered and buffered_bytes + len(payload) > scan_batch_bytes:
                _scan_identity_batch(buffered, candidate_ids, locations)
                buffered = []
                buffered_bytes = 0
            buffered.append(_BufferedIdentityCell(record=record, payload=payload))
            buffered_bytes += len(payload)
            scanned_cells += 1
            scanned_stars += record.star_count
            if scanned_cells >= next_progress_cell:
                if buffered:
                    _scan_identity_batch(buffered, candidate_ids, locations)
                    buffered = []
                    buffered_bytes = 0
                if progress is not None:
                    progress(
                        VisualDuplicatesScanProgress(
                            scanned_cells=scanned_cells,
                            total_cells=total_cells,
                            scanned_stars=scanned_stars,
                            found_endpoints=len(locations),
                            expected_endpoints=len(endpoints),
                        )
                    )
                next_progress_cell += progress_interval_cells
        if buffered:
            _scan_identity_batch(buffered, candidate_ids, locations)
        if progress is not None:
            progress(
                VisualDuplicatesScanProgress(
                    scanned_cells=scanned_cells,
                    total_cells=total_cells,
                    scanned_stars=scanned_stars,
                    found_endpoints=len(locations),
                    expected_endpoints=len(endpoints),
                )
            )
    return locations, scanned_cells, scanned_stars


def _candidate_ids_by_length(
    endpoints: dict[tuple[str, str], VisualDuplicatePair],
    *,
    source: str,
) -> dict[int, np.ndarray]:
    grouped: dict[int, list[int]] = {}
    for endpoint_source, source_id in endpoints:
        if endpoint_source == source:
            grouped.setdefault(len(source_id), []).append(int(source_id))
    return {
        length: np.asarray(sorted(values), dtype=np.uint64)
        for length, values in grouped.items()
    }


def _scan_identity_batch(
    cells: list[_BufferedIdentityCell],
    candidate_ids: dict[str, dict[int, np.ndarray]],
    locations: dict[tuple[str, str], RenderedObjectLocation],
) -> None:
    if not cells:
        return
    cell_starts_list: list[int] = []
    cell_ends_list: list[int] = []
    offset = 0
    for cell in cells:
        cell_starts_list.append(offset)
        offset += len(cell.payload)
        cell_ends_list.append(offset)
    payload = b"".join(cell.payload for cell in cells)
    values = np.frombuffer(payload, dtype=np.uint8)
    cell_starts = np.asarray(cell_starts_list, dtype=np.int64)
    cell_ends = np.asarray(cell_ends_list, dtype=np.int64)
    starts_by_source = {
        source: _find_prefix_offsets(values, prefix)
        for source, prefix in _SOURCE_PREFIXES.items()
    }

    observed_counts = np.zeros(len(cells), dtype=np.int64)
    for starts in starts_by_source.values():
        observed_counts += np.searchsorted(starts, cell_ends, side="left")
        observed_counts -= np.searchsorted(starts, cell_starts, side="left")
    expected_counts = np.fromiter(
        (cell.record.star_count for cell in cells),
        dtype=np.int64,
        count=len(cells),
    )
    bad_counts = np.flatnonzero(observed_counts != expected_counts)
    if len(bad_counts):
        cell = cells[int(bad_counts[0])]
        raise ValueError(
            "Identifiers/order payload contains an unsupported or malformed identity "
            f"at ({cell.record.level}, {cell.record.node_id}); expected "
            f"{cell.record.star_count} rows, found {int(observed_counts[bad_counts[0]])}"
        )

    for source in ("gaia", "hip"):
        matched = _match_candidate_ids(
            values,
            starts=starts_by_source[source],
            source_prefix_length=len(_SOURCE_PREFIXES[source]),
            candidate_ids=candidate_ids[source],
            cell_ends=cell_ends,
        )
        for start, source_id_value, cell_index in matched:
            cell = cells[cell_index]
            cell_start = int(cell_starts[cell_index])
            ordinal = sum(
                int(np.searchsorted(source_starts, start, side="left"))
                - int(np.searchsorted(source_starts, cell_start, side="left"))
                for source_starts in starts_by_source.values()
            )
            identity = (source, str(source_id_value))
            location = RenderedObjectLocation(
                level=cell.record.level,
                node_id=cell.record.node_id,
                ordinal=ordinal,
            )
            previous = locations.get(identity)
            if previous is not None:
                raise ValueError(
                    f"Rendered identity occurs more than once: {source}:{source_id_value}"
                )
            locations[identity] = location


def _find_prefix_offsets(values: np.ndarray, prefix: bytes) -> np.ndarray:
    limit = len(values) - len(prefix) + 1
    if limit <= 0:
        return np.empty(0, dtype=np.int64)
    mask = values[:limit] == prefix[0]
    for offset, expected in enumerate(prefix[1:], start=1):
        mask &= values[offset : offset + limit] == expected
    return np.flatnonzero(mask)


def _match_candidate_ids(
    values: np.ndarray,
    *,
    starts: np.ndarray,
    source_prefix_length: int,
    candidate_ids: dict[int, np.ndarray],
    cell_ends: np.ndarray,
) -> list[tuple[int, int, int]]:
    if not len(starts) or not candidate_ids:
        return []
    length_positions = starts + source_prefix_length
    if int(length_positions[-1]) + 1 >= len(values):
        raise ValueError(
            "Identifiers/order payload is truncated reading source_id length"
        )
    lengths = values[length_positions].astype(np.uint16)
    lengths |= values[length_positions + 1].astype(np.uint16) << 8
    id_starts = length_positions + 2
    matches: list[tuple[int, int, int]] = []

    for digit_count, expected_ids in candidate_ids.items():
        row_indexes = np.flatnonzero(lengths == digit_count)
        if not len(row_indexes):
            continue
        selected_starts = starts[row_indexes]
        selected_id_starts = id_starts[row_indexes]
        selected_cells = np.searchsorted(cell_ends, selected_starts, side="right")
        if np.any(selected_cells >= len(cell_ends)):
            raise ValueError("Identifiers/order row falls outside its cell payload")
        if np.any(selected_id_starts + digit_count > cell_ends[selected_cells]):
            raise ValueError("Identifiers/order source_id crosses a cell boundary")

        decoded = np.zeros(len(row_indexes), dtype=np.uint64)
        for digit_offset in range(digit_count):
            digits = values[selected_id_starts + digit_offset]
            if np.any((digits < ord("0")) | (digits > ord("9"))):
                raise ValueError(
                    "Gaia and Hipparcos source IDs must be decimal integers"
                )
            decoded *= 10
            decoded += digits - ord("0")

        found_at = np.searchsorted(expected_ids, decoded)
        safe_found_at = np.minimum(found_at, len(expected_ids) - 1)
        is_match = (found_at < len(expected_ids)) & (
            expected_ids[safe_found_at] == decoded
        )
        for start, source_id, cell_index in zip(
            selected_starts[is_match],
            decoded[is_match],
            selected_cells[is_match],
            strict=True,
        ):
            matches.append((int(start), int(source_id), int(cell_index)))
    return matches


def _build_sparse_entries(
    pairs: list[VisualDuplicatePair],
    locations: dict[tuple[str, str], RenderedObjectLocation],
) -> tuple[dict[tuple[int, int], list[dict[str, Any]]], dict[str, int]]:
    entries_by_cell: dict[tuple[int, int], list[dict[str, Any]]] = {}
    both = gaia_only = hip_only = neither = 0
    for pair in pairs:
        gaia_identity = ("gaia", pair.gaia_source_id)
        hip_identity = ("hip", pair.hip_source_id)
        gaia_location = locations.get(gaia_identity)
        hip_location = locations.get(hip_identity)
        if gaia_location is not None and hip_location is not None:
            both += 1
        elif gaia_location is not None:
            gaia_only += 1
        elif hip_location is not None:
            hip_only += 1
        else:
            neither += 1

        if gaia_location is not None:
            _append_sparse_entry(
                entries_by_cell,
                pair=pair,
                role="gaia",
                location=gaia_location,
                counterpart_identity=hip_identity,
                counterpart_location=hip_location,
            )
        if hip_location is not None:
            _append_sparse_entry(
                entries_by_cell,
                pair=pair,
                role="hip",
                location=hip_location,
                counterpart_identity=gaia_identity,
                counterpart_location=gaia_location,
            )

    for entries in entries_by_cell.values():
        entries.sort(key=lambda entry: int(entry["ordinal"]))
    return entries_by_cell, {
        "pairs_with_both_endpoints_rendered": both,
        "pairs_with_only_gaia_rendered": gaia_only,
        "pairs_with_only_hip_rendered": hip_only,
        "pairs_with_neither_endpoint_rendered": neither,
        "rendered_candidate_endpoints": len(locations),
    }


def _append_sparse_entry(
    entries_by_cell: dict[tuple[int, int], list[dict[str, Any]]],
    *,
    pair: VisualDuplicatePair,
    role: str,
    location: RenderedObjectLocation,
    counterpart_identity: tuple[str, str],
    counterpart_location: RenderedObjectLocation | None,
) -> None:
    own_source_id = pair.gaia_source_id if role == "gaia" else pair.hip_source_id
    entries_by_cell.setdefault((location.level, location.node_id), []).append(
        {
            "ordinal": location.ordinal,
            "pair_id": pair.pair_id,
            "role": role,
            "identity": {"source": role, "source_id": own_source_id},
            "counterpart_identity": {
                "source": counterpart_identity[0],
                "source_id": counterpart_identity[1],
            },
            "counterpart_ref": (
                counterpart_location.as_counterpart_ref()
                if counterpart_location is not None
                else None
            ),
            "mapping_source": pair.mapping_source,
            "number_of_neighbours": pair.number_of_neighbours,
            "angular_distance_arcsec": pair.angular_distance_arcsec,
        }
    )


def _write_sparse_intermediates(
    out_dir: Path,
    *,
    plan: BuildPlan,
    entries_by_cell: dict[tuple[int, int], list[dict[str, Any]]],
) -> Path:
    shard_entries: list[dict[str, object]] = []
    current_level = -1
    shard_keys = ()
    shard_index = 0
    current_writer: IntermediateShardWriter | None = None

    def close_current_writer() -> None:
        nonlocal current_writer
        if current_writer is None:
            return
        result = current_writer.close()
        current_writer = None
        if result is not None:
            shard_entries.append(result)

    try:
        for (level, node_id), entries in sorted(entries_by_cell.items()):
            if level != current_level:
                close_current_writer()
                current_level = level
                shard_keys = tuple(plan.shard_keys_for_level(level))
                shard_index = 0
            while shard_index < len(shard_keys) and not belongs_to_shard(
                node_id, shard_keys[shard_index]
            ):
                close_current_writer()
                shard_index += 1
            if shard_index >= len(shard_keys):
                raise ValueError(
                    f"Visual-duplicate cell does not match a shard: {level}:{node_id}"
                )
            if current_writer is None:
                current_writer = IntermediateShardWriter(
                    shard_keys[shard_index],
                    out_dir,
                    index_magic=SIDECAR_INDEX_MAGIC,
                    filename_fn=sidecar_shard_filenames(SIDECAR_KIND),
                )
            current_writer.write_generated_cell(
                key=CellKey(level=level, node_id=node_id),
                star_count=len(entries),
                write_payload=lambda target, entries=entries: _write_payload(
                    target, entries
                ),
            )
        close_current_writer()
    except BaseException:
        if current_writer is not None:
            current_writer.abort()
        raise

    return write_manifest(
        out_dir,
        plan.max_level,
        shard_entries,
        artifact_kind=SIDECAR_ARTIFACT_KIND,
        index_magic=SIDECAR_INDEX_MAGIC,
        mag_limit=plan.mag_limit,
    )


def _write_payload(target: BinaryIO, entries: list[dict[str, Any]]) -> None:
    encoded = json.dumps(
        entries,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    with gzip.GzipFile(fileobj=target, mode="wb", mtime=0) as compressed:
        compressed.write(encoded)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fp:
        while chunk := fp.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(value, fp, indent=2, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, path)
