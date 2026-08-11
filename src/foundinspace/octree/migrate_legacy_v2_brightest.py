"""One-off migration for the experimental STAR v2 brightest-level encoding.

The experimental artifact used header flag ``0x0001`` and stored a four-bit
brightest-level delta in the upper nibble of each node's structural flags.  The
final STAR v2 contract keeps the header flags and structural upper nibble zero
and stores the absolute brightest level in the existing reserved byte.

This migrator never rewrites payload bytes.  It appends a rebased, converted
copy of the index and switches the top-level header only after the new index is
durably flushed.  The old index remains in the file as rollback material.
"""

from __future__ import annotations

import fcntl
import os
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import click

from .combine.records import (
    FRONTIER_REF_FMT,
    FRONTIER_REF_SIZE,
    HAS_CHILDREN,
    HAS_PAYLOAD,
    HEADER_FMT,
    HEADER_MAGIC,
    HEADER_SIZE,
    IS_FRONTIER,
    IS_TERMINAL,
    LEVELS_PER_SHARD,
    SHARD_HDR_FMT,
    SHARD_HDR_SIZE,
    SHARD_MAGIC,
    SHARD_NODE_V2_SIZE,
    STAR_FORMAT_VERSION_V2,
    pack_brightest_level,
)

LEGACY_HEADER_FLAG = 0x0001
STRUCTURAL_FLAGS = HAS_PAYLOAD | HAS_CHILDREN | IS_FRONTIER | IS_TERMINAL
_FLAGS_BYTE_OFFSET = 6
_RESERVED_BYTE_OFFSET = 7


class MigrationError(ValueError):
    """The artifact cannot be migrated safely by this one-off tool."""


class SaturatedLegacyDeltaError(MigrationError):
    """At least one four-bit delta lost its exact value through saturation."""


@dataclass(frozen=True, slots=True)
class MigrationReport:
    path: Path
    original_size: int
    old_index_offset: int
    new_index_offset: int | None
    index_length: int
    shard_count: int
    node_count: int
    frontier_reference_count: int
    max_delta: int


@dataclass(frozen=True, slots=True)
class _IndexScan:
    shard_count: int
    node_count: int
    frontier_reference_count: int
    max_delta: int


def preflight_legacy_v2(path: Path) -> MigrationReport:
    """Validate that *path* can be migrated losslessly without payload reads."""
    resolved = Path(path)
    with open(resolved, "rb") as fp:
        fcntl.flock(fp.fileno(), fcntl.LOCK_SH)
        header = _read_header(fp)
        file_size = os.fstat(fp.fileno()).st_size
        old_index_offset, index_length = _validate_legacy_header(
            header, file_size=file_size
        )
        scan = _scan_index(
            fp,
            index_offset=old_index_offset,
            index_length=index_length,
            encoding="legacy",
        )
    return MigrationReport(
        path=resolved,
        original_size=file_size,
        old_index_offset=old_index_offset,
        new_index_offset=None,
        index_length=index_length,
        shard_count=scan.shard_count,
        node_count=scan.node_count,
        frontier_reference_count=scan.frontier_reference_count,
        max_delta=scan.max_delta,
    )


def migrate_legacy_v2(path: Path) -> MigrationReport:
    """Append a converted index and switch the artifact header last.

    Preflight runs under the same exclusive advisory lock as the mutation.  On
    an ordinary exception or interruption, the original header is restored and
    the unreferenced appended index is truncated.
    """
    resolved = Path(path)
    with open(resolved, "r+b", buffering=0) as fp:
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise MigrationError(
                f"Artifact is locked by another process: {resolved}"
            ) from exc

        original_header = _read_header(fp)
        original_size = os.fstat(fp.fileno()).st_size
        old_index_offset, index_length = _validate_legacy_header(
            original_header, file_size=original_size
        )
        preflight = _scan_index(
            fp,
            index_offset=old_index_offset,
            index_length=index_length,
            encoding="legacy",
        )

        new_index_offset = original_size
        shift = new_index_offset - old_index_offset
        try:
            _append_converted_index(
                fp,
                old_index_offset=old_index_offset,
                new_index_offset=new_index_offset,
                index_length=index_length,
                shift=shift,
            )
            fp.flush()
            os.fsync(fp.fileno())

            migrated_header = list(HEADER_FMT.unpack(original_header))
            migrated_header[2] = 0
            migrated_header[3] = new_index_offset
            fp.seek(0)
            fp.write(HEADER_FMT.pack(*migrated_header))
            fp.flush()
            os.fsync(fp.fileno())

            migrated_scan = _validate_migrated(
                fp,
                expected_size=new_index_offset + index_length,
                expected_index_offset=new_index_offset,
                expected_index_length=index_length,
            )
            if migrated_scan.node_count != preflight.node_count:
                raise MigrationError(
                    "Migrated index node count changed: "
                    f"{preflight.node_count} -> {migrated_scan.node_count}"
                )
            if migrated_scan.shard_count != preflight.shard_count:
                raise MigrationError(
                    "Migrated index shard count changed: "
                    f"{preflight.shard_count} -> {migrated_scan.shard_count}"
                )
        except BaseException:
            fp.seek(0)
            fp.write(original_header)
            fp.flush()
            os.fsync(fp.fileno())
            fp.truncate(original_size)
            fp.flush()
            os.fsync(fp.fileno())
            raise

    return MigrationReport(
        path=resolved,
        original_size=original_size,
        old_index_offset=old_index_offset,
        new_index_offset=new_index_offset,
        index_length=index_length,
        shard_count=preflight.shard_count,
        node_count=preflight.node_count,
        frontier_reference_count=preflight.frontier_reference_count,
        max_delta=preflight.max_delta,
    )


def _read_header(fp: BinaryIO) -> bytes:
    fp.seek(0)
    raw = fp.read(HEADER_SIZE)
    if len(raw) != HEADER_SIZE:
        raise MigrationError(
            f"Truncated STAR header: expected {HEADER_SIZE} bytes, got {len(raw)}"
        )
    return raw


def _validate_legacy_header(header: bytes, *, file_size: int) -> tuple[int, int]:
    fields = HEADER_FMT.unpack(header)
    magic, version, flags = fields[:3]
    index_offset = int(fields[3])
    index_length = int(fields[4])
    if magic != HEADER_MAGIC:
        raise MigrationError(f"Not a STAR artifact: magic={magic!r}")
    if int(version) != STAR_FORMAT_VERSION_V2:
        raise MigrationError(f"Expected STAR v2, found version {version}")
    if int(flags) != LEGACY_HEADER_FLAG:
        if int(flags) == 0:
            raise MigrationError(
                "STAR v2 header flags are already zero; this is not the legacy artifact"
            )
        raise MigrationError(
            f"Expected legacy STAR v2 header flag 0x0001, found 0x{int(flags):04x}"
        )
    if index_offset < HEADER_SIZE or index_length <= 0:
        raise MigrationError(
            f"Invalid legacy index range: offset={index_offset}, length={index_length}"
        )
    if index_offset + index_length != file_size:
        raise MigrationError(
            "Legacy index must end at EOF before append migration: "
            f"index_end={index_offset + index_length}, file_size={file_size}"
        )
    return index_offset, index_length


def _scan_index(
    fp: BinaryIO,
    *,
    index_offset: int,
    index_length: int,
    encoding: str,
) -> _IndexScan:
    if encoding not in {"legacy", "absolute"}:
        raise ValueError(f"Unsupported scan encoding: {encoding}")
    index_end = index_offset + index_length
    shard_offset = index_offset
    shard_count = 0
    node_count = 0
    frontier_reference_count = 0
    max_delta = 0
    saturated_count = 0
    first_saturated: tuple[int, int, int] | None = None

    while shard_offset < index_end:
        fields, nodes, frontiers, frontier_count = _read_shard(
            fp, shard_offset=shard_offset, index_end=index_end
        )
        parent_depth = int(fields[9])
        for node_index in range(int(fields[7])):
            record_offset = node_index * SHARD_NODE_V2_SIZE
            local_depth = int(nodes[record_offset + 5])
            flags = int(nodes[record_offset + _FLAGS_BYTE_OFFSET])
            reserved = int(nodes[record_offset + _RESERVED_BYTE_OFFSET])
            level = parent_depth + local_depth
            if level < 0:
                raise MigrationError(
                    f"Negative global node level in shard at {shard_offset}: {level}"
                )
            if encoding == "legacy":
                if reserved != 0:
                    raise MigrationError(
                        "Legacy node spare byte is not zero at "
                        f"shard_offset={shard_offset}, node_index={node_index + 1}"
                    )
                delta = (flags >> 4) & 0x0F
                max_delta = max(max_delta, delta)
                if delta == 0x0F:
                    saturated_count += 1
                    if first_saturated is None:
                        first_saturated = (shard_offset, node_index + 1, level)
                else:
                    _validate_brightest_level(
                        level=level, brightest_level=level + delta
                    )
            else:
                if flags & ~STRUCTURAL_FLAGS:
                    raise MigrationError(
                        "Migrated node retains non-structural flag bits at "
                        f"shard_offset={shard_offset}, node_index={node_index + 1}"
                    )
                _validate_brightest_level(level=level, brightest_level=reserved)

        for frontier_index in range(frontier_count):
            (child_offset,) = FRONTIER_REF_FMT.unpack_from(
                frontiers, frontier_index * FRONTIER_REF_SIZE
            )
            if child_offset and not index_offset <= child_offset < index_end:
                raise MigrationError(
                    "Frontier reference is outside the index: "
                    f"shard_offset={shard_offset}, child_offset={child_offset}"
                )

        shard_count += 1
        node_count += int(fields[7])
        frontier_reference_count += frontier_count
        shard_offset = int(fields[23]) + len(frontiers)

    if shard_offset != index_end:
        raise MigrationError(
            f"Index scan ended at {shard_offset}, expected {index_end}"
        )
    if saturated_count:
        assert first_saturated is not None
        offset, node_index, level = first_saturated
        raise SaturatedLegacyDeltaError(
            f"Found {saturated_count} saturated legacy delta(s); first at "
            f"shard_offset={offset}, node_index={node_index}, level={level}. "
            "Exact values require descendant or payload recovery. No bytes were changed."
        )
    return _IndexScan(
        shard_count=shard_count,
        node_count=node_count,
        frontier_reference_count=frontier_reference_count,
        max_delta=max_delta,
    )


def _read_shard(
    fp: BinaryIO, *, shard_offset: int, index_end: int
) -> tuple[list[object], bytearray, bytearray, int]:
    fp.seek(shard_offset)
    raw_header = fp.read(SHARD_HDR_SIZE)
    if len(raw_header) != SHARD_HDR_SIZE:
        raise MigrationError(f"Truncated shard header at offset {shard_offset}")
    fields: list[object] = list(SHARD_HDR_FMT.unpack(raw_header))
    if fields[0] != SHARD_MAGIC:
        raise MigrationError(
            f"Invalid shard magic at offset {shard_offset}: {fields[0]!r}"
        )
    if int(fields[1]) != STAR_FORMAT_VERSION_V2:
        raise MigrationError(
            f"Expected v2 shard at offset {shard_offset}, found {fields[1]}"
        )
    if int(fields[2]) != LEVELS_PER_SHARD:
        raise MigrationError(
            f"Unexpected levels-per-shard at offset {shard_offset}: {fields[2]}"
        )
    node_count = int(fields[7])
    first_frontier = int(fields[21])
    node_table_offset = int(fields[22])
    frontier_table_offset = int(fields[23])
    if node_table_offset != shard_offset + SHARD_HDR_SIZE:
        raise MigrationError(
            f"Non-contiguous node table at shard offset {shard_offset}"
        )
    expected_frontier_offset = node_table_offset + node_count * SHARD_NODE_V2_SIZE
    if frontier_table_offset != expected_frontier_offset:
        raise MigrationError(
            f"Non-contiguous frontier table at shard offset {shard_offset}"
        )
    if first_frontier < 0 or first_frontier > node_count:
        raise MigrationError(
            f"Invalid first frontier index at shard offset {shard_offset}: {first_frontier}"
        )
    frontier_count = node_count - first_frontier + 1 if first_frontier else 0
    shard_end = frontier_table_offset + frontier_count * FRONTIER_REF_SIZE
    if shard_end > index_end:
        raise MigrationError(f"Shard at offset {shard_offset} exceeds the index")

    fp.seek(node_table_offset)
    nodes = bytearray(fp.read(node_count * SHARD_NODE_V2_SIZE))
    if len(nodes) != node_count * SHARD_NODE_V2_SIZE:
        raise MigrationError(f"Truncated node table at shard offset {shard_offset}")
    frontiers = bytearray(fp.read(frontier_count * FRONTIER_REF_SIZE))
    if len(frontiers) != frontier_count * FRONTIER_REF_SIZE:
        raise MigrationError(f"Truncated frontier table at shard offset {shard_offset}")
    return fields, nodes, frontiers, frontier_count


def _append_converted_index(
    fp: BinaryIO,
    *,
    old_index_offset: int,
    new_index_offset: int,
    index_length: int,
    shift: int,
) -> None:
    old_index_end = old_index_offset + index_length
    old_shard_offset = old_index_offset
    expected_new_offset = new_index_offset
    while old_shard_offset < old_index_end:
        fields, nodes, frontiers, frontier_count = _read_shard(
            fp, shard_offset=old_shard_offset, index_end=old_index_end
        )
        if old_shard_offset + shift != expected_new_offset:
            raise MigrationError("Converted shard offsets are not contiguous")
        parent_depth = int(fields[9])
        node_count = int(fields[7])
        for node_index in range(node_count):
            record_offset = node_index * SHARD_NODE_V2_SIZE
            local_depth = int(nodes[record_offset + 5])
            flags = int(nodes[record_offset + _FLAGS_BYTE_OFFSET])
            reserved = int(nodes[record_offset + _RESERVED_BYTE_OFFSET])
            if reserved != 0:
                raise MigrationError("Legacy node spare byte changed after preflight")
            delta = (flags >> 4) & 0x0F
            if delta == 0x0F:
                raise SaturatedLegacyDeltaError(
                    "Legacy delta became saturated after preflight"
                )
            level = parent_depth + local_depth
            brightest_level = _validate_brightest_level(
                level=level, brightest_level=level + delta
            )
            nodes[record_offset + _FLAGS_BYTE_OFFSET] = flags & STRUCTURAL_FLAGS
            nodes[record_offset + _RESERVED_BYTE_OFFSET] = brightest_level

        fields[22] = int(fields[22]) + shift
        fields[23] = int(fields[23]) + shift
        for frontier_index in range(frontier_count):
            record_offset = frontier_index * FRONTIER_REF_SIZE
            (child_offset,) = FRONTIER_REF_FMT.unpack_from(frontiers, record_offset)
            if child_offset:
                FRONTIER_REF_FMT.pack_into(
                    frontiers, record_offset, int(child_offset) + shift
                )

        fp.seek(expected_new_offset)
        fp.write(SHARD_HDR_FMT.pack(*fields))
        fp.write(nodes)
        fp.write(frontiers)
        old_shard_offset = int(fields[23]) - shift + len(frontiers)
        expected_new_offset = fp.tell()

    if old_shard_offset != old_index_end:
        raise MigrationError("Legacy index conversion did not consume the full index")
    if expected_new_offset != new_index_offset + index_length:
        raise MigrationError(
            "Converted index length changed: "
            f"wrote={expected_new_offset - new_index_offset}, expected={index_length}"
        )


def _validate_brightest_level(*, level: int, brightest_level: int) -> int:
    try:
        return pack_brightest_level(
            level=level,
            brightest_level=brightest_level,
        )
    except ValueError as exc:
        raise MigrationError(str(exc)) from exc


def _validate_migrated(
    fp: BinaryIO,
    *,
    expected_size: int,
    expected_index_offset: int,
    expected_index_length: int,
) -> _IndexScan:
    actual_size = os.fstat(fp.fileno()).st_size
    if actual_size != expected_size:
        raise MigrationError(
            f"Migrated file size is {actual_size}, expected {expected_size}"
        )
    header = HEADER_FMT.unpack(_read_header(fp))
    if header[0] != HEADER_MAGIC or int(header[1]) != STAR_FORMAT_VERSION_V2:
        raise MigrationError("Migrated STAR header magic/version changed")
    if int(header[2]) != 0:
        raise MigrationError(f"Migrated STAR header flags are not zero: {header[2]}")
    if int(header[3]) != expected_index_offset:
        raise MigrationError(
            f"Migrated index offset is {header[3]}, expected {expected_index_offset}"
        )
    if int(header[4]) != expected_index_length:
        raise MigrationError(
            f"Migrated index length is {header[4]}, expected {expected_index_length}"
        )
    return _scan_index(
        fp,
        index_offset=expected_index_offset,
        index_length=expected_index_length,
        encoding="absolute",
    )


def _format_report(report: MigrationReport, *, migrated: bool) -> str:
    lines = [
        f"artifact: {report.path}",
        f"file size before migration: {report.original_size:,} bytes",
        f"legacy index: offset={report.old_index_offset:,}, length={report.index_length:,}",
        f"shards: {report.shard_count:,}",
        f"nodes: {report.node_count:,}",
        f"frontier references: {report.frontier_reference_count:,}",
        f"maximum exact legacy delta: {report.max_delta}",
    ]
    if migrated:
        assert report.new_index_offset is not None
        lines.extend(
            [
                f"new index offset: {report.new_index_offset:,}",
                f"file size after migration: {report.new_index_offset + report.index_length:,} bytes",
                "migration complete: header flags=0, absolute brightest levels enabled",
            ]
        )
    else:
        lines.append("preflight passed: migration is lossless; no bytes changed")
    return "\n".join(lines)


@click.command()
@click.argument(
    "artifact",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--apply",
    is_flag=True,
    help="Append the converted index and switch the artifact header.",
)
def main(artifact: Path, apply: bool) -> None:
    """Preflight or migrate one experimental legacy STAR v2 ARTIFACT."""
    try:
        report = migrate_legacy_v2(artifact) if apply else preflight_legacy_v2(artifact)
    except (OSError, MigrationError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(_format_report(report, migrated=apply))


if __name__ == "__main__":
    main()
