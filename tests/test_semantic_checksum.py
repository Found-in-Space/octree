from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import foundinspace.octree.sources.semantic_checksum as checksum_module
from foundinspace.octree.sources.semantic_checksum import checksum_parquet_files


def _write_parts(
    root: Path,
    *,
    fragment_rows: int,
    metadata_value: str,
) -> list[Path]:
    root.mkdir()
    schema = pa.schema(
        [
            pa.field(
                "source_id",
                pa.string(),
                metadata={"producer": metadata_value},
            ),
            pa.field("value", pa.int64()),
        ],
        metadata={"producer": metadata_value},
    )
    table = pa.Table.from_arrays(
        [
            pa.array([str(index) for index in range(11)]),
            pa.array(range(11), type=pa.int64()),
        ],
        schema=schema,
    )
    paths: list[Path] = []
    for sequence, offset in enumerate(range(0, len(table), fragment_rows)):
        path = root / f"part-{sequence:03d}.parquet"
        pq.write_table(table.slice(offset, fragment_rows), path, compression="zstd")
        paths.append(path)
    return paths


def test_streaming_checksum_is_fragment_and_metadata_independent(
    tmp_path: Path,
) -> None:
    compact = _write_parts(
        tmp_path / "compact",
        fragment_rows=11,
        metadata_value="first",
    )
    fragmented = _write_parts(
        tmp_path / "fragmented",
        fragment_rows=3,
        metadata_value="second",
    )

    compact_checksum = checksum_parquet_files(compact, batch_rows=4)
    fragmented_checksum = checksum_parquet_files(fragmented, batch_rows=4)

    assert compact_checksum == fragmented_checksum
    assert compact_checksum[1] == 11


def test_streaming_checksum_never_uses_whole_file_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _write_parts(
        tmp_path / "parts",
        fragment_rows=3,
        metadata_value="producer",
    )
    monkeypatch.setattr(
        checksum_module.pq,
        "read_table",
        lambda *_args, **_kwargs: pytest.fail("whole-file read is not bounded"),
    )

    checksum, row_count = checksum_parquet_files(paths, batch_rows=2)

    assert checksum.startswith("sha256:")
    assert row_count == 11
