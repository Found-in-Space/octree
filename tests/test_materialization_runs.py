from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.octree.materialization.runs import (
    RunMergeBounds,
    SortedRunLayout,
    iter_merged_batches,
    reduce_sorted_runs,
    write_merged_run,
)

_SCHEMA = pa.schema(
    [
        pa.field("cell_depth", pa.int16(), nullable=False),
        pa.field("cell_index", pa.uint64(), nullable=False),
        pa.field("rank", pa.float64()),
        pa.field("object_id", pa.string(), nullable=False),
        pa.field("payload", pa.binary(), nullable=False),
    ]
)
_LAYOUT = SortedRunLayout(
    schema=_SCHEMA,
    cell_level_column="cell_depth",
    cell_node_column="cell_index",
    overlap_sort_keys=(("rank", "ascending"), ("object_id", "ascending")),
)


def _write_run(
    path: Path,
    rows: list[tuple[int, int, float | None, str, bytes]],
) -> None:
    pq.write_table(
        pa.table(
            {
                "cell_depth": pa.array([row[0] for row in rows], pa.int16()),
                "cell_index": pa.array([row[1] for row in rows], pa.uint64()),
                "rank": pa.array([row[2] for row in rows], pa.float64()),
                "object_id": pa.array([row[3] for row in rows], pa.string()),
                "payload": pa.array([row[4] for row in rows], pa.binary()),
            },
            schema=_SCHEMA,
        ),
        path,
        compression="zstd",
        row_group_size=2,
    )


def _merged(paths: list[Path], *, spill_dir: Path, bounds: RunMergeBounds) -> pa.Table:
    return pa.concat_tables(
        [
            batch
            for _key, batch in iter_merged_batches(
                paths,
                batch_size=100,
                spill_dir=spill_dir,
                layout=_LAYOUT,
                bounds=bounds,
            )
        ],
        promote_options="none",
    )


def test_profile_neutral_overlap_spill_matches_in_memory_and_cleans_up(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write_run(
        first,
        [
            (2, 1, 1.0, "same", b"first-a"),
            (2, 1, 1.0, "same", b"first-b"),
            (2, 1, None, "last", b"first-null"),
        ],
    )
    _write_run(
        second,
        [
            (2, 1, 0.0, "zero", b"second-zero"),
            (2, 1, 1.0, "same", b"second-same"),
            (3, 7, 2.0, "other-cell", b"other"),
        ],
    )
    spill_dir = tmp_path / "spill"
    spill_dir.mkdir()

    in_memory = _merged(
        [first, second],
        spill_dir=spill_dir,
        bounds=RunMergeBounds(overlap_in_memory_max_bytes=1024 * 1024),
    )
    spilled = _merged(
        [first, second],
        spill_dir=spill_dir,
        bounds=RunMergeBounds(overlap_in_memory_max_bytes=1),
    )

    assert spilled.equals(in_memory)
    assert spilled.column("payload").to_pylist() == [
        b"second-zero",
        b"first-a",
        b"first-b",
        b"second-same",
        b"first-null",
        b"other",
    ]
    assert list(spill_dir.iterdir()) == []


def test_profile_neutral_run_reduction_is_bounded_and_deterministic(
    tmp_path: Path,
) -> None:
    inputs: list[Path] = []
    expected_ids: list[str] = []
    for index in range(7):
        path = tmp_path / f"input-{index}.parquet"
        object_id = f"object-{index}"
        _write_run(path, [(4, 3, float(index), object_id, bytes([index]))])
        inputs.append(path)
        expected_ids.append(object_id)

    merge_dir = tmp_path / "merge"
    reduced = reduce_sorted_runs(
        inputs,
        partition_dir=merge_dir,
        batch_size=2,
        fan_in=2,
        layout=_LAYOUT,
    )

    assert 1 <= len(reduced) <= 2
    assert all(path.parent == merge_dir for path in reduced)
    output = tmp_path / "final.parquet"
    write_merged_run(
        reduced,
        output,
        batch_size=3,
        layout=_LAYOUT,
    )
    assert pq.read_table(output).column("object_id").to_pylist() == expected_ids
    assert [
        pq.ParquetFile(output).metadata.row_group(index).num_rows
        for index in range(pq.ParquetFile(output).metadata.num_row_groups)
    ] == [3, 3, 1]

    with pytest.raises(ValueError, match="fan_in must be >= 2"):
        reduce_sorted_runs(
            inputs,
            partition_dir=merge_dir,
            batch_size=2,
            fan_in=1,
            layout=_LAYOUT,
        )
