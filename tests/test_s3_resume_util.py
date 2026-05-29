from __future__ import annotations

from foundinspace.octree.s3_resume.util import (
    MAX_PARTS,
    MiB,
    choose_part_size_bytes,
    iter_part_plan,
    parse_s3_uri,
)


def test_parse_s3_uri() -> None:
    target = parse_s3_uri("s3://bucket-name/path/to/file.bin")
    assert target.bucket == "bucket-name"
    assert target.key == "path/to/file.bin"


def test_choose_part_size_adjusts_for_part_limit() -> None:
    # Large enough that 5 MiB parts would exceed 10,000 parts.
    file_size = (MAX_PARTS + 1) * 5 * MiB
    part_size = choose_part_size_bytes(file_size, 5)
    assert part_size > 5 * MiB
    assert (file_size + part_size - 1) // part_size <= MAX_PARTS


def test_iter_part_plan_has_full_coverage() -> None:
    plan = iter_part_plan(file_size=10 * MiB + 7, part_size=4 * MiB)
    assert [p[0] for p in plan] == [1, 2, 3]
    assert plan[0] == (1, 0, 4 * MiB)
    assert plan[1] == (2, 4 * MiB, 4 * MiB)
    assert plan[2] == (3, 8 * MiB, (2 * MiB) + 7)
