from __future__ import annotations

import json
import struct
from pathlib import Path
from uuid import UUID

from click.testing import CliRunner

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree._cli import cli
from foundinspace.octree.combine import CombinePlan, combine_octree
from foundinspace.octree.combine.records import PackedDescriptorFields
from foundinspace.octree.reader.index import Point
from foundinspace.octree.terminal_memory_benchmark import (
    ExtractedSample,
    LogicalNode,
    SampleSpec,
    TerminalMemoryBenchmarkConfig,
    TraceView,
    _benchmark_sample,
    build_terminal_plan,
    parse_sample_spec,
    safe_magnitude_prefix_count,
)

STAR_RECORD = struct.Struct("<fffhBB")
DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def _logical_node(
    *,
    key: tuple[int, int, int, int],
    center: Point,
    half_size: float,
    magnitudes: tuple[float, ...] = (),
    children: tuple[LogicalNode, ...] = (),
    payload_length: int | None = None,
) -> LogicalNode:
    magnitudes_centi = tuple(sorted(round(value * 100) for value in magnitudes))
    return LogicalNode(
        key=key,
        center=center,
        half_size=half_size,
        payload_offset=0,
        payload_length=(
            payload_length
            if payload_length is not None
            else max(0, len(magnitudes_centi) * 10)
        ),
        magnitudes_centi=magnitudes_centi,
        children=children,
        subtree_star_count=len(magnitudes_centi)
        + sum(child.subtree_star_count for child in children),
        subtree_node_count=1 + sum(child.subtree_node_count for child in children),
        subtree_payload_count=(1 if magnitudes_centi else 0)
        + sum(child.subtree_payload_count for child in children),
    )


def _raw_stars(*magnitudes: float) -> bytes:
    return b"".join(
        STAR_RECORD.pack(0.0, 0.0, 0.0, round(magnitude * 100), 128, 0)
        for magnitude in magnitudes
    )


def _build_octree(tmp_path: Path) -> Path:
    manifest = build_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=1,
                raw_payload=_raw_stars(-1.0),
            ),
            PayloadNode(
                level=1,
                node_id=0,
                star_count=2,
                raw_payload=_raw_stars(0.0, 1.0),
            ),
        ],
        max_level=1,
    )
    output = tmp_path / "stars.octree"
    combine_octree(
        manifest,
        output,
        plan=CombinePlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    return output


def test_parse_sample_spec() -> None:
    sample = parse_sample_spec("sun:1,2.5,-3@11")

    assert sample == SampleSpec(
        name="sun",
        point=Point(1.0, 2.5, -3.0),
        level=11,
    )


def test_terminal_plan_only_collapses_subtrees_with_descendants() -> None:
    grandchild = _logical_node(
        key=(2, 0, 0, 0),
        center=Point(0.0, 0.0, 0.0),
        half_size=25.0,
        magnitudes=(1.0, 2.0),
    )
    packable = _logical_node(
        key=(1, 0, 0, 0),
        center=Point(0.0, 0.0, 0.0),
        half_size=50.0,
        magnitudes=(-1.0, 0.0),
        children=(grandchild,),
    )
    natural_leaf = _logical_node(
        key=(1, 1, 0, 0),
        center=Point(100.0, 0.0, 0.0),
        half_size=50.0,
        magnitudes=tuple(float(index) for index in range(10)),
    )
    root = _logical_node(
        key=(0, 0, 0, 0),
        center=Point(0.0, 0.0, 0.0),
        half_size=100.0,
        children=(packable, natural_leaf),
    )

    plan = build_terminal_plan(root, 5)

    assert plan.terminal is False
    assert plan.children[0].terminal is True
    assert plan.children[0].logical.subtree_star_count == 4
    assert plan.children[1].terminal is False


def test_safe_magnitude_prefix_uses_nearest_cell_distance() -> None:
    node = _logical_node(
        key=(1, 0, 0, 0),
        center=Point(100.0, 0.0, 0.0),
        half_size=10.0,
        magnitudes=(0.0, 1.0, 2.0, 5.0),
    )
    view = TraceView(
        name="outside",
        observer=Point(0.0, 0.0, 0.0),
        limiting_magnitude=6.5,
    )

    assert safe_magnitude_prefix_count(node, view) == 2


def test_trace_replay_separates_waterline_from_chunk_size() -> None:
    near = _logical_node(
        key=(1, 0, 0, 0),
        center=Point(0.0, 0.0, 0.0),
        half_size=100.0,
        magnitudes=(-2.0, -1.0, 0.0, 1.0, 2.0),
    )
    far = _logical_node(
        key=(1, 1, 0, 0),
        center=Point(500.0, 0.0, 0.0),
        half_size=100.0,
        magnitudes=(-2.0, -1.0, 0.0, 1.0, 2.0),
    )
    root = _logical_node(
        key=(0, 0, 0, 0),
        center=Point(250.0, 0.0, 0.0),
        half_size=500.0,
        children=(near, far),
    )
    spec = SampleSpec("route", Point(0.0, 0.0, 0.0), 0)
    sample = ExtractedSample(
        spec=spec,
        source="fixture",
        index_magnitude=6.5,
        root=root,
    )
    views = (
        TraceView("near", Point(0.0, 0.0, 0.0), 6.5),
        TraceView("far", Point(500.0, 0.0, 0.0), 6.5),
    )
    config = TerminalMemoryBenchmarkConfig(
        source=Path("fixture"),
        samples=(spec,),
        views=views,
        waterlines=(10,),
        chunk_star_counts=(2,),
    )

    report = _benchmark_sample(sample, views, config)
    by_policy = {
        (row["policy"], row["chunk_star_count"]): row for row in report["scenarios"]
    }

    assert by_policy[("v1", None)]["max_active_rows"] == 5
    assert by_policy[("terminal-monolithic", None)]["max_active_rows"] == 10
    assert by_policy[("terminal-monolithic", None)]["max_storage_entry_rows"] == 10
    assert by_policy[("terminal-magnitude-chunked", 2)]["max_active_rows"] == 10
    assert by_policy[("terminal-magnitude-chunked", 2)]["max_storage_entry_rows"] == 2
    assert by_policy[("terminal-logical-chunked", 2)]["max_active_rows"] == 5
    assert by_policy[("terminal-logical-chunked", 2)]["max_atomic_live_bytes"] == 2 * 79
    assert by_policy[("terminal-logical-chunked", 2)]["final_raw_cache_rows"] == 10


def test_terminal_memory_benchmark_cli_reads_local_octree(tmp_path: Path) -> None:
    octree = _build_octree(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "terminal-memory-benchmark",
            str(octree),
            "--sample",
            "fixture:0,0,0@0",
            "--waterline",
            "10",
            "--chunk-stars",
            "2",
            "--cache-dir",
            str(tmp_path / "cache"),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["format"].endswith("/v0")
    assert report["samples"][0]["classic"] == {
        "index_record_bytes": 40,
        "node_count": 2,
        "payload_node_count": 2,
        "star_count": 3,
    }
    assert [row["policy"] for row in report["samples"][0]["scenarios"]] == [
        "v1",
        "terminal-monolithic",
        "terminal-magnitude-chunked",
        "terminal-logical-chunked",
    ]
