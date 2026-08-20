from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.config import (
    MORTON_BITS,
    WORLD_CENTER,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.packing_benchmark import (
    PackingBenchmarkConfig,
    Point3,
    run_packing_benchmark,
)
from foundinspace.octree.sources.preparation import (
    PreparationConfig,
    prepare_contributions,
)
from foundinspace.octree.sources.routing import RoutingConfig, route_contributions
from project_helpers import project_text


def _morton_for_node(level: int, node_id: int) -> int:
    return int(node_id) << (3 * (MORTON_BITS - level))


def _node_center(level: int, node_id: int) -> tuple[float, float, float]:
    grid = [0, 0, 0]
    for bit in range(level):
        for axis in range(3):
            grid[axis] |= ((node_id >> (3 * bit + axis)) & 1) << bit
    width = 2.0 * WORLD_HALF_SIZE_PC / (2**level)
    return tuple(
        float(WORLD_CENTER[axis] - WORLD_HALF_SIZE_PC + (value + 0.5) * width)
        for axis, value in enumerate(grid)
    )


def _routing_table(rows: list[dict], *, shard_id: str) -> pa.Table:
    positions = [
        _node_center(
            int(row["level"]),
            int(row["morton_code"]) >> (3 * (MORTON_BITS - int(row["level"]))),
        )
        for row in rows
    ]
    return pa.table(
        {
            "source": pa.array([r.get("source", "gaia") for r in rows], pa.string()),
            "source_id": pa.array([r["source_id"] for r in rows], pa.string()),
            "morton_code": pa.array([r["morton_code"] for r in rows], pa.uint64()),
            "level": pa.array([r["level"] for r in rows], pa.int32()),
            "mag_abs": pa.array([r.get("mag_abs", 7.0) for r in rows], pa.float64()),
            "x_icrs_pc": pa.array(
                [position[0] for position in positions], pa.float64()
            ),
            "y_icrs_pc": pa.array(
                [position[1] for position in positions], pa.float64()
            ),
            "z_icrs_pc": pa.array(
                [position[2] for position in positions], pa.float64()
            ),
            "teff": pa.array([r.get("teff", 5800.0) for r in rows], pa.float64()),
            "healpix_id": pa.array([shard_id for _row in rows], pa.string()),
        }
    )


def _write_input(root: Path, shard_id: str, rows: list[dict]) -> None:
    shard_dir = root / shard_id
    shard_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _routing_table(rows, shard_id=shard_id),
        shard_dir / "part.parquet",
        compression="zstd",
    )


def _build_preparation(
    tmp_path: Path,
    rows: list[dict],
    *,
    bucket_size: int = 100,
) -> tuple[Path, Path]:
    input_root = tmp_path / "input"
    routing_dir = tmp_path / "routing"
    preparation_dir = tmp_path / "preparation"
    _write_input(input_root, "100", rows)
    route_contributions(
        RoutingConfig(
            input_shards_dir=input_root,
            routed_dir=routing_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_rows=bucket_size,
            scan_batch_rows=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )
    prepare_contributions(
        PreparationConfig(
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
            limiting_magnitude=6.5,
            bucket_rows=bucket_size,
            batch_rows=10,
            fragment_target_rows=10,
        )
    )
    return routing_dir, preparation_dir


def _benchmark_config(
    routing_dir: Path,
    preparation_dir: Path,
    *,
    profiles: tuple[str, ...] = ("classic", "unbounded"),
    orders: tuple[str, ...] = ("dfs", "level-major"),
    scenarios: tuple[str, ...] = ("observer-shell",),
) -> PackingBenchmarkConfig:
    return PackingBenchmarkConfig(
        routed_dir=routing_dir,
        prepared_dir=preparation_dir,
        profiles=profiles,
        orders=orders,
        scenarios=scenarios,
        center=Point3(0.0, 0.0, 0.0),
        limiting_magnitude=20.0,
        coalesce_gap_bytes=0,
        batch_rows=10,
    )


def test_packing_benchmark_reports_orders_and_range_metrics(tmp_path: Path) -> None:
    routing_dir, preparation_dir = _build_preparation(
        tmp_path,
        [
            {
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
            },
            {
                "source_id": "b",
                "morton_code": _morton_for_node(2, 1),
                "level": 2,
            },
            {
                "source_id": "c",
                "morton_code": _morton_for_node(1, 1),
                "level": 1,
            },
        ],
    )

    report = run_packing_benchmark(_benchmark_config(routing_dir, preparation_dir))

    assert report["profiles"] == ["classic", "unbounded"]
    assert report["orders"] == ["dfs", "level-major"]
    assert len(report["results"]) == 4
    result = report["results"][0]
    assert result["final_node_count"] == 3
    assert result["selected_node_count"] == 3
    assert result["selected_star_count"] == 3
    assert result["payload_range_count"] == 3
    assert result["raw_payload_bytes"] > 0
    assert 0 < result["useful_ratio"] <= 1


def test_packing_benchmark_classic_folds_deeper_nodes(tmp_path: Path) -> None:
    routing_dir, preparation_dir = _build_preparation(
        tmp_path,
        [
            {
                "source_id": "a",
                "morton_code": _morton_for_node(15, 0),
                "level": 15,
            },
            {
                "source_id": "b",
                "morton_code": _morton_for_node(15, 1),
                "level": 15,
            },
        ],
    )

    report = run_packing_benchmark(
        _benchmark_config(
            routing_dir,
            preparation_dir,
            profiles=("classic", "unbounded"),
            orders=("level-major",),
        )
    )

    assert report["profile_node_counts"] == {"classic": 1, "unbounded": 2}


def test_packing_benchmark_rejects_dirty_preparation_groups(tmp_path: Path) -> None:
    routing_dir, preparation_dir = _build_preparation(
        tmp_path,
        [
            {
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
            }
        ],
    )
    state_path = routing_dir / "pipeline-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["dirty"]["preparation"]["group_keys"] = ["|100|pack"]
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Preparation to be current"):
        run_packing_benchmark(_benchmark_config(routing_dir, preparation_dir))


def _write_project(
    project_path: Path, routing_dir: Path, preparation_dir: Path
) -> None:
    project_path.write_text(
        project_text(
            project_path.parent,
            routed_dir=routing_dir,
            prepared_dir=preparation_dir,
        ),
        encoding="utf-8",
    )


def test_packing_benchmark_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", "packing-order", "--help"])

    assert result.exit_code == 0
    assert "--profile" in result.output
    assert "--order" in result.output
    assert "--scenario" in result.output
    assert "--json" in result.output


def test_packing_benchmark_cli_json(tmp_path: Path) -> None:
    routing_dir, preparation_dir = _build_preparation(
        tmp_path,
        [
            {
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
            }
        ],
    )
    project_path = tmp_path / "project.toml"
    _write_project(project_path, routing_dir, preparation_dir)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "benchmark",
            "packing-order",
            "--project",
            str(project_path),
            "--profile",
            "classic",
            "--order",
            "level-major",
            "--scenario",
            "observer-shell",
            "--magnitude",
            "20",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["results"][0]["profile"] == "classic"
    assert report["results"][0]["order"] == "level-major"
