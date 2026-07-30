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
from foundinspace.octree.sources.stage00 import Stage00Config, run_stage00
from foundinspace.octree.sources.stage01 import Stage01Config, run_stage01
from foundinspace.octree.stage03_benchmark import (
    BENCHMARK_FORMAT,
    Point3,
    Stage03BenchmarkConfig,
    run_stage03_packing_benchmark,
)


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


def _stage00_table(rows: list[dict], *, shard_id: str) -> pa.Table:
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
        _stage00_table(rows, shard_id=shard_id),
        shard_dir / "part.parquet",
        compression="zstd",
    )


def _build_stage01(
    tmp_path: Path,
    rows: list[dict],
    *,
    bucket_size: int = 100,
) -> tuple[Path, Path]:
    input_root = tmp_path / "input"
    stage00_dir = tmp_path / "stage00"
    stage01_dir = tmp_path / "stage01"
    _write_input(input_root, "100", rows)
    run_stage00(
        Stage00Config(
            input_root=input_root,
            output_dir=stage00_dir,
            mag_config=MagLevelConfig(v_mag=6.5),
            bucket_size=bucket_size,
            batch_size=10,
            fragment_target_rows=10,
            compact_after_files=0,
        )
    )
    run_stage01(
        Stage01Config(
            stage00_output_dir=stage00_dir,
            output_dir=stage01_dir,
            v_mag=6.5,
            bucket_size=bucket_size,
            batch_size=10,
            fragment_target_rows=10,
        )
    )
    return stage00_dir, stage01_dir


def _benchmark_config(
    stage00_dir: Path,
    stage01_dir: Path,
    *,
    profiles: tuple[str, ...] = ("classic", "unbounded"),
    orders: tuple[str, ...] = ("dfs", "level-major"),
    scenarios: tuple[str, ...] = ("observer-shell",),
) -> Stage03BenchmarkConfig:
    return Stage03BenchmarkConfig(
        stage00_output_dir=stage00_dir,
        stage01_output_dir=stage01_dir,
        profiles=profiles,
        orders=orders,
        scenarios=scenarios,
        center=Point3(0.0, 0.0, 0.0),
        limiting_magnitude=20.0,
        coalesce_gap_bytes=0,
        batch_size=10,
    )


def test_stage03_benchmark_reports_orders_and_range_metrics(tmp_path: Path) -> None:
    stage00_dir, stage01_dir = _build_stage01(
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

    report = run_stage03_packing_benchmark(_benchmark_config(stage00_dir, stage01_dir))

    assert report["format"] == BENCHMARK_FORMAT
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


def test_stage03_benchmark_classic_folds_deeper_nodes(tmp_path: Path) -> None:
    stage00_dir, stage01_dir = _build_stage01(
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

    report = run_stage03_packing_benchmark(
        _benchmark_config(
            stage00_dir,
            stage01_dir,
            profiles=("classic", "unbounded"),
            orders=("level-major",),
        )
    )

    assert report["profile_node_counts"] == {"classic": 1, "unbounded": 2}


def test_stage03_benchmark_rejects_dirty_stage01_groups(tmp_path: Path) -> None:
    stage00_dir, stage01_dir = _build_stage01(
        tmp_path,
        [
            {
                "source_id": "a",
                "morton_code": _morton_for_node(1, 0),
                "level": 1,
            }
        ],
    )
    state_path = stage00_dir / "stage-state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["dirty"]["stage01_groups"] = ["|100|pack"]
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="dirty Stage 01 groups"):
        run_stage03_packing_benchmark(_benchmark_config(stage00_dir, stage01_dir))


def _write_project(project_path: Path, stage00_dir: Path, stage01_dir: Path) -> None:
    project_path.write_text(
        f"""
format_version = 1

[paths]
merged_healpix_dir = "{(project_path.parent / "merged").as_posix()}"
identifiers_map_path = "{(project_path.parent / "identifiers_map.parquet").as_posix()}"
stage00_output_dir = "{stage00_dir.as_posix()}"
stage01_output_dir = "{stage01_dir.as_posix()}"
stage02_output_path = "{(project_path.parent / "stars.octree").as_posix()}"
identifiers_order_output_path = "{(project_path.parent / "identifiers.order").as_posix()}"
stage03_output_dir = "{(project_path.parent / "stage03").as_posix()}"

[stage00]
batch_size = 1000000
v_mag = 6.5

[stage01]
input_glob = "{(project_path.parent / "missing" / "**" / "*.parquet").as_posix()}"
batch_size = 10
deep_shard_from_level = 99
deep_prefix_bits = 3

[stage02]
max_open_files = 32

[stage03]

[[stage03.sidecars]]
name = "meta"
fields = []
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_stage03_benchmark_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-03-benchmark", "--help"])

    assert result.exit_code == 0
    assert "--profile" in result.output
    assert "--order" in result.output
    assert "--scenario" in result.output
    assert "--json" in result.output


def test_stage03_benchmark_cli_json(tmp_path: Path) -> None:
    stage00_dir, stage01_dir = _build_stage01(
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
    _write_project(project_path, stage00_dir, stage01_dir)
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "stage-03-benchmark",
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
    assert report["format"] == BENCHMARK_FORMAT
    assert report["results"][0]["profile"] == "classic"
    assert report["results"][0]["order"] == "level-major"
