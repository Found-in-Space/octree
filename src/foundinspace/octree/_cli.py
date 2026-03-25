from __future__ import annotations

import math
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from foundinspace.octree.config import (
    DEFAULT_MAG_VIS,
    DEFAULT_MAX_LEVEL,
    LEVEL_CONFIG,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.reader import Point
from foundinspace.octree.reader.stats import StatsReport, collect_stats
from foundinspace.octree.sources.add_shard_columns import run_add_shard_columns
from foundinspace.octree.sources.sort_shards import run_sort_shards


def _resolve_mag_config(
    v_mag: float | None,
    max_level: int | None,
) -> MagLevelConfig:
    vm = DEFAULT_MAG_VIS if v_mag is None else v_mag
    ml = DEFAULT_MAX_LEVEL if max_level is None else max_level
    if vm == DEFAULT_MAG_VIS and ml == DEFAULT_MAX_LEVEL:
        return LEVEL_CONFIG
    return MagLevelConfig(
        v_mag=vm,
        world_half_size=WORLD_HALF_SIZE_PC,
        max_level=ml,
    )


@click.group()
def cli() -> None:
    """Found-in-space octree pipeline."""


@cli.command("stage-00")
@click.argument(
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.argument("output_dir", type=click.Path(path_type=Path))
@click.option(
    "--force",
    is_flag=True,
    help="Recompute morton_code, render, and level even if already present.",
)
@click.option(
    "--v-mag",
    type=float,
    default=None,
    help=f"Indexing magnitude for level assignment (default: {DEFAULT_MAG_VIS}).",
)
@click.option(
    "--max-level",
    type=int,
    default=None,
    help=f"Max octree level (default: {DEFAULT_MAX_LEVEL}).",
)
@click.option(
    "--no-clear-output",
    is_flag=True,
    help="Do not delete OUTPUT_DIR before writing sorted bands.",
)
def stage_00(
    input_dir: Path,
    output_dir: Path,
    force: bool,
    v_mag: float | None,
    max_level: int | None,
    no_clear_output: bool,
) -> None:
    """Enrich all parquet under INPUT_DIR in-place, then sort into bands under OUTPUT_DIR."""
    mag_config = _resolve_mag_config(v_mag, max_level)

    click.echo(f"Stage 00 — add_shard_columns (in-place): {input_dir}")
    run_add_shard_columns(
        input_dir,
        mag_config=mag_config,
        force=force,
        verbose=True,
    )
    click.echo(f"Stage 00 — sort_shards: {input_dir} -> {output_dir}")
    run_sort_shards(
        input_dir,
        output_dir,
        mag_config=mag_config,
        clear_dst=not no_clear_output,
        verbose=True,
    )


@cli.command("stage-01")
@click.argument("input_glob", type=str)
@click.argument("out_dir", type=click.Path(path_type=Path))
@click.option(
    "--max-level",
    type=int,
    default=None,
    help=f"Max octree level (default: {DEFAULT_MAX_LEVEL}).",
)
@click.option(
    "--deep-shard-from-level",
    type=int,
    required=True,
    help="First level to use prefix sharding.",
)
@click.option(
    "--deep-prefix-bits",
    type=int,
    default=3,
    help="Prefix width for deep sharding (default: 3).",
)
@click.option(
    "--batch-size",
    type=int,
    default=100_000,
    help="Row batch size for streaming (default: 100000).",
)
@click.option(
    "--v-mag",
    type=float,
    default=None,
    help=f"Indexing magnitude (default: {DEFAULT_MAG_VIS}).",
)
def stage_01(
    input_glob: str,
    out_dir: Path,
    max_level: int | None,
    deep_shard_from_level: int,
    deep_prefix_bits: int,
    batch_size: int,
    v_mag: float | None,
) -> None:
    """Build intermediate shard files from Stage 00 parquet."""
    from foundinspace.octree.assembly import BuildPlan, build_intermediates

    ml = DEFAULT_MAX_LEVEL if max_level is None else max_level
    vm = DEFAULT_MAG_VIS if v_mag is None else v_mag

    plan = BuildPlan(
        max_level=ml,
        deep_shard_from_level=deep_shard_from_level,
        deep_prefix_bits=deep_prefix_bits,
        batch_size=batch_size,
        mag_limit=vm,
    )

    manifest_path = build_intermediates(input_glob, out_dir, plan=plan)
    click.echo(f"Manifest written to {manifest_path}")


@cli.command("stage-02")
@click.argument(
    "manifest_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.argument("output_path", type=click.Path(path_type=Path))
@click.option(
    "--max-open-files",
    type=int,
    default=32,
    show_default=True,
    help="Maximum number of payload file handles kept open.",
)
@click.option(
    "--retain-relocation-files",
    is_flag=True,
    help="Keep intermediate relocation files created during combine.",
)
def stage_02(
    manifest_path: Path,
    output_path: Path,
    max_open_files: int,
    retain_relocation_files: bool,
) -> None:
    """Combine intermediates into final stars.octree output."""
    from foundinspace.octree.combine import CombinePlan, combine_octree

    plan = CombinePlan(
        max_open_files=max_open_files,
        retain_relocation_files=retain_relocation_files,
    )
    combine_octree(manifest_path, output_path, plan=plan)
    click.echo(f"Wrote {output_path}")


def _parse_point(value: str) -> Point:
    try:
        parts = [p.strip() for p in value.split(",")]
        if len(parts) != 3:
            raise ValueError("expected exactly 3 comma-separated values")
        x, y, z = (float(parts[0]), float(parts[1]), float(parts[2]))
        return Point(x=x, y=y, z=z)
    except Exception as exc:
        raise click.BadParameter(
            f"Invalid --point '{value}', expected X,Y,Z"
        ) from exc


def _format_kb(value: int) -> str:
    return f"{value / 1024.0:,.0f} KB"


def _format_compact_mb(value: int) -> str:
    mb = value / (1024.0 * 1024.0)
    return f"{mb:.1f} MB"


def _format_teff(teff: float) -> str:
    if math.isnan(teff):
        return "n/a"
    return f"{teff:,.0f} K"


def _render_stats(console: Console, report: StatsReport, nearest_n: int) -> None:
    shell_table = Table(title="By level (shell set at Sun)")
    shell_table.add_column("Level", justify="right")
    shell_table.add_column("Nodes", justify="right")
    shell_table.add_column("Stars loaded", justify="right")
    shell_table.add_column("Stars rendered", justify="right")
    shell_table.add_column("Payload size", justify="right")

    for row in report.by_level:
        shell_table.add_row(
            f"{row.level}",
            f"{row.nodes:,}",
            f"{row.stars_loaded:,}",
            f"{row.stars_rendered:,}",
            _format_kb(row.payload_bytes),
        )
    shell_table.add_section()
    shell_table.add_row(
        "Total",
        f"{report.totals.nodes:,}",
        f"{report.totals.stars_loaded:,}",
        f"{report.totals.stars_rendered:,}",
        _format_kb(report.totals.payload_bytes),
    )
    console.print(shell_table)
    console.print(
        f"Coalesced {report.coalesced.output_batches:,} batches "
        f"from {report.coalesced.input_ranges:,}"
    )
    console.print(
        f"Total span bytes: {_format_compact_mb(report.coalesced.total_span_bytes)} "
        f"from {_format_compact_mb(report.coalesced.raw_payload_bytes)}"
    )
    console.print(
        f"Largest batch: {_format_compact_mb(report.coalesced.largest_batch_bytes)}"
    )

    nearest = Table(title=f"Nearest {nearest_n} stars")
    nearest.add_column("Star", justify="right")
    nearest.add_column("Distance", justify="right")
    nearest.add_column("Magnitude", justify="right")
    nearest.add_column("Apparent magnitude", justify="right")
    nearest.add_column("Teff", justify="right")
    for row in report.nearest:
        nearest.add_row(
            f"{row.star_id:,}",
            f"{row.distance_pc:.1f} pc",
            f"{row.magnitude:.1f}",
            f"{row.apparent_magnitude:.1f}",
            _format_teff(row.teff),
        )
    console.print(nearest)


@cli.command("stats")
@click.argument(
    "octree_path",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
)
@click.option(
    "--point",
    type=str,
    default="0,0,0",
    show_default=True,
    help="Query origin in parsecs as X,Y,Z.",
)
@click.option(
    "--magnitude",
    type=float,
    default=6.5,
    show_default=True,
    help="Limiting apparent magnitude for shell visibility query.",
)
@click.option(
    "--radius",
    type=float,
    default=10.0,
    show_default=True,
    help="Distance radius in parsecs for nearest query.",
)
@click.option(
    "--nearest",
    "-n",
    type=int,
    default=10,
    show_default=True,
    help="Number of nearest stars to print.",
)
def stats(
    octree_path: Path,
    point: str,
    magnitude: float,
    radius: float,
    nearest: int,
) -> None:
    """Read a stage-02 octree and print bounded query stats."""
    if not octree_path.exists():
        raise FileNotFoundError(
            f"Octree file not found: {octree_path}. Run stage-02 first."
        )
    if radius < 0:
        raise click.BadParameter("--radius must be >= 0")
    if nearest <= 0:
        raise click.BadParameter("--nearest must be > 0")

    query_point = _parse_point(point)
    report = collect_stats(
        octree_path,
        point=query_point,
        limiting_magnitude=magnitude,
        radius_pc=radius,
        nearest_n=nearest,
    )
    console = Console()
    console.print(
        (
            f"File: {octree_path} | center={report.header.world_center} "
            f"| half_size={report.header.world_half_size:.1f} pc "
            f"| max_level={report.header.max_level} "
            f"| mag_limit={report.header.mag_limit:.2f}"
        )
    )
    _render_stats(console, report, nearest)
