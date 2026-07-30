from __future__ import annotations

import json
import math
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from foundinspace.octree.config import MORTON_BITS
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.project import load_project, render_project_template
from foundinspace.octree.reader import Point
from foundinspace.octree.reader.source import OctreeSource, is_url_source
from foundinspace.octree.reader.stats import StatsReport, collect_stats


@click.group()
def cli() -> None:
    """Found-in-space octree pipeline."""


@cli.group("project")
def project_group() -> None:
    """Manage octree project files."""


@project_group.command("init")
@click.argument(
    "project_path",
    type=click.Path(path_type=Path),
)
def project_init(project_path: Path) -> None:
    """Write a starter project.toml for octree build commands."""
    project_path = project_path.expanduser()
    if project_path.exists():
        raise click.ClickException(f"Project file already exists: {project_path}")
    project_path.parent.mkdir(parents=True, exist_ok=True)
    project_path.write_text(
        render_project_template(),
        encoding="utf-8",
    )
    click.echo(f"Wrote project file to {project_path.resolve()}")


def _load_project_or_die(project_path: Path):
    try:
        return load_project(project_path)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command("stage-00")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--input-root",
    type=click.Path(path_type=Path),
    default=None,
    help="Input shard root. Defaults to paths.merged_healpix_dir.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Stage 00 packed staging output directory. Defaults to paths.stage00_output_dir.",
)
@click.option(
    "--shard",
    "--healpix",
    "shard_ids",
    multiple=True,
    help=(
        "Input shard directory or root-level parquet shard to process. "
        "May be passed multiple times. --healpix is a compatibility alias."
    ),
)
@click.option(
    "--max-pixels",
    type=int,
    default=None,
    help="Process at most this many input shard directories or files.",
)
@click.option(
    "--bucket-size",
    type=int,
    default=None,
    help=(
        "Rows a packed staging node may hold before it becomes lower-mag limited. "
        "Defaults to stage00.bucket_size."
    ),
)
@click.option(
    "--batch-size",
    type=int,
    default=None,
    help="Parquet batch size. Defaults to stage00.batch_size.",
)
@click.option(
    "--fragment-target-rows",
    type=int,
    default=None,
    help="Rows per physical Stage 00 parquet fragment. Defaults to stage00.fragment_target_rows.",
)
@click.option(
    "--max-open-writers",
    type=int,
    default=None,
    help="Maximum open Stage 00 parquet writers. Defaults to stage00.max_open_writers.",
)
@click.option(
    "--compact-after-files",
    type=int,
    default=None,
    help="Compact a node/input-shard/kind group after this many files. Defaults to stage00.compact_after_files.",
)
@click.option(
    "--input-filter",
    type=click.Choice(["none", "raw-cartesian-to-stage00-routing/v1"]),
    default=None,
    help="Explicit pre-filter before Stage 00 routing. Defaults to stage00.input_filter.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Replace an existing Stage 00 output directory.",
)
@click.option(
    "--replace-shards",
    is_flag=True,
    help="Replace existing Stage 00 fragments for the selected --shard values.",
)
def stage_00(
    project_path: Path,
    input_root: Path | None,
    output_dir: Path | None,
    shard_ids: tuple[str, ...],
    max_pixels: int | None,
    bucket_size: int | None,
    batch_size: int | None,
    fragment_target_rows: int | None,
    max_open_writers: int | None,
    compact_after_files: int | None,
    input_filter: str | None,
    force: bool,
    replace_shards: bool,
) -> None:
    """Pack input shards into adaptive Stage 00 staging buckets."""
    from foundinspace.octree.sources.stage00 import Stage00Config, run_stage00

    project = _load_project_or_die(project_path)
    mag_config = MagLevelConfig(
        v_mag=project.stage00.v_mag,
        morton_bits=MORTON_BITS,
    )
    resolved_input = (
        input_root if input_root is not None else project.paths.merged_healpix_dir
    )
    resolved_output = (
        output_dir if output_dir is not None else project.paths.stage00_output_dir
    )
    config = Stage00Config(
        input_root=resolved_input,
        output_dir=resolved_output,
        mag_config=mag_config,
        bucket_size=(
            bucket_size if bucket_size is not None else project.stage00.bucket_size
        ),
        batch_size=batch_size or project.stage00.batch_size,
        fragment_target_rows=(
            fragment_target_rows
            if fragment_target_rows is not None
            else project.stage00.fragment_target_rows
        ),
        max_open_writers=(
            max_open_writers
            if max_open_writers is not None
            else project.stage00.max_open_writers
        ),
        compact_after_files=(
            compact_after_files
            if compact_after_files is not None
            else project.stage00.compact_after_files
        ),
        input_filter=(
            input_filter if input_filter is not None else project.stage00.input_filter
        ),
        shard_ids=tuple(shard_ids),
        max_pixels=max_pixels,
        force=force,
        replace_shards=replace_shards,
    )
    click.echo(
        "Stage 00 — adaptive staging buckets: "
        f"{config.input_root} -> {config.output_dir}; "
        f"mode={'replace-shards' if config.replace_shards else 'full'}; "
        f"bucket_size={config.bucket_size:,}; "
        f"fragment_target_rows={config.fragment_target_rows:,}; "
        f"max_open_writers={config.max_open_writers:,}; "
        f"compact_after_files={config.compact_after_files:,}; "
        f"input_filter={config.input_filter}"
    )
    report_path = run_stage00(config)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    click.echo(
        "Stage 00 summary: "
        f"input_shards={len(report['processed_input_shards'])}, "
        f"rows={report['rows_in']:,}, "
        f"nodes={report['staging_nodes']:,}, "
        f"lower_mag_limited={report['lower_mag_limited_nodes']:,}, "
        f"fragments={report['current_fragment_files']:,}, "
        f"changed_groups={report['changed_group_count']:,}, "
        f"unchanged_groups={report['unchanged_group_count']:,}, "
        f"deleted_groups={report['deleted_group_count']:,}, "
        f"split_rewrites={report['split_rewrites']:,}, "
        f"compaction_rewrites={report['compaction_rewrites']:,}"
    )
    click.echo(f"Stage 00 report written to {report_path}")


@cli.command("stage-01")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Replace existing Stage 01 sorted output and rebuild all groups.",
)
def stage_01(
    project_path: Path,
    force: bool,
) -> None:
    """Sort and compact Stage 00 groups into deterministic Stage 01 groups."""
    from foundinspace.octree.sources.stage01 import Stage01Config, run_stage01

    project = _load_project_or_die(project_path)
    config = Stage01Config(
        stage00_output_dir=project.paths.stage00_output_dir,
        output_dir=project.paths.stage01_output_dir,
        v_mag=project.stage00.v_mag,
        bucket_size=project.stage00.bucket_size,
        input_filter=project.stage00.input_filter,
        batch_size=project.stage01.batch_size,
        fragment_target_rows=project.stage00.fragment_target_rows,
        force=force,
    )
    click.echo(
        "Stage 01 — sort staged groups: "
        f"{config.stage00_output_dir} -> {config.output_dir}; "
        f"fragment_target_rows={config.fragment_target_rows:,}; "
        f"batch_size={config.batch_size:,}; "
        f"force={config.force}"
    )
    report_path = run_stage01(config)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    click.echo(
        "Stage 01 summary: "
        f"processed_groups={report['processed_group_count']:,}, "
        f"changed_groups={report['changed_group_count']:,}, "
        f"unchanged_groups={report['unchanged_group_count']:,}, "
        f"deleted_groups={report['deleted_group_count']:,}, "
        f"in_memory_sorts={report['in_memory_sort_group_count']:,}, "
        f"external_sorts={report['external_sort_group_count']:,}, "
        f"dirty_stage03_nodes={report['dirty_stage03_node_count']:,}, "
        f"files_written={report['output_files_written']:,}"
    )
    click.echo(f"Stage 01 report written to {report_path}")


@cli.command("stage-02")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--retain-relocation-files",
    is_flag=True,
    help="Keep intermediate relocation files created during combine.",
)
@click.option(
    "--max-level",
    type=click.IntRange(min=0, max=MORTON_BITS),
    default=None,
    help="Classic output level cap. Defaults to stage02.classic_max_level.",
)
def stage_02(
    project_path: Path,
    retain_relocation_files: bool,
    max_level: int | None,
) -> None:
    """Build the classic stars.octree from sorted Stage 01 groups."""
    from foundinspace.octree.classic import (
        ClassicBuildConfig,
        build_classic_artifacts,
    )

    project = _load_project_or_die(project_path)
    resolved_max_level = (
        max_level if max_level is not None else project.stage02.classic_max_level
    )
    result = build_classic_artifacts(
        ClassicBuildConfig(
            stage00_output_dir=project.paths.stage00_output_dir,
            stage01_output_dir=project.paths.stage01_output_dir,
            output_path=project.paths.stage02_output_path,
            identifiers_order_path=project.paths.identifiers_order_output_path,
            mag_limit=project.stage00.v_mag,
            max_level=resolved_max_level,
            batch_size=project.stage01.batch_size,
            max_open_files=project.stage02.max_open_files,
            partition_from_level=project.stage02.partition_from_level,
            partition_prefix_bits=project.stage02.partition_prefix_bits,
            retain_relocation_files=retain_relocation_files,
        )
    )
    click.echo(
        "Stage 02 classic summary: "
        f"rows={result.row_count:,}, "
        f"folded_rows={result.folded_row_count:,}, "
        f"cells={result.cell_count:,}, "
        f"max_level={resolved_max_level}, "
        f"dataset_uuid={result.dataset_uuid}"
    )
    click.echo(f"Wrote {result.output_path}")
    click.echo(f"Wrote {result.identifiers_order_path}")


@cli.command("stage-03")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--family",
    "family_name",
    type=str,
    default=None,
    help="Optional single sidecar family to build.",
)
def stage_03(
    project_path: Path,
    family_name: str | None,
) -> None:
    """Build named sidecars from the render octree and identifiers/order artifact."""
    from foundinspace.octree.stage3 import build_stage03_sidecars

    project = _load_project_or_die(project_path)
    manifest_path = build_stage03_sidecars(project, family_name=family_name)
    click.echo(f"Stage 03 manifest written to {manifest_path}")


@cli.command("stage-03-benchmark")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--profile",
    "profiles",
    multiple=True,
    type=click.Choice(["classic", "unbounded"]),
    help="Output profile to simulate. May be passed multiple times.",
)
@click.option(
    "--order",
    "orders",
    multiple=True,
    type=click.Choice(["dfs", "level-major", "tile-level-major"]),
    help="Packing order to simulate. May be passed multiple times.",
)
@click.option(
    "--scenario",
    "scenarios",
    multiple=True,
    type=click.Choice(["observer-shell", "target-frustum"]),
    help="Query scenario to simulate. May be passed multiple times.",
)
@click.option(
    "--center",
    type=str,
    default="0,0,0",
    show_default=True,
    help="Query origin in parsecs as X,Y,Z.",
)
@click.option(
    "--magnitude",
    type=float,
    default=None,
    help="Limiting apparent magnitude. Defaults to stage00.v_mag.",
)
@click.option(
    "--target",
    type=str,
    default="1000,0,0",
    show_default=True,
    help="Target point for target-frustum in parsecs as X,Y,Z.",
)
@click.option(
    "--vertical-fov",
    type=float,
    default=40.0,
    show_default=True,
    help="Vertical FOV in degrees for target-frustum.",
)
@click.option(
    "--aspect-ratio",
    type=float,
    default=16.0 / 9.0,
    show_default=True,
    help="Aspect ratio for target-frustum.",
)
@click.option(
    "--tile-prefix-depth",
    type=int,
    default=4,
    show_default=True,
    help="Top-prefix depth for tile-level-major packing.",
)
@click.option(
    "--coalesce-gap-bytes",
    type=int,
    default=64 * 1024,
    show_default=True,
    help="Maximum gap to merge adjacent payload ranges.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Print machine-readable JSON instead of a table.",
)
def stage_03_benchmark(
    project_path: Path,
    profiles: tuple[str, ...],
    orders: tuple[str, ...],
    scenarios: tuple[str, ...],
    center: str,
    magnitude: float | None,
    target: str,
    vertical_fov: float,
    aspect_ratio: float,
    tile_prefix_depth: int,
    coalesce_gap_bytes: int,
    as_json: bool,
) -> None:
    """Estimate Stage 03 packing order range-read behavior from Stage 01."""
    from foundinspace.octree.stage03_benchmark import (
        PACKING_ORDERS,
        PROFILES,
        SCENARIOS,
        Point3,
        Stage03BenchmarkConfig,
        report_to_json,
        run_stage03_packing_benchmark,
    )

    project = _load_project_or_die(project_path)
    center_point = _parse_point(center)
    target_point = _parse_point(target)
    config = Stage03BenchmarkConfig(
        stage00_output_dir=project.paths.stage00_output_dir,
        stage01_output_dir=project.paths.stage01_output_dir,
        profiles=profiles or PROFILES,
        orders=orders or PACKING_ORDERS,
        scenarios=scenarios or SCENARIOS,
        center=Point3(center_point.x, center_point.y, center_point.z),
        target=Point3(target_point.x, target_point.y, target_point.z),
        limiting_magnitude=magnitude
        if magnitude is not None
        else project.stage00.v_mag,
        vertical_fov_deg=vertical_fov,
        aspect_ratio=aspect_ratio,
        tile_prefix_depth=tile_prefix_depth,
        coalesce_gap_bytes=coalesce_gap_bytes,
        batch_size=project.stage01.batch_size,
    )
    try:
        report = run_stage03_packing_benchmark(config)
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(report_to_json(report), nl=False)
        return

    console = Console()
    console.print(
        "Stage 03 packing benchmark: "
        f"{config.stage01_output_dir} | "
        f"profiles={','.join(config.profiles)} | "
        f"orders={','.join(config.orders)} | "
        f"scenarios={','.join(config.scenarios)}"
    )
    _render_stage03_benchmark(console, report)


def _parse_point(value: str) -> Point:
    try:
        parts = [p.strip() for p in value.split(",")]
        if len(parts) != 3:
            raise ValueError("expected exactly 3 comma-separated values")
        x, y, z = (float(parts[0]), float(parts[1]), float(parts[2]))
        return Point(x=x, y=y, z=z)
    except Exception as exc:
        raise click.BadParameter(f"Invalid --point '{value}', expected X,Y,Z") from exc


def _format_kb(value: int) -> str:
    return f"{value / 1024.0:,.0f} KB"


def _format_mag_spread(row: object) -> str:
    mag_min = getattr(row, "mag_min", float("nan"))
    mag_p25 = getattr(row, "mag_p25", float("nan"))
    mag_p50 = getattr(row, "mag_p50", float("nan"))
    mag_p75 = getattr(row, "mag_p75", float("nan"))
    mag_max = getattr(row, "mag_max", float("nan"))
    if any(math.isnan(v) for v in (mag_min, mag_p25, mag_p50, mag_p75, mag_max)):
        return "-"
    return f"{mag_min:.1f}/{mag_p25:.1f}/{mag_p50:.1f}/{mag_p75:.1f}/{mag_max:.1f}"


def _format_compact_mb(value: int) -> str:
    mb = value / (1024.0 * 1024.0)
    return f"{mb:.1f} MB"


def _format_teff(teff: float) -> str:
    if math.isnan(teff):
        return "n/a"
    return f"{teff:,.0f} K"


def _format_ratio(value: float) -> str:
    return f"{value:.3f}"


def _render_stage03_benchmark(console: Console, report: dict) -> None:
    table = Table(title="Stage 03 Packing Benchmark")
    table.add_column("Profile")
    table.add_column("Order")
    table.add_column("Scenario")
    table.add_column("Nodes", justify="right")
    table.add_column("Selected", justify="right")
    table.add_column("Stars", justify="right")
    table.add_column("Ranges", justify="right")
    table.add_column("Batches", justify="right")
    table.add_column("Raw", justify="right")
    table.add_column("Span", justify="right")
    table.add_column("Useful", justify="right")

    for row in report["results"]:
        table.add_row(
            str(row["profile"]),
            str(row["order"]),
            str(row["scenario"]),
            f"{row['final_node_count']:,}",
            f"{row['selected_node_count']:,}",
            f"{row['selected_star_count']:,}",
            f"{row['payload_range_count']:,}",
            f"{row['coalesced_batch_count']:,}",
            _format_compact_mb(int(row["raw_payload_bytes"])),
            _format_compact_mb(int(row["span_bytes"])),
            _format_ratio(float(row["useful_ratio"])),
        )
    console.print(table)


def _format_identifiers(identifiers: tuple[tuple[str, object], ...]) -> str:
    if not identifiers:
        return "-"
    by_key = dict(identifiers)
    parts: list[str] = []
    proper_name = by_key.get("proper_name")
    if isinstance(proper_name, str) and proper_name.strip():
        parts.append(proper_name.strip())
    for key in ("hip_id", "hd", "gaia_source_id"):
        value = by_key.get(key)
        if value is None:
            continue
        label = "HIP" if key == "hip_id" else ("HD" if key == "hd" else "Gaia")
        parts.append(f"{label} {value}")
    bayer = by_key.get("bayer")
    constellation = by_key.get("constellation")
    flamsteed = by_key.get("flamsteed")
    if bayer is not None:
        parts.append(str(bayer))
    elif flamsteed is not None and constellation is not None:
        parts.append(f"{flamsteed} {constellation}")
    elif flamsteed is not None:
        parts.append(str(flamsteed))
    elif constellation is not None:
        parts.append(str(constellation))

    if parts:
        return " | ".join(parts)
    source = by_key.get("source")
    source_id = by_key.get("source_id")
    if source is not None and source_id is not None:
        source_s = str(source).strip().lower()
        source_id_s = str(source_id).strip()
        if source_s == "gaia":
            return f"Gaia {source_id_s}"
        if source_s == "hip":
            return f"HIP {source_id_s}"
        return f"{source_s}:{source_id_s}"
    return ", ".join(f"{k}={v}" for k, v in identifiers)


def _resolve_octree_source(source: str) -> OctreeSource:
    normalized = source.strip()
    if not normalized:
        raise click.BadParameter("octree source must not be empty")
    if is_url_source(normalized):
        return normalized

    octree_path = Path(normalized).expanduser()
    if not octree_path.exists():
        raise FileNotFoundError(
            f"Octree file not found: {octree_path}. Run stage-02 first."
        )
    return octree_path


def _resolve_meta_octree_source(
    octree_source: OctreeSource,
    meta_octree_opt: str | None,
) -> OctreeSource | None:
    if meta_octree_opt is not None:
        meta_source = _resolve_octree_source(meta_octree_opt)
        if isinstance(meta_source, Path) and not meta_source.is_file():
            raise click.ClickException(f"Metadata octree not found: {meta_source}")
        return meta_source

    return None


def _format_source_label(source: OctreeSource) -> str:
    return str(source)


def _resolve_source_or_none(source: str | None) -> str | None:
    if source is None:
        return None
    normalized = source.strip()
    return normalized or None


def _resolve_meta_option_value(source: str | None) -> str | None:
    normalized = _resolve_source_or_none(source)
    return normalized if normalized is not None else None


def _resolve_stats_sources(
    octree_source_arg: str,
    meta_octree_arg: str | None,
) -> tuple[OctreeSource, OctreeSource | None]:
    octree_source = _resolve_octree_source(octree_source_arg)
    meta_source = _resolve_meta_octree_source(
        octree_source,
        _resolve_meta_option_value(meta_octree_arg),
    )
    return octree_source, meta_source


def _render_stats(console: Console, report: StatsReport, nearest_n: int) -> None:
    shell_table = Table(title="By level (shell set at Sun)")
    shell_table.add_column("Level", justify="right")
    shell_table.add_column("Nodes", justify="right")
    shell_table.add_column("Stars loaded", justify="right")
    shell_table.add_column("Stars rendered", justify="right")
    shell_table.add_column("Mag abs min/p25/p50/p75/max", justify="right")
    shell_table.add_column("Payload size", justify="right")

    for row in report.by_level:
        shell_table.add_row(
            f"{row.level}",
            f"{row.nodes:,}",
            f"{row.stars_loaded:,}",
            f"{row.stars_rendered:,}",
            _format_mag_spread(row),
            _format_kb(row.payload_bytes),
        )
    shell_table.add_section()
    shell_table.add_row(
        "Total",
        f"{report.totals.nodes:,}",
        f"{report.totals.stars_loaded:,}",
        f"{report.totals.stars_rendered:,}",
        _format_mag_spread(report.totals),
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
    nearest.add_column("Identifiers")
    for row in report.nearest:
        nearest.add_row(
            f"{row.star_id:,}",
            f"{row.distance_pc:.1f} pc",
            f"{row.magnitude:.1f}",
            f"{row.apparent_magnitude:.1f}",
            _format_teff(row.teff),
            _format_identifiers(row.identifiers),
        )
    console.print(nearest)


@cli.command("stats")
@click.argument(
    "octree_source",
    type=str,
)
@click.option(
    "--center",
    "--centre",
    "--point",
    "point",
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
    "--stars",
    "-n",
    type=int,
    default=10,
    show_default=True,
    help="Number of nearest stars to print.",
)
@click.option(
    "--meta-octree",
    type=str,
    default=None,
    help="Optional metadata octree path or URL for the Stage 03 `meta` sidecar.",
)
def stats(
    octree_source: str,
    point: str,
    magnitude: float,
    radius: float,
    nearest: int,
    meta_octree: str | None,
) -> None:
    """Read a stage-02 octree and print bounded query stats."""
    if radius < 0:
        raise click.BadParameter("--radius must be >= 0")
    if nearest <= 0:
        raise click.BadParameter("--nearest must be > 0")

    query_point = _parse_point(point)
    resolved_octree_source, meta_octree_source = _resolve_stats_sources(
        octree_source,
        meta_octree,
    )
    report = collect_stats(
        resolved_octree_source,
        point=query_point,
        limiting_magnitude=magnitude,
        radius_pc=radius,
        metadata_path=meta_octree_source,
        nearest_n=nearest,
    )
    console = Console()
    header_line = (
        f"File: {_format_source_label(resolved_octree_source)} "
        f"| query_point=({query_point.x:.1f}, {query_point.y:.1f}, {query_point.z:.1f}) "
        f"| world_center={report.header.world_center} "
        f"| half_size={report.header.world_half_size:.1f} pc "
        f"| max_level={report.header.max_level} "
        f"| mag_limit={report.header.mag_limit:.2f}"
    )
    if report.header.dataset_uuid is not None:
        header_line += f" | octree_uuid={report.header.dataset_uuid}"
    if report.header.parent_dataset_uuid is not None:
        header_line += f" | parent_uuid={report.header.parent_dataset_uuid}"
    if report.header.sidecar_uuid is not None:
        header_line += f" | sidecar_uuid={report.header.sidecar_uuid}"
    console.print(header_line)
    _render_stats(console, report, nearest)
