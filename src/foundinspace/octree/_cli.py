from __future__ import annotations

import json
import math
from pathlib import Path
from uuid import uuid4

import click
from rich.console import Console
from rich.table import Table

from foundinspace.octree.assembly.formats import (
    IDENTIFIERS_MANIFEST_NAME,
    RENDER_MANIFEST_NAME,
)
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
    help="Input HEALPix root. Defaults to paths.merged_healpix_dir.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Stage 00 packed staging output directory. Defaults to paths.stage00_output_dir.",
)
@click.option(
    "--healpix",
    "healpix_ids",
    multiple=True,
    help=(
        "HEALPix pixel directory or root-level parquet shard to process. "
        "May be passed multiple times."
    ),
)
@click.option(
    "--max-pixels",
    type=int,
    default=None,
    help="Process at most this many HEALPix directories.",
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
    help="Compact a node/healpix/kind group after this many files. Defaults to stage00.compact_after_files.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Replace an existing Stage 00 output directory.",
)
def stage_00(
    project_path: Path,
    input_root: Path | None,
    output_dir: Path | None,
    healpix_ids: tuple[str, ...],
    max_pixels: int | None,
    bucket_size: int | None,
    batch_size: int | None,
    fragment_target_rows: int | None,
    max_open_writers: int | None,
    compact_after_files: int | None,
    force: bool,
) -> None:
    """Pack HEALPix rows into adaptive Stage 00 staging buckets."""
    from foundinspace.octree.sources.stage00 import Stage00Config, run_stage00

    project = _load_project_or_die(project_path)
    mag_config = MagLevelConfig(
        v_mag=project.stage00.v_mag,
        max_level=project.stage00.max_level,
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
        max_level=project.stage00.max_level,
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
        healpix_ids=tuple(healpix_ids),
        max_pixels=max_pixels,
        force=force,
    )
    click.echo(
        "Stage 00 — adaptive staging buckets: "
        f"{config.input_root} -> {config.output_dir}; "
        f"bucket_size={config.bucket_size:,}; "
        f"fragment_target_rows={config.fragment_target_rows:,}; "
        f"max_open_writers={config.max_open_writers:,}; "
        f"compact_after_files={config.compact_after_files:,}"
    )
    report_path = run_stage00(config)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    click.echo(
        "Stage 00 summary: "
        f"healpix={len(report['processed_healpix'])}, "
        f"rows={report['rows_in']:,}, "
        f"nodes={report['staging_nodes']:,}, "
        f"lower_mag_limited={report['lower_mag_limited_nodes']:,}, "
        f"fragments={report['current_fragment_files']:,}, "
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
def stage_01(
    project_path: Path,
) -> None:
    """Build intermediate shard files from project-configured Stage 00 parquet."""
    from foundinspace.octree.assembly import BuildPlan, build_intermediates

    project = _load_project_or_die(project_path)

    plan = BuildPlan(
        max_level=project.stage00.max_level,
        deep_shard_from_level=project.stage01.deep_shard_from_level,
        deep_prefix_bits=project.stage01.deep_prefix_bits,
        batch_size=project.stage01.batch_size,
        mag_limit=project.stage00.v_mag,
    )

    manifest_path = build_intermediates(
        project.stage01.input_glob,
        project.paths.stage01_output_dir,
        plan=plan,
    )
    click.echo(f"Render manifest written to {manifest_path}")
    click.echo(
        "Identifiers manifest written to "
        f"{project.paths.stage01_output_dir / IDENTIFIERS_MANIFEST_NAME}"
    )


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
def stage_02(
    project_path: Path,
    retain_relocation_files: bool,
) -> None:
    """Combine intermediates into final stars.octree output using project config."""
    from foundinspace.octree.combine import CombinePlan, combine_octree
    from foundinspace.octree.combine.records import PackedDescriptorFields
    from foundinspace.octree.identifiers_order import combine_identifiers_order

    project = _load_project_or_die(project_path)
    render_manifest_path = project.paths.stage01_output_dir / RENDER_MANIFEST_NAME
    identifiers_manifest_path = (
        project.paths.stage01_output_dir / IDENTIFIERS_MANIFEST_NAME
    )
    output_path = project.paths.stage02_output_path
    identifiers_order_path = project.paths.identifiers_order_output_path
    dataset_uuid = uuid4()
    plan = CombinePlan(
        max_open_files=project.stage02.max_open_files,
        retain_relocation_files=retain_relocation_files,
    )
    combine_octree(
        render_manifest_path,
        output_path,
        plan=plan,
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=dataset_uuid,
        ),
    )
    click.echo(f"Wrote {output_path}")
    combine_identifiers_order(
        identifiers_manifest_path,
        identifiers_order_path,
        parent_dataset_uuid=dataset_uuid,
        artifact_uuid=uuid4(),
    )
    click.echo(f"Wrote {identifiers_order_path}")


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
