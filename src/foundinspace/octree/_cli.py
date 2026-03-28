from __future__ import annotations

import math
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from foundinspace.octree.config import (
    DEFAULT_DEEP_SHARD_FROM_LEVEL,
    DEFAULT_MAG_VIS,
    DEFAULT_MAX_LEVEL,
    LEVEL_CONFIG,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.mag_levels import MagLevelConfig
from foundinspace.octree.paths import (
    IDENTIFIERS_MAP_PATH,
    MERGED_HEALPIX_DIR,
    STAGE00_OUTPUT_DIR,
    STAGE00_PARQUET_GLOB,
    STAGE01_DIR,
)
from foundinspace.octree.reader import Point
from foundinspace.octree.reader.stats import StatsReport, collect_stats
from foundinspace.octree.sources.add_shard_columns import run_enrich_healpix


def _default_stage02_manifest(*_args: object) -> Path:
    import foundinspace.octree.paths as _paths

    return _paths.STAGE01_MANIFEST_PATH


def _default_stage02_output(*_args: object) -> Path:
    import foundinspace.octree.paths as _paths

    return _paths.STAGE02_OUTPUT


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
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=MERGED_HEALPIX_DIR,
)
@click.argument(
    "output_dir",
    required=False,
    type=click.Path(path_type=Path),
    default=STAGE00_OUTPUT_DIR,
)
@click.option(
    "--force",
    is_flag=True,
    help="Recompute output for pixel directories that already exist.",
)
@click.option(
    "--batch-size",
    type=int,
    default=1_000_000,
    show_default=True,
    help="Rows per streaming enrichment batch.",
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
def stage_00(
    input_dir: Path,
    output_dir: Path,
    force: bool,
    batch_size: int,
    v_mag: float | None,
    max_level: int | None,
) -> None:
    """Stream-enrich HEALPix parquet into Stage 00 outputs without mutating input."""
    mag_config = _resolve_mag_config(v_mag, max_level)

    click.echo(f"Stage 00 — per-pixel enrichment: {input_dir} -> {output_dir}")
    processed, skipped = run_enrich_healpix(
        src_root=input_dir,
        output_root=output_dir,
        mag_config=mag_config,
        force=force,
        batch_size=batch_size,
        verbose=True,
    )
    click.echo(
        f"Stage 00 summary: processed_pixels={processed}, skipped_pixels={skipped}"
    )


@cli.command("stage-01")
@click.argument(
    "input_glob",
    required=False,
    default=STAGE00_PARQUET_GLOB,
)
@click.argument(
    "out_dir",
    required=False,
    type=click.Path(path_type=Path),
    default=STAGE01_DIR,
)
@click.option(
    "--max-level",
    type=int,
    default=None,
    help=f"Max octree level (default: {DEFAULT_MAX_LEVEL}).",
)
@click.option(
    "--deep-shard-from-level",
    type=int,
    default=DEFAULT_DEEP_SHARD_FROM_LEVEL,
    show_default=True,
    help=(
        "First level to use prefix sharding "
        f"(default: {DEFAULT_DEEP_SHARD_FROM_LEVEL}, above typical max_level so one shard per level)."
    ),
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
@click.option(
    "--identifiers-map",
    "identifiers_map_opt",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help=(
        "Path to identifiers_map.parquet for metadata sidecar. "
        f"If omitted, use {IDENTIFIERS_MAP_PATH} when that file exists."
    ),
)
@click.option(
    "--sidecar-fields",
    type=str,
    default=None,
    help=(
        "Comma-separated subset of sidecar identifier fields "
        "(default: all). See docs/sidecars.md entry schema."
    ),
)
def stage_01(
    input_glob: str,
    out_dir: Path,
    max_level: int | None,
    deep_shard_from_level: int,
    deep_prefix_bits: int,
    batch_size: int,
    v_mag: float | None,
    identifiers_map_opt: Path | None,
    sidecar_fields: str | None,
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

    if identifiers_map_opt is not None:
        map_path = identifiers_map_opt.expanduser()
        if not map_path.is_file():
            raise click.ClickException(f"Identifiers map not found: {map_path}")
    elif IDENTIFIERS_MAP_PATH.is_file():
        map_path = IDENTIFIERS_MAP_PATH
    else:
        map_path = None

    fields_list: list[str] | None = None
    if sidecar_fields and sidecar_fields.strip():
        fields_list = [p.strip() for p in sidecar_fields.split(",") if p.strip()]

    manifest_path = build_intermediates(
        input_glob,
        out_dir,
        plan=plan,
        identifiers_map_path=map_path,
        sidecar_fields=fields_list,
    )
    click.echo(f"Manifest written to {manifest_path}")


@cli.command("stage-02")
@click.argument(
    "manifest_path",
    required=False,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=_default_stage02_manifest,
)
@click.argument(
    "output_path",
    required=False,
    type=click.Path(path_type=Path),
    default=_default_stage02_output,
)
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
@click.option(
    "--meta/--no-meta",
    "meta_flag",
    default=None,
    help=(
        "Combine the current 'meta' sidecar family into a .meta.octree file. "
        "Default: auto-detect from manifest."
    ),
)
@click.option(
    "--meta-output",
    type=click.Path(path_type=Path),
    default=None,
    help="Output path for metadata octree (default: derived from OUTPUT_PATH).",
)
def stage_02(
    manifest_path: Path,
    output_path: Path,
    max_open_files: int,
    retain_relocation_files: bool,
    meta_flag: bool | None,
    meta_output: Path | None,
) -> None:
    """Combine intermediates into final stars.octree output.

    With no positional arguments, defaults match Stage 01 output and a repo-standard
    final path (see ``foundinspace.octree.paths`` and ``FIS_OCTREE_DIR``):

    - Manifest: ``<octree>/stage01/manifest.json`` (default ``data/octree/...``)
    - Output: ``<octree>/stars.octree``

    When the manifest includes the current ``meta`` sidecar family, also writes ``<stem>.meta.octree``
    next to the render output unless ``--no-meta`` is set.
    """
    from foundinspace.octree.combine import CombinePlan, combine_octree
    from foundinspace.octree.combine.manifest import manifest_has_meta

    plan = CombinePlan(
        max_open_files=max_open_files,
        retain_relocation_files=retain_relocation_files,
    )
    combine_octree(manifest_path, output_path, plan=plan)
    click.echo(f"Wrote {output_path}")

    do_meta = meta_flag
    if do_meta is None:
        do_meta = manifest_has_meta(manifest_path)

    if do_meta:
        meta_out = meta_output
        if meta_out is None:
            meta_out = output_path.parent / f"{output_path.stem}.meta{output_path.suffix}"
        combine_octree(
            manifest_path, meta_out, plan=plan, payload_kind="meta"
        )
        click.echo(f"Wrote {meta_out}")
    elif meta_flag is None:
        click.echo("No metadata sidecar in manifest; skipping meta combine.")


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
    by_key = {k: v for k, v in identifiers}
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
        if constellation is not None:
            parts.append(f"{bayer} {constellation}")
        else:
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


def _resolve_meta_octree_path(
    octree_path: Path,
    meta_octree_opt: Path | None,
) -> Path | None:
    if meta_octree_opt is not None:
        meta_path = meta_octree_opt.expanduser()
        if not meta_path.is_file():
            raise click.ClickException(f"Metadata octree not found: {meta_path}")
        return meta_path
    inferred = octree_path.parent / f"{octree_path.stem}.meta{octree_path.suffix}"
    if inferred.is_file():
        return inferred
    return None


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
    "--stars",
    "-n",
    type=int,
    default=10,
    show_default=True,
    help="Number of nearest stars to print.",
)
@click.option(
    "--meta-octree",
    type=click.Path(path_type=Path, dir_okay=False),
    default=None,
    help=(
        "Optional metadata octree path for nearest-star identifiers "
        "(default: infer <octree_stem>.meta<suffix> when present)."
    ),
)
def stats(
    octree_path: Path,
    point: str,
    magnitude: float,
    radius: float,
    nearest: int,
    meta_octree: Path | None,
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
    meta_octree_path = _resolve_meta_octree_path(octree_path, meta_octree)
    report = collect_stats(
        octree_path,
        point=query_point,
        limiting_magnitude=magnitude,
        radius_pc=radius,
        metadata_path=meta_octree_path,
        nearest_n=nearest,
    )
    console = Console()
    console.print(
        f"File: {octree_path} | center={report.header.world_center} "
        f"| half_size={report.header.world_half_size:.1f} pc "
        f"| max_level={report.header.max_level} "
        f"| mag_limit={report.header.mag_limit:.2f}"
    )
    _render_stats(console, report, nearest)
