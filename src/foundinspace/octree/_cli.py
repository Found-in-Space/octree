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
from foundinspace.octree.visibility import validate_load_factor


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


@cli.group("identity-locator")
def identity_locator_group() -> None:
    """Build and query the optional exact Gaia/HIP identity index."""


def _identity_locator_paths(
    render_path: Path,
    output_path: Path | None,
    report_path: Path | None,
    work_dir: Path | None,
) -> tuple[Path, Path, Path]:
    output = output_path or render_path.with_name(
        f"{render_path.stem}.identity-locator.idx"
    )
    report = report_path or output.with_name(f"{output.stem}.report.json")
    work = work_dir or output.with_name(f".{output.stem}.work")
    return output, report, work


def _identity_locator_progress_reporter():
    last_value: tuple[str, int, int] | None = None

    def report(progress) -> None:
        nonlocal last_value
        value = (progress.phase, progress.completed, progress.total)
        if value == last_value:
            return
        last_value = value
        total = f"/{progress.total:,}" if progress.total else ""
        detail = f" ({progress.detail})" if progress.detail else ""
        click.echo(
            f"Identity locator {progress.phase}: {progress.completed:,}{total}{detail}"
        )

    return report


@identity_locator_group.command("build")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Project supplying the render octree and identifiers/order artifact.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output index path. Defaults to <render-stem>.identity-locator.idx.",
)
@click.option(
    "--report",
    "report_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Build report path. Defaults beside the index.",
)
@click.option(
    "--work-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Restartable scratch directory. Defaults beside the index.",
)
@click.option(
    "--page-size",
    type=click.Choice(("16384", "32768")),
    default="32768",
    show_default=True,
    help="Logical leaf record budget and navigation page size in bytes.",
)
@click.option(
    "--scan-batch-mib",
    type=click.FloatRange(min=0.001),
    default=32.0,
    show_default=True,
    help="Maximum decoded identity payload buffered per scan batch.",
)
@click.option(
    "--merge-fan-in",
    type=click.IntRange(min=2),
    default=32,
    show_default=True,
)
@click.option(
    "--merge-batch-rows",
    type=click.IntRange(min=1),
    default=262_144,
    show_default=True,
)
@click.option(
    "--external-sort-memory-limit",
    default="2GB",
    show_default=True,
)
@click.option("--retain-work", is_flag=True, help="Keep restart/checkpoint data.")
@click.option(
    "--force",
    is_flag=True,
    help="Replace incompatible output, report, and work state.",
)
def identity_locator_build(
    project_path: Path,
    output_path: Path | None,
    report_path: Path | None,
    work_dir: Path | None,
    page_size: str,
    scan_batch_mib: float,
    merge_fan_in: int,
    merge_batch_rows: int,
    external_sort_memory_limit: str,
    retain_work: bool,
    force: bool,
) -> None:
    """Build or resume an exact dataset-scoped identity locator."""
    from foundinspace.octree.identity_locator import (
        IdentityLocatorBuildConfig,
        build_identity_locator,
    )

    project = _load_project_or_die(project_path)
    output, report, work = _identity_locator_paths(
        project.paths.render_output_path,
        output_path,
        report_path,
        work_dir,
    )
    try:
        result = build_identity_locator(
            IdentityLocatorBuildConfig(
                render_octree_path=project.paths.render_output_path,
                identifiers_order_path=project.paths.identifiers_order_output_path,
                output_path=output,
                report_path=report,
                work_dir=work,
                decoded_page_size=int(page_size),
                scan_batch_bytes=max(1, round(scan_batch_mib * 1024 * 1024)),
                merge_fan_in=merge_fan_in,
                merge_batch_rows=merge_batch_rows,
                external_sort_memory_limit=external_sort_memory_limit,
                retain_work=retain_work,
                force=force,
                progress=_identity_locator_progress_reporter(),
            )
        )
    except (FileExistsError, FileNotFoundError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    counts = ", ".join(
        f"{name}={count:,}" for name, count in result.namespace_counts.items()
    )
    click.echo(
        f"Identity locator: {counts}, uuid={result.locator_uuid}, "
        f"sha256={result.output_sha256}"
    )
    click.echo(f"Wrote {result.output_path}")
    click.echo(f"Wrote {result.report_path}")


@identity_locator_group.command("benchmark")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Project supplying the render octree and identifiers/order artifact.",
)
@click.option(
    "--report",
    "report_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Benchmark report path. Defaults beside the render octree.",
)
@click.option(
    "--work-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Shared restartable scan/sort and candidate directory.",
)
@click.option(
    "--scan-batch-mib",
    type=click.FloatRange(min=0.001),
    default=32.0,
    show_default=True,
)
@click.option(
    "--merge-fan-in",
    type=click.IntRange(min=2),
    default=32,
    show_default=True,
)
@click.option(
    "--merge-batch-rows",
    type=click.IntRange(min=1),
    default=262_144,
    show_default=True,
)
@click.option(
    "--external-sort-memory-limit",
    default="2GB",
    show_default=True,
)
@click.option(
    "--repetitions",
    type=click.IntRange(min=1),
    default=3,
    show_default=True,
)
@click.option("--retain-candidates", is_flag=True)
@click.option(
    "--force",
    is_flag=True,
    help="Replace existing benchmark work and report.",
)
def identity_locator_benchmark(
    project_path: Path,
    report_path: Path | None,
    work_dir: Path | None,
    scan_batch_mib: float,
    merge_fan_in: int,
    merge_batch_rows: int,
    external_sort_memory_limit: str,
    repetitions: int,
    retain_candidates: bool,
    force: bool,
) -> None:
    """Benchmark compact leaves at the supported logical page capacities."""
    from foundinspace.octree.identity_locator import (
        IdentityLocatorBenchmarkConfig,
        benchmark_identity_locator,
    )

    project = _load_project_or_die(project_path)
    render_path = project.paths.render_output_path
    report = report_path or render_path.with_name(
        f"{render_path.stem}.identity-locator.benchmark.json"
    )
    work = work_dir or render_path.with_name(
        f".{render_path.stem}.identity-locator-benchmark.work"
    )
    try:
        result = benchmark_identity_locator(
            IdentityLocatorBenchmarkConfig(
                render_octree_path=render_path,
                identifiers_order_path=project.paths.identifiers_order_output_path,
                work_dir=work,
                report_path=report,
                scan_batch_bytes=max(1, round(scan_batch_mib * 1024 * 1024)),
                merge_fan_in=merge_fan_in,
                merge_batch_rows=merge_batch_rows,
                external_sort_memory_limit=external_sort_memory_limit,
                repetitions=repetitions,
                force=force,
                retain_candidates=retain_candidates,
                progress=_identity_locator_progress_reporter(),
            )
        )
    except (FileExistsError, FileNotFoundError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        "Identity locator benchmark winner: "
        f"page_size={result.winning_page_size}, "
        f"leaf_codec={result.winning_leaf_codec}"
    )
    click.echo(f"Winner candidate {result.winner_candidate_path}")
    click.echo(f"Wrote {result.report_path}")


@identity_locator_group.command("lookup")
@click.argument("locator_source", type=str)
@click.argument("identifiers_order_source", type=str)
@click.argument("source", type=str)
@click.argument("source_id", type=str)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def identity_locator_lookup(
    locator_source: str,
    identifiers_order_source: str,
    source: str,
    source_id: str,
    as_json: bool,
) -> None:
    """Resolve SOURCE and SOURCE_ID to a dataset-scoped render location."""
    from foundinspace.octree.identity_locator import IdentityLocatorReader

    try:
        with IdentityLocatorReader(locator_source, identifiers_order_source) as reader:
            ref = reader.lookup(source, source_id)
    except (FileNotFoundError, OSError, TypeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(
            json.dumps(
                None
                if ref is None
                else {
                    "dataset_uuid": str(ref.dataset_uuid),
                    "level": ref.level,
                    "morton_code": ref.morton_code,
                    "ordinal": ref.ordinal,
                },
                sort_keys=True,
            )
        )
    elif ref is None:
        click.echo("Not found")
    else:
        click.echo(
            f"dataset_uuid={ref.dataset_uuid} level={ref.level} "
            f"morton_code={ref.morton_code} ordinal={ref.ordinal}"
        )


@identity_locator_group.command("validate")
@click.argument("locator_source", type=str)
@click.argument("identifiers_order_source", type=str)
@click.option(
    "--report",
    "report_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Build report supplying deterministic present-key samples.",
)
@click.option(
    "--skip-full-checksum",
    is_flag=True,
    help="Skip the complete locator prefix SHA-256 pass.",
)
def identity_locator_validate(
    locator_source: str,
    identifiers_order_source: str,
    report_path: Path | None,
    skip_full_checksum: bool,
) -> None:
    """Validate structure, compatibility, checksums, and sampled round trips."""
    from foundinspace.octree.identity_locator import validate_identity_locator

    try:
        samples = None
        if report_path is not None:
            report = json.loads(report_path.read_text(encoding="utf-8"))
            samples = {
                str(namespace): [int(value) for value in values]
                for namespace, values in report.get("validation_samples", {}).items()
            }
        validation = validate_identity_locator(
            locator_source,
            identifiers_order_source,
            samples=samples,
            full_checksum=not skip_full_checksum,
        )
    except (FileNotFoundError, OSError, TypeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(validation, indent=2, sort_keys=True))


@cli.group("sidecars")
def sidecars_group() -> None:
    """Build optional enrichment artifacts for a published render octree."""


@cli.group("benchmark")
def benchmark_group() -> None:
    """Evaluate packing and runtime design choices."""


@sidecars_group.command("visual-duplicates")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Project supplying the render octree and identifiers/order paths.",
)
@click.option(
    "--evidence",
    "evidence_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="One-to-one visual-duplicate evidence Parquet.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Output sidecar path. Defaults beside the render octree as "
        "<render>.visual-duplicates.octree."
    ),
)
@click.option(
    "--report",
    "report_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Coverage report path. Defaults beside the sidecar.",
)
@click.option(
    "--work-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Sparse intermediate directory. Defaults beside the sidecar.",
)
@click.option(
    "--max-pairs",
    type=click.IntRange(min=1),
    default=1_000_000,
    show_default=True,
    help="Explicit memory bound for evidence pairs.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Replace an existing visual-duplicates sidecar, report, and work data.",
)
def visual_duplicates_sidecar(
    project_path: Path,
    evidence_path: Path,
    output_path: Path | None,
    report_path: Path | None,
    work_dir: Path | None,
    max_pairs: int,
    force: bool,
) -> None:
    """Build the optional sparse Gaia-Hipparcos review sidecar."""
    from foundinspace.octree.sidecars.visual_duplicates import (
        VisualDuplicatesBuildConfig,
        VisualDuplicatesScanProgress,
        build_visual_duplicates_sidecar,
    )

    project = _load_project_or_die(project_path)
    render_path = project.paths.render_output_path
    resolved_output = output_path or (
        project.paths.sidecars_output_dir / "visual-duplicates.octree"
    )
    resolved_report = report_path or resolved_output.with_name(
        f"{resolved_output.stem}.report.json"
    )
    resolved_work_dir = work_dir or (
        project.paths.sidecars_work_dir / "visual-duplicates"
    )

    last_reported_cells = -1

    def report_progress(progress: VisualDuplicatesScanProgress) -> None:
        nonlocal last_reported_cells
        if progress.scanned_cells == last_reported_cells:
            return
        last_reported_cells = progress.scanned_cells
        click.echo(
            "Visual-duplicates scan: "
            f"cells={progress.scanned_cells:,}/{progress.total_cells:,}, "
            f"stars={progress.scanned_stars:,}, "
            f"endpoints={progress.found_endpoints:,}/{progress.expected_endpoints:,}"
        )

    try:
        result = build_visual_duplicates_sidecar(
            VisualDuplicatesBuildConfig(
                render_octree_path=render_path,
                identifiers_order_path=project.paths.identifiers_order_output_path,
                evidence_path=evidence_path,
                output_path=resolved_output,
                work_dir=resolved_work_dir,
                report_path=resolved_report,
                deep_shard_from_level=project.sidecars.shard_from_level,
                deep_prefix_bits=project.sidecars.shard_prefix_bits,
                max_open_files=project.execution.max_open_files,
                max_evidence_pairs=max_pairs,
                force=force,
                progress=report_progress,
            )
        )
    except (FileExistsError, FileNotFoundError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(
        "Visual-duplicates sidecar: "
        f"pairs={result.evidence_pair_count:,}, "
        f"rendered_endpoints={result.rendered_endpoint_count:,}, "
        f"payload_cells={result.payload_cell_count:,}, "
        f"sidecar_uuid={result.sidecar_uuid}"
    )
    click.echo(f"Wrote {result.output_path}")
    click.echo(f"Wrote {result.report_path}")


@cli.command("route")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--shard",
    "shard_ids",
    multiple=True,
    help="Input shard directory or root-level parquet shard. May be repeated.",
)
@click.option(
    "--max-shards",
    type=int,
    default=None,
    help="Process at most this many input shard directories or files.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Replace the configured routed-contribution directory.",
)
@click.option(
    "--replace-shards",
    is_flag=True,
    help="Replace existing routed fragments for the selected --shard values.",
)
def route(
    project_path: Path,
    shard_ids: tuple[str, ...],
    max_shards: int | None,
    force: bool,
    replace_shards: bool,
) -> None:
    """Route input shards into adaptive contribution buckets."""
    from foundinspace.octree.sources.routing import (
        RoutingConfig,
        route_contributions,
    )

    project = _load_project_or_die(project_path)
    mag_config = MagLevelConfig(
        v_mag=project.dataset.limiting_magnitude,
        morton_bits=MORTON_BITS,
    )
    config = RoutingConfig(
        input_shards_dir=project.paths.input_shards_dir,
        routed_dir=project.paths.routed_dir,
        mag_config=mag_config,
        bucket_rows=project.routing.bucket_rows,
        scan_batch_rows=project.routing.scan_batch_rows,
        fragment_target_rows=project.routing.fragment_target_rows,
        max_open_writers=project.routing.max_open_writers,
        compact_after_files=project.routing.compact_after_files,
        input_mode=project.routing.input_mode,
        shard_ids=tuple(shard_ids),
        max_shards=max_shards,
        force=force,
        replace_shards=replace_shards,
    )
    click.echo(
        "Routing contributions: "
        f"{config.input_shards_dir} -> {config.routed_dir}; "
        f"mode={'replace-shards' if config.replace_shards else 'full'}; "
        f"bucket_rows={config.bucket_rows:,}; "
        f"fragment_target_rows={config.fragment_target_rows:,}; "
        f"max_open_writers={config.max_open_writers:,}; "
        f"compact_after_files={config.compact_after_files:,}; "
        f"input_mode={config.input_mode}"
    )
    report_path = route_contributions(config)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    click.echo(
        "Routing summary: "
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
    click.echo(f"Routing report written to {report_path}")


@cli.command("prepare")
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
    help="Replace existing prepared output and rebuild all groups.",
)
def prepare(
    project_path: Path,
    force: bool,
) -> None:
    """Sort routed contributions into deterministic prepared groups."""
    from foundinspace.octree.sources.preparation import (
        PreparationConfig,
        prepare_contributions,
    )

    project = _load_project_or_die(project_path)
    config = PreparationConfig(
        routed_dir=project.paths.routed_dir,
        prepared_dir=project.paths.prepared_dir,
        limiting_magnitude=project.dataset.limiting_magnitude,
        bucket_rows=project.routing.bucket_rows,
        input_mode=project.routing.input_mode,
        batch_rows=project.execution.batch_rows,
        fragment_target_rows=project.routing.fragment_target_rows,
        force=force,
    )
    click.echo(
        "Preparing routed contributions: "
        f"{config.routed_dir} -> {config.prepared_dir}; "
        f"fragment_target_rows={config.fragment_target_rows:,}; "
        f"batch_rows={config.batch_rows:,}; "
        f"force={config.force}"
    )
    report_path = prepare_contributions(config)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    click.echo(
        "Preparation summary: "
        f"processed_groups={report['processed_group_count']:,}, "
        f"changed_groups={report['changed_group_count']:,}, "
        f"unchanged_groups={report['unchanged_group_count']:,}, "
        f"deleted_groups={report['deleted_group_count']:,}, "
        f"in_memory_sorts={report['in_memory_sort_group_count']:,}, "
        f"external_sorts={report['external_sort_group_count']:,}, "
        f"files_written={report['output_files_written']:,}"
    )
    click.echo(f"Preparation report written to {report_path}")


@cli.command("build")
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
    help="Keep intermediate relocation files created during packing.",
)
def build(
    project_path: Path,
    retain_relocation_files: bool,
) -> None:
    """Plan, materialize, and pack the configured base artifacts."""
    from foundinspace.octree.base_build import (
        BaseBuildConfig,
        build_base_artifacts,
    )
    from foundinspace.octree.packing import IndexEmissionStrategy

    project = _load_project_or_die(project_path)
    result = build_base_artifacts(
        BaseBuildConfig(
            routed_dir=project.paths.routed_dir,
            prepared_dir=project.paths.prepared_dir,
            topology_dir=project.paths.topology_dir,
            output_path=project.paths.render_output_path,
            identifiers_order_path=project.paths.identifiers_order_output_path,
            limiting_magnitude=project.dataset.limiting_magnitude,
            max_level=project.profile.max_level,
            batch_rows=project.execution.batch_rows,
            max_open_files=project.execution.max_open_files,
            partition_from_level=project.materialization.partition_from_level,
            partition_prefix_bits=project.materialization.partition_prefix_bits,
            retain_relocation_files=retain_relocation_files,
            star_format_version=project.profile.star_format_version,
            terminal_waterline=project.materialization.terminal_waterline,
            index_emission_strategy=IndexEmissionStrategy(
                project.packing.index_emission_strategy
            ),
            materialized_dir=project.paths.materialized_dir,
            build_work_dir=project.paths.build_work_dir,
        )
    )
    click.echo(
        "Build summary: "
        f"rows={result.row_count:,}, "
        f"folded_rows={result.folded_row_count:,}, "
        f"cells={result.cell_count:,}, "
        f"profile={project.profile.name}, "
        f"max_level={project.profile.max_level}, "
        f"index_emission_strategy={project.packing.index_emission_strategy}, "
        "terminal_waterline="
        f"{project.materialization.terminal_waterline}, "
        f"dataset_uuid={result.dataset_uuid}"
    )
    click.echo(f"Wrote {result.output_path}")
    click.echo(f"Wrote {result.identifiers_order_path}")


@sidecars_group.command("build")
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
def build_sidecars(
    project_path: Path,
    family_name: str | None,
) -> None:
    """Build named sidecars from the render octree and identifiers/order artifact."""
    from foundinspace.octree.sidecars.configured import build_configured_sidecars

    project = _load_project_or_die(project_path)
    manifest_path = build_configured_sidecars(project, family_name=family_name)
    click.echo(f"Sidecars manifest written to {manifest_path}")


@benchmark_group.command("packing-order")
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
    help="Limiting apparent magnitude. Defaults to dataset.limiting_magnitude.",
)
@click.option(
    "--load-factor",
    type=float,
    default=2.0,
    show_default=True,
    help="Loader quality q in [1, 2]; 2 is complete.",
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
def packing_order_benchmark(
    project_path: Path,
    profiles: tuple[str, ...],
    orders: tuple[str, ...],
    scenarios: tuple[str, ...],
    center: str,
    magnitude: float | None,
    load_factor: float,
    target: str,
    vertical_fov: float,
    aspect_ratio: float,
    tile_prefix_depth: int,
    coalesce_gap_bytes: int,
    as_json: bool,
) -> None:
    """Estimate packing-order range-read behavior from prepared groups."""
    from foundinspace.octree.packing_benchmark import (
        PACKING_ORDERS,
        PROFILES,
        SCENARIOS,
        PackingBenchmarkConfig,
        Point3,
        report_to_json,
        run_packing_benchmark,
    )

    project = _load_project_or_die(project_path)
    center_point = _parse_point(center)
    target_point = _parse_point(target)
    config = PackingBenchmarkConfig(
        routed_dir=project.paths.routed_dir,
        prepared_dir=project.paths.prepared_dir,
        profiles=profiles or PROFILES,
        orders=orders or PACKING_ORDERS,
        scenarios=scenarios or SCENARIOS,
        center=Point3(center_point.x, center_point.y, center_point.z),
        target=Point3(target_point.x, target_point.y, target_point.z),
        limiting_magnitude=magnitude
        if magnitude is not None
        else project.dataset.limiting_magnitude,
        load_factor=load_factor,
        vertical_fov_deg=vertical_fov,
        aspect_ratio=aspect_ratio,
        tile_prefix_depth=tile_prefix_depth,
        coalesce_gap_bytes=coalesce_gap_bytes,
        batch_rows=project.execution.batch_rows,
    )
    try:
        report = run_packing_benchmark(config)
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(report_to_json(report), nl=False)
        return

    console = Console()
    console.print(
        "Packing-order benchmark: "
        f"{config.prepared_dir} | "
        f"profiles={','.join(config.profiles)} | "
        f"orders={','.join(config.orders)} | "
        f"scenarios={','.join(config.scenarios)} | "
        f"load_factor={config.load_factor:.2f}"
    )
    _render_packing_benchmark(console, report)


@benchmark_group.command("terminal-memory")
@click.argument("octree_source", type=str)
@click.option(
    "--sample",
    "sample_specs",
    multiple=True,
    required=True,
    help=("Complete subtree sample as NAME:X,Y,Z@LEVEL. May be passed multiple times."),
)
@click.option(
    "--trace",
    "trace_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=(
        "Ordered observer trace JSON. Without it, each sample point is replayed "
        "at the octree index magnitude."
    ),
)
@click.option(
    "--waterline",
    "waterlines",
    multiple=True,
    type=click.IntRange(min=1),
    help="Terminal subtree star cap. May be passed multiple times.",
)
@click.option(
    "--chunk-stars",
    "chunk_star_counts",
    multiple=True,
    type=click.IntRange(min=1),
    help="Independently decompressible terminal chunk size.",
)
@click.option(
    "--decoded-cache-mib",
    type=click.FloatRange(min=0.0),
    default=64.0,
    show_default=True,
    help="Decoded LRU cache budget used by the replay.",
)
@click.option(
    "--load-factor",
    type=float,
    default=2.0,
    show_default=True,
    help="Loader quality q in [1, 2]; 2 is complete.",
)
@click.option(
    "--terminal-directory-record-bytes",
    type=click.IntRange(min=0),
    default=24,
    show_default=True,
    help="Assumed bytes per logical payload entry in a terminal directory.",
)
@click.option(
    "--max-inflight-payloads",
    type=click.IntRange(min=1),
    default=8,
    show_default=True,
    help="Largest simultaneously inflating entry wave used for peak estimates.",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=16,
    show_default=True,
    help="Concurrent payload range readers during sample extraction.",
)
@click.option(
    "--cache-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Optional directory for extracted subtree and magnitude caches.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Print machine-readable JSON instead of summary tables.",
)
def terminal_memory_benchmark(
    octree_source: str,
    sample_specs: tuple[str, ...],
    trace_path: Path | None,
    waterlines: tuple[int, ...],
    chunk_star_counts: tuple[int, ...],
    decoded_cache_mib: float,
    load_factor: float,
    terminal_directory_record_bytes: int,
    max_inflight_payloads: int,
    workers: int,
    cache_dir: Path | None,
    as_json: bool,
) -> None:
    """Replay virtual terminal packing against published STAR v1 subtrees."""
    from foundinspace.octree.terminal_memory_benchmark import (
        DEFAULT_CHUNK_STAR_COUNTS,
        DEFAULT_WATERLINES,
        TerminalMemoryBenchmarkConfig,
        load_trace,
        parse_sample_spec,
        report_to_json,
        run_terminal_memory_benchmark,
    )

    try:
        source = _resolve_octree_source(octree_source)
        samples = tuple(parse_sample_spec(value) for value in sample_specs)
        views = load_trace(trace_path) if trace_path is not None else ()
        report = run_terminal_memory_benchmark(
            TerminalMemoryBenchmarkConfig(
                source=source,
                samples=samples,
                views=views,
                load_factor=load_factor,
                waterlines=waterlines or DEFAULT_WATERLINES,
                chunk_star_counts=(chunk_star_counts or DEFAULT_CHUNK_STAR_COUNTS),
                decoded_cache_bytes=round(decoded_cache_mib * 1024 * 1024),
                terminal_directory_record_bytes=(terminal_directory_record_bytes),
                max_inflight_payloads=max_inflight_payloads,
                workers=workers,
                cache_dir=cache_dir.expanduser() if cache_dir else None,
            )
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(report_to_json(report), nl=False)
        return

    console = Console()
    console.print(
        "Terminal memory benchmark: "
        f"{_format_source_label(source)} | "
        f"samples={len(samples)} | "
        f"views={'trace' if views else 'sample defaults'} | "
        f"load_factor={report['load_factor']:.2f}"
    )
    _render_terminal_memory_benchmark(console, report)


def _render_terminal_memory_benchmark(console: Console, report: dict) -> None:
    for sample in report["samples"]:
        classic = sample["classic"]
        console.print(
            f"[bold]{sample['name']}[/bold] "
            f"level={sample['sample_level']} | "
            f"nodes={classic['node_count']:,} | "
            f"payloads={classic['payload_node_count']:,} | "
            f"stars={classic['star_count']:,}"
        )
        table = Table()
        table.add_column("Layout")
        table.add_column("W", justify="right")
        table.add_column("Chunk", justify="right")
        table.add_column("External", justify="right")
        table.add_column("Active", justify="right")
        table.add_column("Resident", justify="right")
        table.add_column("Peak", justify="right")
        table.add_column("Overfetch", justify="right")
        for scenario in sample["scenarios"]:
            table.add_row(
                _terminal_policy_label(str(scenario["policy"])),
                (
                    "-"
                    if scenario["waterline"] is None
                    else f"{scenario['waterline']:,}"
                ),
                (
                    "-"
                    if scenario["chunk_star_count"] is None
                    else f"{scenario['chunk_star_count']:,}"
                ),
                f"{scenario['external_node_count']:,}",
                f"{scenario['max_active_rows']:,}",
                _format_memory_bytes(int(scenario["max_resident_bytes"])),
                _format_memory_bytes(int(scenario["max_peak_bytes"])),
                f"{float(scenario['max_overfetch_ratio']):.2f}x",
            )
        console.print(table)


def _terminal_policy_label(policy: str) -> str:
    return {
        "v1": "v1",
        "terminal-monolithic": "full",
        "terminal-magnitude-chunked": "mag",
        "terminal-logical-chunked": "logical",
    }.get(policy, policy)


def _format_memory_bytes(value: int) -> str:
    if value < 1024:
        return f"{value:,} B"
    if value < 1024 * 1024:
        return f"{value / 1024.0:,.1f} KB"
    return _format_compact_mb(value)



@cli.command("stage-04")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to octree project TOML.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Optional output path for the identifiers bigfile.",
)
@click.option(
    "--target-block-bytes",
    type=int,
    default=256 * 1024,
    show_default=True,
    help="Target block size for the range-friendly identifier payload blocks.",
)
def stage_04(project_path: Path, output_path: Path | None, target_block_bytes: int) -> None:
    """Build a range-friendly identifiers bigfile for client-side string lookup."""
    from foundinspace.octree.identifier_bigfile import build_identifier_bigfile

    project = _load_project_or_die(project_path)
    out = output_path or (project.paths.stage03_output_dir / "identifiers.bigfile")
    build_identifier_bigfile(
        identifiers_order_path=project.paths.identifiers_order_output_path,
        identifiers_map_path=project.paths.identifiers_map_path,
        output_path=out,
        target_block_bytes=target_block_bytes,
    )
    click.echo(f"Wrote {out}")


@cli.command("stage-04-query")
@click.option(
    "--bigfile",
    "bigfile_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Local identifiers bigfile path to query.",
)
@click.option(
    "--url",
    "bigfile_url",
    type=str,
    default=None,
    help="HTTP URL to query via Range requests.",
)
@click.option(
    "--query",
    "query_text",
    required=True,
    type=str,
    help="Query text (will be normalized to [a-z0-9]).",
)
@click.option(
    "--limit",
    type=int,
    default=50,
    show_default=True,
    help="Maximum number of matches to return.",
)
@click.option(
    "--exact/--prefix",
    "exact",
    default=False,
    show_default=True,
    help="Use exact-match mode instead of prefix mode.",
)
def stage_04_query(
    bigfile_path: Path | None,
    bigfile_url: str | None,
    query_text: str,
    limit: int,
    exact: bool,
) -> None:
    """Run local or HTTP-range query tests against an identifiers bigfile."""
    from foundinspace.octree.identifier_bigfile import query_identifier_bigfile

    try:
        matches, stats = query_identifier_bigfile(
            query=query_text,
            path=bigfile_path,
            url=bigfile_url,
            limit=limit,
            exact=exact,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(
        f"Query metrics: requests={stats.requests}, bytes={stats.bytes_fetched}, elapsed_ms={stats.elapsed_ms:.2f}"
    )
    for match in matches:
        click.echo(
            f"{match.term}\tflag={match.flag}\tlevel={match.level}\tnode={match.node_id}\tordinal={match.ordinal}"
        )


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


def _render_packing_benchmark(console: Console, report: dict) -> None:
    table = Table(title="Sidecars Packing Benchmark")
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
            f"Octree file not found: {octree_path}. Run build first."
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
    "--load-factor",
    type=float,
    default=2.0,
    show_default=True,
    help="Loader quality q in [1, 2]; 2 is complete.",
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
    help="Optional metadata octree path or URL for the Sidecars `meta` sidecar.",
)
def stats(
    octree_source: str,
    point: str,
    magnitude: float,
    load_factor: float,
    radius: float,
    nearest: int,
    meta_octree: str | None,
) -> None:
    """Read a build octree and print bounded query stats."""
    if radius < 0:
        raise click.BadParameter("--radius must be >= 0")
    if nearest <= 0:
        raise click.BadParameter("--nearest must be > 0")
    try:
        load_factor = validate_load_factor(load_factor)
    except ValueError as exc:
        raise click.BadParameter(str(exc), param_hint="--load-factor") from exc

    query_point = _parse_point(point)
    resolved_octree_source, meta_octree_source = _resolve_stats_sources(
        octree_source,
        meta_octree,
    )
    report = collect_stats(
        resolved_octree_source,
        point=query_point,
        limiting_magnitude=magnitude,
        load_factor=load_factor,
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
        f"| mag_limit={report.header.mag_limit:.2f} "
        f"| load_factor={report.load_factor:.2f} "
        f"| m_complete={report.m_complete:.3f}"
    )
    if report.header.dataset_uuid is not None:
        header_line += f" | octree_uuid={report.header.dataset_uuid}"
    if report.header.parent_dataset_uuid is not None:
        header_line += f" | parent_uuid={report.header.parent_dataset_uuid}"
    if report.header.sidecar_uuid is not None:
        header_line += f" | sidecar_uuid={report.header.sidecar_uuid}"
    console.print(header_line)
    _render_stats(console, report, nearest)
