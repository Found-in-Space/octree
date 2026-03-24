from __future__ import annotations

from pathlib import Path

import click

from foundinspace.octree.config import (
    DEFAULT_MAG_VIS,
    DEFAULT_MAX_LEVEL,
    LEVEL_CONFIG,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.mag_levels import MagLevelConfig
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

    plan = BuildPlan(
        max_level=ml,
        deep_shard_from_level=deep_shard_from_level,
        deep_prefix_bits=deep_prefix_bits,
        batch_size=batch_size,
    )

    manifest_path = build_intermediates(input_glob, out_dir, plan=plan)
    click.echo(f"Manifest written to {manifest_path}")
