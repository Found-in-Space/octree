from __future__ import annotations

import json
import math
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from .classic_materialization import (
    ClassicMaterializationPlan,
    Stage01GroupInput,
    classic_input_identity,
    load_published_materialization,
    materialize_classic_groups,
)
from .combine import CombinePlan, combine_octree
from .combine.records import PackedDescriptorFields
from .config import (
    DEFAULT_CLASSIC_MAX_LEVEL,
    DEFAULT_CLASSIC_PARTITION_FROM_LEVEL,
    DEFAULT_CLASSIC_PARTITION_PREFIX_BITS,
    MORTON_BITS,
)
from .identifiers_order import combine_identifiers_order
from .sources.stage00 import (
    STAGE_STATE_FORMAT,
    STAGE_STATE_NAME,
    TREE_MANIFEST_FORMAT,
    TREE_MANIFEST_NAME,
)

CLASSIC_INTERMEDIATES_DIR_NAME = "classic-intermediates"
CLASSIC_WORK_DIR_NAME = ".classic-intermediates.work"


@dataclass(frozen=True, slots=True)
class ClassicBuildConfig:
    stage00_output_dir: Path
    stage01_output_dir: Path
    output_path: Path
    identifiers_order_path: Path
    mag_limit: float
    max_level: int = DEFAULT_CLASSIC_MAX_LEVEL
    batch_size: int = 100_000
    max_open_files: int = 32
    partition_from_level: int = DEFAULT_CLASSIC_PARTITION_FROM_LEVEL
    partition_prefix_bits: int = DEFAULT_CLASSIC_PARTITION_PREFIX_BITS
    retain_relocation_files: bool = False

    def validate(self) -> None:
        if not self.stage00_output_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.stage00_output_dir}")
        if not self.stage01_output_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {self.stage01_output_dir}")
        if self.max_level < 0 or self.max_level > MORTON_BITS:
            raise ValueError(f"max_level must be in 0..{MORTON_BITS}")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if self.max_open_files <= 0:
            raise ValueError("max_open_files must be > 0")
        if self.partition_from_level < 0:
            raise ValueError("partition_from_level must be >= 0")
        if self.partition_prefix_bits < 0:
            raise ValueError("partition_prefix_bits must be >= 0")
        if not math.isfinite(self.mag_limit):
            raise ValueError("mag_limit must be finite")


@dataclass(frozen=True, slots=True)
class ClassicBuildResult:
    output_path: Path
    identifiers_order_path: Path
    intermediates_dir: Path
    dataset_uuid: UUID
    identifiers_uuid: UUID
    row_count: int
    folded_row_count: int
    cell_count: int


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _tracked_stage01_groups(
    stage00_output_dir: Path,
    stage01_output_dir: Path,
) -> list[Stage01GroupInput]:
    manifest_path = stage00_output_dir / TREE_MANIFEST_NAME
    state_path = stage00_output_dir / STAGE_STATE_NAME
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 tree manifest: {manifest_path}")
    if not state_path.is_file():
        raise FileNotFoundError(f"Missing Stage 00 state: {state_path}")

    manifest = _read_json(manifest_path)
    state = _read_json(state_path)
    if manifest.get("format") != TREE_MANIFEST_FORMAT:
        raise ValueError(
            f"Unsupported Stage 00 tree manifest format: {manifest.get('format')!r}"
        )
    if state.get("format") != STAGE_STATE_FORMAT:
        raise ValueError(f"Unsupported Stage 00 state format: {state.get('format')!r}")
    if state.get("tree_identity") != manifest.get("tree_identity"):
        raise ValueError("Stage 00 state identity does not match tree manifest")

    dirty = state.get("dirty", {})
    if dirty.get("stage01_groups"):
        raise ValueError("Classic build requires no dirty Stage 01 groups")
    if dirty.get("deleted_stage00_groups"):
        raise ValueError("Classic build requires no deleted Stage 00 groups")
    if "stage01_groups" not in state:
        raise ValueError("Classic build requires Stage 01 to run first")

    groups: list[Stage01GroupInput] = []
    seen: set[Path] = set()
    for group in sorted(state.get("stage01_groups", []), key=lambda row: row["key"]):
        files: list[Path] = []
        for rel_path in group.get("files", []):
            path = stage01_output_dir / str(rel_path)
            if path in seen:
                raise ValueError(f"Duplicate Stage 01 group file in state: {path}")
            if not path.is_file():
                raise FileNotFoundError(f"Missing Stage 01 group file: {path}")
            seen.add(path)
            files.append(path)
        groups.append(
            Stage01GroupInput(
                key=str(group["key"]),
                checksum=str(group["checksum"]),
                row_count=int(group["row_count"]),
                files=tuple(files),
                natural_max_level=max(
                    (
                        int(str(node).split(":", 1)[0])
                        for node in group.get("final_nodes", [])
                    ),
                    default=None,
                ),
            )
        )
    if not groups:
        raise ValueError("Classic build found no Stage 01 groups")
    return groups


def _publish_intermediates(
    *,
    temporary_dir: Path,
    final_dir: Path,
) -> None:
    backup_dir = final_dir.with_name(f".{final_dir.name}.{uuid4().hex}.backup")
    moved_existing = False
    if final_dir.exists():
        os.replace(final_dir, backup_dir)
        moved_existing = True
    try:
        os.replace(temporary_dir, final_dir)
    except Exception:
        if moved_existing and not final_dir.exists():
            os.replace(backup_dir, final_dir)
        raise
    if moved_existing:
        shutil.rmtree(backup_dir)


def build_classic_artifacts(
    config: ClassicBuildConfig,
    *,
    dataset_uuid: UUID | None = None,
    identifiers_uuid: UUID | None = None,
) -> ClassicBuildResult:
    """Build the traditional magnitude-level octree from staged parquet groups."""
    config.validate()
    stage01_groups = _tracked_stage01_groups(
        config.stage00_output_dir,
        config.stage01_output_dir,
    )
    materialization_plan = ClassicMaterializationPlan(
        max_level=config.max_level,
        mag_limit=config.mag_limit,
        batch_size=config.batch_size,
        max_open_files=config.max_open_files,
        partition_from_level=config.partition_from_level,
        partition_prefix_bits=config.partition_prefix_bits,
    )
    input_identity = classic_input_identity(stage01_groups, materialization_plan)

    intermediates_dir = config.stage01_output_dir / CLASSIC_INTERMEDIATES_DIR_NAME
    materialized = load_published_materialization(
        intermediates_dir,
        input_identity=input_identity,
    )
    if materialized is None:
        work_dir = config.stage01_output_dir / CLASSIC_WORK_DIR_NAME
        materialized = materialize_classic_groups(
            groups=stage01_groups,
            work_dir=work_dir,
            plan=materialization_plan,
        )
        work_artifacts_dir = materialized.render_manifest_path.parent
        _publish_intermediates(
            temporary_dir=work_artifacts_dir,
            final_dir=intermediates_dir,
        )
        shutil.rmtree(work_dir, ignore_errors=True)
    render_manifest_path = intermediates_dir / materialized.render_manifest_path.name
    identifiers_manifest_path = (
        intermediates_dir / materialized.identifiers_manifest_path.name
    )

    resolved_dataset_uuid = dataset_uuid or uuid4()
    resolved_identifiers_uuid = identifiers_uuid or uuid4()
    output_tmp = config.output_path.with_name(
        f".{config.output_path.name}.{os.getpid()}.tmp"
    )
    identifiers_tmp = config.identifiers_order_path.with_name(
        f".{config.identifiers_order_path.name}.{os.getpid()}.tmp"
    )
    output_tmp.unlink(missing_ok=True)
    identifiers_tmp.unlink(missing_ok=True)
    try:
        combine_octree(
            render_manifest_path,
            output_tmp,
            plan=CombinePlan(
                max_open_files=config.max_open_files,
                retain_relocation_files=config.retain_relocation_files,
            ),
            descriptor=PackedDescriptorFields(
                artifact_kind="render",
                dataset_uuid=resolved_dataset_uuid,
            ),
        )
        combine_identifiers_order(
            identifiers_manifest_path,
            identifiers_tmp,
            parent_dataset_uuid=resolved_dataset_uuid,
            artifact_uuid=resolved_identifiers_uuid,
        )
        config.identifiers_order_path.parent.mkdir(parents=True, exist_ok=True)
        config.output_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(identifiers_tmp, config.identifiers_order_path)
        os.replace(output_tmp, config.output_path)
    finally:
        output_tmp.unlink(missing_ok=True)
        identifiers_tmp.unlink(missing_ok=True)

    return ClassicBuildResult(
        output_path=config.output_path,
        identifiers_order_path=config.identifiers_order_path,
        intermediates_dir=intermediates_dir,
        dataset_uuid=resolved_dataset_uuid,
        identifiers_uuid=resolved_identifiers_uuid,
        row_count=materialized.row_count,
        folded_row_count=materialized.folded_row_count,
        cell_count=materialized.cell_count,
    )
