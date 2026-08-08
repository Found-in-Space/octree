from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
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
from .combine import CombinePlan, IndexEmissionStrategy, combine_octree
from .combine.records import (
    DESCRIPTOR_SIZE,
    HEADER_SIZE,
    PackedDescriptorFields,
    unpack_descriptor,
)
from .config import (
    DEFAULT_CLASSIC_MAX_LEVEL,
    DEFAULT_CLASSIC_PARTITION_FROM_LEVEL,
    DEFAULT_CLASSIC_PARTITION_PREFIX_BITS,
    DEFAULT_STAR_FORMAT_VERSION,
    DEFAULT_TERMINAL_WATERLINE,
    MORTON_BITS,
)
from .identifiers_order import combine_identifiers_order
from .identifiers_order import read_header as read_identifiers_header
from .sources.stage00 import (
    STAGE_STATE_FORMAT,
    STAGE_STATE_NAME,
    TREE_MANIFEST_FORMAT,
    TREE_MANIFEST_NAME,
)
from .terminal_packing import TerminalMap

CLASSIC_INTERMEDIATES_DIR_NAME = "classic-intermediates"
CLASSIC_WORK_DIR_NAME = ".classic-intermediates.work"
CLASSIC_FINAL_STATE_NAME = "classic-final-products.json"
CLASSIC_FINAL_STATE_FORMAT = "foundinspace.octree.classic-final-products/v1"
CLASSIC_COMBINE_ALGORITHM = "streaming-index-skeletons/v1"


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
    star_format_version: int = DEFAULT_STAR_FORMAT_VERSION
    terminal_waterline: int = DEFAULT_TERMINAL_WATERLINE
    index_emission_strategy: IndexEmissionStrategy = (
        IndexEmissionStrategy.TEMP_PWRITE_BATCHED
    )
    intermediates_dir: Path | None = None
    work_dir: Path | None = None

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
        if self.star_format_version not in (1, 2):
            raise ValueError("star_format_version must be 1 or 2")
        if self.terminal_waterline <= 0:
            raise ValueError("terminal_waterline must be > 0")
        strategy = IndexEmissionStrategy(self.index_emission_strategy)
        if strategy not in (
            IndexEmissionStrategy.TEMP_PWRITE_BATCHED,
            IndexEmissionStrategy.FORWARD,
        ):
            raise ValueError(
                "index_emission_strategy must be temp-pwrite-batched or forward"
            )


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


def _final_base_identity(config: ClassicBuildConfig, *, input_identity: str) -> str:
    value = {
        "format": CLASSIC_FINAL_STATE_FORMAT,
        "algorithm": CLASSIC_COMBINE_ALGORITHM,
        "input_identity": input_identity,
        "star_format_version": config.star_format_version,
        "terminal_waterline": (
            config.terminal_waterline if config.star_format_version == 2 else None
        ),
        "output_path": str(config.output_path.resolve()),
        "identifiers_order_path": str(config.identifiers_order_path.resolve()),
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _file_stat_record(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _load_final_state(path: Path) -> dict[str, Any] | None:
    try:
        raw = _read_json(path)
        return raw if raw.get("format") == CLASSIC_FINAL_STATE_FORMAT else None
    except (OSError, TypeError, ValueError):
        return None


def _state_uuid(
    state: dict[str, Any] | None, *, base_identity: str, key: str
) -> UUID | None:
    if state is None or state.get("base_identity") != base_identity:
        return None
    try:
        return UUID(str(state[key]))
    except (KeyError, TypeError, ValueError):
        return None


def _final_pair_is_valid(
    state: dict[str, Any],
    *,
    config: ClassicBuildConfig,
    base_identity: str,
    dataset_uuid: UUID,
    identifiers_uuid: UUID,
) -> bool:
    try:
        if state.get("base_identity") != base_identity:
            return False
        if state.get("dataset_uuid") != str(dataset_uuid):
            return False
        if state.get("identifiers_uuid") != str(identifiers_uuid):
            return False
        for key in ("row_count", "folded_row_count", "cell_count"):
            if int(state[key]) < 0:
                return False
        if _file_stat_record(config.output_path) != state.get("render_file"):
            return False
        if _file_stat_record(config.identifiers_order_path) != state.get(
            "identifiers_file"
        ):
            return False
        with open(config.output_path, "rb") as fp:
            fp.seek(HEADER_SIZE)
            descriptor = unpack_descriptor(fp.read(DESCRIPTOR_SIZE))
        identifiers = read_identifiers_header(config.identifiers_order_path)
        return (
            descriptor.dataset_uuid == dataset_uuid
            and identifiers.parent_dataset_uuid == dataset_uuid
            and identifiers.artifact_uuid == identifiers_uuid
        )
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _published_topology_is_valid(
    intermediates_dir: Path, *, config: ClassicBuildConfig
) -> bool:
    try:
        manifest = _read_json(intermediates_dir / "render-manifest.json")
        terminal_path = manifest.get("terminal_map_path")
        if config.star_format_version == 1:
            return terminal_path is None
        if not isinstance(terminal_path, str) or not terminal_path:
            return False
        terminal_map = TerminalMap(intermediates_dir / terminal_path)
        return (
            terminal_map.max_level == config.max_level
            and terminal_map.waterline == config.terminal_waterline
        )
    except (OSError, TypeError, ValueError):
        return False


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(value, fp, indent=2, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, path)


def _clean_incomplete_final_products(*paths: Path) -> None:
    for path in paths:
        if not path.parent.is_dir():
            continue
        for temporary in path.parent.glob(f".{path.name}.*.tmp"):
            if temporary.is_file():
                temporary.unlink()


@contextmanager
def _final_pair_locks(config: ClassicBuildConfig):
    work_dir = config.work_dir or (config.stage01_output_dir / CLASSIC_WORK_DIR_NAME)
    work_dir.mkdir(parents=True, exist_ok=True)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.identifiers_order_path.parent.mkdir(parents=True, exist_ok=True)
    lock_paths = sorted(
        {
            work_dir / ".classic-build.lock",
            config.output_path.parent
            / f".{config.output_path.name}.classic-build.lock",
            config.identifiers_order_path.parent
            / f".{config.identifiers_order_path.name}.classic-build.lock",
        },
        key=lambda path: str(path.resolve()),
    )
    lock_files = []
    try:
        for path in lock_paths:
            fp = open(path, "a+b")  # noqa: SIM115
            lock_files.append(fp)
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        while lock_files:
            fp = lock_files.pop()
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
            fp.close()


def _serialize_final_pair_build(function):
    @wraps(function)
    def locked(
        config: ClassicBuildConfig,
        *,
        dataset_uuid: UUID | None = None,
        identifiers_uuid: UUID | None = None,
    ) -> ClassicBuildResult:
        with _final_pair_locks(config):
            return function(
                config,
                dataset_uuid=dataset_uuid,
                identifiers_uuid=identifiers_uuid,
            )

    return locked


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
                natural_max_level=_group_natural_max_level(group),
            )
        )
    if not groups:
        raise ValueError("Classic build found no Stage 01 groups")
    return groups


def _group_natural_max_level(group: dict[str, Any]) -> int | None:
    value = group.get("natural_max_level")
    if value is not None:
        return int(value)
    return max(
        (int(str(node).split(":", 1)[0]) for node in group.get("final_nodes", [])),
        default=None,
    )


def _publish_intermediates(
    *,
    temporary_dir: Path,
    final_dir: Path,
) -> None:
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    incoming_dir = final_dir.with_name(f".{final_dir.name}.{uuid4().hex}.incoming")
    publication_dir = temporary_dir
    copied_to_final_device = False
    if temporary_dir.stat().st_dev != final_dir.parent.stat().st_dev:
        # os.replace is atomic only within one filesystem. Build a complete
        # staging tree beside the destination before disturbing the published
        # tree when callers configure work and intermediates on different
        # filesystems.
        try:
            shutil.copytree(temporary_dir, incoming_dir, copy_function=shutil.copy2)
        except Exception:
            shutil.rmtree(incoming_dir, ignore_errors=True)
            raise
        publication_dir = incoming_dir
        copied_to_final_device = True

    backup_dir = final_dir.with_name(f".{final_dir.name}.{uuid4().hex}.backup")
    moved_existing = False
    if final_dir.exists():
        os.replace(final_dir, backup_dir)
        moved_existing = True
    try:
        os.replace(publication_dir, final_dir)
    except Exception:
        if moved_existing and not final_dir.exists():
            os.replace(backup_dir, final_dir)
        raise
    finally:
        if incoming_dir.exists():
            shutil.rmtree(incoming_dir)
    if moved_existing:
        shutil.rmtree(backup_dir)
    if copied_to_final_device:
        shutil.rmtree(temporary_dir, ignore_errors=True)


@_serialize_final_pair_build
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
        star_format_version=config.star_format_version,
        terminal_waterline=config.terminal_waterline,
    )
    input_identity = classic_input_identity(stage01_groups, materialization_plan)

    intermediates_dir = config.intermediates_dir or (
        config.stage01_output_dir / CLASSIC_INTERMEDIATES_DIR_NAME
    )
    work_dir = config.work_dir or (config.stage01_output_dir / CLASSIC_WORK_DIR_NAME)
    final_state_path = work_dir / CLASSIC_FINAL_STATE_NAME
    _clean_incomplete_final_products(
        config.output_path,
        config.identifiers_order_path,
        final_state_path,
    )
    base_identity = _final_base_identity(config, input_identity=input_identity)
    final_state = _load_final_state(final_state_path)
    cached_dataset_uuid = _state_uuid(
        final_state, base_identity=base_identity, key="dataset_uuid"
    )
    cached_identifiers_uuid = _state_uuid(
        final_state, base_identity=base_identity, key="identifiers_uuid"
    )
    resolved_dataset_uuid = dataset_uuid or cached_dataset_uuid or uuid4()
    resolved_identifiers_uuid = identifiers_uuid or cached_identifiers_uuid or uuid4()
    if (
        final_state is not None
        and _published_topology_is_valid(intermediates_dir, config=config)
        and _final_pair_is_valid(
            final_state,
            config=config,
            base_identity=base_identity,
            dataset_uuid=resolved_dataset_uuid,
            identifiers_uuid=resolved_identifiers_uuid,
        )
    ):
        return ClassicBuildResult(
            output_path=config.output_path,
            identifiers_order_path=config.identifiers_order_path,
            intermediates_dir=intermediates_dir,
            dataset_uuid=resolved_dataset_uuid,
            identifiers_uuid=resolved_identifiers_uuid,
            row_count=int(final_state["row_count"]),
            folded_row_count=int(final_state["folded_row_count"]),
            cell_count=int(final_state["cell_count"]),
        )

    materialized = load_published_materialization(
        intermediates_dir,
        input_identity=input_identity,
        plan=materialization_plan,
    )
    if materialized is None:
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
        # The work directory owns content-addressed normalized runs, topology
        # inputs and completed partitions. materialize_classic_groups prunes
        # superseded products after successful publication assembly; retaining
        # this bounded checkpoint is what makes later shard rebuilds incremental.
    render_manifest_path = intermediates_dir / materialized.render_manifest_path.name
    identifiers_manifest_path = (
        intermediates_dir / materialized.identifiers_manifest_path.name
    )

    output_tmp = config.output_path.with_name(
        f".{config.output_path.name}.{os.getpid()}.{uuid4().hex}.tmp"
    )
    identifiers_tmp = config.identifiers_order_path.with_name(
        f".{config.identifiers_order_path.name}.{os.getpid()}.{uuid4().hex}.tmp"
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
                star_format_version=config.star_format_version,
                index_emission_strategy=IndexEmissionStrategy(
                    config.index_emission_strategy
                ),
                cache_dir=work_dir / ".combine-index-cache",
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
        _atomic_write_json(
            final_state_path,
            {
                "format": CLASSIC_FINAL_STATE_FORMAT,
                "base_identity": base_identity,
                "dataset_uuid": str(resolved_dataset_uuid),
                "identifiers_uuid": str(resolved_identifiers_uuid),
                "render_file": _file_stat_record(config.output_path),
                "identifiers_file": _file_stat_record(config.identifiers_order_path),
                "row_count": materialized.row_count,
                "folded_row_count": materialized.folded_row_count,
                "cell_count": materialized.cell_count,
            },
        )
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
