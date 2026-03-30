from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import DEFAULT_DEEP_SHARD_FROM_LEVEL, DEFAULT_MAG_VIS, DEFAULT_MAX_LEVEL

FORMAT_VERSION = 1
_META_MODES = {"auto", "on", "off"}
_DEFAULT_MERGED_HEALPIX_DIR = "../data/processed/merged/healpix"
_DEFAULT_IDENTIFIERS_MAP_PATH = "../data/processed/identifiers_map.parquet"
_DEFAULT_STAGE00_OUTPUT_DIR = "artifacts/stage00"
_DEFAULT_STAGE01_OUTPUT_DIR = "artifacts/stage01"
_DEFAULT_STAGE02_OUTPUT_PATH = "artifacts/stars.octree"


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    merged_healpix_dir: Path
    identifiers_map_path: Path
    stage00_output_dir: Path
    stage01_output_dir: Path
    stage02_output_path: Path


@dataclass(frozen=True, slots=True)
class Stage00ProjectConfig:
    batch_size: int
    v_mag: float
    max_level: int


@dataclass(frozen=True, slots=True)
class Stage01ProjectConfig:
    input_glob: str
    batch_size: int
    deep_shard_from_level: int
    deep_prefix_bits: int
    sidecar_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Stage02ProjectConfig:
    manifest_path: Path
    max_open_files: int
    meta_mode: str
    meta_output_path: Path


@dataclass(frozen=True, slots=True)
class OctreeProject:
    project_path: Path
    paths: ProjectPaths
    stage00: Stage00ProjectConfig
    stage01: Stage01ProjectConfig
    stage02: Stage02ProjectConfig


def _reject_env_expansion(value: str, *, field_name: str) -> None:
    if "$" in value:
        raise ValueError(
            f"{field_name} must not contain environment-variable syntax: {value!r}"
        )


def _require_table(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Missing or invalid [{key}] table in project file")
    return value


def _require_int(raw: dict[str, Any], key: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _require_float(raw: dict[str, Any], key: str) -> float:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric")
    return float(value)


def _require_str(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _resolve_path(project_dir: Path, value: str, *, field_name: str) -> Path:
    _reject_env_expansion(value, field_name=field_name)
    raw_path = Path(value)
    if raw_path.is_absolute():
        return raw_path
    return project_dir / raw_path


def _resolve_glob(project_dir: Path, value: str, *, field_name: str) -> str:
    _reject_env_expansion(value, field_name=field_name)
    raw_path = Path(value)
    if raw_path.is_absolute():
        return raw_path.as_posix()
    return (project_dir / raw_path).as_posix()


def load_project(project_path: Path) -> OctreeProject:
    resolved_project_path = project_path.expanduser().resolve()
    with resolved_project_path.open("rb") as fp:
        raw = tomllib.load(fp)

    format_version = raw.get("format_version")
    if format_version != FORMAT_VERSION:
        raise ValueError(
            f"format_version must be {FORMAT_VERSION}, got {format_version!r}"
        )

    project_dir = resolved_project_path.parent
    paths_raw = _require_table(raw, "paths")
    stage00_raw = _require_table(raw, "stage00")
    stage01_raw = _require_table(raw, "stage01")
    stage02_raw = _require_table(raw, "stage02")

    paths = ProjectPaths(
        merged_healpix_dir=_resolve_path(
            project_dir,
            _require_str(paths_raw, "merged_healpix_dir"),
            field_name="paths.merged_healpix_dir",
        ),
        identifiers_map_path=_resolve_path(
            project_dir,
            _require_str(paths_raw, "identifiers_map_path"),
            field_name="paths.identifiers_map_path",
        ),
        stage00_output_dir=_resolve_path(
            project_dir,
            _require_str(paths_raw, "stage00_output_dir"),
            field_name="paths.stage00_output_dir",
        ),
        stage01_output_dir=_resolve_path(
            project_dir,
            _require_str(paths_raw, "stage01_output_dir"),
            field_name="paths.stage01_output_dir",
        ),
        stage02_output_path=_resolve_path(
            project_dir,
            _require_str(paths_raw, "stage02_output_path"),
            field_name="paths.stage02_output_path",
        ),
    )

    stage00 = Stage00ProjectConfig(
        batch_size=_require_int(stage00_raw, "batch_size"),
        v_mag=_require_float(stage00_raw, "v_mag"),
        max_level=_require_int(stage00_raw, "max_level"),
    )
    if stage00.batch_size <= 0:
        raise ValueError("stage00.batch_size must be > 0")
    if stage00.max_level < 0:
        raise ValueError("stage00.max_level must be >= 0")

    sidecar_fields_raw = stage01_raw.get("sidecar_fields", [])
    if not isinstance(sidecar_fields_raw, list) or not all(
        isinstance(v, str) and v.strip() for v in sidecar_fields_raw
    ):
        raise ValueError("stage01.sidecar_fields must be a list of non-empty strings")
    stage01 = Stage01ProjectConfig(
        input_glob=_resolve_glob(
            project_dir,
            _require_str(stage01_raw, "input_glob"),
            field_name="stage01.input_glob",
        ),
        batch_size=_require_int(stage01_raw, "batch_size"),
        deep_shard_from_level=_require_int(stage01_raw, "deep_shard_from_level"),
        deep_prefix_bits=_require_int(stage01_raw, "deep_prefix_bits"),
        sidecar_fields=tuple(v.strip() for v in sidecar_fields_raw),
    )
    if stage01.batch_size <= 0:
        raise ValueError("stage01.batch_size must be > 0")
    if stage01.deep_shard_from_level < 0:
        raise ValueError("stage01.deep_shard_from_level must be >= 0")
    if stage01.deep_prefix_bits < 0:
        raise ValueError("stage01.deep_prefix_bits must be >= 0")

    meta_mode = _require_str(stage02_raw, "meta_mode").lower()
    if meta_mode not in _META_MODES:
        raise ValueError(
            f"stage02.meta_mode must be one of {sorted(_META_MODES)}, got {meta_mode!r}"
        )
    stage02 = Stage02ProjectConfig(
        manifest_path=_resolve_path(
            project_dir,
            _require_str(stage02_raw, "manifest_path"),
            field_name="stage02.manifest_path",
        ),
        max_open_files=_require_int(stage02_raw, "max_open_files"),
        meta_mode=meta_mode,
        meta_output_path=_resolve_path(
            project_dir,
            _require_str(stage02_raw, "meta_output_path"),
            field_name="stage02.meta_output_path",
        ),
    )
    if stage02.max_open_files <= 0:
        raise ValueError("stage02.max_open_files must be > 0")

    return OctreeProject(
        project_path=resolved_project_path,
        paths=paths,
        stage00=stage00,
        stage01=stage01,
        stage02=stage02,
    )


def render_project_template() -> str:
    stage00_output_dir = _DEFAULT_STAGE00_OUTPUT_DIR
    stage01_output_dir = _DEFAULT_STAGE01_OUTPUT_DIR
    stage02_output_path = _DEFAULT_STAGE02_OUTPUT_PATH
    stage00_input_glob = f"{stage00_output_dir}/**/*.parquet"
    stage02_manifest_path = f"{stage01_output_dir}/manifest.json"

    stage02_output_name = Path(stage02_output_path)
    meta_output_path = stage02_output_name.with_name(
        f"{stage02_output_name.stem}.meta{stage02_output_name.suffix}"
    ).as_posix()

    return (
        f"format_version = {FORMAT_VERSION}\n\n"
        "[paths]\n"
        f'merged_healpix_dir = "{_DEFAULT_MERGED_HEALPIX_DIR}"\n'
        f'identifiers_map_path = "{_DEFAULT_IDENTIFIERS_MAP_PATH}"\n'
        f'stage00_output_dir = "{stage00_output_dir}"\n'
        f'stage01_output_dir = "{stage01_output_dir}"\n'
        f'stage02_output_path = "{stage02_output_path}"\n\n'
        "[stage00]\n"
        'batch_size = 1000000\n'
        f"v_mag = {DEFAULT_MAG_VIS}\n"
        f"max_level = {DEFAULT_MAX_LEVEL}\n\n"
        "[stage01]\n"
        f'input_glob = "{stage00_input_glob}"\n'
        'batch_size = 100000\n'
        f"deep_shard_from_level = {DEFAULT_DEEP_SHARD_FROM_LEVEL}\n"
        "deep_prefix_bits = 3\n"
        "sidecar_fields = []\n\n"
        "[stage02]\n"
        f'manifest_path = "{stage02_manifest_path}"\n'
        "max_open_files = 32\n"
        'meta_mode = "auto"\n'
        f'meta_output_path = "{meta_output_path}"\n'
    )
