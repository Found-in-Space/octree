from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.project import (
    FORMAT_VERSION,
    load_project,
    render_project_template,
)


def test_load_project_resolves_relative_paths_from_project_file_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir("/")
    project_dir = tmp_path / "build"
    project_dir.mkdir()
    project_path = project_dir / "project.toml"
    project_path.write_text(
        """
format_version = 1

[paths]
merged_healpix_dir = "../processed/merged/healpix"
identifiers_map_path = "../processed/identifiers_map.parquet"
stage00_output_dir = "artifacts/stage00"
stage01_output_dir = "artifacts/stage01"
stage02_output_path = "artifacts/stars.octree"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "artifacts/stage00/**/*.parquet"
batch_size = 100000
deep_shard_from_level = 99
deep_prefix_bits = 3
sidecar_fields = []

[stage02]
manifest_path = "artifacts/stage01/manifest.json"
max_open_files = 32
meta_mode = "auto"
meta_output_path = "artifacts/stars.meta.octree"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.paths.stage00_output_dir == project_dir / "artifacts" / "stage00"
    assert project.stage01.input_glob == (
        project_dir / "artifacts" / "stage00" / "**" / "*.parquet"
    ).as_posix()
    assert project.stage02.manifest_path == (
        project_dir / "artifacts" / "stage01" / "manifest.json"
    )


def test_load_project_rejects_env_style_path_strings(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        """
format_version = 1

[paths]
merged_healpix_dir = "${FIS_PROCESSED_DIR}/merged/healpix"
identifiers_map_path = "data/processed/identifiers_map.parquet"
stage00_output_dir = "artifacts/stage00"
stage01_output_dir = "artifacts/stage01"
stage02_output_path = "artifacts/stars.octree"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "artifacts/stage00/**/*.parquet"
batch_size = 100000
deep_shard_from_level = 99
deep_prefix_bits = 3
sidecar_fields = []

[stage02]
manifest_path = "artifacts/stage01/manifest.json"
max_open_files = 32
meta_mode = "auto"
meta_output_path = "artifacts/stars.meta.octree"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="environment-variable syntax"):
        load_project(project_path)


def test_render_project_template_contains_complete_v1_config(tmp_path: Path) -> None:
    rendered = render_project_template()

    assert f"format_version = {FORMAT_VERSION}" in rendered
    assert "[paths]" in rendered
    assert '[stage02]\n' in rendered
    assert 'meta_mode = "auto"' in rendered
    assert 'input_glob = ' in rendered
    assert 'merged_healpix_dir = "../data/processed/merged/healpix"' in rendered
    assert 'stage02_output_path = "artifacts/stars.octree"' in rendered
    assert "FIS_PROCESSED_DIR" not in rendered
    assert "FIS_OCTREE_DIR" not in rendered


def test_project_init_writes_complete_toml(tmp_path: Path) -> None:
    runner = CliRunner()
    project_path = tmp_path / "build" / "project.toml"

    result = runner.invoke(cli, ["project", "init", str(project_path)])

    assert result.exit_code == 0
    rendered = project_path.read_text(encoding="utf-8")
    assert f"format_version = {FORMAT_VERSION}" in rendered
    assert "[stage00]" in rendered
    assert "[stage01]" in rendered
    assert "[stage02]" in rendered
