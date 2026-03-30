from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.project import FORMAT_VERSION, load_project, render_project_template


def _project_text(project_dir: Path) -> str:
    return f"""
format_version = 1

[paths]
merged_healpix_dir = "../processed/merged/healpix"
identifiers_map_path = "../processed/identifiers_map.parquet"
stage00_output_dir = "artifacts/stage00"
stage01_output_dir = "artifacts/stage01"
stage02_output_path = "artifacts/stars.octree"
identifiers_order_output_path = "artifacts/identifiers.order"
stage03_output_dir = "artifacts/stage03"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "artifacts/stage00/**/*.parquet"
batch_size = 100000
deep_shard_from_level = 99
deep_prefix_bits = 3

[stage02]
max_open_files = 32

[stage03]

[[stage03.sidecars]]
name = "meta"
fields = ["proper_name"]
""".strip() + "\n"


def test_load_project_resolves_relative_paths_from_project_file_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir("/")
    project_dir = tmp_path / "build"
    project_dir.mkdir()
    project_path = project_dir / "project.toml"
    project_path.write_text(_project_text(project_dir), encoding="utf-8")

    project = load_project(project_path)

    assert project.paths.stage00_output_dir == project_dir / "artifacts" / "stage00"
    assert project.paths.identifiers_order_output_path == (
        project_dir / "artifacts" / "identifiers.order"
    )
    assert project.paths.stage03_output_dir == project_dir / "artifacts" / "stage03"
    assert project.stage03.sidecars[0].name == "meta"
    assert project.stage03.sidecars[0].fields == ("proper_name",)


def test_load_project_rejects_env_style_path_strings(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text(tmp_path).replace(
            'merged_healpix_dir = "../processed/merged/healpix"',
            'merged_healpix_dir = "${FIS_PROCESSED_DIR}/merged/healpix"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="environment-variable syntax"):
        load_project(project_path)


def test_load_project_rejects_removed_stage01_sidecar_fields(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text(tmp_path).replace(
            "deep_prefix_bits = 3\n",
            "deep_prefix_bits = 3\nsidecar_fields = []\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown key\\(s\\) in \\[stage01\\]"):
        load_project(project_path)


def test_load_project_rejects_removed_stage02_manifest_fields(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text(tmp_path).replace(
            "[stage02]\nmax_open_files = 32\n",
            '[stage02]\nmax_open_files = 32\nmanifest_path = "old.json"\n',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unknown key\\(s\\) in \\[stage02\\]"):
        load_project(project_path)


def test_render_project_template_contains_complete_v1_config() -> None:
    rendered = render_project_template()

    assert f"format_version = {FORMAT_VERSION}" in rendered
    assert "[paths]" in rendered
    assert "[stage03]" in rendered
    assert 'identifiers_order_output_path = "artifacts/identifiers.order"' in rendered
    assert 'stage03_output_dir = "artifacts/stage03"' in rendered
    assert 'name = "meta"' in rendered
    assert "FIS_PROCESSED_DIR" not in rendered


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
    assert "[stage03]" in rendered
