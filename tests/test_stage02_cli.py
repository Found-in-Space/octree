from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from click.testing import CliRunner

from foundinspace.octree._cli import cli


def _write_project(
    project_path: Path,
    *,
    stage01_dir: Path,
    output: Path,
    identifiers_output: Path,
    max_open_files: int = 32,
) -> None:
    project_path.write_text(
        f"""
format_version = 1

[paths]
merged_healpix_dir = "{(project_path.parent / "merged").as_posix()}"
identifiers_map_path = "{(project_path.parent / "identifiers_map.parquet").as_posix()}"
stage00_output_dir = "{(project_path.parent / "stage00").as_posix()}"
stage01_output_dir = "{stage01_dir.as_posix()}"
stage02_output_path = "{output.as_posix()}"
identifiers_order_output_path = "{identifiers_output.as_posix()}"
stage03_output_dir = "{(project_path.parent / "stage03").as_posix()}"

[stage00]
batch_size = 1000000
v_mag = 6.5

[stage01]
input_glob = "{(project_path.parent / "stage00" / "**" / "*.parquet").as_posix()}"
batch_size = 100000
deep_shard_from_level = 99
deep_prefix_bits = 3

[stage02]
max_open_files = {max_open_files}

[stage03]

[[stage03.sidecars]]
name = "meta"
fields = []
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_stage02_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", "--help"])
    assert result.exit_code == 0
    assert "--project" in result.output
    assert "--retain-relocation-files" in result.output
    assert "--max-level" in result.output
    assert "--star-format-version" in result.output
    assert "--terminal-waterline" in result.output
    assert "--intermediates-dir" in result.output
    assert "--work-dir" in result.output


def test_stage02_requires_project() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02"])
    assert result.exit_code != 0
    assert "--project" in result.output


def test_stage02_builds_classic_output_from_project(
    monkeypatch, tmp_path: Path
) -> None:
    stage01_dir = tmp_path / "stage01"
    stage01_dir.mkdir()
    output = tmp_path / "stars.octree"
    identifiers_output = tmp_path / "identifiers.order"
    project_path = tmp_path / "project.toml"
    _write_project(
        project_path,
        stage01_dir=stage01_dir,
        output=output,
        identifiers_output=identifiers_output,
        max_open_files=7,
    )
    calls = []

    def _fake_build(config):
        calls.append(config)
        return SimpleNamespace(
            row_count=12,
            folded_row_count=3,
            cell_count=4,
            dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            output_path=config.output_path,
            identifiers_order_path=config.identifiers_order_path,
        )

    monkeypatch.setattr(
        "foundinspace.octree.classic.build_classic_artifacts",
        _fake_build,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stage-02",
            "--project",
            str(project_path),
            "--retain-relocation-files",
            "--max-level",
            "13",
            "--star-format-version",
            "1",
            "--terminal-waterline",
            "250",
            "--intermediates-dir",
            str(tmp_path / "v2-intermediates"),
            "--work-dir",
            str(tmp_path / "v2-work"),
        ],
    )

    assert result.exit_code == 0
    assert calls[0].stage01_output_dir == stage01_dir
    assert calls[0].output_path == output
    assert calls[0].identifiers_order_path == identifiers_output
    assert calls[0].max_open_files == 7
    assert calls[0].retain_relocation_files is True
    assert calls[0].max_level == 13
    assert calls[0].star_format_version == 1
    assert calls[0].terminal_waterline == 250
    assert calls[0].intermediates_dir == tmp_path / "v2-intermediates"
    assert calls[0].work_dir == tmp_path / "v2-work"
    assert "rows=12" in result.output
    assert "folded_rows=3" in result.output
    assert "star_format_version=1" in result.output
    assert "terminal_waterline=disabled" in result.output
