from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.packing import IndexEmissionStrategy
from project_helpers import project_text


def _write_project(
    project_path: Path,
    *,
    preparation_dir: Path,
    output: Path,
    identifiers_output: Path,
    max_open_files: int = 32,
    profile_name: str = "terminal-packed",
    index_emission_strategy: str = "temp-pwrite-batched",
) -> None:
    project_path.write_text(
        project_text(
            project_path.parent,
            prepared_dir=preparation_dir,
            render_output_path=output,
            identifiers_order_output_path=identifiers_output,
            max_open_files=max_open_files,
            profile_name=profile_name,
            index_emission_strategy=index_emission_strategy,
        ),
        encoding="utf-8",
    )


def test_build_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["build", "--help"])
    assert result.exit_code == 0
    assert "--project" in result.output
    assert "--retain-relocation-files" in result.output


def test_build_requires_project() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["build"])
    assert result.exit_code != 0
    assert "--project" in result.output


def test_build_builds_classic_output_from_project(monkeypatch, tmp_path: Path) -> None:
    preparation_dir = tmp_path / "preparation"
    preparation_dir.mkdir()
    output = tmp_path / "stars.octree"
    identifiers_output = tmp_path / "identifiers.order"
    project_path = tmp_path / "project.toml"
    _write_project(
        project_path,
        preparation_dir=preparation_dir,
        output=output,
        identifiers_output=identifiers_output,
        max_open_files=7,
        profile_name="classic",
        index_emission_strategy="forward",
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
        "foundinspace.octree.base_build.build_base_artifacts",
        _fake_build,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "build",
            "--project",
            str(project_path),
            "--retain-relocation-files",
        ],
    )

    assert result.exit_code == 0
    assert calls[0].prepared_dir == preparation_dir
    assert calls[0].output_path == output
    assert calls[0].identifiers_order_path == identifiers_output
    assert calls[0].max_open_files == 7
    assert calls[0].retain_relocation_files is True
    assert calls[0].max_level == 14
    assert calls[0].star_format_version == 1
    assert calls[0].terminal_waterline == 1_000
    assert calls[0].index_emission_strategy == IndexEmissionStrategy.FORWARD
    assert calls[0].materialized_dir == tmp_path / "materialized"
    assert calls[0].build_work_dir == tmp_path / "work"
    assert "rows=12" in result.output
    assert "folded_rows=3" in result.output
    assert "profile=classic" in result.output
    assert "terminal_waterline=1000" in result.output
    assert "index_emission_strategy=forward" in result.output


def test_build_defaults_to_batched_temporary_index(monkeypatch, tmp_path: Path) -> None:
    preparation_dir = tmp_path / "preparation"
    preparation_dir.mkdir()
    project_path = tmp_path / "project.toml"
    _write_project(
        project_path,
        preparation_dir=preparation_dir,
        output=tmp_path / "stars.octree",
        identifiers_output=tmp_path / "identifiers.order",
    )
    calls = []

    def _fake_build(config):
        calls.append(config)
        return SimpleNamespace(
            row_count=1,
            folded_row_count=0,
            cell_count=1,
            dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            output_path=config.output_path,
            identifiers_order_path=config.identifiers_order_path,
        )

    monkeypatch.setattr(
        "foundinspace.octree.base_build.build_base_artifacts",
        _fake_build,
    )
    result = CliRunner().invoke(
        cli,
        ["build", "--project", str(project_path)],
    )

    assert result.exit_code == 0
    assert calls[0].index_emission_strategy == IndexEmissionStrategy.TEMP_PWRITE_BATCHED
