from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli
from project_helpers import project_text


def _write_project(project_path: Path, out_dir: Path) -> None:
    project_path.write_text(
        project_text(project_path.parent, prepared_dir=out_dir),
        encoding="utf-8",
    )


class TestPreparationCLI:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["prepare", "--help"])
        assert result.exit_code == 0
        assert "--project" in result.output
        assert "--force" in result.output
        assert "INPUT_GLOB" not in result.output
        assert "OUT_DIR" not in result.output

    def test_requires_project(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["prepare"])
        assert result.exit_code != 0
        assert "--project" in result.output

    def test_non_empty_output_dir(self, tmp_path: Path):
        project_path = tmp_path / "project.toml"
        out_dir = tmp_path / "preparation"
        out_dir.mkdir()
        (out_dir / "existing.txt").write_text("occupied")
        _write_project(project_path, out_dir)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "prepare",
                "--project",
                str(project_path),
            ],
        )
        assert result.exit_code != 0
