from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli


def _write_project(project_path: Path, out_dir: Path) -> None:
    project_path.write_text(
        f"""
format_version = 1

[paths]
merged_healpix_dir = "{(project_path.parent / "merged").as_posix()}"
identifiers_map_path = "{(project_path.parent / "identifiers_map.parquet").as_posix()}"
stage00_output_dir = "{(project_path.parent / "stage00").as_posix()}"
stage01_output_dir = "{out_dir.as_posix()}"
stage02_output_path = "{(project_path.parent / "stars.octree").as_posix()}"
identifiers_order_output_path = "{(project_path.parent / "identifiers.order").as_posix()}"
stage03_output_dir = "{(project_path.parent / "stage03").as_posix()}"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "{(project_path.parent / "missing" / "**" / "*.parquet").as_posix()}"
batch_size = 100000
deep_shard_from_level = 99
deep_prefix_bits = 3

[stage02]
max_open_files = 32

[stage03]

[[stage03.sidecars]]
name = "meta"
fields = []
""".strip()
        + "\n",
        encoding="utf-8",
    )


class TestStage01CLI:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["stage-01", "--help"])
        assert result.exit_code == 0
        assert "--project" in result.output
        assert "INPUT_GLOB" not in result.output
        assert "OUT_DIR" not in result.output

    def test_requires_project(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["stage-01"])
        assert result.exit_code != 0
        assert "--project" in result.output

    def test_non_empty_output_dir(self, tmp_path: Path):
        project_path = tmp_path / "project.toml"
        out_dir = tmp_path / "stage01"
        out_dir.mkdir()
        (out_dir / "existing.txt").write_text("occupied")
        _write_project(project_path, out_dir)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "stage-01",
                "--project",
                str(project_path),
            ],
        )
        assert result.exit_code != 0
