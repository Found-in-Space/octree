from __future__ import annotations

from click.testing import CliRunner

from foundinspace.octree._cli import cli


class TestStage01CLI:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["stage-01", "--help"])
        assert result.exit_code == 0
        assert "INPUT_GLOB" in result.output
        assert "OUT_DIR" in result.output
        assert "--deep-shard-from-level" in result.output
        assert "--max-level" in result.output
        assert "--deep-prefix-bits" in result.output
        assert "--batch-size" in result.output
        assert "--identifiers-map" in result.output
        assert "--sidecar-fields" in result.output

    def test_help_shows_default_deep_shard_level(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["stage-01", "--help"])
        assert result.exit_code == 0
        assert "--deep-shard-from-level" in result.output
        assert "99" in result.output

    def test_non_empty_output_dir(self, tmp_path):
        (tmp_path / "existing.txt").write_text("occupied")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "stage-01",
                "nonexistent/*.parquet",
                str(tmp_path),
                "--deep-shard-from-level",
                "8",
            ],
        )
        assert result.exit_code != 0
