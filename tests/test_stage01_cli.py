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

    def test_missing_required_deep_shard_from_level(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["stage-01", "*.parquet", "/tmp/out"])
        assert result.exit_code != 0
        assert "deep-shard-from-level" in result.output.lower() or \
               "deep-shard-from-level" in (result.output + str(result.exception)).lower()

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
