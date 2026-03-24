from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli


def test_stage02_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", "--help"])
    assert result.exit_code == 0
    assert "MANIFEST_PATH" in result.output
    assert "OUTPUT_PATH" in result.output
    assert "--max-open-files" in result.output
    assert "--retain-relocation-files" in result.output


def test_stage02_invokes_combine(monkeypatch, tmp_path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    called: dict[str, object] = {}

    def _fake(manifest_path: Path, output_path: Path, *, plan) -> None:
        called["manifest"] = manifest_path
        called["output"] = output_path
        called["max_open_files"] = plan.max_open_files
        called["retain"] = plan.retain_relocation_files

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stage-02",
            str(manifest),
            str(output),
            "--max-open-files",
            "7",
            "--retain-relocation-files",
        ],
    )

    assert result.exit_code == 0
    assert called["manifest"] == manifest
    assert called["output"] == output
    assert called["max_open_files"] == 7
    assert called["retain"] is True
