from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from click.testing import CliRunner

from foundinspace.octree._cli import cli
from project_helpers import project_text


def _write_project(project_path: Path) -> None:
    root = project_path.parent
    project_path.write_text(
        project_text(root),
        encoding="utf-8",
    )


def test_visual_duplicates_sidecar_help_is_purpose_named() -> None:
    runner = CliRunner()

    result = runner.invoke(cli, ["sidecars", "visual-duplicates", "--help"])

    assert result.exit_code == 0
    assert "--evidence" in result.output
    assert "--project" in result.output


def test_visual_duplicates_sidecar_cli_is_explicit_and_optional(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    evidence_path = tmp_path / "display-map.parquet"
    _write_project(project_path)
    evidence_path.write_bytes(b"evidence")
    calls: list[object] = []
    sidecar_uuid = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")

    def fake_build(config):
        calls.append(config)
        return SimpleNamespace(
            output_path=config.output_path,
            report_path=config.report_path,
            evidence_pair_count=1,
            rendered_endpoint_count=2,
            payload_cell_count=2,
            sidecar_uuid=sidecar_uuid,
        )

    monkeypatch.setattr(
        "foundinspace.octree.sidecars.visual_duplicates.build_visual_duplicates_sidecar",
        fake_build,
    )
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "sidecars",
            "visual-duplicates",
            "--project",
            str(project_path),
            "--evidence",
            str(evidence_path),
        ],
    )

    assert result.exit_code == 0, result.output
    config = calls[0]
    assert config.output_path == tmp_path / "sidecars" / "visual-duplicates.octree"
    assert config.report_path == tmp_path / "sidecars" / "visual-duplicates.report.json"
    assert config.work_dir == tmp_path / "sidecars-work" / "visual-duplicates"
    assert "Stage" not in result.output
