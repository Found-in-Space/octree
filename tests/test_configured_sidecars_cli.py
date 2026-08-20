from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli
from project_helpers import project_text


def _write_project(project_path: Path) -> None:
    project_path.write_text(
        project_text(project_path.parent),
        encoding="utf-8",
    )


def test_sidecars_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["sidecars", "build", "--help"])
    assert result.exit_code == 0
    assert "--project" in result.output
    assert "--family" in result.output


def test_sidecars_requires_project() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["sidecars", "build"])
    assert result.exit_code != 0
    assert "--project" in result.output


def test_sidecars_invokes_sidecar_builder(monkeypatch, tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    _write_project(project_path)
    calls: list[dict[str, object]] = []

    def _fake_build(project, *, family_name):
        calls.append(
            {
                "project": project,
                "family_name": family_name,
            }
        )
        return tmp_path / "sidecars" / "manifest.json"

    monkeypatch.setattr(
        "foundinspace.octree.sidecars.configured.build_configured_sidecars", _fake_build
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "sidecars",
            "build",
            "--project",
            str(project_path),
            "--family",
            "meta",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["family_name"] == "meta"
    assert "Sidecars manifest written to" in result.output
