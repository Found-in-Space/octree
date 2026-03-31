from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli


def _write_project(project_path: Path) -> None:
    project_path.write_text(
        f"""
format_version = 1

[paths]
merged_healpix_dir = "{(project_path.parent / "merged").as_posix()}"
identifiers_map_path = "{(project_path.parent / "identifiers_map.parquet").as_posix()}"
stage00_output_dir = "{(project_path.parent / "stage00").as_posix()}"
stage01_output_dir = "{(project_path.parent / "stage01").as_posix()}"
stage02_output_path = "{(project_path.parent / "stars.octree").as_posix()}"
identifiers_order_output_path = "{(project_path.parent / "identifiers.order").as_posix()}"
stage03_output_dir = "{(project_path.parent / "stage03").as_posix()}"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "{(project_path.parent / "stage00" / "**" / "*.parquet").as_posix()}"
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


def test_stage03_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-03", "--help"])
    assert result.exit_code == 0
    assert "--project" in result.output
    assert "--family" in result.output


def test_stage03_requires_project() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-03"])
    assert result.exit_code != 0
    assert "--project" in result.output


def test_stage03_invokes_sidecar_builder(monkeypatch, tmp_path: Path) -> None:
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
        return tmp_path / "stage03" / "manifest.json"

    monkeypatch.setattr(
        "foundinspace.octree.stage3.build_stage03_sidecars", _fake_build
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stage-03",
            "--project",
            str(project_path),
            "--family",
            "meta",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["family_name"] == "meta"
    assert "Stage 03 manifest written to" in result.output
