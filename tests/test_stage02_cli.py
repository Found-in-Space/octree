from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.assembly.formats import IDENTIFIERS_MANIFEST_NAME, RENDER_MANIFEST_NAME


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
merged_healpix_dir = "{(project_path.parent / 'merged').as_posix()}"
identifiers_map_path = "{(project_path.parent / 'identifiers_map.parquet').as_posix()}"
stage00_output_dir = "{(project_path.parent / 'stage00').as_posix()}"
stage01_output_dir = "{stage01_dir.as_posix()}"
stage02_output_path = "{output.as_posix()}"
identifiers_order_output_path = "{identifiers_output.as_posix()}"
stage03_output_dir = "{(project_path.parent / 'stage03').as_posix()}"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "{(project_path.parent / 'stage00' / '**' / '*.parquet').as_posix()}"
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


def test_stage02_requires_project() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02"])
    assert result.exit_code != 0
    assert "--project" in result.output


def test_stage02_invokes_render_and_identifiers_outputs(monkeypatch, tmp_path: Path) -> None:
    stage01_dir = tmp_path / "stage01"
    stage01_dir.mkdir()
    (stage01_dir / RENDER_MANIFEST_NAME).write_text("{}")
    (stage01_dir / IDENTIFIERS_MANIFEST_NAME).write_text("{}")
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
    render_calls: list[dict[str, object]] = []
    ident_calls: list[dict[str, object]] = []

    def _fake_combine(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        descriptor,
    ) -> None:
        render_calls.append(
            {
                "manifest": manifest_path,
                "output": output_path,
                "max_open_files": plan.max_open_files,
                "retain": plan.retain_relocation_files,
                "descriptor": descriptor,
            }
        )

    def _fake_identifiers(
        manifest_path: Path,
        output_path: Path,
        *,
        parent_dataset_uuid,
        artifact_uuid,
    ) -> None:
        ident_calls.append(
            {
                "manifest": manifest_path,
                "output": output_path,
                "parent_dataset_uuid": parent_dataset_uuid,
                "artifact_uuid": artifact_uuid,
            }
        )

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake_combine)
    monkeypatch.setattr(
        "foundinspace.octree.identifiers_order.combine_identifiers_order",
        _fake_identifiers,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stage-02",
            "--project",
            str(project_path),
            "--retain-relocation-files",
        ],
    )

    assert result.exit_code == 0
    assert render_calls[0]["manifest"] == stage01_dir / RENDER_MANIFEST_NAME
    assert render_calls[0]["output"] == output
    assert render_calls[0]["max_open_files"] == 7
    assert render_calls[0]["retain"] is True
    assert ident_calls[0]["manifest"] == stage01_dir / IDENTIFIERS_MANIFEST_NAME
    assert ident_calls[0]["output"] == identifiers_output
    assert ident_calls[0]["parent_dataset_uuid"] == render_calls[0]["descriptor"].dataset_uuid
