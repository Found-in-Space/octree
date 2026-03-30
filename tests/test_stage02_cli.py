from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from foundinspace.octree._cli import cli


def _write_project(
    project_path: Path,
    *,
    manifest: Path,
    output: Path,
    max_open_files: int = 32,
    meta_mode: str = "auto",
    meta_output: Path | None = None,
) -> None:
    meta_target = meta_output if meta_output is not None else output.parent / (
        f"{output.stem}.meta{output.suffix}"
    )
    project_path.write_text(
        f"""
format_version = 1

[paths]
merged_healpix_dir = "{(project_path.parent / 'merged').as_posix()}"
identifiers_map_path = "{(project_path.parent / 'identifiers_map.parquet').as_posix()}"
stage00_output_dir = "{(project_path.parent / 'stage00').as_posix()}"
stage01_output_dir = "{(project_path.parent / 'stage01').as_posix()}"
stage02_output_path = "{output.as_posix()}"

[stage00]
batch_size = 1000000
v_mag = 6.5
max_level = 14

[stage01]
input_glob = "{(project_path.parent / 'stage00' / '**' / '*.parquet').as_posix()}"
batch_size = 100000
deep_shard_from_level = 99
deep_prefix_bits = 3
sidecar_fields = []

[stage02]
manifest_path = "{manifest.as_posix()}"
max_open_files = {max_open_files}
meta_mode = "{meta_mode}"
meta_output_path = "{meta_target.as_posix()}"
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
    assert "MANIFEST_PATH" not in result.output
    assert "--meta" not in result.output
    assert "--max-open-files" not in result.output


def test_stage02_requires_project() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02"])
    assert result.exit_code != 0
    assert "--project" in result.output


def test_stage02_invokes_combine(monkeypatch, tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    project_path = tmp_path / "project.toml"
    _write_project(project_path, manifest=manifest, output=output, max_open_files=7)
    called: list[dict[str, object]] = []

    def _fake(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        payload_kind: str = "render",
        **_: object,
    ) -> None:
        called.append(
            {
                "manifest": manifest_path,
                "output": output_path,
                "max_open_files": plan.max_open_files,
                "retain": plan.retain_relocation_files,
                "payload_kind": payload_kind,
            }
        )

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake)

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
    assert len(called) == 1
    assert called[0]["manifest"] == manifest
    assert called[0]["output"] == output
    assert called[0]["max_open_files"] == 7
    assert called[0]["retain"] is True
    assert called[0]["payload_kind"] == "render"


def test_stage02_auto_detects_meta_and_calls_combine_twice(
    monkeypatch, tmp_path: Path
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    project_path = tmp_path / "project.toml"
    _write_project(project_path, manifest=manifest, output=output, meta_mode="auto")
    calls: list[dict[str, object]] = []

    def _fake(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        payload_kind: str = "render",
        **_: object,
    ) -> None:
        calls.append(
            {
                "manifest": manifest_path,
                "output": output_path,
                "payload_kind": payload_kind,
            }
        )

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake)
    monkeypatch.setattr(
        "foundinspace.octree.combine.manifest.manifest_has_meta",
        lambda _p: True,
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", "--project", str(project_path)])

    assert result.exit_code == 0
    assert len(calls) == 2
    assert calls[0]["payload_kind"] == "render"
    assert calls[1]["payload_kind"] == "meta"
    assert calls[1]["output"] == tmp_path / "stars.meta.octree"


def test_stage02_meta_mode_off_skips_meta_combine(
    monkeypatch, tmp_path: Path
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    project_path = tmp_path / "project.toml"
    _write_project(project_path, manifest=manifest, output=output, meta_mode="off")
    calls: list[dict[str, object]] = []

    def _fake(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        payload_kind: str = "render",
        **_: object,
    ) -> None:
        calls.append({"payload_kind": payload_kind})

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake)

    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", "--project", str(project_path)])

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["payload_kind"] == "render"


def test_stage02_meta_mode_on_uses_project_meta_output(
    monkeypatch, tmp_path: Path
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    custom_meta = tmp_path / "custom.meta.octree"
    project_path = tmp_path / "project.toml"
    _write_project(
        project_path,
        manifest=manifest,
        output=output,
        meta_mode="on",
        meta_output=custom_meta,
    )
    calls: list[Path] = []

    def _fake(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        payload_kind: str = "render",
        **_: object,
    ) -> None:
        if payload_kind == "meta":
            calls.append(output_path)

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake)

    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", "--project", str(project_path)])

    assert result.exit_code == 0
    assert calls == [custom_meta]
