from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

import foundinspace.octree.paths as oct_paths
from foundinspace.octree._cli import cli


def test_stage02_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", "--help"])
    assert result.exit_code == 0
    assert "MANIFEST_PATH" in result.output
    assert "OUTPUT_PATH" in result.output
    assert "manifest.json" in result.output
    assert "stars.octree" in result.output
    assert "stage01" in result.output
    assert "--max-open-files" in result.output
    assert "--retain-relocation-files" in result.output
    assert "--meta" in result.output
    assert "--meta-output" in result.output


def test_stage02_invokes_combine(monkeypatch, tmp_path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    called: dict[str, object] = {}

    def _fake(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        payload_kind: str = "render",
        **_: object,
    ) -> None:
        called["manifest"] = manifest_path
        called["output"] = output_path
        called["max_open_files"] = plan.max_open_files
        called["retain"] = plan.retain_relocation_files
        called["payload_kind"] = payload_kind

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
    assert called["payload_kind"] == "render"


def test_stage02_invokes_combine_with_path_defaults(
    monkeypatch, tmp_path: Path
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    monkeypatch.setattr(oct_paths, "STAGE01_MANIFEST_PATH", manifest)
    monkeypatch.setattr(oct_paths, "STAGE02_OUTPUT", output)
    called: dict[str, object] = {}

    def _fake(
        manifest_path: Path,
        output_path: Path,
        *,
        plan,
        payload_kind: str = "render",
        **_: object,
    ) -> None:
        called["manifest"] = manifest_path
        called["output"] = output_path
        called["payload_kind"] = payload_kind

    monkeypatch.setattr("foundinspace.octree.combine.combine_octree", _fake)

    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02"])

    assert result.exit_code == 0
    assert called["manifest"] == manifest
    assert called["output"] == output
    assert called["payload_kind"] == "render"


def test_stage02_auto_detects_meta_and_calls_combine_twice(
    monkeypatch, tmp_path: Path
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
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
    result = runner.invoke(cli, ["stage-02", str(manifest), str(output)])

    assert result.exit_code == 0
    assert len(calls) == 2
    assert calls[0]["payload_kind"] == "render"
    assert calls[1]["payload_kind"] == "meta"
    assert calls[1]["output"] == tmp_path / "stars.meta.octree"


def test_stage02_no_meta_skips_meta_combine(monkeypatch, tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
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
    monkeypatch.setattr(
        "foundinspace.octree.combine.manifest.manifest_has_meta",
        lambda _p: True,
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["stage-02", str(manifest), str(output), "--no-meta"])

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["payload_kind"] == "render"


def test_stage02_meta_output_option(monkeypatch, tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    output = tmp_path / "stars.octree"
    custom_meta = tmp_path / "custom.meta.octree"
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
    monkeypatch.setattr(
        "foundinspace.octree.combine.manifest.manifest_has_meta",
        lambda _p: True,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "stage-02",
            str(manifest),
            str(output),
            "--meta",
            "--meta-output",
            str(custom_meta),
        ],
    )

    assert result.exit_code == 0
    assert calls == [custom_meta]
