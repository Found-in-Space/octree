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
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_stage04_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-04", "--help"])
    assert result.exit_code == 0
    assert "--project" in result.output
    assert "--target-block-bytes" in result.output


def test_stage04_query_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["stage-04-query", "--help"])
    assert result.exit_code == 0
    assert "--bigfile" in result.output
    assert "--url" in result.output
    assert "--query" in result.output


def test_stage04_invokes_builder(monkeypatch, tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    _write_project(project_path)

    calls: list[dict[str, object]] = []

    def _fake_build(**kwargs):
        calls.append(kwargs)
        return kwargs["output_path"]

    monkeypatch.setattr(
        "foundinspace.octree.identifier_bigfile.build_identifier_bigfile", _fake_build
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["stage-04", "--project", str(project_path), "--target-block-bytes", "1024"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["target_block_bytes"] == 1024
    assert str(calls[0]["output_path"]).endswith("stage03/identifiers.bigfile")
    assert "Wrote" in result.output


def test_stage04_query_invokes_query_function(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class _FakeMatch:
        term = "betelgeuse"
        flag = 4
        level = 1
        node_id = 5
        ordinal = 0

    class _FakeStats:
        requests = 4
        bytes_fetched = 2048
        elapsed_ms = 3.5

    def _fake_query_identifier_bigfile(**kwargs):
        calls.append(kwargs)
        return [_FakeMatch()], _FakeStats()

    monkeypatch.setattr(
        "foundinspace.octree.identifier_bigfile.query_identifier_bigfile",
        _fake_query_identifier_bigfile,
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["stage-04-query", "--url", "https://example.com/id.bigfile", "--query", "betelg"],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["query"] == "betelg"
    assert calls[0]["url"] == "https://example.com/id.bigfile"
    assert "requests=4" in result.output
    assert "betelgeuse" in result.output
