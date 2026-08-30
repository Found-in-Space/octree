from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.project import load_project, render_project_template


def _project_text() -> str:
    return """
[paths]
input_shards_dir = "../processed/merged/healpix"
identifiers_map_path = "../processed/identifiers_map.parquet"
routed_dir = "artifacts/routed"
prepared_dir = "artifacts/prepared"
topology_dir = "artifacts/topology"
materialized_dir = "artifacts/materialized"
build_work_dir = "artifacts/work"
render_output_path = "artifacts/stars.octree"
identifiers_order_output_path = "artifacts/identifiers.order"
sidecars_output_dir = "artifacts/sidecars"
sidecars_work_dir = "artifacts/sidecars-work"

[dataset]
limiting_magnitude = 6.5

[execution]
batch_rows = 100000
max_open_files = 32

[routing]
input_mode = "cartesian"
scan_batch_rows = 1000000
bucket_rows = 1000000
fragment_target_rows = 100000
max_open_writers = 128
compact_after_files = 64

[materialization]
partition_from_level = 8
partition_prefix_bits = 6
terminal_waterline = 1000

[profile]
name = "terminal-packed"
max_level = 14

[packing]
index_emission_strategy = "temp-pwrite-batched"

[sidecars]
shard_from_level = 99
shard_prefix_bits = 3

[[sidecars.families]]
name = "meta"
fields = ["proper_name"]
""".lstrip()


def test_load_project_resolves_relative_paths_from_project_file_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir("/")
    project_dir = tmp_path / "build"
    project_dir.mkdir()
    project_path = project_dir / "project.toml"
    project_path.write_text(_project_text(), encoding="utf-8")

    project = load_project(project_path)

    assert project.paths.routed_dir == project_dir / "artifacts" / "routed"
    assert project.paths.prepared_dir == project_dir / "artifacts" / "prepared"
    assert project.paths.topology_dir == project_dir / "artifacts" / "topology"
    assert project.paths.materialized_dir == project_dir / "artifacts" / "materialized"
    assert (
        project.paths.render_output_path == project_dir / "artifacts" / "stars.octree"
    )
    assert project.routing.bucket_rows == 1_000_000
    assert project.routing.input_mode == "cartesian"
    assert project.profile.name == "terminal-packed"
    assert project.profile.star_format_version == 2
    assert project.materialization.terminal_waterline == 1_000
    assert project.sidecars.families[0].name == "meta"
    assert project.sidecars.families[0].fields == ("proper_name",)


def test_load_project_rejects_env_style_path_strings(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'input_shards_dir = "../processed/merged/healpix"',
            'input_shards_dir = "${FIS_PROCESSED_DIR}/merged/healpix"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="environment-variable syntax"):
        load_project(project_path)


def test_load_project_requires_semantic_tables(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace("[packing]\n", "[removed-packing]\n"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"\[packing\]"):
        load_project(project_path)


def test_load_project_rejects_unknown_routing_input_mode(tmp_path: Path) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace(
            'input_mode = "cartesian"', 'input_mode = "implicit-magic"'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="routing.input_mode"):
        load_project(project_path)


def test_classic_profile_selects_star_v1_with_shared_terminal_waterline(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "project.toml"
    project_path.write_text(
        _project_text().replace('name = "terminal-packed"', 'name = "classic"'),
        encoding="utf-8",
    )

    project = load_project(project_path)

    assert project.profile.star_format_version == 1
    assert project.materialization.terminal_waterline == 1_000


def test_render_project_template_is_complete_and_unversioned() -> None:
    rendered = render_project_template()

    assert "format_version" not in rendered
    for table in (
        "[paths]",
        "[dataset]",
        "[execution]",
        "[routing]",
        "[materialization]",
        "[profile]",
        "[packing]",
        "[sidecars]",
    ):
        assert table in rendered
    assert 'input_mode = "cartesian"' in rendered
    assert "bucket_rows = 1000000" in rendered
    assert 'name = "terminal-packed"' in rendered
    assert "terminal_waterline = 1000" in rendered
    assert 'render_output_path = "products/stars-v2.octree"' in rendered
    assert 'name = "meta"' in rendered
    assert "FIS_PROCESSED_DIR" not in rendered


def test_project_init_writes_complete_toml(tmp_path: Path) -> None:
    runner = CliRunner()
    project_path = tmp_path / "build" / "project.toml"

    result = runner.invoke(cli, ["project", "init", str(project_path)])

    assert result.exit_code == 0
    rendered = project_path.read_text(encoding="utf-8")
    assert "format_version" not in rendered
    assert "[routing]" in rendered
    assert "[materialization]" in rendered
    assert "[profile]" in rendered
    assert "[packing]" in rendered
    assert "[sidecars]" in rendered
