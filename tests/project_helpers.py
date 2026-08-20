from __future__ import annotations

from pathlib import Path


def project_text(
    root: Path,
    *,
    input_shards_dir: Path | None = None,
    routed_dir: Path | None = None,
    prepared_dir: Path | None = None,
    materialized_dir: Path | None = None,
    build_work_dir: Path | None = None,
    render_output_path: Path | None = None,
    identifiers_order_output_path: Path | None = None,
    sidecars_output_dir: Path | None = None,
    sidecars_work_dir: Path | None = None,
    input_mode: str = "pre-routed",
    max_open_files: int = 32,
    index_emission_strategy: str = "temp-pwrite-batched",
    profile_name: str = "terminal-packed",
    max_level: int = 14,
    terminal_waterline: int = 1_000,
    sidecar_fields: tuple[str, ...] = (),
) -> str:
    input_shards_dir = input_shards_dir or root / "input"
    routed_dir = routed_dir or root / "routed"
    prepared_dir = prepared_dir or root / "prepared"
    materialized_dir = materialized_dir or root / "materialized"
    build_work_dir = build_work_dir or root / "work"
    render_output_path = render_output_path or root / "stars.octree"
    identifiers_order_output_path = (
        identifiers_order_output_path or root / "identifiers.order"
    )
    sidecars_output_dir = sidecars_output_dir or root / "sidecars"
    sidecars_work_dir = sidecars_work_dir or root / "sidecars-work"
    fields = ", ".join(f'"{field}"' for field in sidecar_fields)
    return f"""
[paths]
input_shards_dir = "{input_shards_dir.as_posix()}"
identifiers_map_path = "{(root / "identifiers-map.parquet").as_posix()}"
routed_dir = "{routed_dir.as_posix()}"
prepared_dir = "{prepared_dir.as_posix()}"
materialized_dir = "{materialized_dir.as_posix()}"
build_work_dir = "{build_work_dir.as_posix()}"
render_output_path = "{render_output_path.as_posix()}"
identifiers_order_output_path = "{identifiers_order_output_path.as_posix()}"
sidecars_output_dir = "{sidecars_output_dir.as_posix()}"
sidecars_work_dir = "{sidecars_work_dir.as_posix()}"

[dataset]
limiting_magnitude = 6.5

[execution]
batch_rows = 100000
max_open_files = {max_open_files}

[routing]
input_mode = "{input_mode}"
scan_batch_rows = 1000000
bucket_rows = 1000000
fragment_target_rows = 100000
max_open_writers = 128
compact_after_files = 64

[materialization]
partition_from_level = 8
partition_prefix_bits = 6

[profile]
name = "{profile_name}"
max_level = {max_level}
terminal_waterline = {terminal_waterline}

[packing]
index_emission_strategy = "{index_emission_strategy}"

[sidecars]
shard_from_level = 99
shard_prefix_bits = 3

[[sidecars.families]]
name = "meta"
fields = [{fields}]
""".lstrip()
