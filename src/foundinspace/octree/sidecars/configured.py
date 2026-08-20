from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from uuid import UUID, uuid4

from ..assembly.formats import (
    SIDECAR_ARTIFACT_KIND,
    SIDECAR_INDEX_MAGIC,
    SIDECAR_MANIFEST_FORMAT,
)
from ..assembly.manifest import write_manifest
from ..assembly.meta_encoder import IdentifiersMap, write_meta_payload
from ..assembly.plan import BuildPlan
from ..assembly.types import CellKey
from ..assembly.writer import (
    IntermediateShardWriter,
    belongs_to_shard,
    sidecar_shard_filenames,
)
from ..identifiers_order import IdentifiersOrderReader
from ..identifiers_order import read_header as read_identifiers_order_header
from ..packing import PackingPlan, pack_octree
from ..packing.records import PackedDescriptorFields
from ..project import OctreeProject, SidecarFamilyConfig
from ..reader import read_header


@dataclass(frozen=True, slots=True)
class _MetaBuilder:
    ident_map: IdentifiersMap

    @classmethod
    def from_project(
        cls, project: OctreeProject, config: SidecarFamilyConfig
    ) -> _MetaBuilder:
        return cls(
            ident_map=IdentifiersMap(
                project.paths.identifiers_map_path,
                fields=list(config.fields) or None,
            )
        )

    def write_payload(
        self,
        identities: Iterable[tuple[str, str]],
        target: BinaryIO,
    ) -> None:
        write_meta_payload(identities, self.ident_map, target)

    def __enter__(self) -> _MetaBuilder:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.ident_map.close()


def _family_builder(project: OctreeProject, config: SidecarFamilyConfig):
    if config.name == "meta":
        return _MetaBuilder.from_project(project, config)
    raise ValueError(f"Unsupported sidecar family: {config.name}")


def _write_sidecars_manifest(
    out_dir: Path,
    *,
    render_octree_path: Path,
    identifiers_order_path: Path,
    parent_dataset_uuid: UUID,
    sidecars: list[dict[str, str]],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "format": SIDECAR_MANIFEST_FORMAT,
        "render_octree_path": str(render_octree_path),
        "identifiers_order_path": str(identifiers_order_path),
        "parent_dataset_uuid": str(parent_dataset_uuid),
        "sidecars": sidecars,
    }
    path = out_dir / "manifest.json"
    tmp_path = out_dir / ".manifest.json.tmp"
    with open(tmp_path, "w") as fp:
        json.dump(manifest, fp, indent=2)
        fp.write("\n")
    os.replace(tmp_path, path)
    return path


def _build_family_intermediates(
    *,
    project: OctreeProject,
    config: SidecarFamilyConfig,
    render_header,
    order_path: Path,
    out_dir: Path,
) -> Path:
    plan = BuildPlan(
        max_level=render_header.max_level,
        deep_shard_from_level=project.sidecars.shard_from_level,
        deep_prefix_bits=project.sidecars.shard_prefix_bits,
        batch_size=project.execution.batch_rows,
        mag_limit=project.dataset.limiting_magnitude,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    shard_entries: list[dict] = []
    with (
        _family_builder(project, config) as builder,
        IdentifiersOrderReader(order_path) as reader,
    ):
        iter_cells = reader.iter_cell_identities()
        current_level = -1
        shard_keys = ()
        shard_index = 0
        current_writer: IntermediateShardWriter | None = None

        def close_current_writer() -> None:
            nonlocal current_writer
            if current_writer is None:
                return
            result = current_writer.close()
            current_writer = None
            if result is not None:
                shard_entries.append(result)

        for record, identities in iter_cells:
            if record.level != current_level:
                close_current_writer()
                current_level = record.level
                shard_keys = tuple(plan.shard_keys_for_level(current_level))
                shard_index = 0

            while shard_index < len(shard_keys) and not belongs_to_shard(
                record.node_id, shard_keys[shard_index]
            ):
                close_current_writer()
                shard_index += 1
            if shard_index >= len(shard_keys):
                raise ValueError(
                    f"Identifiers/order cell does not match any shard at level {record.level}: {record.node_id}"
                )
            if current_writer is None:
                current_writer = IntermediateShardWriter(
                    shard_keys[shard_index],
                    out_dir,
                    index_magic=SIDECAR_INDEX_MAGIC,
                    filename_fn=sidecar_shard_filenames(config.name),
                )

            current_writer.write_generated_cell(
                key=CellKey(level=record.level, node_id=record.node_id),
                star_count=record.star_count,
                write_payload=lambda target, identities=identities: (
                    builder.write_payload(identities, target)
                ),
            )
        close_current_writer()

    return write_manifest(
        out_dir,
        render_header.max_level,
        shard_entries,
        artifact_kind=SIDECAR_ARTIFACT_KIND,
        index_magic=SIDECAR_INDEX_MAGIC,
        mag_limit=render_header.mag_limit,
    )


def build_configured_sidecars(
    project: OctreeProject,
    *,
    family_name: str | None = None,
) -> Path:
    render_path = project.paths.render_output_path
    identifiers_order_path = project.paths.identifiers_order_output_path
    render_header = read_header(render_path)
    if render_header.artifact_kind != "render" or render_header.dataset_uuid is None:
        raise ValueError("Sidecars requires a render octree with dataset_uuid metadata")

    order_header = read_identifiers_order_header(identifiers_order_path)
    if order_header.parent_dataset_uuid != render_header.dataset_uuid:
        raise ValueError(
            "Identifiers/order artifact does not match render octree dataset_uuid"
        )

    selected = list(project.sidecars.families)
    if family_name is not None:
        selected = [cfg for cfg in selected if cfg.name == family_name]
        if not selected:
            raise ValueError(f"No sidecar configured for family: {family_name}")

    sidecar_descriptors: list[dict[str, str]] = []
    for config in selected:
        family_out = project.paths.sidecars_output_dir / f"{config.name}.octree"
        family_intermediate_dir = (
            project.paths.sidecars_work_dir / "intermediates" / config.name
        )
        family_manifest = _build_family_intermediates(
            project=project,
            config=config,
            render_header=render_header,
            order_path=identifiers_order_path,
            out_dir=family_intermediate_dir,
        )
        sidecar_uuid = uuid4()
        pack_octree(
            family_manifest,
            family_out,
            plan=PackingPlan(max_open_files=project.execution.max_open_files),
            descriptor=PackedDescriptorFields(
                artifact_kind="sidecar",
                parent_dataset_uuid=render_header.dataset_uuid,
                sidecar_uuid=sidecar_uuid,
                sidecar_kind=config.name,
            ),
        )
        sidecar_descriptors.append(
            {
                "name": config.name,
                "output_path": str(family_out),
                "parent_dataset_uuid": str(render_header.dataset_uuid),
                "sidecar_uuid": str(sidecar_uuid),
            }
        )

    return _write_sidecars_manifest(
        project.paths.sidecars_output_dir,
        render_octree_path=render_path,
        identifiers_order_path=identifiers_order_path,
        parent_dataset_uuid=render_header.dataset_uuid,
        sidecars=sidecar_descriptors,
    )
