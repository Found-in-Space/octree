# Stage 03: Sidecar Families

## Purpose

Stage 03 builds named sidecar artifacts from an already-published Stage 02 base dataset package.

Current inputs:

- `stars.octree`
- `identifiers.order`
- canonical enrichment inputs such as `identifiers_map.parquet`

Current outputs:

- one final sidecar octree per configured family under `paths.stage03_output_dir`
- one Stage 03 manifest at `paths.stage03_output_dir/manifest.json`

The first implemented family is `meta`.

## CLI

```bash
uv run fis-octree stage-03 --project project.toml
uv run fis-octree stage-03 --project project.toml --family meta
```

If `--family` is omitted, Stage 03 builds every configured family in `[[stage03.sidecars]]`.

## Project Configuration

Stage 03 reads:

- `paths.identifiers_map_path`
- `paths.stage02_output_path`
- `paths.identifiers_order_output_path`
- `paths.stage03_output_dir`
- `stage03.sidecars`

Each sidecar entry must include:

- `name`

The `meta` family may also include:

- `fields`

`fields` limits which enrichment columns from `identifiers_map.parquet` are emitted into the sidecar payload. `source` and `source_id` are always present.

## Validation

Stage 03 requires:

- a render octree whose descriptor carries `artifact_kind = render` and `dataset_uuid`
- an `identifiers.order` artifact whose `parent_dataset_uuid` matches that render `dataset_uuid`

Each built sidecar octree carries:

- `artifact_kind = sidecar`
- `parent_dataset_uuid`
- `sidecar_uuid`
- `sidecar_kind`

Readers should validate `parent_dataset_uuid` before accepting the sidecar.

## Stage 03 Manifest

`paths.stage03_output_dir/manifest.json` records:

- `format`
- `render_octree_path`
- `identifiers_order_path`
- `parent_dataset_uuid`
- `sidecars`

Each sidecar entry records:

- `name`
- `output_path`
- `parent_dataset_uuid`
- `sidecar_uuid`

## Rebuild Policy

Stage 03 sidecars are immutable derived artifacts.

When enrichment data changes:

1. keep the Stage 02 render octree unchanged
2. keep the Stage 02 `identifiers.order` artifact unchanged
3. rebuild the affected sidecar family
4. publish a new `sidecar_uuid`
