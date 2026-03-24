# found-in-space-octree

CLI tools for building octree pipeline artifacts from parquet star catalogs.

## CLI entrypoint

Use the installed CLI command:

```bash
uv run fis-octree --help
```

## Stage 00: enrich + sort

Stage 00 does two things:

1. Enrich input parquet files in-place with `morton_code`, `render`, and `level`
2. Write sorted bright/medium/faint outputs into a separate directory

### Basic usage

```bash
uv run fis-octree stage-00 data/raw-parquet data/stage00-sorted
```

### Common options

```bash
uv run fis-octree stage-00 data/raw-parquet data/stage00-sorted \
  --v-mag 6.5 \
  --max-level 13
```

```bash
uv run fis-octree stage-00 data/raw-parquet data/stage00-sorted \
  --force \
  --no-clear-output
```

## Stage 01: build intermediates

Stage 01 reads Stage 00 parquet files and produces:

- per-shard `.index` and `.payload` files
- `manifest.json`

### Basic usage

```bash
uv run fis-octree stage-01 "data/stage00-sorted/**/*.parquet" data/stage01-intermediates \
  --deep-shard-from-level 8
```

### With explicit shard tuning

```bash
uv run fis-octree stage-01 "data/stage00-sorted/**/*.parquet" data/stage01-intermediates \
  --max-level 13 \
  --deep-shard-from-level 8 \
  --deep-prefix-bits 3 \
  --batch-size 100000
```

## Help

Show command help:

```bash
uv run fis-octree --help
uv run fis-octree stage-00 --help
uv run fis-octree stage-01 --help
```
