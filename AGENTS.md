# Agent Instructions

## Python Tooling: Use `uv`

- Use `uv` for all Python dependency and environment operations in this repository.
- Do not use `pip`, `poetry`, or `conda` commands directly.

### Standard commands

- Sync/install dependencies: `uv sync`
- Run Python entrypoints/tools: `uv run <command>`
- Run tests: `uv run pytest`
- CI: on push/PR to `main`, GitHub Actions runs `uv run pytest` with coverage (see `.github/workflows/ci.yml`).
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`

### Pre-commit (Ruff)

- Install git hooks once: `uv run pre-commit install`
- Run on all files: `uv run pre-commit run --all-files`

Hooks: `ruff-check` (lint + fix) and `ruff-format`, scoped to `src/` and `tests/`.

### Examples

- `uv run fis-octree --help`
- `uv run pytest tests/test_stage02_cli.py`
