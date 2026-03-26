"""Metadata sidecar encoding (gzip JSON per cell). See docs/sidecars.md."""

from __future__ import annotations

import gzip
import json
import math
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .encoder import _flush_cell
from .types import EncodedCell

ALL_IDENTIFIER_FIELDS = [
    "gaia_source_id",
    "hip_id",
    "hd",
    "bayer",
    "flamsteed",
    "constellation",
    "proper_name",
]
INTEGER_FIELDS = frozenset({"gaia_source_id", "hip_id", "hd", "flamsteed"})
STRING_FIELDS = frozenset({"bayer", "constellation", "proper_name"})


def _is_empty_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if pd.isna(value):
        return True
    return isinstance(value, str) and value.strip() == ""


def _ordered_identifier_entry(
    row: pd.Series,
    use_set: frozenset[str],
) -> dict[str, Any]:
    """Fields appear in ALL_IDENTIFIER_FIELDS order (deterministic JSON keys)."""
    out: dict[str, Any] = {}
    for name in ALL_IDENTIFIER_FIELDS:
        if name not in use_set or name not in row.index:
            continue
        val = row[name]
        if _is_empty_value(val):
            continue
        if name in INTEGER_FIELDS:
            try:
                out[name] = int(val)
            except (TypeError, ValueError):
                continue
        elif name in STRING_FIELDS:
            out[name] = str(val).strip()
        else:
            out[name] = val
    return out


class IdentifiersMap:
    """Lookup keyed by (source, source_id) from identifiers_map.parquet."""

    def __init__(
        self,
        parquet_path: Path,
        *,
        fields: list[str] | None = None,
    ) -> None:
        path = Path(parquet_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"Identifiers map not found: {path}")

        use_list = list(ALL_IDENTIFIER_FIELDS if fields is None else fields)
        unknown = set(use_list) - set(ALL_IDENTIFIER_FIELDS)
        if unknown:
            raise ValueError(f"Unknown sidecar identifier field(s): {sorted(unknown)}")
        use_set = frozenset(use_list)

        table = pq.read_table(path)
        df = table.to_pandas()
        for col in ("source", "source_id"):
            if col not in df.columns:
                raise ValueError(f"Identifiers map missing required column: {col}")

        self._data: dict[tuple[str, str], dict[str, Any]] = {}
        for _, row in df.iterrows():
            src = str(row["source"])
            sid = str(row["source_id"])
            entry = _ordered_identifier_entry(row, use_set)
            self._data[(src, sid)] = entry

    def __len__(self) -> int:
        return len(self._data)

    def lookup(self, source: str, source_id: str) -> dict[str, Any]:
        return dict(self._data.get((source, source_id), {}))


def build_meta_payload(
    identities: list[tuple[str, str]],
    ident_map: IdentifiersMap,
) -> bytes:
    """gzip(JSON array of per-star objects) in cell order."""
    entries = []
    for source, source_id in identities:
        # Always include the canonical identity key for every star.
        entry: dict[str, Any] = {
            "source": str(source),
            "source_id": str(source_id),
        }
        entry.update(ident_map.lookup(source, source_id))
        entries.append(entry)
    raw = json.dumps(entries, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw)


def iter_encoded_cells_with_meta(
    rows: Iterator[tuple[int, bytes, str, str]],
    level: int,
    ident_map: IdentifiersMap,
) -> Iterator[tuple[EncodedCell, bytes]]:
    """Like ``iter_encoded_cells`` but also emit gzip JSON meta blob per cell."""
    current_node_id: int | None = None
    current_renders: list[bytes] = []
    current_identities: list[tuple[str, str]] = []

    for node_id, render, source, source_id in rows:
        if current_node_id is not None and node_id != current_node_id:
            render_cell = _flush_cell(level, current_node_id, current_renders)
            meta_blob = build_meta_payload(current_identities, ident_map)
            yield render_cell, meta_blob
            current_renders = []
            current_identities = []
        current_node_id = node_id
        current_renders.append(render)
        current_identities.append((source, source_id))

    if current_node_id is not None and current_renders:
        render_cell = _flush_cell(level, current_node_id, current_renders)
        meta_blob = build_meta_payload(current_identities, ident_map)
        yield render_cell, meta_blob
