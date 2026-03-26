"""Streaming row source for Stage 01.

Produces ``(node_id, render, source, source_id)`` tuples ordered by
``(node_id, mag_abs, source_id)`` for one shard stream, using DuckDB to query
Stage 00 parquet output (see docs/sidecars.md R2).

Query shape (precomputed-render mode)::

    SELECT morton_code >> :shift AS node_id, render, source, source_id
    FROM   read_parquet(:glob)
    WHERE  level = :level
      AND  mag_abs IS NOT NULL
      [AND (morton_code >> :top_shift) = :prefix]   -- deep-sharded levels
    ORDER BY node_id, mag_abs, source_id
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb

from ..config import MORTON_BITS
from ..duckdb_util import configure_connection
from .types import ShardKey


def iter_sorted_rows(
    parquet_glob: str,
    *,
    level: int,
    shard: ShardKey,
    batch_size: int,
) -> Iterator[tuple[int, bytes, str, str]]:
    """Yield ``(node_id, render, source, source_id)`` in R2 sort order."""
    shift = 3 * (MORTON_BITS - level)
    escaped = parquet_glob.replace("'", "''")

    where_parts = [
        f"level = {level}",
        "mag_abs IS NOT NULL",
    ]
    if shard.prefix_bits > 0:
        top_shift = 3 * MORTON_BITS - shard.prefix_bits
        where_parts.append(f"(morton_code >> {top_shift}) = {shard.prefix}")

    where = " AND ".join(where_parts)
    query = (
        f"SELECT (morton_code >> {shift}) AS node_id, render, source, source_id "
        f"FROM read_parquet('{escaped}') "
        f"WHERE {where} "
        f"ORDER BY node_id, mag_abs, source_id"
    )

    con = duckdb.connect()
    configure_connection(con)
    try:
        con.execute(query)
        while True:
            batch = con.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                node_id, render, source, source_id = row
                yield (
                    int(node_id),
                    bytes(render),
                    str(source),
                    str(source_id),
                )
    finally:
        con.close()
