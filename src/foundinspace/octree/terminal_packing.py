from __future__ import annotations

import json
import os
import shutil
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from .config import MORTON_BITS

TERMINAL_MAP_FORMAT = "foundinspace.octree.terminal-map/v1"
TERMINAL_MAP_NAME = "terminal-map.json"
TERMINAL_MAP_DIR_NAME = "terminal-map"
TERMINAL_COUNTS_DB_NAME = "terminal-counts.sqlite3"


class TerminalMap:
    def __init__(self, manifest_path: Path):
        self.manifest_path = Path(manifest_path)
        raw = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        if raw.get("format") != TERMINAL_MAP_FORMAT:
            raise ValueError(f"Unsupported terminal map format: {raw.get('format')!r}")
        self.max_level = int(raw["max_level"])
        self.waterline = int(raw["waterline"])
        self.terminal_count = int(raw["terminal_count"])
        if self.max_level < 0 or self.max_level > MORTON_BITS:
            raise ValueError(f"Invalid terminal map max_level: {self.max_level}")
        if self.waterline <= 0:
            raise ValueError(f"Invalid terminal map waterline: {self.waterline}")
        if self.terminal_count < 0:
            raise ValueError(
                f"Invalid terminal map terminal_count: {self.terminal_count}"
            )
        level_entries = raw.get("levels")
        if not isinstance(level_entries, list):
            raise ValueError("Terminal map levels must be a list")
        self._by_level: dict[int, np.ndarray] = {}
        counted_terminals = 0
        for entry in level_entries:
            level = int(entry["level"])
            count = int(entry["count"])
            if level < 0 or level > self.max_level:
                raise ValueError(f"Invalid terminal map level: {level}")
            if level in self._by_level:
                raise ValueError(f"Duplicate terminal map level: {level}")
            if count <= 0:
                raise ValueError(
                    f"Invalid terminal map count at level {level}: {count}"
                )
            path = self.manifest_path.parent / str(entry["path"])
            if path.stat().st_size != count * np.dtype("<u8").itemsize:
                raise ValueError(f"Invalid terminal map byte length: {path}")
            nodes = np.memmap(
                path,
                dtype="<u8",
                mode="r",
                shape=(count,),
            )
            _validate_terminal_level(nodes, level=level, path=path)
            self._by_level[level] = nodes
            counted_terminals += count
        if counted_terminals != self.terminal_count:
            raise ValueError(
                "Terminal map count mismatch: "
                f"manifest={self.terminal_count}, levels={counted_terminals}"
            )

    @property
    def levels(self) -> tuple[int, ...]:
        return tuple(sorted(self._by_level))

    def contains(self, level: int, node_id: int) -> bool:
        nodes = self._by_level.get(int(level))
        if nodes is None or len(nodes) == 0:
            return False
        index = int(np.searchsorted(nodes, np.uint64(node_id)))
        return index < len(nodes) and int(nodes[index]) == int(node_id)

    def remap(
        self,
        levels: np.ndarray,
        node_ids: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        mapped_levels = np.asarray(levels, dtype=np.int16).copy()
        mapped_nodes = np.asarray(node_ids, dtype=np.uint64).copy()
        unresolved = np.ones(len(mapped_levels), dtype=np.bool_)
        for terminal_level in self.levels:
            candidates = np.flatnonzero(unresolved & (mapped_levels >= terminal_level))
            if len(candidates) == 0:
                continue
            shifts = (
                3 * (mapped_levels[candidates].astype(np.int64) - int(terminal_level))
            ).astype(np.uint64)
            ancestors = np.right_shift(mapped_nodes[candidates], shifts)
            terminals = self._by_level[terminal_level]
            positions = np.searchsorted(terminals, ancestors)
            in_bounds = positions < len(terminals)
            matches = np.zeros(len(candidates), dtype=np.bool_)
            if np.any(in_bounds):
                matches[in_bounds] = (
                    terminals[positions[in_bounds]] == ancestors[in_bounds]
                )
            selected = candidates[matches]
            mapped_levels[selected] = terminal_level
            mapped_nodes[selected] = ancestors[matches]
            unresolved[selected] = False
        return mapped_levels, mapped_nodes


def _validate_terminal_level(
    nodes: np.ndarray,
    *,
    level: int,
    path: Path,
) -> None:
    previous: int | None = None
    for start in range(0, len(nodes), 1_000_000):
        chunk = nodes[start : start + 1_000_000]
        first = int(chunk[0])
        if previous is not None and first <= previous:
            raise ValueError(f"Non-ascending terminal node IDs: {path}")
        if len(chunk) > 1 and np.any(chunk[1:] <= chunk[:-1]):
            raise ValueError(f"Non-ascending terminal node IDs: {path}")
        previous = int(chunk[-1])
    if previous is not None and previous >= 1 << (3 * level):
        raise ValueError(f"Terminal node ID exceeds level {level}: {path}")


def build_terminal_map(
    *,
    groups: Sequence[Any],
    work_dir: Path,
    artifacts_dir: Path,
    max_level: int,
    waterline: int,
    batch_size: int,
) -> Path:
    manifest_path = artifacts_dir / TERMINAL_MAP_NAME
    if _terminal_map_is_valid(
        manifest_path,
        max_level=max_level,
        waterline=waterline,
    ):
        return manifest_path

    database_path = work_dir / TERMINAL_COUNTS_DB_NAME
    connection = sqlite3.connect(database_path)
    try:
        _initialize_database(
            connection,
            max_level=max_level,
            waterline=waterline,
        )
        for group in groups:
            _count_group(
                connection,
                group,
                max_level=max_level,
                batch_size=batch_size,
            )
        expected_rows = sum(int(group.row_count) for group in groups)
        counted_rows = int(
            connection.execute(
                "SELECT COALESCE(SUM(star_count), 0) FROM own_counts"
            ).fetchone()[0]
        )
        if counted_rows != expected_rows:
            raise ValueError(
                "Terminal count map row mismatch: "
                f"expected={expected_rows}, actual={counted_rows}"
            )
        _calculate_subtrees_and_terminals(
            connection,
            max_level=max_level,
            waterline=waterline,
        )
        return _export_terminal_map(
            connection,
            artifacts_dir=artifacts_dir,
            max_level=max_level,
            waterline=waterline,
        )
    finally:
        connection.close()


def _initialize_database(
    connection: sqlite3.Connection,
    *,
    max_level: int,
    waterline: int,
) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS completed_groups (
            group_key TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            row_count INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS own_counts (
            level INTEGER NOT NULL,
            node_id INTEGER NOT NULL,
            star_count INTEGER NOT NULL,
            PRIMARY KEY (level, node_id)
        ) WITHOUT ROWID;
        """
    )
    expected = {
        "max_level": str(int(max_level)),
        "waterline": str(int(waterline)),
    }
    actual = dict(connection.execute("SELECT key, value FROM metadata"))
    if actual and actual != expected:
        connection.close()
        raise ValueError(
            "Terminal count database policy does not match the materialization plan"
        )
    with connection:
        connection.executemany(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
            expected.items(),
        )


def _count_group(
    connection: sqlite3.Connection,
    group: Any,
    *,
    max_level: int,
    batch_size: int,
) -> None:
    completed = connection.execute(
        "SELECT checksum, row_count FROM completed_groups WHERE group_key = ?",
        (str(group.key),),
    ).fetchone()
    if completed is not None:
        if completed != (str(group.checksum), int(group.row_count)):
            raise ValueError(
                f"Terminal count checkpoint mismatch for Stage 01 group {group.key}"
            )
        return

    counted_rows = 0
    with connection:
        for path in group.files:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(
                batch_size=batch_size,
                columns=["level", "morton_code"],
            ):
                source_levels = np.asarray(batch.column(0), dtype=np.int32)
                morton_codes = np.asarray(batch.column(1), dtype=np.uint64)
                if len(source_levels) == 0:
                    continue
                final_levels = np.minimum(source_levels, max_level).astype(
                    np.uint64,
                    copy=False,
                )
                final_nodes = np.empty(len(final_levels), dtype=np.uint64)
                for level_raw in np.unique(final_levels):
                    level = int(level_raw)
                    indices = np.flatnonzero(final_levels == level_raw)
                    final_nodes[indices] = morton_codes[indices] >> np.uint64(
                        3 * (MORTON_BITS - level)
                    )
                keys = np.empty(
                    len(final_nodes),
                    dtype=np.dtype([("level", "<u2"), ("node_id", "<u8")]),
                )
                keys["level"] = final_levels
                keys["node_id"] = final_nodes
                unique_keys, counts = np.unique(keys, return_counts=True)
                connection.executemany(
                    """
                    INSERT INTO own_counts(level, node_id, star_count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(level, node_id) DO UPDATE SET
                        star_count = star_count + excluded.star_count
                    """,
                    (
                        (
                            int(key["level"]),
                            int(key["node_id"]),
                            int(count),
                        )
                        for key, count in zip(unique_keys, counts, strict=True)
                    ),
                )
                counted_rows += len(final_levels)
        if counted_rows != int(group.row_count):
            raise ValueError(
                f"Terminal count row mismatch for Stage 01 group {group.key}: "
                f"expected={group.row_count}, actual={counted_rows}"
            )
        connection.execute(
            """
            INSERT INTO completed_groups(group_key, checksum, row_count)
            VALUES (?, ?, ?)
            """,
            (str(group.key), str(group.checksum), counted_rows),
        )


def _calculate_subtrees_and_terminals(
    connection: sqlite3.Connection,
    *,
    max_level: int,
    waterline: int,
) -> None:
    with connection:
        connection.executescript(
            """
            DROP TABLE IF EXISTS nodes;
            CREATE TABLE nodes (
                level INTEGER NOT NULL,
                node_id INTEGER NOT NULL,
                own_count INTEGER NOT NULL,
                subtree_count INTEGER NOT NULL,
                has_descendants INTEGER NOT NULL DEFAULT 0,
                terminal INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (level, node_id)
            ) WITHOUT ROWID;
            INSERT INTO nodes(level, node_id, own_count, subtree_count)
            SELECT level, node_id, star_count, star_count
            FROM own_counts;
            """
        )
        for child_level in range(max_level, 0, -1):
            parent_level = child_level - 1
            connection.execute(
                """
                INSERT INTO nodes(
                    level,
                    node_id,
                    own_count,
                    subtree_count,
                    has_descendants
                )
                SELECT ?, node_id >> 3, 0, SUM(subtree_count), 1
                FROM nodes
                WHERE level = ?
                GROUP BY node_id >> 3
                ON CONFLICT(level, node_id) DO UPDATE SET
                    subtree_count = nodes.own_count + excluded.subtree_count,
                    has_descendants = 1
                """,
                (parent_level, child_level),
            )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS nodes_terminal_idx "
            "ON nodes(terminal, level, node_id)"
        )
        for level in range(max_level + 1):
            connection.execute(
                """
                UPDATE nodes AS candidate
                SET terminal = 1
                WHERE candidate.level = ?
                  AND candidate.has_descendants = 1
                  AND candidate.subtree_count BETWEEN 1 AND ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM nodes AS ancestor
                      WHERE ancestor.terminal = 1
                        AND ancestor.level < candidate.level
                        AND ancestor.node_id = (
                            candidate.node_id >> (
                                3 * (candidate.level - ancestor.level)
                            )
                        )
                  )
                """,
                (level, waterline),
            )


def _export_terminal_map(
    connection: sqlite3.Connection,
    *,
    artifacts_dir: Path,
    max_level: int,
    waterline: int,
) -> Path:
    terminal_dir = artifacts_dir / TERMINAL_MAP_DIR_NAME
    if terminal_dir.exists():
        shutil.rmtree(terminal_dir)
    terminal_dir.mkdir(parents=True)

    levels: list[dict[str, Any]] = []
    terminal_count = 0
    for level in range(max_level + 1):
        count = int(
            connection.execute(
                "SELECT COUNT(*) FROM nodes WHERE level = ? AND terminal = 1",
                (level,),
            ).fetchone()[0]
        )
        if count == 0:
            continue
        filename = f"level-{level:02d}.u64"
        path = terminal_dir / filename
        temporary = terminal_dir / f".{filename}.tmp"
        with open(temporary, "wb") as fp:
            cursor = connection.execute(
                """
                SELECT node_id
                FROM nodes
                WHERE level = ? AND terminal = 1
                ORDER BY node_id
                """,
                (level,),
            )
            while rows := cursor.fetchmany(100_000):
                np.asarray([row[0] for row in rows], dtype="<u8").tofile(fp)
        os.replace(temporary, path)
        levels.append(
            {
                "level": level,
                "path": f"{TERMINAL_MAP_DIR_NAME}/{filename}",
                "count": count,
            }
        )
        terminal_count += count

    manifest = {
        "format": TERMINAL_MAP_FORMAT,
        "max_level": int(max_level),
        "waterline": int(waterline),
        "terminal_count": terminal_count,
        "levels": levels,
    }
    manifest_path = artifacts_dir / TERMINAL_MAP_NAME
    temporary_manifest = artifacts_dir / f".{TERMINAL_MAP_NAME}.tmp"
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_manifest, manifest_path)
    return manifest_path


def _terminal_map_is_valid(
    manifest_path: Path,
    *,
    max_level: int,
    waterline: int,
) -> bool:
    if not manifest_path.is_file():
        return False
    try:
        terminal_map = TerminalMap(manifest_path)
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        return terminal_map.max_level == int(max_level) and int(
            raw["waterline"]
        ) == int(waterline)
    except (KeyError, OSError, TypeError, ValueError):
        return False
