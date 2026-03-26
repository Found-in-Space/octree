from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from .formats import (
    DEFAULT_FLAGS,
    INDEX_FILE_HDR,
    INDEX_HEADER_SIZE,
    INDEX_MAGIC,
    INDEX_RECORD,
    INDEX_VERSION,
)
from .types import EncodedCell, ShardKey


def shard_filenames(shard: ShardKey) -> tuple[str, str]:
    if shard.prefix_bits == 0:
        base = f"level-{shard.level:02d}"
    else:
        base = f"level-{shard.level:02d}-p{shard.prefix_bits}-{shard.prefix}"
    return f"{base}.index", f"{base}.payload"


def meta_shard_filenames(shard: ShardKey) -> tuple[str, str]:
    idx, pay = shard_filenames(shard)
    return idx.replace(".index", ".meta-index"), pay.replace(
        ".payload", ".meta-payload"
    )


def belongs_to_shard(node_id: int, shard: ShardKey) -> bool:
    if shard.prefix_bits == 0 and shard.prefix == 0:
        return True
    return (node_id >> (3 * shard.level - shard.prefix_bits)) == shard.prefix


class IntermediateShardWriter:
    def __init__(
        self,
        shard: ShardKey,
        out_dir: Path,
        *,
        index_magic: bytes | None = None,
        filename_fn: Callable[[ShardKey], tuple[str, str]] | None = None,
        manifest_index_key: str = "index_path",
        manifest_payload_key: str = "payload_path",
    ) -> None:
        self._shard = shard
        self._out_dir = out_dir
        self._record_count = 0
        self._last_node_id: int | None = None
        self._closed = False
        self._index_magic = index_magic if index_magic is not None else INDEX_MAGIC
        self._filename_fn = filename_fn if filename_fn is not None else shard_filenames
        self._manifest_index_key = manifest_index_key
        self._manifest_payload_key = manifest_payload_key

        index_name, payload_name = self._filename_fn(shard)
        self._index_path = out_dir / index_name
        self._payload_path = out_dir / payload_name

        self._index_fp = open(self._index_path, "wb")  # noqa: SIM115
        self._payload_fp = open(self._payload_path, "wb")  # noqa: SIM115

        self._write_header(0)

    def _write_header(self, record_count: int) -> None:
        self._index_fp.seek(0)
        self._index_fp.write(
            INDEX_FILE_HDR.pack(
                self._index_magic,
                INDEX_VERSION,
                INDEX_HEADER_SIZE,
                self._shard.level,
                self._shard.prefix_bits,
                DEFAULT_FLAGS,
                INDEX_RECORD.size,
                self._shard.prefix,
                record_count,
            )
        )

    def write_cell(self, cell: EncodedCell) -> None:
        if cell.key.level != self._shard.level:
            raise ValueError(
                f"Level mismatch: cell level {cell.key.level} != "
                f"shard level {self._shard.level}"
            )
        if not belongs_to_shard(cell.key.node_id, self._shard):
            raise ValueError(
                f"node_id {cell.key.node_id} does not belong to shard "
                f"({self._shard.level}, p{self._shard.prefix_bits}, "
                f"{self._shard.prefix})"
            )
        if self._last_node_id is not None and cell.key.node_id <= self._last_node_id:
            raise ValueError(
                f"Non-monotonic node_id: {cell.key.node_id} <= {self._last_node_id}"
            )

        payload_offset = self._payload_fp.tell()
        self._payload_fp.write(cell.payload)

        self._index_fp.write(
            INDEX_RECORD.pack(
                cell.key.node_id,
                payload_offset,
                len(cell.payload),
                cell.star_count,
            )
        )
        self._record_count += 1
        self._last_node_id = cell.key.node_id

    def close(self) -> dict | None:
        if self._closed:
            return None
        self._closed = True

        if self._record_count == 0:
            self._index_fp.close()
            self._payload_fp.close()
            self._index_path.unlink(missing_ok=True)
            self._payload_path.unlink(missing_ok=True)
            return None

        self._write_header(self._record_count)
        self._index_fp.flush()
        self._payload_fp.flush()
        self._index_fp.close()
        self._payload_fp.close()

        index_name, payload_name = self._filename_fn(self._shard)
        return {
            "level": self._shard.level,
            "prefix_bits": self._shard.prefix_bits,
            "prefix": self._shard.prefix,
            self._manifest_index_key: index_name,
            self._manifest_payload_key: payload_name,
            "record_count": self._record_count,
        }

    def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._index_fp.close()
        self._payload_fp.close()
        self._index_path.unlink(missing_ok=True)
        self._payload_path.unlink(missing_ok=True)
