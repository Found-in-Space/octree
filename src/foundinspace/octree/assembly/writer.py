from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

from .formats import (
    DEFAULT_FLAGS,
    INDEX_FILE_HDR,
    INDEX_HEADER_SIZE,
    INDEX_MAGIC,
    INDEX_RECORD,
    INDEX_VERSION,
)
from .types import CellKey, EncodedCell, ShardKey


def _shard_base_name(shard: ShardKey) -> str:
    if shard.prefix_bits == 0:
        return f"level-{shard.level:02d}"
    return f"level-{shard.level:02d}-p{shard.prefix_bits}-{shard.prefix}"


def shard_filenames(shard: ShardKey) -> tuple[str, str]:
    base = _shard_base_name(shard)
    return f"{base}.index", f"{base}.payload"


def identifiers_shard_filenames(shard: ShardKey) -> tuple[str, str]:
    base = _shard_base_name(shard)
    return f"{base}.ident-index", f"{base}.ident-payload"


def sidecar_shard_filenames(kind: str):
    normalized = kind.strip()
    if not normalized:
        raise ValueError("sidecar kind must not be empty")

    def _filenames(shard: ShardKey) -> tuple[str, str]:
        base = _shard_base_name(shard)
        return f"{base}.{normalized}.index", f"{base}.{normalized}.payload"

    return _filenames


def meta_shard_filenames(shard: ShardKey) -> tuple[str, str]:
    return sidecar_shard_filenames("meta")(shard)


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
        self._validate_cell_key(cell.key)
        payload_offset = self._payload_fp.tell()
        self._payload_fp.write(cell.payload)
        self._write_index_record(
            key=cell.key,
            payload_offset=payload_offset,
            payload_length=len(cell.payload),
            star_count=cell.star_count,
        )

    def write_cell_payload_file(
        self,
        *,
        key: CellKey,
        payload_path: Path,
        star_count: int,
    ) -> None:
        self._validate_cell_key(key)
        payload_offset = self._payload_fp.tell()
        payload_length = 0
        with open(payload_path, "rb") as source:
            while chunk := source.read(1 << 20):
                self._payload_fp.write(chunk)
                payload_length += len(chunk)
        self._write_index_record(
            key=key,
            payload_offset=payload_offset,
            payload_length=payload_length,
            star_count=star_count,
        )

    def write_generated_cell(
        self,
        *,
        key: CellKey,
        star_count: int,
        write_payload: Callable[[BinaryIO], None],
    ) -> None:
        """Write a cell payload directly to the shard output stream."""
        self._validate_cell_key(key)
        payload_offset = self._payload_fp.tell()
        try:
            write_payload(self._payload_fp)
        except BaseException:
            self._payload_fp.seek(payload_offset)
            self._payload_fp.truncate()
            raise
        payload_length = self._payload_fp.tell() - payload_offset
        self._write_index_record(
            key=key,
            payload_offset=payload_offset,
            payload_length=payload_length,
            star_count=star_count,
        )

    def _validate_cell_key(self, key: CellKey) -> None:
        if key.level != self._shard.level:
            raise ValueError(
                f"Level mismatch: cell level {key.level} != "
                f"shard level {self._shard.level}"
            )
        if not belongs_to_shard(key.node_id, self._shard):
            raise ValueError(
                f"node_id {key.node_id} does not belong to shard "
                f"({self._shard.level}, p{self._shard.prefix_bits}, "
                f"{self._shard.prefix})"
            )
        if self._last_node_id is not None and key.node_id <= self._last_node_id:
            raise ValueError(
                f"Non-monotonic node_id: {key.node_id} <= {self._last_node_id}"
            )

    def _write_index_record(
        self,
        *,
        key: CellKey,
        payload_offset: int,
        payload_length: int,
        star_count: int,
    ) -> None:
        self._index_fp.write(
            INDEX_RECORD.pack(
                key.node_id,
                payload_offset,
                payload_length,
                star_count,
            )
        )
        self._record_count += 1
        self._last_node_id = key.node_id

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
