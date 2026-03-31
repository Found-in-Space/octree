from __future__ import annotations

import gzip
import shutil
import struct
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from .assembly.formats import INDEX_FILE_HDR, INDEX_RECORD
from .assembly.identity_encoder import decode_identity_rows
from .combine.lookup import FixedRecordFile
from .combine.manifest import read_combine_manifest

HEADER_FMT = struct.Struct("<4sHH16s16sQQQQQ")
HEADER_MAGIC = b"OIOR"
HEADER_VERSION = 1
HEADER_SIZE = HEADER_FMT.size
DIRECTORY_RECORD_FMT = struct.Struct("<H2xQIQQ")
DIRECTORY_RECORD_SIZE = DIRECTORY_RECORD_FMT.size


@dataclass(frozen=True, slots=True)
class IdentifiersOrderHeader:
    version: int
    parent_dataset_uuid: UUID
    artifact_uuid: UUID
    directory_offset: int
    directory_length: int
    payload_offset: int
    payload_length: int
    record_count: int


@dataclass(frozen=True, slots=True)
class IdentifiersOrderRecord:
    level: int
    node_id: int
    star_count: int
    payload_offset: int
    payload_length: int


def _pack_header(
    *,
    parent_dataset_uuid: UUID,
    artifact_uuid: UUID,
    directory_offset: int,
    directory_length: int,
    payload_offset: int,
    payload_length: int,
    record_count: int,
) -> bytes:
    return HEADER_FMT.pack(
        HEADER_MAGIC,
        HEADER_VERSION,
        HEADER_SIZE,
        parent_dataset_uuid.bytes,
        artifact_uuid.bytes,
        directory_offset,
        directory_length,
        payload_offset,
        payload_length,
        record_count,
    )


def read_header(path: Path) -> IdentifiersOrderHeader:
    with open(path, "rb") as fp:
        raw = fp.read(HEADER_SIZE)
    if len(raw) != HEADER_SIZE:
        raise ValueError("Identifiers/order file too small for header")
    (
        magic,
        version,
        header_size,
        parent_dataset_uuid_raw,
        artifact_uuid_raw,
        directory_offset,
        directory_length,
        payload_offset,
        payload_length,
        record_count,
    ) = HEADER_FMT.unpack(raw)
    if magic != HEADER_MAGIC:
        raise ValueError(f"Invalid identifiers/order magic: {magic!r}")
    if version != HEADER_VERSION:
        raise ValueError(f"Unsupported identifiers/order version: {version}")
    if header_size != HEADER_SIZE:
        raise ValueError(f"Unsupported identifiers/order header size: {header_size}")
    return IdentifiersOrderHeader(
        version=version,
        parent_dataset_uuid=UUID(bytes=parent_dataset_uuid_raw),
        artifact_uuid=UUID(bytes=artifact_uuid_raw),
        directory_offset=directory_offset,
        directory_length=directory_length,
        payload_offset=payload_offset,
        payload_length=payload_length,
        record_count=record_count,
    )


class IdentifiersOrderReader:
    def __init__(self, path: Path):
        self._path = Path(path)
        self.header = read_header(self._path)
        self._fp = open(self._path, "rb")  # noqa: SIM115

    def close(self) -> None:
        self._fp.close()

    def __enter__(self) -> IdentifiersOrderReader:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def iter_cells(self):
        self._fp.seek(self.header.directory_offset)
        for idx in range(self.header.record_count):
            raw = self._fp.read(DIRECTORY_RECORD_SIZE)
            if len(raw) != DIRECTORY_RECORD_SIZE:
                raise ValueError("Identifiers/order directory truncated")
            level, node_id, star_count, payload_offset, payload_length = (
                DIRECTORY_RECORD_FMT.unpack(raw)
            )
            payload_abs = self.header.payload_offset + payload_offset
            self._fp.seek(payload_abs)
            payload = self._fp.read(payload_length)
            if len(payload) != payload_length:
                raise ValueError("Identifiers/order payload truncated")
            identities = decode_identity_rows(payload, star_count=star_count)
            yield (
                IdentifiersOrderRecord(
                    level=level,
                    node_id=node_id,
                    star_count=star_count,
                    payload_offset=payload_offset,
                    payload_length=payload_length,
                ),
                identities,
            )
            next_dir_offset = (
                self.header.directory_offset + (idx + 1) * DIRECTORY_RECORD_SIZE
            )
            self._fp.seek(next_dir_offset)


def combine_identifiers_order(
    manifest_path: Path,
    output_path: Path,
    *,
    parent_dataset_uuid: UUID,
    artifact_uuid: UUID,
) -> None:
    manifest = read_combine_manifest(manifest_path)
    if manifest.artifact_kind != "identifiers":
        raise ValueError(
            f"Expected identifiers manifest, got {manifest.artifact_kind!r}"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload_tmp = output_path.with_name(f".{output_path.name}.payload.tmp")
    directory_tmp = output_path.with_name(f".{output_path.name}.directory.tmp")
    record_count = 0
    try:
        with open(payload_tmp, "wb") as payload_fp, open(directory_tmp, "wb") as dir_fp:
            for shard in manifest.shards:
                index_file = FixedRecordFile(
                    shard.index_path,
                    header_struct=INDEX_FILE_HDR,
                    record_struct=INDEX_RECORD,
                    magic=manifest.index_magic,
                )
                try:
                    with open(shard.payload_path, "rb") as shard_payload_fp:
                        for (
                            node_id,
                            pay_off,
                            pay_len,
                            star_count,
                        ) in index_file.iter_records():
                            shard_payload_fp.seek(pay_off)
                            compressed = shard_payload_fp.read(pay_len)
                            if len(compressed) != pay_len:
                                raise ValueError(
                                    f"Truncated identifiers intermediate payload for node {node_id}"
                                )
                            raw = gzip.decompress(compressed)
                            dir_fp.write(
                                DIRECTORY_RECORD_FMT.pack(
                                    shard.key.level,
                                    int(node_id),
                                    int(star_count),
                                    int(payload_fp.tell()),
                                    len(raw),
                                )
                            )
                            payload_fp.write(raw)
                            record_count += 1
                finally:
                    index_file.close()

        directory_length = directory_tmp.stat().st_size
        payload_length = payload_tmp.stat().st_size
        payload_offset = HEADER_SIZE + directory_length
        header = _pack_header(
            parent_dataset_uuid=parent_dataset_uuid,
            artifact_uuid=artifact_uuid,
            directory_offset=HEADER_SIZE,
            directory_length=directory_length,
            payload_offset=payload_offset,
            payload_length=payload_length,
            record_count=record_count,
        )
        with open(output_path, "wb") as out_fp:
            out_fp.write(header)
            with open(directory_tmp, "rb") as dir_fp:
                shutil.copyfileobj(dir_fp, out_fp)
            with open(payload_tmp, "rb") as payload_fp:
                shutil.copyfileobj(payload_fp, out_fp)
    finally:
        payload_tmp.unlink(missing_ok=True)
        directory_tmp.unlink(missing_ok=True)
