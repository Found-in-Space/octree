"""Binary format primitives for the v1 exact identity locator."""

from __future__ import annotations

import gzip
import hashlib
import struct
from dataclasses import dataclass
from uuid import UUID

HEADER_MAGIC = b"OILR"
PAGE_MAGIC = b"OILP"
FOOTER_MAGIC = b"OILF"
FORMAT_VERSION = 1

HEADER_FMT = struct.Struct("<4sHHIQ16s16s16sQQIIII32sQQ12x")
NAMESPACE_FMT = struct.Struct("<16sHHHHQQQQQQQII32s8x")
PAGE_HEADER_FMT = struct.Struct("<4sHHBBHIIII32s4x")
FOOTER_FMT = struct.Struct("<4sHHQ32s16x")
LEAF_RECORD_FMT = struct.Struct("<QII")
CHILD_RECORD_FMT = struct.Struct("<QQQ")

HEADER_SIZE = 160
NAMESPACE_SIZE = 128
PAGE_HEADER_SIZE = 64
FOOTER_SIZE = 64

assert HEADER_FMT.size == HEADER_SIZE
assert NAMESPACE_FMT.size == NAMESPACE_SIZE
assert PAGE_HEADER_FMT.size == PAGE_HEADER_SIZE
assert FOOTER_FMT.size == FOOTER_SIZE

PAGE_KIND_LEAF = 1
PAGE_KIND_INTERNAL = 2
CODEC_NONE = 0
CODEC_GZIP = 1
GZIP_COMPRESSLEVEL = 1
KEY_CODEC_U64_DECIMAL = 1
VALUE_CODEC_CELL_U32_ORDINAL_U32 = 1

CODEC_NAMES = {CODEC_NONE: "none", CODEC_GZIP: "gzip"}
CODEC_CODES = {value: key for key, value in CODEC_NAMES.items()}


@dataclass(frozen=True, slots=True)
class IdentityLocatorHeader:
    total_length: int
    locator_uuid: UUID
    parent_dataset_uuid: UUID
    identifiers_order_uuid: UUID
    namespace_directory_offset: int
    namespace_directory_length: int
    namespace_count: int
    decoded_page_size: int
    navigation_codec_mask: int
    leaf_codec_mask: int
    build_identity: bytes
    integrity_offset: int
    integrity_length: int


@dataclass(frozen=True, slots=True)
class NamespaceDescriptor:
    name: str
    key_codec: int
    value_codec: int
    leaf_codec: int
    flags: int
    record_count: int
    root_offset: int
    root_length: int
    minimum_key: int
    maximum_key: int
    leaf_page_count: int
    navigation_page_count: int
    decoded_record_size: int
    content_checksum: bytes


@dataclass(frozen=True, slots=True)
class DecodedPage:
    kind: int
    codec: int
    entry_count: int
    decoded: bytes


def pack_header(header: IdentityLocatorHeader) -> bytes:
    if len(header.build_identity) != 32:
        raise ValueError("Identity locator build identity must contain 32 bytes")
    return HEADER_FMT.pack(
        HEADER_MAGIC,
        FORMAT_VERSION,
        HEADER_SIZE,
        0,
        header.total_length,
        header.locator_uuid.bytes,
        header.parent_dataset_uuid.bytes,
        header.identifiers_order_uuid.bytes,
        header.namespace_directory_offset,
        header.namespace_directory_length,
        header.namespace_count,
        header.decoded_page_size,
        header.navigation_codec_mask,
        header.leaf_codec_mask,
        header.build_identity,
        header.integrity_offset,
        header.integrity_length,
    )


def unpack_header(raw: bytes) -> IdentityLocatorHeader:
    if len(raw) != HEADER_SIZE:
        raise ValueError(
            f"Invalid identity locator header size: {len(raw)} != {HEADER_SIZE}"
        )
    (
        magic,
        version,
        header_size,
        _flags,
        total_length,
        locator_uuid,
        parent_dataset_uuid,
        identifiers_order_uuid,
        namespace_directory_offset,
        namespace_directory_length,
        namespace_count,
        decoded_page_size,
        navigation_codec_mask,
        leaf_codec_mask,
        build_identity,
        integrity_offset,
        integrity_length,
    ) = HEADER_FMT.unpack(raw)
    if magic != HEADER_MAGIC:
        raise ValueError(f"Invalid identity locator magic: {magic!r}")
    if version != FORMAT_VERSION:
        raise ValueError(f"Unsupported identity locator version: {version}")
    if header_size != HEADER_SIZE:
        raise ValueError(f"Unsupported identity locator header size: {header_size}")
    if total_length < HEADER_SIZE + FOOTER_SIZE:
        raise ValueError("Identity locator total length is too small")
    if decoded_page_size < LEAF_RECORD_FMT.size:
        raise ValueError("Identity locator decoded page size is too small")
    return IdentityLocatorHeader(
        total_length=int(total_length),
        locator_uuid=UUID(bytes=locator_uuid),
        parent_dataset_uuid=UUID(bytes=parent_dataset_uuid),
        identifiers_order_uuid=UUID(bytes=identifiers_order_uuid),
        namespace_directory_offset=int(namespace_directory_offset),
        namespace_directory_length=int(namespace_directory_length),
        namespace_count=int(namespace_count),
        decoded_page_size=int(decoded_page_size),
        navigation_codec_mask=int(navigation_codec_mask),
        leaf_codec_mask=int(leaf_codec_mask),
        build_identity=bytes(build_identity),
        integrity_offset=int(integrity_offset),
        integrity_length=int(integrity_length),
    )


def pack_namespace(descriptor: NamespaceDescriptor) -> bytes:
    name = descriptor.name.encode("ascii")
    if not name or len(name) > 15:
        raise ValueError("Identity namespace names must be 1..15 ASCII bytes")
    if len(descriptor.content_checksum) != 32:
        raise ValueError("Identity namespace checksum must contain 32 bytes")
    return NAMESPACE_FMT.pack(
        name.ljust(16, b"\x00"),
        descriptor.key_codec,
        descriptor.value_codec,
        descriptor.leaf_codec,
        descriptor.flags,
        descriptor.record_count,
        descriptor.root_offset,
        descriptor.root_length,
        descriptor.minimum_key,
        descriptor.maximum_key,
        descriptor.leaf_page_count,
        descriptor.navigation_page_count,
        descriptor.decoded_record_size,
        0,
        descriptor.content_checksum,
    )


def unpack_namespace(raw: bytes) -> NamespaceDescriptor:
    if len(raw) != NAMESPACE_SIZE:
        raise ValueError("Invalid identity namespace descriptor size")
    (
        name_raw,
        key_codec,
        value_codec,
        leaf_codec,
        flags,
        record_count,
        root_offset,
        root_length,
        minimum_key,
        maximum_key,
        leaf_page_count,
        navigation_page_count,
        decoded_record_size,
        _reserved,
        content_checksum,
    ) = NAMESPACE_FMT.unpack(raw)
    try:
        name = name_raw.split(b"\x00", 1)[0].decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("Identity namespace name is not ASCII") from exc
    if not name:
        raise ValueError("Identity namespace name is empty")
    if key_codec != KEY_CODEC_U64_DECIMAL:
        raise ValueError(f"Unsupported identity namespace key codec: {key_codec}")
    if value_codec != VALUE_CODEC_CELL_U32_ORDINAL_U32:
        raise ValueError(f"Unsupported identity namespace value codec: {value_codec}")
    if leaf_codec not in CODEC_NAMES:
        raise ValueError(f"Unsupported identity namespace leaf codec: {leaf_codec}")
    if decoded_record_size != LEAF_RECORD_FMT.size:
        raise ValueError(
            f"Unsupported identity namespace record size: {decoded_record_size}"
        )
    return NamespaceDescriptor(
        name=name,
        key_codec=int(key_codec),
        value_codec=int(value_codec),
        leaf_codec=int(leaf_codec),
        flags=int(flags),
        record_count=int(record_count),
        root_offset=int(root_offset),
        root_length=int(root_length),
        minimum_key=int(minimum_key),
        maximum_key=int(maximum_key),
        leaf_page_count=int(leaf_page_count),
        navigation_page_count=int(navigation_page_count),
        decoded_record_size=int(decoded_record_size),
        content_checksum=bytes(content_checksum),
    )


def pack_page(*, kind: int, codec: int, entry_count: int, decoded: bytes) -> bytes:
    if kind not in (PAGE_KIND_LEAF, PAGE_KIND_INTERNAL):
        raise ValueError(f"Unsupported identity locator page kind: {kind}")
    if codec == CODEC_NONE:
        encoded = decoded
    elif codec == CODEC_GZIP:
        encoded = gzip.compress(
            decoded,
            compresslevel=GZIP_COMPRESSLEVEL,
            mtime=0,
        )
    else:
        raise ValueError(f"Unsupported identity locator page codec: {codec}")
    checksum = hashlib.sha256(encoded).digest()
    header = PAGE_HEADER_FMT.pack(
        PAGE_MAGIC,
        FORMAT_VERSION,
        PAGE_HEADER_SIZE,
        kind,
        codec,
        0,
        entry_count,
        len(decoded),
        len(encoded),
        0,
        checksum,
    )
    return header + encoded


def unpack_page(raw: bytes) -> DecodedPage:
    if len(raw) < PAGE_HEADER_SIZE:
        raise ValueError("Identity locator page is truncated")
    (
        magic,
        version,
        header_size,
        kind,
        codec,
        _flags,
        entry_count,
        decoded_length,
        encoded_length,
        _reserved,
        checksum,
    ) = PAGE_HEADER_FMT.unpack(raw[:PAGE_HEADER_SIZE])
    if magic != PAGE_MAGIC:
        raise ValueError(f"Invalid identity locator page magic: {magic!r}")
    if version != FORMAT_VERSION or header_size != PAGE_HEADER_SIZE:
        raise ValueError("Unsupported identity locator page version or header size")
    if kind not in (PAGE_KIND_LEAF, PAGE_KIND_INTERNAL):
        raise ValueError(f"Unsupported identity locator page kind: {kind}")
    if codec not in CODEC_NAMES:
        raise ValueError(f"Unsupported identity locator page codec: {codec}")
    if len(raw) != PAGE_HEADER_SIZE + encoded_length:
        raise ValueError("Identity locator page encoded length mismatch")
    encoded = raw[PAGE_HEADER_SIZE:]
    if hashlib.sha256(encoded).digest() != checksum:
        raise ValueError("Identity locator page checksum mismatch")
    if codec == CODEC_NONE:
        decoded = encoded
    else:
        try:
            decoded = gzip.decompress(encoded)
        except (EOFError, OSError) as exc:
            raise ValueError("Identity locator gzip page is invalid") from exc
    if len(decoded) != decoded_length:
        raise ValueError("Identity locator page decoded length mismatch")
    record_size = (
        LEAF_RECORD_FMT.size if kind == PAGE_KIND_LEAF else CHILD_RECORD_FMT.size
    )
    if decoded_length != entry_count * record_size:
        raise ValueError("Identity locator page entry count mismatch")
    return DecodedPage(
        kind=int(kind),
        codec=int(codec),
        entry_count=int(entry_count),
        decoded=decoded,
    )


def pack_footer(*, hashed_length: int, prefix_checksum: bytes) -> bytes:
    if len(prefix_checksum) != 32:
        raise ValueError("Identity locator footer checksum must contain 32 bytes")
    return FOOTER_FMT.pack(
        FOOTER_MAGIC,
        FORMAT_VERSION,
        FOOTER_SIZE,
        hashed_length,
        prefix_checksum,
    )


def unpack_footer(raw: bytes) -> tuple[int, bytes]:
    if len(raw) != FOOTER_SIZE:
        raise ValueError("Identity locator footer is truncated")
    magic, version, footer_size, hashed_length, checksum = FOOTER_FMT.unpack(raw)
    if magic != FOOTER_MAGIC:
        raise ValueError(f"Invalid identity locator footer magic: {magic!r}")
    if version != FORMAT_VERSION or footer_size != FOOTER_SIZE:
        raise ValueError("Unsupported identity locator footer")
    return int(hashed_length), bytes(checksum)
