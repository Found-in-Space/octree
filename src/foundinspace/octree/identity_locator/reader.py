"""Local and strict HTTP range reader for exact identity locators."""

from __future__ import annotations

import gzip
import hashlib
import re
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import UUID

import numpy as np

from ..assembly.identity_encoder import decode_identity_rows
from ..identifiers_order import (
    DIRECTORY_RECORD_FMT,
    DIRECTORY_RECORD_SIZE,
    IdentifiersOrderHeader,
    IdentifiersOrderRecord,
)
from ..identifiers_order import (
    HEADER_SIZE as IDENTIFIERS_HEADER_SIZE,
)
from ..identifiers_order import (
    unpack_header as unpack_identifiers_header,
)
from ..reader.source import OctreeSource
from .format import (
    CODEC_NONE,
    FOOTER_SIZE,
    HEADER_SIZE,
    NAMESPACE_SIZE,
    PAGE_KIND_INTERNAL,
    PAGE_KIND_LEAF,
    NamespaceDescriptor,
    unpack_footer,
    unpack_header,
    unpack_namespace,
    unpack_page,
)
from .leaf import decode_compact_leaf, lookup_compact_leaf

_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_MAX_U64 = 2**64 - 1
_CHILD_DTYPE = np.dtype([("maximum", "<u8"), ("offset", "<u8"), ("length", "<u8")])


@dataclass(frozen=True, slots=True)
class StarRef:
    dataset_uuid: UUID
    level: int
    morton_code: int
    ordinal: int


class _RangeSource(Protocol):
    size: int

    def read_range(self, offset: int, length: int) -> bytes: ...

    def close(self) -> None: ...


class _LocalRangeSource:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.size = path.stat().st_size
        self._fp = open(path, "rb")  # noqa: SIM115

    def read_range(self, offset: int, length: int) -> bytes:
        _validate_range(offset, length, self.size)
        self._fp.seek(offset)
        raw = self._fp.read(length)
        if len(raw) != length:
            raise ValueError(f"Range source is truncated at {offset}+{length}")
        return raw

    def close(self) -> None:
        self._fp.close()


class _StrictHttpRangeSource:
    def __init__(self, url: str) -> None:
        self.url = url
        self.size = -1
        self._validator_name: str | None = None
        self._validator_value: str | None = None

    def read_range(self, offset: int, length: int) -> bytes:
        if offset < 0 or length <= 0:
            raise ValueError("HTTP ranges require non-negative offsets and length > 0")
        if self.size >= 0:
            _validate_range(offset, length, self.size)
        end = offset + length - 1
        headers = {"Range": f"bytes={offset}-{end}", "Accept-Encoding": "identity"}
        if self._validator_name == "ETag" and self._validator_value is not None:
            headers["If-Match"] = self._validator_value
        elif (
            self._validator_name == "Last-Modified"
            and self._validator_value is not None
        ):
            headers["If-Unmodified-Since"] = self._validator_value
        request = Request(self.url, headers=headers)
        with urlopen(request) as response:
            status = getattr(response, "status", None)
            if status != 206:
                raise ValueError(
                    f"HTTP identity source ignored the byte range: status={status}"
                )
            response_headers = getattr(response, "headers", {})
            content_encoding = _header_value(response_headers, "Content-Encoding")
            if content_encoding not in (None, "", "identity"):
                raise ValueError(
                    "HTTP identity source must use Content-Encoding: identity"
                )
            content_range = _header_value(response_headers, "Content-Range")
            match = _CONTENT_RANGE_RE.fullmatch(content_range or "")
            if match is None:
                raise ValueError("HTTP identity source omitted a valid Content-Range")
            got_start, got_end, got_size = (int(value) for value in match.groups())
            if got_start != offset or got_end != end:
                raise ValueError("HTTP identity source returned the wrong byte range")
            if self.size < 0:
                self.size = got_size
            elif self.size != got_size:
                raise ValueError("HTTP identity source object length changed")
            self._observe_validator(response_headers)
            raw = response.read()
        if len(raw) != length:
            raise ValueError("HTTP identity range response is truncated")
        return raw

    def _observe_validator(self, headers: object) -> None:
        etag = _header_value(headers, "ETag")
        modified = _header_value(headers, "Last-Modified")
        name, value = ("ETag", etag) if etag else ("Last-Modified", modified)
        if not value:
            raise ValueError("HTTP identity source has no stable validator")
        if name == "ETag" and value.startswith("W/"):
            raise ValueError("HTTP identity source ETag must be strong")
        if self._validator_value is None:
            self._validator_name = name
            self._validator_value = value
        elif self._validator_name != name or self._validator_value != value:
            raise ValueError("HTTP identity source validator changed")

    def close(self) -> None:
        return None


class _CachedRangeSource:
    def __init__(self, source: _RangeSource, *, max_entries: int = 128) -> None:
        self._source = source
        self.size = source.size
        self._max_entries = max_entries
        self._cache: OrderedDict[tuple[int, int], bytes] = OrderedDict()
        self.fetched_bytes = 0
        self.request_count = 0

    def read_range(self, offset: int, length: int) -> bytes:
        key = (offset, length)
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached
        raw = self._source.read_range(offset, length)
        self.size = self._source.size
        self.fetched_bytes += len(raw)
        self.request_count += 1
        self._cache[key] = raw
        while len(self._cache) > self._max_entries:
            self._cache.popitem(last=False)
        return raw

    def close(self) -> None:
        self._cache.clear()
        self._source.close()


def _header_value(headers: object, name: str) -> str | None:
    getter = getattr(headers, "get", None)
    if getter is None:
        return None
    value = getter(name)
    return str(value) if value is not None else None


def _is_url(source: OctreeSource) -> bool:
    if not isinstance(source, str):
        return False
    parsed = urlparse(source.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _open_range_source(source: OctreeSource) -> _CachedRangeSource:
    if _is_url(source):
        return _CachedRangeSource(_StrictHttpRangeSource(str(source).strip()))
    path = source if isinstance(source, Path) else Path(str(source))
    return _CachedRangeSource(_LocalRangeSource(path.expanduser()))


def _validate_range(offset: int, length: int, total: int) -> None:
    if offset < 0 or length <= 0:
        raise ValueError("Range offsets must be >= 0 and lengths must be > 0")
    if offset > total or length > total - offset:
        raise ValueError(f"Range {offset}+{length} exceeds object length {total}")


def _parse_u64(value: int | str) -> int:
    if isinstance(value, bool):
        raise ValueError("Identity source ID must be an unsigned decimal integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        if not value or not value.isascii() or not value.isdecimal():
            raise ValueError("Identity source ID must be an unsigned decimal integer")
        if len(value) > 1 and value.startswith("0"):
            raise ValueError("Identity source ID must not contain leading zeroes")
        parsed = int(value)
    else:
        raise TypeError("Identity source ID must be int or str")
    if parsed < 0 or parsed > _MAX_U64:
        raise ValueError("Identity source ID is outside uint64")
    return parsed


def _read_identifiers_header(source: _CachedRangeSource) -> IdentifiersOrderHeader:
    raw = source.read_range(0, IDENTIFIERS_HEADER_SIZE)
    header = unpack_identifiers_header(raw)
    if header.directory_length != header.record_count * DIRECTORY_RECORD_SIZE:
        raise ValueError("Identifiers/order directory length is inconsistent")
    if header.directory_offset + header.directory_length > source.size:
        raise ValueError("Identifiers/order directory exceeds object length")
    if header.payload_offset + header.payload_length > source.size:
        raise ValueError("Identifiers/order payload exceeds object length")
    return header


class IdentityLocatorReader:
    """Resolve exact canonical identities through bounded positional reads."""

    def __init__(
        self,
        locator_source: OctreeSource,
        identifiers_order_source: OctreeSource,
    ) -> None:
        self._locator = _open_range_source(locator_source)
        self._identifiers = _open_range_source(identifiers_order_source)
        try:
            self.header = unpack_header(self._locator.read_range(0, HEADER_SIZE))
            if (
                self._locator.size >= 0
                and self.header.total_length != self._locator.size
            ):
                raise ValueError("Identity locator object length does not match header")
            self._validate_header_ranges()
            directory = self._locator.read_range(
                self.header.namespace_directory_offset,
                self.header.namespace_directory_length,
            )
            if self.header.namespace_directory_length != (
                self.header.namespace_count * NAMESPACE_SIZE
            ):
                raise ValueError("Identity namespace directory length is inconsistent")
            descriptors = [
                unpack_namespace(directory[offset : offset + NAMESPACE_SIZE])
                for offset in range(0, len(directory), NAMESPACE_SIZE)
            ]
            self.namespaces = {
                descriptor.name: descriptor for descriptor in descriptors
            }
            if len(self.namespaces) != len(descriptors):
                raise ValueError("Identity locator contains duplicate namespaces")
            self.identifiers_header = _read_identifiers_header(self._identifiers)
            if (
                self.identifiers_header.artifact_uuid
                != self.header.identifiers_order_uuid
            ):
                raise ValueError(
                    "Identity locator does not match identifiers/order artifact UUID"
                )
            if (
                self.identifiers_header.parent_dataset_uuid
                != self.header.parent_dataset_uuid
            ):
                raise ValueError(
                    "Identity locator and identifiers/order parent datasets differ"
                )
            for descriptor in descriptors:
                if descriptor.record_count:
                    self._validate_locator_range(
                        descriptor.root_offset, descriptor.root_length
                    )
                elif descriptor.root_offset or descriptor.root_length:
                    raise ValueError("Empty identity namespace has a root page")
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> IdentityLocatorReader:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if hasattr(self, "_locator"):
            self._locator.close()
        if hasattr(self, "_identifiers"):
            self._identifiers.close()

    @property
    def range_metrics(self) -> dict[str, int]:
        """Return physical bytes and finite reads performed by this reader."""
        return {
            "locator_bytes": self._locator.fetched_bytes,
            "locator_requests": self._locator.request_count,
            "identifiers_order_bytes": self._identifiers.fetched_bytes,
            "identifiers_order_requests": self._identifiers.request_count,
            "total_bytes": (
                self._locator.fetched_bytes + self._identifiers.fetched_bytes
            ),
            "total_requests": (
                self._locator.request_count + self._identifiers.request_count
            ),
        }

    def _validate_header_ranges(self) -> None:
        if self.header.namespace_directory_offset < HEADER_SIZE:
            raise ValueError("Identity namespace directory overlaps its header")
        if (
            self.header.namespace_directory_offset
            + self.header.namespace_directory_length
            > self.header.integrity_offset
        ):
            raise ValueError("Identity namespace directory exceeds content bounds")
        if self.header.integrity_length != FOOTER_SIZE:
            raise ValueError("Unsupported identity locator integrity footer size")
        if (
            self.header.integrity_offset + self.header.integrity_length
            != self.header.total_length
        ):
            raise ValueError("Identity locator integrity range is inconsistent")
        footer = self._locator.read_range(
            self.header.integrity_offset, self.header.integrity_length
        )
        hashed_length, _checksum = unpack_footer(footer)
        if hashed_length != self.header.integrity_offset:
            raise ValueError("Identity locator footer hash range is inconsistent")

    def _validate_locator_range(self, offset: int, length: int) -> None:
        if offset < HEADER_SIZE or length <= 0:
            raise ValueError("Identity locator page range is invalid")
        if offset > self.header.integrity_offset or length > (
            self.header.integrity_offset - offset
        ):
            raise ValueError("Identity locator page exceeds content bounds")

    def lookup(self, source: str, source_id: int | str) -> StarRef | None:
        namespace = str(source).strip().lower()
        descriptor = self.namespaces.get(namespace)
        if descriptor is None:
            raise ValueError(f"Identity namespace is not present: {namespace!r}")
        key = _parse_u64(source_id)
        if not descriptor.record_count:
            return None
        if key < descriptor.minimum_key or key > descriptor.maximum_key:
            return None
        offset = descriptor.root_offset
        length = descriptor.root_length
        for _depth in range(64):
            self._validate_locator_range(offset, length)
            page = unpack_page(self._locator.read_range(offset, length))
            if page.kind == PAGE_KIND_INTERNAL:
                offset, length = self._select_child(page.decoded, key)
                continue
            if page.kind != PAGE_KIND_LEAF:
                raise ValueError("Identity locator traversal reached an unknown page")
            if page.codec != descriptor.leaf_codec:
                raise ValueError("Identity locator leaf codec differs from namespace")
            location = self._find_leaf_record(
                page.decoded,
                key,
                entry_count=page.entry_count,
            )
            if location is None:
                return None
            cell_record, ordinal = location
            order_record = self._read_directory_record(cell_record)
            if ordinal >= order_record.star_count:
                raise ValueError("Identity locator ordinal exceeds render cell")
            return StarRef(
                dataset_uuid=self.header.parent_dataset_uuid,
                level=order_record.level,
                morton_code=order_record.node_id,
                ordinal=ordinal,
            )
        raise ValueError("Identity locator navigation depth exceeds the v1 bound")

    @staticmethod
    def _select_child(decoded: bytes, key: int) -> tuple[int, int]:
        children = np.frombuffer(decoded, dtype=_CHILD_DTYPE)
        maximums = children["maximum"]
        if len(maximums) > 1 and np.any(maximums[1:] <= maximums[:-1]):
            raise ValueError("Identity locator child fences are not increasing")
        index = int(np.searchsorted(maximums, np.uint64(key), side="left"))
        if index >= len(children):
            raise ValueError("Identity locator navigation has no candidate child")
        return int(children["offset"][index]), int(children["length"][index])

    @staticmethod
    def _find_leaf_record(
        decoded: bytes,
        key: int,
        *,
        entry_count: int,
    ) -> tuple[int, int] | None:
        return lookup_compact_leaf(
            decoded,
            entry_count=entry_count,
            source_id=key,
        )

    def _read_directory_record(self, record_index: int) -> IdentifiersOrderRecord:
        if record_index < 0 or record_index >= self.identifiers_header.record_count:
            raise ValueError("Identity locator cell record is out of range")
        offset = (
            self.identifiers_header.directory_offset
            + record_index * DIRECTORY_RECORD_SIZE
        )
        raw = self._identifiers.read_range(offset, DIRECTORY_RECORD_SIZE)
        level, node_id, star_count, payload_offset, payload_length = (
            DIRECTORY_RECORD_FMT.unpack(raw)
        )
        if payload_offset > self.identifiers_header.payload_length or payload_length > (
            self.identifiers_header.payload_length - payload_offset
        ):
            raise ValueError("Identifiers/order payload range is invalid")
        return IdentifiersOrderRecord(
            level=int(level),
            node_id=int(node_id),
            star_count=int(star_count),
            payload_offset=int(payload_offset),
            payload_length=int(payload_length),
        )

    def verify_lookup(self, source: str, source_id: int | str) -> StarRef | None:
        ref = self.lookup(source, source_id)
        if ref is None:
            return None
        descriptor = self.namespaces[str(source).strip().lower()]
        key = _parse_u64(source_id)
        cell_record, ordinal = self._lookup_location(descriptor, key)
        record = self._read_directory_record(cell_record)
        compressed = self._identifiers.read_range(
            self.identifiers_header.payload_offset + record.payload_offset,
            record.payload_length,
        )
        try:
            raw = gzip.decompress(compressed)
        except (EOFError, OSError) as exc:
            raise ValueError("Identifiers/order payload is not valid gzip") from exc
        identities = decode_identity_rows(raw, star_count=record.star_count)
        got_source, got_source_id = identities[ordinal]
        if got_source != descriptor.name or _parse_u64(got_source_id) != key:
            raise ValueError("Identity locator result does not round-trip")
        return ref

    def _lookup_location(
        self, descriptor: NamespaceDescriptor, key: int
    ) -> tuple[int, int]:
        offset, length = descriptor.root_offset, descriptor.root_length
        for _depth in range(64):
            page = unpack_page(self._locator.read_range(offset, length))
            if page.kind == PAGE_KIND_INTERNAL:
                offset, length = self._select_child(page.decoded, key)
                continue
            result = self._find_leaf_record(
                page.decoded,
                key,
                entry_count=page.entry_count,
            )
            if result is None:
                raise ValueError("Identity locator key disappeared during verification")
            return result
        raise ValueError("Identity locator navigation depth exceeds the v1 bound")

    def validate_prefix_checksum(self, *, chunk_bytes: int = 8 * 1024 * 1024) -> None:
        if chunk_bytes <= 0:
            raise ValueError("Integrity chunk size must be > 0")
        footer = self._locator.read_range(
            self.header.integrity_offset, self.header.integrity_length
        )
        hashed_length, expected = unpack_footer(footer)
        digest = hashlib.sha256()
        for offset in range(0, hashed_length, chunk_bytes):
            length = min(chunk_bytes, hashed_length - offset)
            digest.update(self._locator.read_range(offset, length))
        if digest.digest() != expected:
            raise ValueError("Identity locator whole-object checksum mismatch")

    def validate_structure(self) -> dict[str, dict[str, int]]:
        """Read every tree page and validate ordering, fences, counts, and hashes."""
        result: dict[str, dict[str, int]] = {}
        for namespace, descriptor in self.namespaces.items():
            if not descriptor.record_count:
                if descriptor.leaf_page_count or descriptor.navigation_page_count:
                    raise ValueError("Empty identity namespace declares pages")
                if descriptor.content_checksum != hashlib.sha256(b"").digest():
                    raise ValueError("Empty identity namespace checksum differs")
                result[namespace] = {
                    "records": 0,
                    "leaf_pages": 0,
                    "navigation_pages": 0,
                }
                continue

            content_digest = hashlib.sha256()
            previous_key: int | None = None
            leaf_pages = 0
            navigation_pages = 0

            def walk(
                offset: int,
                length: int,
                depth: int,
                *,
                descriptor: NamespaceDescriptor = descriptor,
                content_digest: Any = content_digest,
            ) -> tuple[int, int, int]:
                nonlocal previous_key, leaf_pages, navigation_pages
                if depth >= 64:
                    raise ValueError("Identity locator navigation depth exceeds v1")
                self._validate_locator_range(offset, length)
                page = unpack_page(self._locator.read_range(offset, length))
                if page.kind == PAGE_KIND_LEAF:
                    if page.codec != descriptor.leaf_codec:
                        raise ValueError(
                            "Identity locator leaf codec differs from namespace"
                        )
                    records = decode_compact_leaf(
                        page.decoded,
                        entry_count=page.entry_count,
                    )
                    if not len(records):
                        raise ValueError("Identity locator leaf page is empty")
                    keys = records["source_id"]
                    if len(keys) > 1 and np.any(keys[1:] <= keys[:-1]):
                        raise ValueError(
                            "Identity locator leaf keys are not increasing"
                        )
                    first_key = int(keys[0])
                    last_key = int(keys[-1])
                    if previous_key is not None and first_key <= previous_key:
                        raise ValueError(
                            "Identity locator leaf ranges are not increasing"
                        )
                    if np.any(
                        records["cell_record"] >= self.identifiers_header.record_count
                    ):
                        raise ValueError("Identity locator cell record is out of range")
                    previous_key = last_key
                    leaf_pages += 1
                    content_digest.update(records.tobytes())
                    return first_key, last_key, len(records)

                if page.codec != CODEC_NONE:
                    raise ValueError("Identity locator navigation page is compressed")
                children = np.frombuffer(page.decoded, dtype=_CHILD_DTYPE)
                if not len(children):
                    raise ValueError("Identity locator navigation page is empty")
                maximums = children["maximum"]
                if len(maximums) > 1 and np.any(maximums[1:] <= maximums[:-1]):
                    raise ValueError("Identity locator child fences are not increasing")
                navigation_pages += 1
                total_records = 0
                subtree_minimum: int | None = None
                subtree_maximum: int | None = None
                for child in children:
                    child_offset = int(child["offset"])
                    child_length = int(child["length"])
                    if child_offset <= offset:
                        raise ValueError(
                            "Identity locator child does not follow its parent"
                        )
                    minimum, maximum, record_count = walk(
                        child_offset,
                        child_length,
                        depth + 1,
                    )
                    if maximum != int(child["maximum"]):
                        raise ValueError("Identity locator child fence is incorrect")
                    subtree_minimum = (
                        minimum if subtree_minimum is None else subtree_minimum
                    )
                    subtree_maximum = maximum
                    total_records += record_count
                assert subtree_minimum is not None and subtree_maximum is not None
                return subtree_minimum, subtree_maximum, total_records

            minimum, maximum, records = walk(
                descriptor.root_offset,
                descriptor.root_length,
                0,
            )
            if records != descriptor.record_count:
                raise ValueError("Identity locator namespace record count differs")
            if minimum != descriptor.minimum_key or maximum != descriptor.maximum_key:
                raise ValueError("Identity locator namespace key bounds differ")
            if leaf_pages != descriptor.leaf_page_count:
                raise ValueError("Identity locator leaf page count differs")
            if navigation_pages != descriptor.navigation_page_count:
                raise ValueError("Identity locator navigation page count differs")
            if content_digest.digest() != descriptor.content_checksum:
                raise ValueError("Identity locator namespace content checksum differs")
            result[namespace] = {
                "records": records,
                "leaf_pages": leaf_pages,
                "navigation_pages": navigation_pages,
            }
        return result


def validate_identity_locator(
    locator_source: OctreeSource,
    identifiers_order_source: OctreeSource,
    *,
    samples: dict[str, list[int]] | None = None,
    full_checksum: bool = True,
) -> dict[str, object]:
    """Validate compatibility, integrity, and deterministic lookup samples."""
    with IdentityLocatorReader(locator_source, identifiers_order_source) as reader:
        if full_checksum:
            reader.validate_prefix_checksum()
        structure = reader.validate_structure()
        checked_present = 0
        checked_absent = 0
        sample_values = samples or {
            name: (
                [descriptor.minimum_key, descriptor.maximum_key]
                if descriptor.record_count
                else []
            )
            for name, descriptor in reader.namespaces.items()
        }
        for namespace, keys in sample_values.items():
            for key in dict.fromkeys(keys):
                if reader.verify_lookup(namespace, key) is None:
                    raise ValueError(
                        "Identity locator present-key sample is absent: "
                        f"{namespace}:{key}"
                    )
                checked_present += 1
            descriptor = reader.namespaces[namespace]
            absent = []
            if descriptor.record_count and descriptor.minimum_key > 0:
                absent.append(descriptor.minimum_key - 1)
            if descriptor.record_count and descriptor.maximum_key < _MAX_U64:
                absent.append(descriptor.maximum_key + 1)
            for key in absent:
                if reader.lookup(namespace, key) is not None:
                    raise ValueError("Identity locator returned an absent boundary key")
                checked_absent += 1
        return {
            "locator_uuid": str(reader.header.locator_uuid),
            "parent_dataset_uuid": str(reader.header.parent_dataset_uuid),
            "identifiers_order_uuid": str(reader.header.identifiers_order_uuid),
            "checked_present": checked_present,
            "checked_absent": checked_absent,
            "full_checksum": full_checksum,
            "structure": structure,
        }
