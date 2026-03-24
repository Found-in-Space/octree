from __future__ import annotations

import mmap
import os
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from ..assembly.formats import INDEX_FILE_HDR, INDEX_MAGIC, INDEX_RECORD
from ..assembly.types import ShardKey
from .manifest import CombineManifest
from .records import RELOC_HEADER_FMT, RELOC_MAGIC, RELOC_RECORD_FMT


@dataclass(frozen=True, slots=True)
class FileHeader:
    level: int
    prefix_bits: int
    prefix: int
    record_count: int
    record_size: int
    flags: int


def _shard_node_range(level: int, prefix_bits: int, prefix: int) -> tuple[int, int]:
    total_bits = 3 * level
    shift = total_bits - prefix_bits
    lo = int(prefix) << shift
    hi = ((int(prefix) + 1) << shift) - 1 if shift > 0 else int(prefix)
    return lo, hi


class FixedRecordFile:
    def __init__(self, path: Path, header_struct, record_struct, magic: bytes):
        self._path = path
        self._header_struct = header_struct
        self._record_struct = record_struct
        self._fd = os.open(path, os.O_RDONLY)
        self._size = os.path.getsize(path)
        if self._size < header_struct.size:
            os.close(self._fd)
            raise ValueError(f"File too small: {path}")
        self._mm = mmap.mmap(self._fd, self._size, access=mmap.ACCESS_READ)

        (
            got_magic,
            _version,
            header_size,
            level,
            prefix_bits,
            flags,
            record_size,
            prefix,
            record_count,
        ) = header_struct.unpack_from(self._mm, 0)

        if got_magic != magic:
            self.close()
            raise ValueError(f"Bad magic for {path}: {got_magic!r} != {magic!r}")
        if header_size != header_struct.size:
            self.close()
            raise ValueError(f"Bad header_size for {path}: {header_size}")
        if record_size != record_struct.size:
            self.close()
            raise ValueError(f"Bad record_size for {path}: {record_size}")
        payload_bytes = self._size - header_size
        expected_bytes = int(record_count) * record_struct.size
        if payload_bytes != expected_bytes:
            self.close()
            raise ValueError(
                f"Record bytes mismatch for {path}: {payload_bytes} != {expected_bytes}"
            )
        self._header_size = header_size
        self._record_count = int(record_count)
        self._header = FileHeader(
            level=int(level),
            prefix_bits=int(prefix_bits),
            prefix=int(prefix),
            record_count=int(record_count),
            record_size=int(record_size),
            flags=int(flags),
        )

    @property
    def header(self) -> FileHeader:
        return self._header

    def __len__(self) -> int:
        return self._record_count

    def read_record(self, i: int) -> tuple:
        if i < 0 or i >= self._record_count:
            raise IndexError(i)
        off = self._header_size + i * self._record_struct.size
        return self._record_struct.unpack_from(self._mm, off)

    def lower_bound_u64(self, key: int) -> int:
        lo = 0
        hi = self._record_count
        while lo < hi:
            mid = (lo + hi) // 2
            rec_key = self.read_record(mid)[0]
            if rec_key < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def find_u64_key(self, key: int) -> tuple | None:
        i = self.lower_bound_u64(key)
        if i >= self._record_count:
            return None
        rec = self.read_record(i)
        return rec if rec[0] == key else None

    def any_key_in_range(self, lo_key: int, hi_key: int) -> bool:
        if lo_key > hi_key:
            return False
        i = self.lower_bound_u64(lo_key)
        if i >= self._record_count:
            return False
        return self.read_record(i)[0] <= hi_key

    def iter_records(self):
        for i in range(self._record_count):
            yield self.read_record(i)

    def close(self) -> None:
        if hasattr(self, "_mm"):
            self._mm.close()
            del self._mm
        if hasattr(self, "_fd"):
            os.close(self._fd)
            del self._fd


@dataclass(frozen=True, slots=True)
class IndexedShard:
    key: ShardKey
    index_path: Path
    node_lo: int
    node_hi: int


class _FixedRecordCache:
    def __init__(self, max_open_files: int):
        if max_open_files <= 0:
            raise ValueError("max_open_files must be > 0")
        self._max_open = max_open_files
        self._cache: OrderedDict[Path, FixedRecordFile] = OrderedDict()

    @property
    def open_file_count(self) -> int:
        return len(self._cache)

    def get(
        self,
        path: Path,
        *,
        header_struct,
        record_struct,
        magic: bytes,
    ) -> FixedRecordFile:
        key = path.resolve()
        existing = self._cache.pop(key, None)
        if existing is None:
            existing = FixedRecordFile(
                path=path,
                header_struct=header_struct,
                record_struct=record_struct,
                magic=magic,
            )
        self._cache[key] = existing
        while len(self._cache) > self._max_open:
            _, fr = self._cache.popitem(last=False)
            fr.close()
        return existing

    def close_all(self) -> None:
        while self._cache:
            _, fr = self._cache.popitem(last=False)
            fr.close()


class IntermediateLookup:
    def __init__(self, manifest: CombineManifest, *, max_open_files: int = 32):
        self._by_level: dict[int, list[IndexedShard]] = {}
        self._reader_cache = _FixedRecordCache(max_open_files=max_open_files)
        for entry in manifest.shards:
            lo, hi = _shard_node_range(
                entry.key.level, entry.key.prefix_bits, entry.key.prefix
            )
            shard = IndexedShard(
                key=entry.key,
                index_path=entry.index_path,
                node_lo=lo,
                node_hi=hi,
            )
            self._by_level.setdefault(entry.key.level, []).append(shard)
        for level in self._by_level:
            self._by_level[level].sort(key=lambda s: (s.key.prefix_bits, s.key.prefix))

    @property
    def shards_by_level(self) -> dict[int, list[IndexedShard]]:
        return self._by_level

    @property
    def open_file_count(self) -> int:
        return self._reader_cache.open_file_count

    def _reader(self, shard: IndexedShard) -> FixedRecordFile:
        return self._reader_cache.get(
            shard.index_path,
            header_struct=INDEX_FILE_HDR,
            record_struct=INDEX_RECORD,
            magic=INDEX_MAGIC,
        )

    def has_payload_node(self, level: int, node_id: int) -> bool:
        return self.find_payload(level, node_id) is not None

    def find_payload(self, level: int, node_id: int) -> tuple[ShardKey, tuple] | None:
        for shard in self._by_level.get(level, ()):
            if node_id < shard.node_lo or node_id > shard.node_hi:
                continue
            rec = self._reader(shard).find_u64_key(node_id)
            if rec is not None:
                return shard.key, rec
        return None

    def descendant_exists(self, level: int, node_id: int, max_level: int) -> bool:
        for d in range(level, max_level + 1):
            shift = 3 * (d - level)
            lo = node_id << shift
            hi = ((node_id + 1) << shift) - 1 if shift > 0 else node_id
            for shard in self._by_level.get(d, ()):
                if hi < shard.node_lo or lo > shard.node_hi:
                    continue
                q_lo = max(lo, shard.node_lo)
                q_hi = min(hi, shard.node_hi)
                if self._reader(shard).any_key_in_range(q_lo, q_hi):
                    return True
        return False

    def close(self) -> None:
        self._reader_cache.close_all()


@dataclass(frozen=True, slots=True)
class RelocShard:
    key: ShardKey
    path: Path
    node_lo: int
    node_hi: int


class RelocationLookup:
    def __init__(self, relocation_files: tuple[Path, ...], *, max_open_files: int = 32):
        self._by_level: dict[int, list[RelocShard]] = {}
        self._reader_cache = _FixedRecordCache(max_open_files=max_open_files)
        for path in relocation_files:
            rf = FixedRecordFile(path, RELOC_HEADER_FMT, RELOC_RECORD_FMT, RELOC_MAGIC)
            h = rf.header
            rf.close()
            key = ShardKey(level=h.level, prefix_bits=h.prefix_bits, prefix=h.prefix)
            lo, hi = _shard_node_range(key.level, key.prefix_bits, key.prefix)
            shard = RelocShard(key=key, path=path, node_lo=lo, node_hi=hi)
            self._by_level.setdefault(key.level, []).append(shard)
        for level in self._by_level:
            self._by_level[level].sort(key=lambda s: (s.key.prefix_bits, s.key.prefix))

    @property
    def open_file_count(self) -> int:
        return self._reader_cache.open_file_count

    def _reader(self, shard: RelocShard) -> FixedRecordFile:
        return self._reader_cache.get(
            shard.path,
            header_struct=RELOC_HEADER_FMT,
            record_struct=RELOC_RECORD_FMT,
            magic=RELOC_MAGIC,
        )

    def get_payload(self, level: int, node_id: int) -> tuple[int, int, int] | None:
        for shard in self._by_level.get(level, ()):
            if node_id < shard.node_lo or node_id > shard.node_hi:
                continue
            rec = self._reader(shard).find_u64_key(node_id)
            if rec is not None:
                return int(rec[1]), int(rec[2]), int(rec[3])
        return None

    def close(self) -> None:
        self._reader_cache.close_all()


class FileHandleCache:
    def __init__(self, max_open_files: int):
        if max_open_files <= 0:
            raise ValueError("max_open_files must be > 0")
        self._max_open = max_open_files
        self._entries: OrderedDict[Path, object] = OrderedDict()

    def open(self, path: Path, mode: str):
        key = path.resolve()
        fp = self._entries.pop(key, None)
        if fp is None:
            fp = open(path, mode)  # noqa: SIM115
        self._entries[key] = fp
        while len(self._entries) > self._max_open:
            _, old = self._entries.popitem(last=False)
            old.close()
        return fp

    def close_all(self) -> None:
        while self._entries:
            _, fp = self._entries.popitem(last=False)
            fp.close()
