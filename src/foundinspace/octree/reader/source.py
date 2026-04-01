from __future__ import annotations

import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse
from urllib.request import Request, urlopen

DEFAULT_HTTP_BLOCK_SIZE = 256 * 1024
DEFAULT_HTTP_CACHE_BLOCKS = 128


class SeekableBinaryReader(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def seek(self, offset: int, whence: int = 0) -> int: ...

    def tell(self) -> int: ...

    def close(self) -> None: ...


OctreeSource = Path | str


def is_url_source(source: OctreeSource) -> bool:
    if not isinstance(source, str):
        return False
    parsed = urlparse(source.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


class HttpRangeReader:
    def __init__(
        self,
        url: str,
        *,
        block_size: int = DEFAULT_HTTP_BLOCK_SIZE,
        max_cached_blocks: int = DEFAULT_HTTP_CACHE_BLOCKS,
    ) -> None:
        self.url = url
        self.block_size = max(1, int(block_size))
        self.max_cached_blocks = max(1, int(max_cached_blocks))
        self._position = 0
        self._closed = False
        self._cache: OrderedDict[int, bytes] = OrderedDict()

    def __enter__(self) -> HttpRangeReader:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._cache.clear()
        self._closed = True

    def tell(self) -> int:
        self._assert_open()
        return self._position

    def seek(self, offset: int, whence: int = 0) -> int:
        self._assert_open()
        if whence == 0:
            position = int(offset)
        elif whence == 1:
            position = self._position + int(offset)
        else:
            raise ValueError(f"Unsupported whence for HttpRangeReader: {whence}")
        if position < 0:
            raise ValueError(f"Cannot seek to negative position: {position}")
        self._position = position
        return self._position

    def read(self, size: int = -1) -> bytes:
        self._assert_open()
        if size is None or size < 0:
            raise ValueError("HttpRangeReader requires a finite read size")
        if size == 0:
            return b""

        start = self._position
        end = start + size - 1
        chunks: list[bytes] = []
        cursor = start

        while cursor <= end:
            block_index = cursor // self.block_size
            block_start = block_index * self.block_size
            block = self._read_block(block_index)
            block_offset = cursor - block_start
            if block_offset >= len(block):
                break
            take = min(end - cursor + 1, len(block) - block_offset)
            chunks.append(block[block_offset : block_offset + take])
            cursor += take
            if len(block) < self.block_size and block_offset + take >= len(block):
                break

        data = b"".join(chunks)
        self._position = start + len(data)
        return data

    def _assert_open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed HttpRangeReader")

    def _read_block(self, block_index: int) -> bytes:
        cached = self._cache.get(block_index)
        if cached is not None:
            self._cache.move_to_end(block_index)
            return cached

        start = block_index * self.block_size
        end = start + self.block_size - 1
        request = Request(
            self.url,
            headers={
                "Range": f"bytes={start}-{end}",
            },
        )
        t0 = time.perf_counter()
        with urlopen(request) as response:
            block = response.read()
        elapsed = time.perf_counter() - t0
        mib_s = (len(block) / (1024 * 1024)) / elapsed if elapsed > 0 else float("inf")
        print(
            f"octree http range bytes={start}-{end} size={len(block)} "
            f"time={elapsed * 1000:.1f}ms throughput={mib_s:.2f} MiB/s url={self.url}",
            file=sys.stderr,
        )

        self._cache[block_index] = block
        self._cache.move_to_end(block_index)
        while len(self._cache) > self.max_cached_blocks:
            self._cache.popitem(last=False)
        return block


def open_octree_source(source: OctreeSource) -> SeekableBinaryReader:
    if isinstance(source, Path):
        return open(source, "rb")
    if isinstance(source, str):
        normalized = source.strip()
        if is_url_source(normalized):
            return HttpRangeReader(normalized)
        return open(Path(normalized), "rb")
    raise TypeError(f"Unsupported octree source: {type(source)!r}")
