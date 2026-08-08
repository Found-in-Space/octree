"""Isolated A/B benchmark for STAR Phase-B index emission.

Run the standard 2k/8k/16k dense and sparse matrix with:
    uv run python benchmarks/benchmark_combine_index.py

Add the 64k sparse case with:
    uv run python benchmarks/benchmark_combine_index.py --include-64k
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from foundinspace.octree.assembly.formats import (
    DEFAULT_FLAGS,
    INDEX_FILE_HDR,
    INDEX_HEADER_SIZE,
    INDEX_MAGIC,
    INDEX_RECORD,
    INDEX_VERSION,
    RENDER_ARTIFACT_KIND,
)
from foundinspace.octree.assembly.manifest import write_manifest
from foundinspace.octree.assembly.types import CellKey, EncodedCell, ShardKey
from foundinspace.octree.assembly.writer import IntermediateShardWriter
from foundinspace.octree.combine.pipeline import (
    CombinePlan,
    IndexEmissionStrategy,
    write_final_shard_index,
)
from foundinspace.octree.combine.records import RELOC_MAGIC


class _MeasuredSink:
    def __init__(self, fp):
        self._fp = fp
        self.write_calls = 0
        self.write_bytes = 0
        self.backward_seeks = 0

    def tell(self):
        return self._fp.tell()

    def write(self, data):
        self.write_calls += 1
        self.write_bytes += len(data)
        return self._fp.write(data)

    def seek(self, offset, whence=0):
        current = self._fp.tell()
        if whence == os.SEEK_SET:
            target = offset
        elif whence == os.SEEK_CUR:
            target = current + offset
        elif whence == os.SEEK_END:
            end = self._fp.seek(0, os.SEEK_END)
            target = end + offset
            self._fp.seek(current)
        else:
            raise ValueError(f"Unsupported whence: {whence}")
        if target < current:
            self.backward_seeks += 1
        return self._fp.seek(offset, whence)


def _fixture(root: Path, count: int, *, sparse: bool) -> tuple[Path, tuple[Path, ...]]:
    manifest = root / "manifest.json"
    relocation = root / "level-14.reloc"
    if manifest.is_file() and relocation.is_file():
        return manifest, (relocation,)
    root.mkdir(parents=True)
    shard = ShardKey(level=14, prefix_bits=0, prefix=0)
    writer = IntermediateShardWriter(shard, root)
    for index in range(count):
        node_id = index << 15 if sparse else index
        writer.write_cell(EncodedCell(CellKey(14, node_id), b"x", 1))
    entry = writer.close()
    assert entry is not None
    manifest = write_manifest(
        root,
        14,
        [entry],
        artifact_kind=RENDER_ARTIFACT_KIND,
        index_magic=INDEX_MAGIC,
        mag_limit=6.5,
    )
    with open(relocation, "wb") as fp:
        fp.write(
            INDEX_FILE_HDR.pack(
                RELOC_MAGIC,
                INDEX_VERSION,
                INDEX_HEADER_SIZE,
                14,
                0,
                DEFAULT_FLAGS,
                INDEX_RECORD.size,
                0,
                count,
            )
        )
        buffer = bytearray()
        for index in range(count):
            node_id = index << 15 if sparse else index
            buffer.extend(INDEX_RECORD.pack(node_id, 192 + index, 1, 1))
            if len(buffer) >= 1 << 20:
                fp.write(buffer)
                buffer.clear()
        if buffer:
            fp.write(buffer)
    return manifest, (relocation,)


def _measure(
    root: Path,
    count: int,
    *,
    sparse: bool,
    strategy: IndexEmissionStrategy,
) -> dict[str, object]:
    manifest, relocations = _fixture(root, count, sparse=sparse)
    plan = CombinePlan(
        max_open_files=4,
        cache_dir=root / "cache",
        index_emission_strategy=strategy,
    )
    import foundinspace.octree.combine.streaming_index as streaming_index

    pwrite_calls = 0
    pwrite_bytes = 0
    strategy_temporary_bytes = 0
    real_pwrite = os.pwrite
    real_child_runs = streaming_index._build_child_offset_runs
    real_temporary = streaming_index._emit_index_via_temporary

    def measured_pwrite(fd, data, offset):
        nonlocal pwrite_calls, pwrite_bytes
        pwrite_calls += 1
        pwrite_bytes += len(data)
        return real_pwrite(fd, data, offset)

    def measured_child_runs(*args, **kwargs):
        nonlocal strategy_temporary_bytes
        paths, expected_end = real_child_runs(*args, **kwargs)
        strategy_temporary_bytes = sum(path.stat().st_size for path in paths.values())
        return paths, expected_end

    def measured_temporary(*args, **kwargs):
        nonlocal strategy_temporary_bytes
        result = real_temporary(*args, **kwargs)
        strategy_temporary_bytes = (
            (Path(kwargs["scratch"]) / "completed-index.bin").stat().st_size
        )
        return result

    os.pwrite = measured_pwrite
    streaming_index._build_child_offset_runs = measured_child_runs
    streaming_index._emit_index_via_temporary = measured_temporary
    try:
        with tempfile.TemporaryFile() as output:
            output.write(b"\x00" * 192)
            sink = _MeasuredSink(output)
            cpu_start = time.process_time()
            wall_start = time.perf_counter()
            result = write_final_shard_index(manifest, relocations, sink, plan=plan)
            wall_seconds = time.perf_counter() - wall_start
            cpu_seconds = time.process_time() - cpu_start
            output_size = result.index_length
    finally:
        os.pwrite = real_pwrite
        streaming_index._build_child_offset_runs = real_child_runs
        streaming_index._emit_index_via_temporary = real_temporary

    return {
        "shape": "sparse" if sparse else "dense",
        "nodes": count,
        "strategy": strategy.value,
        "wall_seconds": wall_seconds,
        "cpu_seconds": cpu_seconds,
        "output_write_calls": sink.write_calls,
        "output_write_bytes": sink.write_bytes,
        "pwrite_calls": pwrite_calls,
        "pwrite_bytes": pwrite_bytes,
        "backward_seeks": sink.backward_seeks,
        "strategy_temporary_bytes": strategy_temporary_bytes,
        "index_bytes": output_size,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }


def _run_worker(args: argparse.Namespace) -> None:
    result = _measure(
        Path(args.root),
        args.count,
        sparse=args.shape == "sparse",
        strategy=IndexEmissionStrategy(args.strategy),
    )
    if not args.prime:
        print(json.dumps(result, sort_keys=True))


def _subprocess_measure(
    root: Path,
    *,
    shape: str,
    count: int,
    strategy: IndexEmissionStrategy,
    cache_mode: str,
) -> dict[str, object]:
    _fixture(root, count, sparse=shape == "sparse")
    command = [
        sys.executable,
        __file__,
        "--worker",
        "--root",
        str(root),
        "--shape",
        shape,
        "--count",
        str(count),
        "--strategy",
        strategy.value,
    ]
    if cache_mode == "warm":
        subprocess.run(
            [*command, "--prime"], check=True, capture_output=True, text=True
        )
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    result = json.loads(completed.stdout)
    result["cache"] = cache_mode
    return result


def _print_results(results: list[dict[str, object]]) -> None:
    columns = [
        "cache",
        "shape",
        "nodes",
        "strategy",
        "wall_seconds",
        "cpu_seconds",
        "output_write_calls",
        "output_write_bytes",
        "pwrite_calls",
        "pwrite_bytes",
        "backward_seeks",
        "strategy_temporary_bytes",
        "index_bytes",
        "peak_rss_kib",
    ]
    print(",".join(columns))
    for result in results:
        print(",".join(str(result[column]) for column in columns))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-64k", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--prime", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--root", help=argparse.SUPPRESS)
    parser.add_argument("--shape", choices=("dense", "sparse"), help=argparse.SUPPRESS)
    parser.add_argument("--count", type=int, help=argparse.SUPPRESS)
    parser.add_argument(
        "--strategy",
        choices=tuple(strategy.value for strategy in IndexEmissionStrategy),
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    if args.worker:
        _run_worker(args)
        return

    cases = [
        (shape, count)
        for shape in ("dense", "sparse")
        for count in (2_000, 8_000, 16_000)
    ]
    if args.include_64k:
        cases.append(("sparse", 64_000))
    strategies = list(IndexEmissionStrategy)
    results: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory() as temporary:
        base = Path(temporary)
        for cache_mode in ("cold", "warm"):
            for shape, count in cases:
                for strategy in strategies:
                    root = base / f"{cache_mode}-{shape}-{count}-{strategy.value}"
                    results.append(
                        _subprocess_measure(
                            root,
                            shape=shape,
                            count=count,
                            strategy=strategy,
                            cache_mode=cache_mode,
                        )
                    )
    _print_results(results)


if __name__ == "__main__":
    main()
