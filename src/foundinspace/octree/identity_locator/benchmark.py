"""Benchmark the v1 identity locator page-size and leaf-codec matrix."""

from __future__ import annotations

import json
import math
import os
import shutil
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .builder import (
    DEFAULT_EXTERNAL_SORT_MEMORY_LIMIT,
    DEFAULT_LEAF_CODEC,
    DEFAULT_MERGE_BATCH_ROWS,
    DEFAULT_MERGE_FAN_IN,
    DEFAULT_SCAN_BATCH_BYTES,
    IdentityLocatorBuildConfig,
    IdentityLocatorBuildProgress,
    build_identity_locator,
)
from .reader import IdentityLocatorReader

BENCHMARK_FORMAT = "foundinspace.octree.identity-locator-benchmark/v1"
CANDIDATES = (
    (16 * 1024, DEFAULT_LEAF_CODEC),
    (32 * 1024, DEFAULT_LEAF_CODEC),
)


@dataclass(frozen=True, slots=True)
class IdentityLocatorBenchmarkConfig:
    render_octree_path: Path
    identifiers_order_path: Path
    work_dir: Path
    report_path: Path
    scan_batch_bytes: int = DEFAULT_SCAN_BATCH_BYTES
    merge_fan_in: int = DEFAULT_MERGE_FAN_IN
    merge_batch_rows: int = DEFAULT_MERGE_BATCH_ROWS
    external_sort_memory_limit: str = DEFAULT_EXTERNAL_SORT_MEMORY_LIMIT
    repetitions: int = 3
    force: bool = False
    retain_candidates: bool = False
    progress: Callable[[IdentityLocatorBuildProgress], None] | None = None

    def validate(self) -> None:
        if self.repetitions <= 0:
            raise ValueError("Identity locator benchmark repetitions must be > 0")


@dataclass(frozen=True, slots=True)
class IdentityLocatorBenchmarkResult:
    report_path: Path
    winning_page_size: int
    winning_leaf_codec: str
    winner_candidate_path: Path


def _atomic_write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temporary, "w", encoding="utf-8") as fp:
        json.dump(value, fp, indent=2, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temporary, path)


def _sample_keys(report: dict[str, Any]) -> list[tuple[str, int]]:
    values: list[tuple[str, int]] = []
    for namespace, keys in report.get("validation_samples", {}).items():
        values.extend((str(namespace), int(key)) for key in keys)
    return values


def _sample_workloads(
    report: dict[str, Any],
) -> dict[str, tuple[list[tuple[str, int]], bool]]:
    present = _sample_keys(report)
    clustered_present = present[:2] * 4
    absent: list[tuple[str, int]] = []
    for namespace, count in report["namespace_counts"].items():
        if not count:
            continue
        minimum = int(report["namespace_minimum_keys"][namespace])
        maximum = int(report["namespace_maximum_keys"][namespace])
        if minimum > 0:
            absent.append((namespace, minimum - 1))
        if maximum < 2**64 - 1:
            absent.append((namespace, maximum + 1))
    return {
        "uniform_present": (present, True),
        "clustered_present": (clustered_present, True),
        "uniform_absent": (absent, False),
        "clustered_absent": (absent[:1] * 4, False),
    }


def _percentile_95(values: list[int]) -> int:
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * 0.95) - 1]


def _measure_candidate(
    locator_path: Path,
    identifiers_path: Path,
    *,
    keys: list[tuple[str, int]],
    expected_present: bool,
    repetitions: int,
) -> dict[str, float | int]:
    if not keys:
        return {
            "cold_median_seconds": 0.0,
            "warm_median_seconds": 0.0,
            "p95_transferred_bytes": 0,
            "lookup_count": 0,
        }
    cold_samples: list[float] = []
    transferred_bytes: list[int] = []
    for _repeat in range(repetitions):
        for namespace, key in keys:
            started = time.perf_counter()
            with IdentityLocatorReader(locator_path, identifiers_path) as reader:
                found = reader.lookup(namespace, key) is not None
                if found != expected_present:
                    raise ValueError("Benchmark sample returned the wrong presence")
                transferred_bytes.append(reader.range_metrics["total_bytes"])
            cold_samples.append(time.perf_counter() - started)

    warm_samples: list[float] = []
    with IdentityLocatorReader(locator_path, identifiers_path) as reader:
        for _repeat in range(repetitions):
            for namespace, key in keys:
                started = time.perf_counter()
                found = reader.lookup(namespace, key) is not None
                if found != expected_present:
                    raise ValueError("Benchmark sample returned the wrong presence")
                warm_samples.append(time.perf_counter() - started)
    return {
        "cold_median_seconds": statistics.median(cold_samples),
        "warm_median_seconds": statistics.median(warm_samples),
        "p95_transferred_bytes": _percentile_95(transferred_bytes),
        "lookup_count": len(cold_samples),
    }


def _select_winner(results: list[dict[str, object]]) -> dict[str, object]:
    best_latency = min(float(result["cold_median_seconds"]) for result in results)
    tied = [
        result
        for result in results
        if float(result["cold_median_seconds"]) <= best_latency * 1.05
    ]

    def rank(result: dict[str, object]) -> tuple[float, int, int]:
        preference = 0 if result["page_size"] == 32 * 1024 else 1
        return (
            float(result["p95_transferred_bytes"]),
            int(result["output_size"]),
            preference,
        )

    return min(tied, key=rank)


def benchmark_identity_locator(
    config: IdentityLocatorBenchmarkConfig,
) -> IdentityLocatorBenchmarkResult:
    """Build the compact page-capacity candidates and select the lookup winner."""
    config.validate()
    work_dir = config.work_dir.expanduser().resolve()
    report_path = config.report_path.expanduser().resolve()
    candidates_dir = work_dir / "candidates"
    if config.force:
        if work_dir.exists():
            shutil.rmtree(work_dir)
        report_path.unlink(missing_ok=True)
    results: list[dict[str, object]] = []
    candidate_paths: dict[tuple[int, str], Path] = {}

    for page_size, codec in CANDIDATES:
        label = f"{page_size}-{codec}"
        output_path = candidates_dir / f"identity-locator-{label}.idx"
        candidate_report_path = candidates_dir / f"identity-locator-{label}.report.json"
        build_result = build_identity_locator(
            IdentityLocatorBuildConfig(
                render_octree_path=config.render_octree_path,
                identifiers_order_path=config.identifiers_order_path,
                output_path=output_path,
                report_path=candidate_report_path,
                work_dir=work_dir,
                decoded_page_size=page_size,
                leaf_codec=codec,
                scan_batch_bytes=config.scan_batch_bytes,
                merge_fan_in=config.merge_fan_in,
                merge_batch_rows=config.merge_batch_rows,
                external_sort_memory_limit=config.external_sort_memory_limit,
                retain_work=True,
                progress=config.progress,
            )
        )
        build_report = json.loads(candidate_report_path.read_text(encoding="utf-8"))
        workloads = _sample_workloads(build_report)
        if not workloads["uniform_present"][0]:
            raise ValueError("Identity locator benchmark has no present-key samples")
        measurements = {
            name: _measure_candidate(
                build_result.output_path,
                config.identifiers_order_path,
                keys=keys,
                expected_present=expected_present,
                repetitions=config.repetitions,
            )
            for name, (keys, expected_present) in workloads.items()
        }
        all_measurements = [
            measurement
            for measurement in measurements.values()
            if measurement["lookup_count"]
        ]
        cold = statistics.median(
            float(measurement["cold_median_seconds"])
            for measurement in all_measurements
        )
        warm = statistics.median(
            float(measurement["warm_median_seconds"])
            for measurement in all_measurements
        )
        p95_transferred = max(
            int(measurement["p95_transferred_bytes"])
            for measurement in all_measurements
        )
        leaf_pages = sum(build_report["namespace_leaf_pages"].values())
        average_leaf_bytes = (
            build_report["encoded_leaf_bytes"] / leaf_pages if leaf_pages else 0.0
        )
        result: dict[str, object] = {
            "page_size": page_size,
            "leaf_codec": codec,
            "candidate_path": str(build_result.output_path),
            "candidate_sha256": build_result.output_sha256,
            "output_size": build_report["output_size"],
            "encoded_leaf_bytes": build_report["encoded_leaf_bytes"],
            "average_encoded_leaf_page_bytes": average_leaf_bytes,
            "cold_median_seconds": cold,
            "warm_median_seconds": warm,
            "p95_transferred_bytes": p95_transferred,
            "workloads": measurements,
            "sample_count": sum(
                len(keys) for keys, _expected_present in workloads.values()
            ),
            "repetitions": config.repetitions,
            "build_timings_seconds": build_report["timings_seconds"],
            "cumulative_checkpointed_timings_seconds": build_report.get(
                "cumulative_checkpointed_timings_seconds"
            ),
        }
        results.append(result)
        candidate_paths[(page_size, codec)] = build_result.output_path

    winner = _select_winner(results)
    winning_key = (int(winner["page_size"]), str(winner["leaf_codec"]))
    benchmark_report: dict[str, object] = {
        "format": BENCHMARK_FORMAT,
        "selection_policy": (
            "lowest cold median; within 5% prefer lower p95 transferred bytes, "
            "then artifact size, then the default 32 KiB logical page capacity"
        ),
        "identifiers_order_path": str(config.identifiers_order_path.resolve()),
        "results": results,
        "winner": winner,
    }
    _atomic_write_json(report_path, benchmark_report)
    winner_path = candidate_paths[winning_key]
    if not config.retain_candidates:
        for key, path in candidate_paths.items():
            if key == winning_key:
                continue
            path.unlink(missing_ok=True)
            path.with_suffix(".report.json").unlink(missing_ok=True)
    return IdentityLocatorBenchmarkResult(
        report_path=report_path,
        winning_page_size=winning_key[0],
        winning_leaf_codec=winning_key[1],
        winner_candidate_path=winner_path,
    )
