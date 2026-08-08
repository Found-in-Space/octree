from __future__ import annotations

import builtins
import os
import threading

import pytest

from combine_helpers import PayloadNode, build_intermediates
from foundinspace.octree.combine.pipeline import (
    CombinePlan,
    IndexEmissionStrategy,
    _write_final_shard_index_legacy,
    relocate_payloads_dfs,
    write_final_shard_index,
)
from foundinspace.octree.combine.records import (
    SHARD_HDR_FMT,
    SHARD_MAGIC,
    SHARD_NODE_FMT,
)


def test_write_final_shard_index_writes_shard_block(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=1, star_count=1, raw_payload=b"b"),
            PayloadNode(level=2, node_id=1, star_count=1, raw_payload=b"ba"),
        ],
        max_level=2,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(
            manifest_path, fp, plan=CombinePlan(max_open_files=2)
        )
        phase_b = write_final_shard_index(
            manifest_path,
            phase_a.relocation_files,
            fp,
            plan=CombinePlan(max_open_files=2),
        )
    data = out.read_bytes()
    assert phase_b.index_offset >= phase_a.payload_end_offset
    assert phase_b.index_length > 0

    shard_hdr = SHARD_HDR_FMT.unpack_from(data, phase_b.index_offset)
    assert shard_hdr[0] == SHARD_MAGIC
    node_count = shard_hdr[7]
    assert node_count >= 1
    node_table_offset = shard_hdr[22]
    node = SHARD_NODE_FMT.unpack_from(data, node_table_offset)
    assert node[3] >= 1  # local_depth


def test_write_final_shard_index_node_count_limit(monkeypatch, tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"x")],
        max_level=0,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=CombinePlan())

        import foundinspace.octree.combine.streaming_index as streaming

        monkeypatch.setattr(
            streaming,
            "_read_skeleton",
            lambda *args, **kwargs: [(0, 0, 0, 1, 0, 0)] * 65536,
        )
        with pytest.raises(ValueError, match="node_count exceeds u16"):
            write_final_shard_index(
                manifest_path,
                phase_a.relocation_files,
                fp,
                plan=CombinePlan(),
            )


def test_missing_relocation_for_payload_node_raises(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"child"),
        ],
        max_level=1,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        relocate_payloads_dfs(manifest_path, fp, plan=CombinePlan())

        with pytest.raises(ValueError, match="Missing relocation entry"):
            write_final_shard_index(
                manifest_path,
                (),
                fp,
                plan=CombinePlan(),
            )


def test_shard_node_ordering_is_deterministic_and_path_sorted(tmp_path) -> None:
    manifest_path = build_intermediates(
        tmp_path,
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=0, star_count=1, raw_payload=b"a"),
            PayloadNode(level=1, node_id=7, star_count=1, raw_payload=b"h"),
            PayloadNode(level=2, node_id=56, star_count=1, raw_payload=b"hh"),
        ],
        max_level=2,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 64)
        phase_a = relocate_payloads_dfs(
            manifest_path, fp, plan=CombinePlan(max_open_files=2)
        )
        phase_b = write_final_shard_index(
            manifest_path,
            phase_a.relocation_files,
            fp,
            plan=CombinePlan(max_open_files=2),
        )
    data = out.read_bytes()
    shard_hdr = SHARD_HDR_FMT.unpack_from(data, phase_b.index_offset)
    node_count = shard_hdr[7]
    node_table_offset = shard_hdr[22]
    keys: list[tuple[int, int]] = []
    for i in range(node_count):
        rec = SHARD_NODE_FMT.unpack_from(
            data, node_table_offset + i * SHARD_NODE_FMT.size
        )
        local_path = rec[1]
        local_depth = rec[3]
        keys.append((local_depth, local_path))
    assert keys == sorted(keys)


def test_streaming_phase_b_does_not_use_random_lookup(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=6, node_id=7, star_count=1, raw_payload=b"deep"),
        ],
        max_level=6,
    )
    out = tmp_path / "out.octree"
    with open(out, "wb") as fp:
        fp.write(b"\x00" * 192)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=CombinePlan())

        def forbidden(*_args, **_kwargs):
            raise AssertionError("Phase B performed a random lookup")

        import foundinspace.octree.combine.lookup as lookup

        monkeypatch.setattr(lookup.IntermediateLookup, "find_payload", forbidden)
        monkeypatch.setattr(lookup.IntermediateLookup, "descendant_exists", forbidden)
        write_final_shard_index(
            manifest_path, phase_a.relocation_files, fp, plan=CombinePlan()
        )


@pytest.mark.parametrize(
    "strategy",
    [
        IndexEmissionStrategy.FORWARD,
        IndexEmissionStrategy.TEMP_PWRITE_BATCHED,
        IndexEmissionStrategy.TEMP_PWRITE_PER_CHILD,
    ],
)
def test_streaming_phase_b_matches_legacy_bytes_on_deep_sparse_fixture(
    tmp_path, strategy: IndexEmissionStrategy
) -> None:
    class NoBackwardSeek:
        def __init__(self, fp):
            self._fp = fp

        def tell(self):
            return self._fp.tell()

        def write(self, data):
            return self._fp.write(data)

        def seek(self, offset, whence=0):
            current = self._fp.tell()
            if whence == 0:
                target = offset
            elif whence == 1:
                target = current + offset
            else:
                raise AssertionError("Phase B attempted an end-relative seek")
            if target < current:
                raise AssertionError("Phase B attempted a backward output seek")
            return self._fp.seek(offset, whence)

    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root"),
            PayloadNode(level=1, node_id=7, star_count=1, raw_payload=b"one"),
            PayloadNode(level=5, node_id=0, star_count=1, raw_payload=b"five-a"),
            PayloadNode(level=5, node_id=31, star_count=2, raw_payload=b"five-b"),
            PayloadNode(level=10, node_id=1, star_count=1, raw_payload=b"ten"),
            PayloadNode(
                level=14,
                node_id=(1 << 42) - 1,
                star_count=1,
                raw_payload=b"frontier",
            ),
        ],
        max_level=14,
    )

    def compile_index(path, *, legacy: bool) -> None:
        plan = CombinePlan(
            max_open_files=2,
            cache_dir=tmp_path
            / ("legacy-cache" if legacy else f"stream-cache-{strategy.value}"),
            index_emission_strategy=strategy,
        )
        with open(path, "wb") as fp:
            fp.write(b"\x00" * 192)
            phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
            writer = (
                _write_final_shard_index_legacy if legacy else write_final_shard_index
            )
            output = fp if legacy else NoBackwardSeek(fp)
            writer(manifest_path, phase_a.relocation_files, output, plan=plan)

    streaming = tmp_path / "streaming.bin"
    legacy = tmp_path / "legacy.bin"
    compile_index(streaming, legacy=False)
    compile_index(legacy, legacy=True)

    assert streaming.read_bytes() == legacy.read_bytes()


def test_payload_only_change_reuses_topology_plan_and_skeletons(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_dir = tmp_path / "durable-cache"

    def compile_manifest(manifest_path, output_path) -> None:
        plan = CombinePlan(max_open_files=2, cache_dir=cache_dir)
        with open(output_path, "wb") as fp:
            fp.write(b"\x00" * 192)
            phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
            write_final_shard_index(
                manifest_path, phase_a.relocation_files, fp, plan=plan
            )

    first_manifest = build_intermediates(
        tmp_path / "first",
        [PayloadNode(level=6, node_id=7, star_count=1, raw_payload=b"before")],
        max_level=6,
    )
    compile_manifest(first_manifest, tmp_path / "first.bin")
    skeletons = sorted((cache_dir / "packs").glob("*.pack"))
    before = {
        path.name: (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in skeletons
    }

    second_manifest = build_intermediates(
        tmp_path / "second",
        [PayloadNode(level=6, node_id=7, star_count=1, raw_payload=b"after-longer")],
        max_level=6,
    )
    import foundinspace.octree.combine.streaming_index as streaming_index

    monkeypatch.setattr(
        streaming_index,
        "_build_topology_runs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("topology was rebuilt for a payload-only change")
        ),
    )
    compile_manifest(second_manifest, tmp_path / "second.bin")

    assert {
        path.name: (path.stat().st_ino, path.stat().st_mtime_ns, path.read_bytes())
        for path in (cache_dir / "packs").glob("*.pack")
    } == before


def test_streaming_compiler_cleans_incomplete_temporary_tree(tmp_path) -> None:
    cache_dir = tmp_path / "cache"
    incomplete = cache_dir / ".compile-dead.tmp"
    incomplete.mkdir(parents=True)
    (incomplete / "partial").write_bytes(b"partial")
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=0, node_id=0, star_count=1, raw_payload=b"root")],
        max_level=0,
    )
    plan = CombinePlan(cache_dir=cache_dir)
    with open(tmp_path / "out.bin", "wb") as fp:
        fp.write(b"\x00" * 192)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
        write_final_shard_index(manifest_path, phase_a.relocation_files, fp, plan=plan)

    assert not incomplete.exists()


def test_topology_change_replaces_only_intersecting_spatial_pack(
    tmp_path,
) -> None:
    cache_dir = tmp_path / "cache"

    def compile_nodes(directory, nodes, output):
        manifest = build_intermediates(directory, nodes, max_level=6)
        plan = CombinePlan(max_open_files=2, cache_dir=cache_dir)
        with open(output, "wb") as fp:
            fp.write(b"\x00" * 192)
            phase_a = relocate_payloads_dfs(manifest, fp, plan=plan)
            write_final_shard_index(manifest, phase_a.relocation_files, fp, plan=plan)
        return manifest

    def active_skeletons(manifest_path):
        import foundinspace.octree.combine.streaming_index as streaming_index
        from foundinspace.octree.combine.manifest import read_combine_manifest

        manifest = read_combine_manifest(manifest_path, deep_validation=False)
        identity = streaming_index._topology_identity(
            manifest,
            cache_dir=cache_dir,
            star_format_version=1,
            skeleton_pack_count=256,
        )
        plan_path = streaming_index._load_topology_plan(cache_dir, identity=identity)
        assert plan_path is not None
        return {
            (entry.parent_level, entry.parent_node_id): entry
            for entry in streaming_index._iter_plan(plan_path)
        }

    original = [
        PayloadNode(level=6, node_id=0, star_count=1, raw_payload=b"a"),
        PayloadNode(level=6, node_id=2048, star_count=1, raw_payload=b"b"),
    ]
    first_manifest = compile_nodes(tmp_path / "first", original, tmp_path / "first.bin")
    first_active = active_skeletons(first_manifest)
    sibling = first_active[(4, 32)]
    sibling_pack = cache_dir / "packs" / f"{sibling.pack_digest.hex()}.pack"
    sibling_before = (
        sibling_pack.stat().st_ino,
        sibling_pack.stat().st_mtime_ns,
        sibling_pack.read_bytes(),
    )

    second_manifest = compile_nodes(
        tmp_path / "second",
        [
            *original,
            PayloadNode(level=6, node_id=1, star_count=1, raw_payload=b"a2"),
        ],
        tmp_path / "second.bin",
    )
    second_active = active_skeletons(second_manifest)

    assert second_active[(-1, 0)].digest == first_active[(-1, 0)].digest
    assert second_active[(4, 32)].digest == first_active[(4, 32)].digest
    assert second_active[(4, 32)].pack_digest == sibling.pack_digest
    assert second_active[(4, 0)].digest != first_active[(4, 0)].digest
    assert second_active[(4, 0)].pack_digest != first_active[(4, 0)].pack_digest
    assert (
        sibling_pack.stat().st_ino,
        sibling_pack.stat().st_mtime_ns,
        sibling_pack.read_bytes(),
    ) == sibling_before


@pytest.mark.parametrize(
    "strategy",
    [
        IndexEmissionStrategy.FORWARD,
        IndexEmissionStrategy.TEMP_PWRITE_BATCHED,
        IndexEmissionStrategy.TEMP_PWRITE_PER_CHILD,
    ],
)
def test_sparse_skeleton_packs_bound_files_and_underlying_opens(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    strategy: IndexEmissionStrategy,
) -> None:
    cache_dir = tmp_path / "cache"
    pack_count = 16
    nodes = [
        PayloadNode(
            level=6,
            node_id=ancestor << 6,
            star_count=1,
            raw_payload=b"payload",
        )
        for ancestor in range(0, 1 << 12, 32)
    ]
    manifest_path = build_intermediates(tmp_path / "intermediates", nodes, max_level=6)
    plan = CombinePlan(
        max_open_files=2,
        cache_dir=cache_dir,
        skeleton_pack_count=pack_count,
        index_emission_strategy=strategy,
    )
    with open(tmp_path / "first.bin", "wb") as fp:
        fp.write(b"\x00" * 192)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
        write_final_shard_index(manifest_path, phase_a.relocation_files, fp, plan=plan)

    packs = sorted((cache_dir / "packs").glob("*.pack"))
    assert len(packs) == pack_count

    with open(tmp_path / "second.bin", "wb") as fp:
        fp.write(b"\x00" * 192)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
        pack_opens: list[str] = []
        real_open = builtins.open

        def tracking_open(path, mode="r", *args, **kwargs):
            if str(path).endswith(".pack"):
                pack_opens.append(mode)
            return real_open(path, mode, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", tracking_open)
        write_final_shard_index(manifest_path, phase_a.relocation_files, fp, plan=plan)

    # One metadata-header open and at most one emitter open per spatial pack.
    assert len(pack_opens) <= 2 * len(packs)
    assert len(nodes) >= 4 * len(pack_opens)


@pytest.mark.parametrize(
    ("strategy", "expected_calls"),
    [
        (IndexEmissionStrategy.TEMP_PWRITE_BATCHED, 1),
        (IndexEmissionStrategy.TEMP_PWRITE_PER_CHILD, 2),
    ],
)
def test_temporary_index_batches_frontier_pwrite_by_parent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    strategy: IndexEmissionStrategy,
    expected_calls: int,
) -> None:
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(level=6, node_id=0, star_count=1, raw_payload=b"left"),
            PayloadNode(level=6, node_id=64, star_count=1, raw_payload=b"right"),
        ],
        max_level=6,
    )
    plan = CombinePlan(
        max_open_files=2,
        cache_dir=tmp_path / "cache",
        index_emission_strategy=strategy,
    )
    calls: list[int] = []
    real_pwrite = os.pwrite

    def tracking_pwrite(fd, data, offset):
        calls.append(len(data))
        return real_pwrite(fd, data, offset)

    monkeypatch.setattr(os, "pwrite", tracking_pwrite)
    with open(tmp_path / "out.bin", "wb") as fp:
        fp.write(b"\x00" * 192)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
        write_final_shard_index(manifest_path, phase_a.relocation_files, fp, plan=plan)

    assert len(calls) == expected_calls
    assert sum(calls) == 16


def test_pwrite_all_retries_partial_writes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import foundinspace.octree.combine.streaming_index as streaming_index

    path = tmp_path / "partial-pwrite.bin"
    path.write_bytes(b"\x00" * 12)
    calls = 0
    real_pwrite = os.pwrite

    def partial_pwrite(fd, data, offset):
        nonlocal calls
        calls += 1
        return real_pwrite(fd, data[:3], offset)

    monkeypatch.setattr(os, "pwrite", partial_pwrite)
    with open(path, "r+b") as fp:
        streaming_index._pwrite_all(fp.fileno(), b"abcdefghijkl", 0)

    assert calls == 4
    assert path.read_bytes() == b"abcdefghijkl"


def test_temporary_index_is_cleaned_after_pwrite_failure(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import foundinspace.octree.combine.streaming_index as streaming_index

    cache_dir = tmp_path / "cache"
    manifest_path = build_intermediates(
        tmp_path / "intermediates",
        [PayloadNode(level=6, node_id=0, star_count=1, raw_payload=b"deep")],
        max_level=6,
    )
    plan = CombinePlan(
        cache_dir=cache_dir,
        index_emission_strategy=IndexEmissionStrategy.TEMP_PWRITE_BATCHED,
    )
    with open(tmp_path / "out.bin", "wb") as fp:
        fp.write(b"\x00" * 192)
        phase_a = relocate_payloads_dfs(manifest_path, fp, plan=plan)
        monkeypatch.setattr(
            streaming_index,
            "_pwrite_all",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected")),
        )
        with pytest.raises(OSError, match="injected"):
            write_final_shard_index(
                manifest_path, phase_a.relocation_files, fp, plan=plan
            )

    assert not list(cache_dir.glob(".compile-*.tmp"))


def test_cache_lock_preserves_live_compiler_temporary_tree(tmp_path) -> None:
    import foundinspace.octree.combine.streaming_index as streaming_index

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    live = cache_dir / ".compile-live.tmp"
    started = threading.Event()
    finished = threading.Event()

    def competing_cleanup() -> None:
        started.set()
        with streaming_index._cache_lock(cache_dir):
            streaming_index._clean_incomplete_trees(cache_dir)
        finished.set()

    with streaming_index._cache_lock(cache_dir):
        live.mkdir()
        (live / "partial").write_bytes(b"still in use")
        thread = threading.Thread(target=competing_cleanup)
        thread.start()
        assert started.wait(timeout=1)
        assert not finished.wait(timeout=0.1)
        assert live.is_dir()

    thread.join(timeout=2)
    assert finished.is_set()
    assert not live.exists()
