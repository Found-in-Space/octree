from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from click.testing import CliRunner

from foundinspace.octree._cli import cli
from foundinspace.octree.identifiers_order import pack_identifiers_order
from foundinspace.octree.identity_locator import (
    IdentityLocatorBenchmarkConfig,
    IdentityLocatorBuildConfig,
    IdentityLocatorReader,
    benchmark_identity_locator,
    build_identity_locator,
    validate_identity_locator,
)
from foundinspace.octree.identity_locator.format import (
    CODEC_COMPACT,
    FOOTER_SIZE,
    HEADER_SIZE,
    NAMESPACE_SIZE,
    PAGE_HEADER_SIZE,
    pack_page,
    unpack_header,
    unpack_namespace,
    unpack_page,
)
from foundinspace.octree.identity_locator.leaf import (
    decode_compact_leaf,
    encode_compact_leaf,
    parse_compact_leaf,
)
from foundinspace.octree.packing import PackingPlan, pack_octree
from foundinspace.octree.packing.records import PackedDescriptorFields
from packing_helpers import (
    PayloadNode,
    build_identifiers_intermediates,
    build_intermediates,
)
from project_helpers import project_text

DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
ORDER_UUID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _build_dataset(
    root: Path,
    cells: list[list[tuple[str, str]]],
    *,
    order_parent_uuid: UUID = DATASET_UUID,
) -> tuple[Path, Path]:
    nodes = [
        PayloadNode(
            level=min(index, 1),
            node_id=index,
            star_count=len(identities),
            raw_payload=b"\x00" * (16 * len(identities)),
            identities=identities,
        )
        for index, identities in enumerate(cells)
    ]
    render_manifest = build_intermediates(
        root / "render-intermediates",
        nodes,
        max_level=1,
    )
    identifiers_manifest = build_identifiers_intermediates(
        root / "identity-intermediates",
        nodes,
        max_level=1,
    )
    render_path = root / "stars.octree"
    order_path = root / "identifiers.order"
    pack_octree(
        render_manifest,
        render_path,
        plan=PackingPlan(max_open_files=2),
        descriptor=PackedDescriptorFields(
            artifact_kind="render",
            dataset_uuid=DATASET_UUID,
        ),
    )
    pack_identifiers_order(
        identifiers_manifest,
        order_path,
        parent_dataset_uuid=order_parent_uuid,
        artifact_uuid=ORDER_UUID,
    )
    return render_path, order_path


def _config(
    root: Path,
    render_path: Path,
    order_path: Path,
    *,
    page_size: int = 128,
    codec: str = "delta-dict-b32",
    retain_work: bool = False,
) -> IdentityLocatorBuildConfig:
    return IdentityLocatorBuildConfig(
        render_octree_path=render_path,
        identifiers_order_path=order_path,
        output_path=root / "stars.identity-locator.idx",
        report_path=root / "stars.identity-locator.report.json",
        work_dir=root / "locator-work",
        decoded_page_size=page_size,
        leaf_codec=codec,
        scan_batch_bytes=64,
        merge_fan_in=2,
        merge_batch_rows=3,
        retain_work=retain_work,
    )


def _write_project(project_path: Path, render_path: Path, order_path: Path) -> None:
    root = project_path.parent
    project_path.write_text(
        project_text(
            root,
            render_output_path=render_path,
            identifiers_order_output_path=order_path,
        ),
        encoding="utf-8",
    )


def test_format_sizes_are_fixed() -> None:
    assert HEADER_SIZE == 160
    assert NAMESPACE_SIZE == 128
    assert PAGE_HEADER_SIZE == 64
    assert FOOTER_SIZE == 64


def test_builder_and_reader_round_trip_multiple_tree_levels(
    tmp_path: Path,
) -> None:
    cells = [
        [("gaia", str(value)) for value in range(index * 6 + 1, index * 6 + 7)]
        for index in range(6)
    ]
    cells[0].insert(0, ("manual", "sun"))
    cells[1].append(("hip", "71683"))
    render_path, order_path = _build_dataset(tmp_path, cells)
    result = build_identity_locator(
        _config(tmp_path, render_path, order_path, retain_work=True)
    )

    assert result.namespace_counts == {"gaia": 36, "hip": 1}
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["validation"]["checked_present"] >= 3
    assert report["skipped_namespace_rows"] == 1

    with IdentityLocatorReader(result.output_path, order_path) as reader:
        first = reader.verify_lookup("gaia", "1")
        middle = reader.verify_lookup("gaia", 19)
        hip = reader.verify_lookup("HIP", "71683")
        assert reader.lookup("gaia", "37") is None

    assert first is not None
    assert (first.level, first.morton_code, first.ordinal) == (0, 0, 1)
    assert middle is not None
    assert middle.morton_code == 3
    assert hip is not None
    assert hip.morton_code == 1
    validate_identity_locator(result.output_path, order_path)


def test_builder_is_byte_deterministic(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", "10"), ("hip", "2")], [("gaia", "3")]],
    )
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first = build_identity_locator(_config(first_root, render_path, order_path))
    second = build_identity_locator(_config(second_root, render_path, order_path))

    assert first.locator_uuid == second.locator_uuid
    assert first.output_sha256 == second.output_sha256
    assert first.output_path.read_bytes() == second.output_path.read_bytes()


def test_builder_rejects_duplicate_identity_across_runs(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", "10")], [("gaia", "10")]],
    )
    with pytest.raises(ValueError, match="Duplicate rendered identity"):
        build_identity_locator(_config(tmp_path, render_path, order_path))


@pytest.mark.parametrize("bad_id", ["01", "-1", "1.5", "18446744073709551616"])
def test_builder_rejects_noncanonical_numeric_ids(
    tmp_path: Path,
    bad_id: str,
) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", bad_id)]],
    )
    with pytest.raises(ValueError, match="source ID|source IDs"):
        build_identity_locator(_config(tmp_path, render_path, order_path))


def test_builder_rejects_mismatched_render_and_order(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", "1")]],
        order_parent_uuid=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
    )
    with pytest.raises(ValueError, match="does not match"):
        build_identity_locator(_config(tmp_path, render_path, order_path))


def test_builder_handles_empty_target_namespaces(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("manual", "sun"), ("future", "value")]],
    )
    result = build_identity_locator(_config(tmp_path, render_path, order_path))

    assert result.namespace_counts == {"gaia": 0, "hip": 0}
    with IdentityLocatorReader(result.output_path, order_path) as reader:
        assert reader.lookup("gaia", "1") is None
        assert reader.lookup("hip", "1") is None


def test_existing_output_requires_force_for_new_settings(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "1")]])
    config = _config(tmp_path, render_path, order_path)
    first = build_identity_locator(config)
    changed = replace(config, decoded_page_size=256)

    with pytest.raises(FileExistsError, match="does not match"):
        build_identity_locator(changed)
    second = build_identity_locator(replace(changed, force=True))

    assert second.locator_uuid != first.locator_uuid


def test_builder_supports_u64_boundaries_and_future_namespace_fallback(
    tmp_path: Path,
) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [
            [
                ("manual", "\x04\x00gaia\x01\x001"),
                ("future", "value"),
                ("gaia", "0"),
                ("hip", "18446744073709551615"),
            ]
        ],
    )
    result = build_identity_locator(_config(tmp_path, render_path, order_path))

    with IdentityLocatorReader(result.output_path, order_path) as reader:
        gaia = reader.verify_lookup("gaia", "0")
        hip = reader.verify_lookup("hip", "18446744073709551615")

    assert gaia is not None and gaia.ordinal == 2
    assert hip is not None and hip.ordinal == 3


def test_builder_resumes_after_checkpointed_scan(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", str(index + 1))] for index in range(4)],
    )
    config = replace(
        _config(tmp_path, render_path, order_path, retain_work=True),
        progress_interval_cells=1,
    )

    def stop_after_checkpoint(progress) -> None:
        if progress.phase == "scan" and 0 < progress.completed < progress.total:
            raise RuntimeError("simulated interruption")

    with pytest.raises(RuntimeError, match="simulated interruption"):
        build_identity_locator(replace(config, progress=stop_after_checkpoint))
    state = json.loads((config.work_dir / "state.json").read_text(encoding="utf-8"))
    assert 0 < state["next_record"] < 4

    result = build_identity_locator(config)
    assert result.namespace_counts["gaia"] == 4


def test_force_replacement_preserves_old_output_until_atomic_publish(
    monkeypatch,
    tmp_path: Path,
) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "1")]])
    config = _config(tmp_path, render_path, order_path)
    first = build_identity_locator(config)
    old_output = first.output_path.read_bytes()
    old_report = first.report_path.read_bytes()

    def fail_assembly(*_args, **_kwargs):
        raise RuntimeError("simulated assembly failure")

    monkeypatch.setattr(
        "foundinspace.octree.identity_locator.builder._assemble_locator",
        fail_assembly,
    )
    with pytest.raises(RuntimeError, match="simulated assembly failure"):
        build_identity_locator(replace(config, force=True))

    assert first.output_path.read_bytes() == old_output
    assert first.report_path.read_bytes() == old_report


def test_reader_rejects_corrupt_page(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", "1"), ("gaia", "2")]],
    )
    result = build_identity_locator(_config(tmp_path, render_path, order_path))
    raw = bytearray(result.output_path.read_bytes())
    raw[HEADER_SIZE + 2 * NAMESPACE_SIZE + PAGE_HEADER_SIZE] ^= 0x01
    result.output_path.write_bytes(raw)

    with (
        IdentityLocatorReader(result.output_path, order_path) as reader,
        pytest.raises(ValueError, match="checksum"),
    ):
        reader.lookup("gaia", "1")


@pytest.mark.parametrize(
    ("cell_record", "ordinal", "message"),
    [
        (1, 0, "cell record is out of range"),
        (0, 2, "ordinal exceeds"),
    ],
)
def test_reader_rejects_validly_checksummed_out_of_range_reference(
    tmp_path: Path,
    cell_record: int,
    ordinal: int,
    message: str,
) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "1")]])
    result = build_identity_locator(_config(tmp_path, render_path, order_path))
    raw = bytearray(result.output_path.read_bytes())
    header = unpack_header(raw[:HEADER_SIZE])
    directory_start = header.namespace_directory_offset
    descriptor = unpack_namespace(
        raw[directory_start : directory_start + NAMESPACE_SIZE]
    )
    page_start = descriptor.root_offset
    page_end = page_start + descriptor.root_length
    page = unpack_page(raw[page_start:page_end])
    records = decode_compact_leaf(page.decoded, entry_count=page.entry_count).copy()
    records["cell_record"][0] = cell_record
    records["ordinal"][0] = ordinal
    key_shift = parse_compact_leaf(
        page.decoded,
        entry_count=page.entry_count,
    ).key_shift
    decoded = encode_compact_leaf(records, key_shift=key_shift)
    replacement = pack_page(
        kind=page.kind,
        codec=CODEC_COMPACT,
        entry_count=page.entry_count,
        decoded=decoded,
    )
    assert len(replacement) == descriptor.root_length
    raw[page_start:page_end] = replacement
    result.output_path.write_bytes(raw)

    with (
        IdentityLocatorReader(result.output_path, order_path) as reader,
        pytest.raises(ValueError, match=message),
    ):
        reader.lookup("gaia", "1")


def test_reader_rejects_truncated_locator_object(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "1")]])
    result = build_identity_locator(_config(tmp_path, render_path, order_path))
    result.output_path.write_bytes(result.output_path.read_bytes()[:-1])

    with pytest.raises(ValueError, match="object length"):
        IdentityLocatorReader(result.output_path, order_path)


def test_validator_rejects_absent_present_key_sample(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "1")]])
    result = build_identity_locator(_config(tmp_path, render_path, order_path))

    with pytest.raises(ValueError, match="present-key sample is absent"):
        validate_identity_locator(
            result.output_path,
            order_path,
            samples={"gaia": [2]},
        )


class _FakeHttpResponse:
    def __init__(self, raw: bytes, *, status: int, headers: dict[str, str]) -> None:
        self._raw = raw
        self.status = status
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self) -> bytes:
        return self._raw


class _FakeRangeServer:
    def __init__(
        self,
        values: dict[str, bytes],
        *,
        status: int = 206,
        encoding: str = "identity",
        truncate: bool = False,
        changing_validator: bool = False,
        omit_validator: bool = False,
    ) -> None:
        self.values = values
        self.status = status
        self.encoding = encoding
        self.truncate = truncate
        self.changing_validator = changing_validator
        self.omit_validator = omit_validator
        self.requests: list[tuple[str, str]] = []

    def __call__(self, request):
        range_header = request.get_header("Range")
        assert range_header is not None
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_header)
        assert match is not None
        start, end = (int(value) for value in match.groups())
        value = self.values[request.full_url]
        raw = value[start : end + 1]
        if self.truncate:
            raw = raw[:-1]
        self.requests.append((request.full_url, range_header))
        suffix = len(self.requests) if self.changing_validator else 1
        headers = {
            "Content-Range": f"bytes {start}-{end}/{len(value)}",
            "Content-Encoding": self.encoding,
        }
        if not self.omit_validator:
            headers["ETag"] = f'"test-{suffix}"'
        return _FakeHttpResponse(raw, status=self.status, headers=headers)


def test_http_range_lookup_and_reader_cache(monkeypatch, tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", "1"), ("hip", "2")]],
    )
    result = build_identity_locator(_config(tmp_path, render_path, order_path))
    locator_url = "https://example.test/locator"
    order_url = "https://example.test/identifiers"
    server = _FakeRangeServer(
        {
            locator_url: result.output_path.read_bytes(),
            order_url: order_path.read_bytes(),
        }
    )
    monkeypatch.setattr(
        "foundinspace.octree.identity_locator.reader.urlopen",
        server,
    )

    with IdentityLocatorReader(locator_url, order_url) as reader:
        assert reader.lookup("gaia", "1") is not None
        after_first = reader.range_metrics.copy()
        assert reader.lookup("gaia", "1") is not None
        assert reader.range_metrics == after_first
    assert server.requests


@pytest.mark.parametrize(
    ("server_options", "message"),
    [
        ({"status": 200}, "ignored the byte range"),
        ({"encoding": "gzip"}, "Content-Encoding"),
        ({"truncate": True}, "truncated"),
        ({"changing_validator": True}, "validator changed"),
        ({"omit_validator": True}, "no stable validator"),
    ],
)
def test_http_range_reader_rejects_invalid_responses(
    monkeypatch,
    tmp_path: Path,
    server_options: dict[str, object],
    message: str,
) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "1")]])
    result = build_identity_locator(_config(tmp_path, render_path, order_path))
    locator_url = "https://example.test/locator"
    order_url = "https://example.test/identifiers"
    server = _FakeRangeServer(
        {
            locator_url: result.output_path.read_bytes(),
            order_url: order_path.read_bytes(),
        },
        **server_options,
    )
    monkeypatch.setattr(
        "foundinspace.octree.identity_locator.reader.urlopen",
        server,
    )

    with pytest.raises(ValueError, match=message):
        IdentityLocatorReader(locator_url, order_url)


def test_benchmark_builds_compact_page_size_candidates_from_shared_runs(
    tmp_path: Path,
) -> None:
    render_path, order_path = _build_dataset(
        tmp_path,
        [[("gaia", str(value)) for value in range(1, 12)]],
    )
    result = benchmark_identity_locator(
        IdentityLocatorBenchmarkConfig(
            render_octree_path=render_path,
            identifiers_order_path=order_path,
            work_dir=tmp_path / "benchmark-work",
            report_path=tmp_path / "benchmark.json",
            scan_batch_bytes=64,
            merge_fan_in=2,
            merge_batch_rows=3,
            repetitions=1,
            retain_candidates=True,
        )
    )
    report = json.loads(result.report_path.read_text(encoding="utf-8"))

    assert len(report["results"]) == 2
    assert result.winner_candidate_path.is_file()
    assert {candidate["leaf_codec"] for candidate in report["results"]} == {
        "delta-dict-b32"
    }
    assert {candidate["page_size"] for candidate in report["results"]} == {
        16 * 1024,
        32 * 1024,
    }
    assert all("p95_transferred_bytes" in value for value in report["results"])


def test_identity_locator_cli_build_defaults_and_lookup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "123")]])
    project_path = tmp_path / "project.toml"
    _write_project(project_path, render_path, order_path)
    calls: list[object] = []

    def fake_build(config):
        calls.append(config)
        return SimpleNamespace(
            namespace_counts={"gaia": 1, "hip": 0},
            locator_uuid=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
            output_sha256="d" * 64,
            output_path=config.output_path,
            report_path=config.report_path,
        )

    monkeypatch.setattr(
        "foundinspace.octree.identity_locator.build_identity_locator", fake_build
    )
    result = CliRunner().invoke(
        cli,
        ["identity-locator", "build", "--project", str(project_path)],
    )

    assert result.exit_code == 0, result.output
    config = calls[0]
    assert config.output_path == tmp_path / "stars.identity-locator.idx"
    assert config.report_path == tmp_path / "stars.identity-locator.report.json"
    assert config.work_dir == tmp_path / ".stars.identity-locator.work"


def test_identity_locator_cli_lookup_and_validate_json(tmp_path: Path) -> None:
    render_path, order_path = _build_dataset(tmp_path, [[("gaia", "123")]])
    build = build_identity_locator(_config(tmp_path, render_path, order_path))
    runner = CliRunner()

    lookup = runner.invoke(
        cli,
        [
            "identity-locator",
            "lookup",
            str(build.output_path),
            str(order_path),
            "gaia",
            "123",
            "--json",
        ],
    )
    validation = runner.invoke(
        cli,
        [
            "identity-locator",
            "validate",
            str(build.output_path),
            str(order_path),
            "--report",
            str(build.report_path),
        ],
    )

    assert lookup.exit_code == 0, lookup.output
    assert json.loads(lookup.output)["ordinal"] == 0
    assert validation.exit_code == 0, validation.output
    assert json.loads(validation.output)["checked_present"] >= 1
