from __future__ import annotations

import gzip
from itertools import islice
from uuid import UUID

import pytest

import foundinspace.octree.identifiers_order as identifiers_order_module
from combine_helpers import PayloadNode, build_identifiers_intermediates
from foundinspace.octree.identifiers_order import (
    IdentifiersOrderReader,
    combine_identifiers_order,
    read_header,
)

DATASET_UUID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
ARTIFACT_UUID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def test_combine_identifiers_order_round_trip(tmp_path) -> None:
    manifest_path = build_identifiers_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=2,
                raw_payload=b"",
                identities=[("manual", "sun"), ("hip", "71683")],
            ),
            PayloadNode(
                level=1,
                node_id=5,
                star_count=1,
                raw_payload=b"",
                identities=[("gaia", "123")],
            ),
        ],
        max_level=1,
    )
    output_path = tmp_path / "identifiers.order"

    combine_identifiers_order(
        manifest_path,
        output_path,
        parent_dataset_uuid=DATASET_UUID,
        artifact_uuid=ARTIFACT_UUID,
    )

    header = read_header(output_path)
    assert header.parent_dataset_uuid == DATASET_UUID
    assert header.artifact_uuid == ARTIFACT_UUID
    assert header.record_count == 2
    assert header.directory_offset > 0
    assert header.payload_offset > header.directory_offset

    with IdentifiersOrderReader(output_path) as reader:
        records = list(reader.iter_cells())

    assert [
        (record.level, record.node_id, record.star_count)
        for record, _identities in records
    ] == [
        (0, 0, 2),
        (1, 5, 1),
    ]
    assert records[0][1] == [("manual", "sun"), ("hip", "71683")]
    assert records[1][1] == [("gaia", "123")]


def test_reader_streams_large_cell_identities_in_small_chunks(
    tmp_path,
    monkeypatch,
) -> None:
    identities = [("gaia", str(index)) for index in range(37)]
    manifest_path = build_identifiers_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=len(identities),
                raw_payload=b"",
                identities=identities,
            )
        ],
        max_level=0,
    )
    output_path = tmp_path / "identifiers.order"
    combine_identifiers_order(
        manifest_path,
        output_path,
        parent_dataset_uuid=DATASET_UUID,
        artifact_uuid=ARTIFACT_UUID,
    )
    monkeypatch.setattr(
        gzip,
        "decompress",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("whole-cell gzip decompression was used")
        ),
    )
    monkeypatch.setattr(
        identifiers_order_module,
        "IDENTITY_COMPRESSED_READ_BYTES",
        7,
    )

    with IdentifiersOrderReader(output_path) as reader:
        cells = reader.iter_cell_identities()
        record, identity_stream = next(cells)
        decoded: list[tuple[str, str]] = []
        while chunk := list(islice(identity_stream, 4)):
            assert len(chunk) <= 4
            decoded.extend(chunk)
        assert list(cells) == []

    assert record.star_count == 37
    assert decoded == identities


def test_reader_exposes_bounded_raw_identity_payloads(tmp_path) -> None:
    identities = [("manual", "sun"), ("hip", "71683"), ("gaia", "123")]
    manifest_path = build_identifiers_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=0,
                node_id=0,
                star_count=len(identities),
                raw_payload=b"",
                identities=identities,
            )
        ],
        max_level=0,
    )
    output_path = tmp_path / "identifiers.order"
    combine_identifiers_order(
        manifest_path,
        output_path,
        parent_dataset_uuid=DATASET_UUID,
        artifact_uuid=ARTIFACT_UUID,
    )

    with IdentifiersOrderReader(output_path) as reader:
        [(record, payload)] = list(
            reader.iter_cell_identity_payloads(max_uncompressed_bytes=1024)
        )
    assert record.star_count == len(identities)
    assert b"manual" in payload
    assert b"71683" in payload

    with (
        IdentifiersOrderReader(output_path) as reader,
        pytest.raises(ValueError, match="memory bound"),
    ):
        list(reader.iter_cell_identity_payloads(max_uncompressed_bytes=4))
