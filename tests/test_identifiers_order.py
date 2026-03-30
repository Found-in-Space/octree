from __future__ import annotations

from uuid import UUID

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
