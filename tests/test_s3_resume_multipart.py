from __future__ import annotations

from pathlib import Path
from typing import Any

from foundinspace.octree.s3_resume.multipart import list_all_parts
from foundinspace.octree.s3_resume.state import (
    make_new_state,
    merge_remote_parts,
    uploaded_parts_for_completion,
)


class _FakeS3:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = responses
        self.calls = 0

    def list_parts(self, **_: Any) -> dict[str, Any]:
        response = self._responses[self.calls]
        self.calls += 1
        return response


def test_list_parts_paginates() -> None:
    client = _FakeS3(
        responses=[
            {
                "IsTruncated": True,
                "NextPartNumberMarker": 2,
                "Parts": [
                    {"PartNumber": 1, "ETag": '"a"'},
                    {"PartNumber": 2, "ETag": '"b"'},
                ],
            },
            {
                "IsTruncated": False,
                "Parts": [
                    {"PartNumber": 3, "ETag": '"c"'},
                ],
            },
        ]
    )
    parts = list_all_parts(client, bucket="b", key="k", upload_id="u")
    assert sorted(parts) == [1, 2, 3]
    assert client.calls == 2


def test_completion_payload_sorted(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"x" * 20)
    state = make_new_state(
        source_path=source,
        bucket="bucket",
        key="obj",
        region=None,
        profile="profile",
        part_size=7,
        checksum_algorithm="none",
        object_params={},
    )
    merge_remote_parts(
        state,
        {
            1: {"ETag": '"1"'},
            2: {"ETag": '"2"'},
            3: {"ETag": '"3"'},
        },
    )
    parts = uploaded_parts_for_completion(state)
    assert [p["PartNumber"] for p in parts] == [1, 2, 3]


def test_completion_payload_includes_part_checksums(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"x" * 20)
    state = make_new_state(
        source_path=source,
        bucket="bucket",
        key="obj",
        region=None,
        profile="profile",
        part_size=20,
        checksum_algorithm="sha256",
        object_params={},
    )
    merge_remote_parts(
        state,
        {1: {"ETag": '"1"', "Checksum": "checksum-value"}},
    )

    assert uploaded_parts_for_completion(state) == [
        {
            "PartNumber": 1,
            "ETag": '"1"',
            "ChecksumSHA256": "checksum-value",
        }
    ]
