from __future__ import annotations

import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from combine_helpers import PayloadNode, build_identifiers_intermediates
from foundinspace.octree.identifier_bigfile import (
    BLOCK_RECORD_STRUCT,
    HEADER_STRUCT,
    SHARD_RECORD_STRUCT,
    build_identifier_bigfile,
    query_identifier_bigfile,
)
from foundinspace.octree.identifiers_order import combine_identifiers_order


def _decode_block(data: bytes) -> list[tuple[str, int, int, int, int]]:
    count = struct.unpack_from("<I", data, 0)[0]
    offsets = list(struct.unpack_from(f"<{count}I", data, 4))
    base = 4 + count * 4
    out: list[tuple[str, int, int, int, int]] = []
    for offset in offsets:
        pos = base + offset
        term_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        term = data[pos : pos + term_len].decode("utf-8")
        pos += term_len
        flag, level, node_id, ordinal = struct.unpack_from("<BHQI", data, pos)
        out.append((term, flag, level, node_id, ordinal))
    return out


def test_build_identifier_bigfile_includes_normalized_and_resolved_ids(tmp_path: Path) -> None:
    manifest_path = build_identifiers_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=1,
                node_id=5,
                star_count=2,
                raw_payload=b"",
                identities=[("manual", "Alpha Ori"), ("hip", "27989")],
            )
        ],
        max_level=1,
    )
    order_path = tmp_path / "identifiers.order"
    combine_identifiers_order(
        manifest_path,
        order_path,
        parent_dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        artifact_uuid=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    )

    table = pa.table(
        {
            "source": ["manual", "hip"],
            "source_id": ["Alpha Ori", "27989"],
            "proper_name": ["Betelgeuse", None],
            "hip_id": [27989, 27989],
            "gaia_source_id": [None, None],
            "hd": [39801, None],
        }
    )
    map_path = tmp_path / "identifiers_map.parquet"
    pq.write_table(table, map_path)

    output_path = tmp_path / "identifiers.bigfile"
    build_identifier_bigfile(
        identifiers_order_path=order_path,
        identifiers_map_path=map_path,
        output_path=output_path,
        target_block_bytes=120,
    )

    data = output_path.read_bytes()
    (
        _magic,
        _version,
        _header_size,
        _target_block,
        _top_count,
        shard_count,
        block_count,
        _top_offset,
        shard_offset,
        block_index_offset,
        blocks_offset,
    ) = HEADER_STRUCT.unpack_from(data, 0)
    assert shard_count > 0
    assert block_count > 0

    terms: set[str] = set()
    for shard_idx in range(shard_count):
        _shard_key, _reserved, block_start, block_count_for_shard = SHARD_RECORD_STRUCT.unpack_from(
            data,
            shard_offset + shard_idx * SHARD_RECORD_STRUCT.size,
        )
        for rel_idx in range(block_count_for_shard):
            block_record = BLOCK_RECORD_STRUCT.unpack_from(
                data,
                block_index_offset + (block_start + rel_idx) * BLOCK_RECORD_STRUCT.size,
            )
            rel_offset = block_record[0]
            rel_length = block_record[1]
            block_bytes = data[
                blocks_offset + rel_offset : blocks_offset + rel_offset + rel_length
            ]
            decoded = _decode_block(block_bytes)
            terms.update(term for term, *_rest in decoded)

    assert "alphaori" in terms
    assert "betelgeuse" in terms
    assert "27989" in terms
    assert "39801" in terms


def test_same_term_different_types_stay_in_same_block(tmp_path: Path) -> None:
    manifest_path = build_identifiers_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=0,
                node_id=1,
                star_count=2,
                raw_payload=b"",
                identities=[("hip", "27989"), ("manual", "27989")],
            )
        ],
        max_level=0,
    )
    order_path = tmp_path / "identifiers.order"
    combine_identifiers_order(
        manifest_path,
        order_path,
        parent_dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        artifact_uuid=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    )
    map_path = tmp_path / "identifiers_map.parquet"
    pq.write_table(pa.table({"source": [], "source_id": []}), map_path)

    output_path = tmp_path / "identifiers.bigfile"
    build_identifier_bigfile(
        identifiers_order_path=order_path,
        identifiers_map_path=map_path,
        output_path=output_path,
        target_block_bytes=60,
    )

    data = output_path.read_bytes()
    header = HEADER_STRUCT.unpack_from(data, 0)
    block_index_offset = header[9]
    blocks_offset = header[10]
    block_count = header[6]

    matches = 0
    for idx in range(block_count):
        block_record = BLOCK_RECORD_STRUCT.unpack_from(
            data,
            block_index_offset + idx * BLOCK_RECORD_STRUCT.size,
        )
        rel_offset = block_record[0]
        rel_length = block_record[1]
        block_bytes = data[
            blocks_offset + rel_offset : blocks_offset + rel_offset + rel_length
        ]
        decoded = _decode_block(block_bytes)
        same = [row for row in decoded if row[0] == "27989"]
        if same:
            matches += len(same)
            assert {flag for _term, flag, *_rest in same} == {0, 2}
    assert matches == 2


def test_query_identifier_bigfile_local_and_http_range(tmp_path: Path) -> None:
    manifest_path = build_identifiers_intermediates(
        tmp_path / "intermediates",
        [
            PayloadNode(
                level=1,
                node_id=7,
                star_count=1,
                raw_payload=b"",
                identities=[("manual", "Alpha Ori")],
            )
        ],
        max_level=1,
    )
    order_path = tmp_path / "identifiers.order"
    combine_identifiers_order(
        manifest_path,
        order_path,
        parent_dataset_uuid=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        artifact_uuid=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    )
    map_path = tmp_path / "identifiers_map.parquet"
    pq.write_table(
        pa.table(
            {
                "source": ["manual"],
                "source_id": ["Alpha Ori"],
                "proper_name": ["Betelgeuse"],
                "hip_id": [27989],
            }
        ),
        map_path,
    )
    bigfile_path = tmp_path / "identifiers.bigfile"
    build_identifier_bigfile(
        identifiers_order_path=order_path,
        identifiers_map_path=map_path,
        output_path=bigfile_path,
        target_block_bytes=100,
    )

    local_matches, local_stats = query_identifier_bigfile(
        query="betelg",
        path=bigfile_path,
        limit=10,
    )
    assert any(row.term == "betelgeuse" for row in local_matches)
    assert local_stats.requests > 0

    data = bigfile_path.read_bytes()

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            header = self.headers.get("Range")
            if header is None:
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            _, span = header.split("=")
            start_raw, end_raw = span.split("-")
            start = int(start_raw)
            end = int(end_raw)
            chunk = data[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.end_headers()
            self.wfile.write(chunk)

        def log_message(self, format: str, *args):  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/identifiers.bigfile"
        remote_matches, remote_stats = query_identifier_bigfile(
            query="betelg",
            url=url,
            limit=10,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert any(row.term == "betelgeuse" for row in remote_matches)
    assert remote_stats.requests >= 3
