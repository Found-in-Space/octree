from __future__ import annotations

import gzip
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from foundinspace.octree.assembly.meta_encoder import (
    IdentifiersMap,
    build_meta_payload,
    iter_encoded_cells_with_meta,
    write_meta_payload,
)
from foundinspace.octree.assembly.types import CellKey, EncodedCell

_NULL_IDENT = dict.fromkeys(
    (
        "gaia_source_id",
        "hip_id",
        "hd",
        "bayer",
        "flamsteed",
        "constellation",
        "proper_name",
    ),
    None,
)


def _write_ident_map(path, rows: list[dict]) -> None:
    if not rows:
        table = pa.table(
            {
                "source": pa.array([], type=pa.string()),
                "source_id": pa.array([], type=pa.string()),
                "gaia_source_id": pa.array([], type=pa.int64()),
                "hip_id": pa.array([], type=pa.int64()),
                "hd": pa.array([], type=pa.int64()),
                "bayer": pa.array([], type=pa.string()),
                "flamsteed": pa.array([], type=pa.int64()),
                "constellation": pa.array([], type=pa.string()),
                "proper_name": pa.array([], type=pa.string()),
            }
        )
    else:
        table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


class TestIdentifiersMap:
    def test_small_map_uses_bounded_memory_backend(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "hip",
                    "source_id": "1",
                    "proper_name": "Alpha",
                }
            ],
        )

        with IdentifiersMap(p, memory_limit_bytes=4096) as ident_map:
            assert ident_map.backend == "memory"
            assert 0 < ident_map.estimated_memory_bytes <= 4096
            assert ident_map.lookup("hip", "1") == {"proper_name": "Alpha"}

    def test_lookup_hit(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "hip",
                    "source_id": "71683",
                    "hip_id": 71683,
                    "proper_name": "Rigil Kentaurus",
                    "gaia_source_id": None,
                    "hd": None,
                    "bayer": None,
                    "flamsteed": None,
                    "constellation": None,
                }
            ],
        )
        m = IdentifiersMap(p)
        d = m.lookup("hip", "71683")
        assert d["hip_id"] == 71683
        assert d["proper_name"] == "Rigil Kentaurus"

    def test_lookup_miss(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [{"source": "hip", "source_id": "1"} | _NULL_IDENT],
        )
        m = IdentifiersMap(p)
        assert m.lookup("gaia", "999") == {}

    def test_field_subset(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "manual",
                    "source_id": "sun",
                    "proper_name": "Sol",
                    "gaia_source_id": None,
                    "hip_id": None,
                    "hd": None,
                    "bayer": None,
                    "flamsteed": None,
                    "constellation": None,
                }
            ],
        )
        m = IdentifiersMap(p, fields=["proper_name"])
        assert m.lookup("manual", "sun") == {"proper_name": "Sol"}

    def test_unknown_field_raises(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [{"source": "x", "source_id": "1"} | _NULL_IDENT],
        )
        with pytest.raises(ValueError, match="Unknown"):
            IdentifiersMap(p, fields=["typo_field"])

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            IdentifiersMap(tmp_path / "nope.parquet")

    def test_duplicate_key_uses_last_parquet_row(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "hip",
                    "source_id": "1",
                    "proper_name": "First",
                },
                {
                    "source": "hip",
                    "source_id": "1",
                    "proper_name": "Last",
                },
            ],
        )
        with IdentifiersMap(p, fields=["proper_name"]) as ident_map:
            assert len(ident_map) == 1
            assert ident_map.lookup("hip", "1") == {"proper_name": "Last"}

    def test_lookup_after_close_raises(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(p, [])
        ident_map = IdentifiersMap(p)
        ident_map.close()

        with pytest.raises(RuntimeError, match="closed"):
            ident_map.lookup("gaia", "1")

    def test_memory_and_sqlite_backends_are_byte_equivalent(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "hip",
                    "source_id": "1",
                    "proper_name": "First",
                    "hip_id": 1,
                },
                {
                    "source": "hip",
                    "source_id": "1",
                    "proper_name": "Last",
                    "hip_id": 1,
                },
                {
                    "source": "manual",
                    "source_id": "sun",
                    "proper_name": " Sol ",
                    "hip_id": None,
                },
            ],
        )
        identities = [("manual", "sun"), ("missing", "2"), ("hip", "1")]

        with (
            IdentifiersMap(p, memory_limit_bytes=1024 * 1024) as memory_map,
            IdentifiersMap(p, memory_limit_bytes=0) as sqlite_map,
        ):
            assert memory_map.backend == "memory"
            assert sqlite_map.backend == "sqlite"
            assert len(memory_map) == len(sqlite_map) == 2
            assert list(memory_map.iter_json_entries(identities)) == list(
                sqlite_map.iter_json_entries(identities)
            )
            assert build_meta_payload(identities, memory_map) == build_meta_payload(
                identities,
                sqlite_map,
            )


class TestBuildMetaPayload:
    def test_empty_objects_for_unknown(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(p, [])
        m = IdentifiersMap(p)
        blob = build_meta_payload([("gaia", "1"), ("gaia", "2")], m)
        arr = json.loads(gzip.decompress(blob).decode())
        assert arr == [
            {"source": "gaia", "source_id": "1"},
            {"source": "gaia", "source_id": "2"},
        ]

    def test_streaming_writer_matches_bytes_wrapper(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "manual",
                    "source_id": "sun",
                    "proper_name": "Sol",
                }
            ],
        )
        identities = [("manual", "sun"), ("gaia", "1")]
        with IdentifiersMap(p, fields=["proper_name"]) as ident_map:
            expected = build_meta_payload(identities, ident_map)
            target = io.BytesIO()
            write_meta_payload(identities, ident_map, target)

        assert target.getvalue() == expected


class TestIterEncodedCellsWithMeta:
    def test_alignment(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(p, [])
        m = IdentifiersMap(p)
        rows = iter(
            [
                (1, b"\xaa" * 16, "gaia", "10"),
                (1, b"\xbb" * 16, "gaia", "11"),
                (2, b"\xcc" * 16, "hip", "5"),
            ]
        )
        out = list(iter_encoded_cells_with_meta(rows, level=3, ident_map=m))
        assert len(out) == 2
        r0, meta0 = out[0]
        assert r0.key == CellKey(level=3, node_id=1)
        assert r0.star_count == 2
        j0 = json.loads(gzip.decompress(meta0).decode())
        assert j0 == [
            {"source": "gaia", "source_id": "10"},
            {"source": "gaia", "source_id": "11"},
        ]

        r1, meta1 = out[1]
        assert r1.key.node_id == 2
        assert r1.star_count == 1
        j1 = json.loads(gzip.decompress(meta1).decode())
        assert j1 == [{"source": "hip", "source_id": "5"}]

    def test_meta_matches_ident_map(self, tmp_path):
        p = tmp_path / "id.parquet"
        _write_ident_map(
            p,
            [
                {
                    "source": "hip",
                    "source_id": "42",
                    "hip_id": 42,
                    "proper_name": "X",
                    "gaia_source_id": None,
                    "hd": None,
                    "bayer": None,
                    "flamsteed": None,
                    "constellation": None,
                }
            ],
        )
        im = IdentifiersMap(p)
        rows = iter([(7, b"\x00" * 16, "hip", "42")])
        ((rend, blob),) = list(
            iter_encoded_cells_with_meta(rows, level=2, ident_map=im)
        )
        assert isinstance(rend, EncodedCell)
        arr = json.loads(gzip.decompress(blob).decode())
        assert arr[0]["source"] == "hip"
        assert arr[0]["source_id"] == "42"
        assert arr[0]["hip_id"] == 42
        assert arr[0]["proper_name"] == "X"
