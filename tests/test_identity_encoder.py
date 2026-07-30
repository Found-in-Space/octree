from __future__ import annotations

import gzip
from io import BytesIO

import pyarrow as pa

from foundinspace.octree.assembly.identity_encoder import (
    encode_identity_rows,
    write_identity_arrays,
)


def test_write_identity_arrays_matches_row_encoder_for_sliced_utf8() -> None:
    sources = pa.array(["unused", "gaia", "café", ""], type=pa.string()).slice(1)
    source_ids = pa.array(["unused", "1", "α-2", ""], type=pa.string()).slice(1)
    output = BytesIO()

    write_identity_arrays(output, sources, source_ids)

    expected = gzip.decompress(
        encode_identity_rows(
            [
                ("gaia", "1"),
                ("café", "α-2"),
                ("", ""),
            ]
        )
    )
    assert output.getvalue() == expected
