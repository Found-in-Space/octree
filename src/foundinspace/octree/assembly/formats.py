from __future__ import annotations

import struct

MANIFEST_FORMAT = "three_dee.octree.intermediates/v1"
PAYLOAD_CODEC = "gzip"

INDEX_FILE_HDR = struct.Struct("<4sHHBBHIQQ")
INDEX_RECORD = struct.Struct("<QQII")

INDEX_MAGIC = b"OIDX"
INDEX_VERSION = 1
INDEX_HEADER_SIZE = INDEX_FILE_HDR.size  # 32

FLAG_GZIP = 1 << 0
FLAG_SORTED = 1 << 1
DEFAULT_FLAGS = FLAG_GZIP | FLAG_SORTED
