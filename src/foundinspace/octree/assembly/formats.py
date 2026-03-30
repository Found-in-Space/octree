from __future__ import annotations

import struct

MANIFEST_FORMAT = "three_dee.octree.intermediates/v1"
SIDECAR_MANIFEST_FORMAT = "three_dee.octree.sidecars/v1"
PAYLOAD_CODEC = "gzip"

RENDER_ARTIFACT_KIND = "render"
IDENTIFIERS_ARTIFACT_KIND = "identifiers"
SIDECAR_ARTIFACT_KIND = "sidecar"

RENDER_MANIFEST_NAME = "render-manifest.json"
IDENTIFIERS_MANIFEST_NAME = "identifiers-manifest.json"

INDEX_FILE_HDR = struct.Struct("<4sHHBBHIQQ")
INDEX_RECORD = struct.Struct("<QQII")

INDEX_MAGIC = b"OIDX"
IDENTIFIERS_INDEX_MAGIC = b"OIDN"
SIDECAR_INDEX_MAGIC = b"OSIX"
META_INDEX_MAGIC = SIDECAR_INDEX_MAGIC
INDEX_VERSION = 1
INDEX_HEADER_SIZE = INDEX_FILE_HDR.size  # 32

FLAG_GZIP = 1 << 0
FLAG_SORTED = 1 << 1
DEFAULT_FLAGS = FLAG_GZIP | FLAG_SORTED
