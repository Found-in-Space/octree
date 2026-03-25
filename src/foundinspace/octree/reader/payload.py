from __future__ import annotations

import gzip
import logging
import math
import struct
from dataclasses import dataclass
from typing import BinaryIO

from ..encoding.teff import TEFF_HI, TEFF_LO, TEFF_SENTINEL
from .index import NodeEntry, Point

LOGGER = logging.getLogger(__name__)
STAR_RECORD_FMT = struct.Struct("<fffhBB")


@dataclass(frozen=True, slots=True)
class Star:
    position: Point
    magnitude: float
    teff: float
    node: NodeEntry

    def apparent_magnitude_at(self, point: Point) -> float:
        distance_pc = max(self.position.distance_to(point), 1e-6)
        return self.magnitude + 5.0 * (math.log10(distance_pc) - 1.0)


def decode_payload(fp: BinaryIO, node: NodeEntry, record_size: int) -> list[Star]:
    if not node.has_payload:
        return []
    fp.seek(node.payload_offset)
    compressed = fp.read(node.payload_length)
    if len(compressed) != node.payload_length:
        LOGGER.warning(
            "Truncated payload read for node at level=%s grid=%s,%s,%s",
            node.level,
            node.grid.x,
            node.grid.y,
            node.grid.z,
        )
        return []
    try:
        raw = gzip.decompress(compressed)
    except Exception:
        LOGGER.warning(
            "Failed to decompress payload for node at level=%s grid=%s,%s,%s",
            node.level,
            node.grid.x,
            node.grid.y,
            node.grid.z,
            exc_info=True,
        )
        return []

    if record_size != STAR_RECORD_FMT.size:
        raise ValueError(
            f"Unsupported payload record size: expected {STAR_RECORD_FMT.size}, got {record_size}"
        )

    usable_bytes = (len(raw) // record_size) * record_size
    stars: list[Star] = []
    ratio = TEFF_HI / TEFF_LO
    for offset in range(0, usable_bytes, record_size):
        x_rel, y_rel, z_rel, mag_centi, teff_log8, _pad = STAR_RECORD_FMT.unpack_from(
            raw, offset
        )
        teff = (
            float("nan")
            if teff_log8 == TEFF_SENTINEL
            else TEFF_LO * (ratio ** (teff_log8 / 255.0))
        )
        stars.append(
            Star(
                position=Point(
                    x=node.center.x + float(x_rel) * node.half_size,
                    y=node.center.y + float(y_rel) * node.half_size,
                    z=node.center.z + float(z_rel) * node.half_size,
                ),
                magnitude=float(mag_centi) / 100.0,
                teff=float(teff),
                node=node,
            )
        )
    return stars
