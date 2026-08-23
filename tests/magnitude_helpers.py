from __future__ import annotations

import math

from foundinspace.octree.config import MORTON_BITS, WORLD_HALF_SIZE_PC
from foundinspace.octree.mag_levels import _mag_threshold_at_level


def represented_magnitude_for_level(
    level: int,
    seed: float | None,
    *,
    index_magnitude: float = 6.5,
    world_half_size: float = WORLD_HALF_SIZE_PC,
    morton_bits: int = MORTON_BITS,
) -> float | None:
    """Return a deterministic centimagnitude safely inside a natural band."""
    if seed is None:
        return None
    if not math.isfinite(float(seed)):
        return float(seed)
    finite_seed = float(seed)
    offset = (finite_seed * 0.1) % 1.3
    if level < morton_bits:
        upper = _mag_threshold_at_level(index_magnitude, world_half_size, level)
        return round(upper - 1.4 + offset, 2)
    lower = _mag_threshold_at_level(
        index_magnitude,
        world_half_size,
        morton_bits - 1,
    )
    return round(lower + 0.1 + offset, 2)
