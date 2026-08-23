from __future__ import annotations

import math

DEFAULT_LOAD_FACTOR = 2.0
MIN_LOAD_FACTOR = 1.0
MAX_LOAD_FACTOR = 2.0


def validate_load_factor(load_factor: float) -> float:
    """Return a finite loader quality factor in the supported range."""
    value = float(load_factor)
    if not math.isfinite(value) or not MIN_LOAD_FACTOR <= value <= MAX_LOAD_FACTOR:
        raise ValueError(
            "load_factor must be finite and in "
            f"[{MIN_LOAD_FACTOR}, {MAX_LOAD_FACTOR}], got {load_factor}"
        )
    return value


def half_size_at_level(world_half_size: float, level: int) -> float:
    """Return H(level) for an octree rooted at world_half_size."""
    return float(world_half_size) / (2**int(level))


def load_radius_for_magnitude_shell(
    brightest_half_size: float,
    limiting_magnitude: float,
    index_magnitude: float,
    *,
    load_factor: float = DEFAULT_LOAD_FACTOR,
) -> float:
    """Return q H(B) scaled from index to display magnitude."""
    factor = validate_load_factor(load_factor)
    return factor * float(brightest_half_size) * (
        10.0 ** ((float(limiting_magnitude) - float(index_magnitude)) / 5.0)
    )


def complete_through_magnitude(
    limiting_magnitude: float,
    *,
    load_factor: float = DEFAULT_LOAD_FACTOR,
) -> float:
    """Return the magnitude through which a loader factor is complete."""
    factor = validate_load_factor(load_factor)
    return float(limiting_magnitude) + 5.0 * math.log10(factor / 2.0)
