"""Level-from-magnitude mapping for the octree build.

Derived from v_mag (indexing magnitude) and world_half_size (root half-width).
Replaces the former mag_levels.yaml and R_vis formula paths.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass

import numpy as np

from foundinspace.octree.config import MORTON_BITS


@dataclass(slots=True)
class Level:
    """Single octree level with its full-width magnitude thresholds.

    ``m_min`` is exclusive except for the root saturation band. ``m_max`` is
    inclusive except for the deepest saturation band.
    """

    id: int
    m_min: float
    m_max: float

    @property
    def steps_at_level(self) -> int:
        """Return the number of steps per axis at this level."""
        return 2**self.id


def _half_size_at_level(world_half_size: float, level: int) -> float:
    """Level L cell half-size (pc): H0 / 2^L."""
    return world_half_size / (2**level)


def _node_width_at_level(world_half_size: float, level: int) -> float:
    """Level L cell width (pc): 2 H(L)."""
    return 2.0 * _half_size_at_level(world_half_size, level)


def _mag_threshold_at_level(v_mag: float, world_half_size: float, level: int) -> float:
    """Magnitude whose visibility radius equals H(L)."""
    h = _half_size_at_level(world_half_size, level)
    return v_mag + 5.0 - 5.0 * math.log10(h)


class MagLevelConfig:
    """Level-from-magnitude mapping derived from v_mag and world_half_size.

    Interior placement rule: assign natural level N where
    H(N) <= r_V < 2 H(N), with
    r_V = 10^((v_mag - mag_abs + 5) / 5) pc.
    """

    def __init__(
        self,
        v_mag: float = 6.5,
        world_half_size: float = 200_000.0,
        morton_bits: int = MORTON_BITS,
    ) -> None:
        self.v_mag = v_mag
        self.world_half_size = world_half_size
        self.morton_bits = morton_bits
        self._levels_cache: list[Level] | None = None
        self._thresholds_cache: np.ndarray | None = None

    def _thresholds(self) -> np.ndarray:
        if self._thresholds_cache is None:
            self._thresholds_cache = np.asarray(
                [
                    _mag_threshold_at_level(
                        self.v_mag,
                        self.world_half_size,
                        level,
                    )
                    for level in range(self.morton_bits)
                ],
                dtype=np.float64,
            )
        return self._thresholds_cache

    def _build_levels(self) -> list[Level]:
        if self._levels_cache is not None:
            return self._levels_cache
        thresholds = self._thresholds()
        levels = [
            Level(
                id=level_id,
                m_min=-math.inf if level_id == 0 else float(thresholds[level_id - 1]),
                m_max=(
                    math.inf
                    if level_id == self.morton_bits
                    else float(thresholds[level_id])
                ),
            )
            for level_id in range(self.morton_bits + 1)
        ]
        self._levels_cache = levels
        return levels

    def levels(self) -> Iterator[Level]:
        """Yield disjoint, exhaustive magnitude bands for every level."""
        yield from self._build_levels()

    def get_level(self, level_id: int) -> Level | None:
        """Return the Level with the given id, or None if out of range."""
        levs = self._build_levels()
        for lev in levs:
            if lev.id == level_id:
                return lev
        return None

    def level_for_mag(self, mag_abs: float) -> int:
        """Return level id for a single absolute magnitude."""
        if math.isnan(mag_abs):
            raise ValueError("magnitude has no level assigned")
        return int(np.searchsorted(self._thresholds(), mag_abs, side="left"))

    def assign_level_array(self, mag_abs: np.ndarray) -> np.ndarray:
        """Assign level id per star from mag_abs. Returns int32 array of level ids."""
        values = np.asarray(mag_abs, dtype=np.float64)
        invalid = int(np.count_nonzero(np.isnan(values)))
        if invalid:
            raise ValueError(
                f"{invalid} star(s) have no level assigned; check magnitude range."
            )
        return np.searchsorted(self._thresholds(), values, side="left").astype(
            np.int32,
            copy=False,
        )
