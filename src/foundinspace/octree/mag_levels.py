"""Level-from-magnitude mapping for the octree build.

Derived from v_mag (indexing magnitude) and world_half_size (root half-width).
Replaces the former mag_levels.yaml and R_vis formula paths.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterator

import numpy as np


@dataclass(slots=True)
class Level:
    """Single octree level with magnitude band [m_min, m_max]."""

    id: int
    m_min: float
    m_max: float
    
    @property
    def steps_at_level(self) -> int:
        """Return the number of steps per axis at this level."""
        return 2**self.id


# Practical faint limit when max_level is None: stop when M(L) exceeds this
_FAINT_MAG_LIMIT = 25.0


def _half_size_at_level(world_half_size: float, level: int) -> float:
    """Level L cell half-size (pc): H0 / 2^L."""
    return world_half_size / (2**level)


def _mag_threshold_at_level(v_mag: float, world_half_size: float, level: int) -> float:
    """Faintest absolute magnitude at level L: M(L) = v_mag + 5 - 5*log10(h(L))."""
    h = _half_size_at_level(world_half_size, level)
    return v_mag + 5.0 - 5.0 * math.log10(h)


class MagLevelConfig:
    """Level-from-magnitude mapping derived from v_mag and world_half_size.

    Placement rule: star at level L where r_V <= H(L) = world_half_size / 2^L,
    with r_V = 10^((v_mag - mag_abs + 5) / 5) pc.
    """

    def __init__(
        self,
        v_mag: float = 6.5,
        world_half_size: float = 200_000.0,
        max_level: int | None = None,
    ) -> None:
        self.v_mag = v_mag
        self.world_half_size = world_half_size
        self.max_level = max_level
        self._levels_cache: list[Level] | None = None

    def _build_levels(self) -> list[Level]:
        if self._levels_cache is not None:
            return self._levels_cache
        levels: list[Level] = []
        L = 0
        m_prev = -math.inf
        while True:
            m_curr = _mag_threshold_at_level(
                self.v_mag, self.world_half_size, L
            )
            if self.max_level is not None and L == self.max_level:
                levels.append(Level(id=L, m_min=m_prev, m_max=math.inf))
                break
            if self.max_level is None and m_curr > _FAINT_MAG_LIMIT:
                levels.append(Level(id=L, m_min=m_prev, m_max=math.inf))
                break
            levels.append(Level(id=L, m_min=m_prev, m_max=m_curr))
            m_prev = m_curr
            L += 1
        self._levels_cache = levels
        return levels

    def levels(self) -> Iterator[Level]:
        """Yield Level(id, m_min, m_max) for each level. Bands are disjoint and cover all mag_abs."""
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
        levs = self._build_levels()
        for lev in levs:
            if lev.m_min < mag_abs <= lev.m_max:
                return lev.id
        if levs and mag_abs <= levs[0].m_max:
            return levs[0].id
        return levs[-1].id if levs else 0

    def assign_level_array(self, mag_abs: np.ndarray) -> np.ndarray:
        """Assign level id per star from mag_abs. Returns int32 array of level ids."""
        levs = self._build_levels()
        out = np.full(mag_abs.shape[0], -1, dtype=np.int32)
        for lev in levs:
            if lev.m_min == -math.inf:
                mask = mag_abs <= lev.m_max
            elif lev.m_max == math.inf:
                mask = mag_abs > lev.m_min
            else:
                mask = (mag_abs > lev.m_min) & (mag_abs <= lev.m_max)
            out[mask] = lev.id
        unset = (out == -1).sum()
        if unset > 0:
            raise ValueError(
                f"{unset} star(s) have no level assigned; check magnitude range."
            )
        return out
