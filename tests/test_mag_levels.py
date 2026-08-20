from __future__ import annotations

import math

import numpy as np
import pytest

from foundinspace.octree.mag_levels import (
    MagLevelConfig,
    _half_size_at_level,
    _mag_threshold_at_level,
)

V_MAG = 6.5
WORLD_HALF_SIZE = 200_000.0


def _visibility_radius(mag_abs: float) -> float:
    return 10.0 ** ((V_MAG - mag_abs + 5.0) / 5.0)


def test_sun_is_assigned_to_deepest_containing_level() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)

    level = config.level_for_mag(4.83)

    assert level == 13
    radius = _visibility_radius(4.83)
    assert radius <= _half_size_at_level(WORLD_HALF_SIZE, level)
    assert radius > _half_size_at_level(WORLD_HALF_SIZE, level + 1)


def test_threshold_belongs_to_the_deeper_level() -> None:
    config = MagLevelConfig(
        v_mag=V_MAG,
        world_half_size=WORLD_HALF_SIZE,
        morton_bits=8,
    )

    for level in range(1, config.morton_bits):
        threshold = _mag_threshold_at_level(V_MAG, WORLD_HALF_SIZE, level)
        just_below = np.nextafter(threshold, -math.inf)
        just_above = np.nextafter(threshold, math.inf)

        assert config.level_for_mag(just_below) == level - 1
        assert config.level_for_mag(threshold) == level
        assert config.level_for_mag(just_above) == level


def test_assigned_level_half_size_contains_visibility_radius() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)
    brightest_representable = _mag_threshold_at_level(V_MAG, WORLD_HALF_SIZE, 0)
    faintest_threshold = _mag_threshold_at_level(
        V_MAG, WORLD_HALF_SIZE, config.morton_bits
    )
    magnitudes = np.linspace(brightest_representable, faintest_threshold + 5.0, 500)

    levels = config.assign_level_array(magnitudes)

    for mag_abs, level in zip(magnitudes, levels, strict=True):
        radius = _visibility_radius(float(mag_abs))
        half_size = _half_size_at_level(WORLD_HALF_SIZE, int(level))
        assert radius <= half_size or math.isclose(radius, half_size, rel_tol=1e-12)


def test_scalar_and_vector_assignments_agree() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)
    thresholds = [
        _mag_threshold_at_level(V_MAG, WORLD_HALF_SIZE, level)
        for level in range(1, config.morton_bits + 1)
    ]
    magnitudes = np.asarray(
        [-math.inf, -20.0, -5.0, 0.0, 4.83, 5.0, 10.0, *thresholds, math.inf],
        dtype=np.float64,
    )

    vector_levels = config.assign_level_array(magnitudes)
    scalar_levels = np.asarray(
        [config.level_for_mag(float(value)) for value in magnitudes],
        dtype=np.int32,
    )

    np.testing.assert_array_equal(vector_levels, scalar_levels)
    assert vector_levels[0] == 0
    assert vector_levels[-1] == config.morton_bits


def test_array_assignment_still_rejects_nan() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)

    with pytest.raises(ValueError, match="no level assigned"):
        config.assign_level_array(np.asarray([4.83, math.nan]))
