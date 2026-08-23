from __future__ import annotations

import itertools
import math

import numpy as np
import pytest

from foundinspace.octree.mag_levels import (
    MagLevelConfig,
    _half_size_at_level,
    _mag_threshold_at_level,
    _node_width_at_level,
)
from foundinspace.octree.magnitudes import (
    RENDER_MAG_MAX_TICKS,
    RENDER_MAG_MIN_TICKS,
    RENDER_MAG_NONFINITE,
    encode_render_magnitude_ticks,
    quantize_render_magnitudes,
)
from foundinspace.octree.sources.routing_columns import _compute_level
from foundinspace.octree.visibility import (
    complete_through_magnitude,
    load_radius_for_magnitude_shell,
    validate_load_factor,
)

V_MAG = 6.5
WORLD_HALF_SIZE = 200_000.0


def _visibility_radius(mag_abs: np.ndarray | float, v_mag: float = V_MAG):
    return 10.0 ** ((v_mag - mag_abs + 5.0) / 5.0)


def _aabb_distance(
    point: tuple[float, float, float],
    center: tuple[float, float, float],
    half_size: float,
) -> float:
    offsets = [
        max(abs(coordinate - midpoint) - half_size, 0.0)
        for coordinate, midpoint in zip(point, center, strict=True)
    ]
    return math.sqrt(sum(offset * offset for offset in offsets))


def test_representative_stars_use_full_width_levels() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)

    assert config.level_for_mag(-5.0) == 7
    assert config.level_for_mag(4.83) == 14


def test_exact_threshold_belongs_to_the_coarser_full_width_level() -> None:
    config = MagLevelConfig(
        v_mag=V_MAG,
        world_half_size=WORLD_HALF_SIZE,
        morton_bits=8,
    )

    for level in range(config.morton_bits):
        threshold = _mag_threshold_at_level(V_MAG, WORLD_HALF_SIZE, level)
        just_below = np.nextafter(threshold, -math.inf)
        just_above = np.nextafter(threshold, math.inf)

        assert config.level_for_mag(just_below) == level
        assert config.level_for_mag(threshold) == level
        assert config.level_for_mag(just_above) == level + 1


def test_root_and_deepest_levels_are_explicit_saturation_bands() -> None:
    config = MagLevelConfig(
        v_mag=V_MAG,
        world_half_size=WORLD_HALF_SIZE,
        morton_bits=8,
    )
    root_threshold = _mag_threshold_at_level(V_MAG, WORLD_HALF_SIZE, 0)
    deepest_threshold = _mag_threshold_at_level(
        V_MAG,
        WORLD_HALF_SIZE,
        config.morton_bits - 1,
    )

    assert config.level_for_mag(-math.inf) == 0
    assert config.level_for_mag(root_threshold) == 0
    assert config.level_for_mag(np.nextafter(root_threshold, math.inf)) == 1
    assert config.level_for_mag(deepest_threshold) == config.morton_bits - 1
    assert (
        config.level_for_mag(np.nextafter(deepest_threshold, math.inf))
        == config.morton_bits
    )
    assert config.level_for_mag(math.inf) == config.morton_bits


def test_encoded_interior_bands_satisfy_full_width_invariant() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)
    source_magnitudes = np.arange(-14.0, 15.0, 0.001, dtype=np.float64)
    encoded_magnitudes = quantize_render_magnitudes(source_magnitudes)
    levels = _compute_level(source_magnitudes, config)
    radii = _visibility_radius(encoded_magnitudes)

    for level_raw in np.unique(levels):
        level = int(level_raw)
        if level in (0, config.morton_bits):
            continue
        selected = radii[levels == level]
        assert np.all(selected >= _half_size_at_level(config.world_half_size, level))
        assert np.all(selected < _node_width_at_level(config.world_half_size, level))


def test_scalar_and_vector_assignments_agree() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)
    thresholds = [
        _mag_threshold_at_level(V_MAG, WORLD_HALF_SIZE, level)
        for level in range(config.morton_bits)
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


def test_level_assignment_rejects_nan_for_scalar_and_array() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)

    with pytest.raises(ValueError, match="no level assigned"):
        config.level_for_mag(math.nan)
    with pytest.raises(ValueError, match="no level assigned"):
        config.assign_level_array(np.asarray([4.83, math.nan]))


def test_render_magnitude_quantization_is_shared_and_bounded() -> None:
    source = np.asarray(
        [-1000.0, -1.235, -1.225, math.nan, math.inf, 1000.0],
        dtype=np.float64,
    )

    ticks = encode_render_magnitude_ticks(source)
    represented = quantize_render_magnitudes(source)

    assert ticks.tolist() == [
        RENDER_MAG_MIN_TICKS,
        -124,
        -123,
        round(RENDER_MAG_NONFINITE * 100),
        round(RENDER_MAG_NONFINITE * 100),
        RENDER_MAG_MAX_TICKS,
    ]
    np.testing.assert_array_equal(represented, ticks.astype(np.float64) / 100.0)

    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)
    np.testing.assert_array_equal(
        _compute_level(source, config),
        config.assign_level_array(represented),
    )
    assert _compute_level(source, config).tolist() == [
        0,
        config.level_for_mag(-1.24),
        config.level_for_mag(-1.23),
        config.morton_bits,
        config.morton_bits,
        config.morton_bits,
    ]


def test_routing_uses_the_rendered_magnitude_at_a_rounding_boundary() -> None:
    config = MagLevelConfig(v_mag=V_MAG, world_half_size=WORLD_HALF_SIZE)
    source = np.arange(-14.0, 15.0, 0.0001, dtype=np.float64)
    represented = quantize_render_magnitudes(source)

    np.testing.assert_array_equal(
        _compute_level(source, config),
        config.assign_level_array(represented),
    )


def test_loader_factor_controls_shell_and_completeness() -> None:
    assert load_radius_for_magnitude_shell(1.0, 6.5, 6.5) == pytest.approx(2.0)
    assert load_radius_for_magnitude_shell(
        1.0,
        6.5,
        6.5,
        load_factor=1.0,
    ) == pytest.approx(1.0)
    assert complete_through_magnitude(6.5, load_factor=1.0) == pytest.approx(
        6.5 - 5.0 * math.log10(2.0)
    )
    assert complete_through_magnitude(6.5) == pytest.approx(6.5)

    for invalid in (0.99, 2.01, math.nan, math.inf):
        with pytest.raises(ValueError, match="load_factor"):
            validate_load_factor(invalid)


def test_complete_shell_selects_the_immediate_neighbourhood() -> None:
    half_size = 1.0
    radius = load_radius_for_magnitude_shell(half_size, 6.5, 6.5)
    centers = tuple(itertools.product((-3.0, -1.0, 1.0, 3.0, 5.0), repeat=3))

    origin_count = sum(
        _aabb_distance((0.0, 0.0, 0.0), center, half_size) < radius
        for center in centers
    )
    cell_center_count = sum(
        _aabb_distance((1.0, 1.0, 1.0), center, half_size) < radius
        for center in centers
    )

    assert radius == pytest.approx(2.0)
    assert origin_count == 8
    assert cell_center_count == 27
