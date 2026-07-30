from __future__ import annotations

import numpy as np

from foundinspace.octree.config import (
    MORTON_BITS,
    WORLD_CENTER,
    WORLD_HALF_SIZE_PC,
)
from foundinspace.octree.encoding.teff import encode_teff

RENDER_RECORD_SIZE = 16
_RENDER_DTYPE = np.dtype(
    [
        ("x", "<f4"),
        ("y", "<f4"),
        ("z", "<f4"),
        ("mag", "<i2"),
        ("teff", "u1"),
        ("pad", "u1"),
    ]
)
assert _RENDER_DTYPE.itemsize == RENDER_RECORD_SIZE


def encode_render_records(
    *,
    morton_codes: np.ndarray,
    positions: np.ndarray,
    mag_abs: np.ndarray,
    teff: np.ndarray,
    levels: np.ndarray,
    node_ids: np.ndarray | None = None,
    center: np.ndarray | None = None,
    half_size: float = WORLD_HALF_SIZE_PC,
) -> np.ndarray:
    """Encode raw star fields relative to their selected final nodes.

    ``levels`` are the actual output levels, not necessarily the natural
    magnitude-assigned levels calculated during routing.
    """
    morton_codes = np.asarray(morton_codes, dtype=np.uint64)
    positions = np.asarray(positions, dtype=np.float64)
    mag_abs = np.asarray(mag_abs, dtype=np.float64)
    teff = np.asarray(teff, dtype=np.float64)
    levels = np.asarray(levels, dtype=np.int32)
    resolved_node_ids = (
        None if node_ids is None else np.asarray(node_ids, dtype=np.uint64)
    )
    n = len(morton_codes)
    if positions.shape != (n, 3):
        raise ValueError(f"positions must have shape ({n}, 3), got {positions.shape}")
    for name, values in (
        ("mag_abs", mag_abs),
        ("teff", teff),
        ("levels", levels),
    ):
        if len(values) != n:
            raise ValueError(f"{name} must contain {n} values, got {len(values)}")
    if resolved_node_ids is not None and len(resolved_node_ids) != n:
        raise ValueError(
            f"node_ids must contain {n} values, got {len(resolved_node_ids)}"
        )
    if n == 0:
        return np.empty((0, RENDER_RECORD_SIZE), dtype=np.uint8)
    if np.any((levels < 0) | (levels > MORTON_BITS)):
        invalid = int(levels[(levels < 0) | (levels > MORTON_BITS)][0])
        raise ValueError(f"level must be in 0..{MORTON_BITS}, got {invalid}")

    world_center = (
        np.asarray(WORLD_CENTER, dtype=np.float64)
        if center is None
        else np.asarray(center, dtype=np.float64)
    )
    if world_center.shape != (3,):
        raise ValueError(f"center must have shape (3,), got {world_center.shape}")
    if not np.isfinite(half_size) or half_size <= 0:
        raise ValueError("half_size must be finite and > 0")

    normalized_mag = np.where(np.isfinite(mag_abs), mag_abs, 99.0)
    normalized_teff = np.where(np.isfinite(teff), teff, 5800.0)
    teff_log8 = encode_teff(normalized_teff)
    render_out = np.zeros(n, dtype=_RENDER_DTYPE)

    for level_raw in np.unique(levels):
        level = int(level_raw)
        indices = np.flatnonzero(levels == level)
        if resolved_node_ids is None:
            shift = 3 * (MORTON_BITS - level)
            selected_node_ids = morton_codes[indices] >> np.uint64(shift)
        else:
            selected_node_ids = resolved_node_ids[indices]

        if len(selected_node_ids) < 2 or np.all(
            selected_node_ids[1:] >= selected_node_ids[:-1]
        ):
            starts = np.concatenate(
                (
                    np.array([0], dtype=np.int64),
                    np.flatnonzero(
                        selected_node_ids[1:] != selected_node_ids[:-1]
                    ).astype(np.int64)
                    + 1,
                )
            )
            unique_nodes = selected_node_ids[starts]
            inverse = np.repeat(
                np.arange(len(starts), dtype=np.int64),
                np.diff(np.append(starts, len(selected_node_ids))),
            )
        else:
            unique_nodes, inverse = np.unique(
                selected_node_ids,
                return_inverse=True,
            )
        grid_x = np.zeros(len(unique_nodes), dtype=np.uint32)
        grid_y = np.zeros(len(unique_nodes), dtype=np.uint32)
        grid_z = np.zeros(len(unique_nodes), dtype=np.uint32)
        for bit in range(level):
            grid_x |= ((unique_nodes >> (3 * bit)) & 1).astype(np.uint32) << bit
            grid_y |= ((unique_nodes >> (3 * bit + 1)) & 1).astype(np.uint32) << bit
            grid_z |= ((unique_nodes >> (3 * bit + 2)) & 1).astype(np.uint32) << bit

        node_half_size = max(half_size / (2**level), 1e-20)
        node_width = 2.0 * node_half_size
        center_x = (
            world_center[0] + (grid_x.astype(np.float64) + 0.5) * node_width - half_size
        )
        center_y = (
            world_center[1] + (grid_y.astype(np.float64) + 0.5) * node_width - half_size
        )
        center_z = (
            world_center[2] + (grid_z.astype(np.float64) + 0.5) * node_width - half_size
        )

        selected_positions = positions[indices]
        records = render_out[indices]
        records["x"] = np.clip(
            (selected_positions[:, 0] - center_x[inverse]) / node_half_size,
            -1.0,
            1.0,
        )
        records["y"] = np.clip(
            (selected_positions[:, 1] - center_y[inverse]) / node_half_size,
            -1.0,
            1.0,
        )
        records["z"] = np.clip(
            (selected_positions[:, 2] - center_z[inverse]) / node_half_size,
            -1.0,
            1.0,
        )
        records["mag"] = np.clip(
            np.round(normalized_mag[indices] * 100.0),
            -32768,
            32767,
        )
        records["teff"] = teff_log8[indices]
        render_out[indices] = records

    render_bytes = np.ascontiguousarray(
        render_out.view(np.uint8).reshape(n, RENDER_RECORD_SIZE)
    )
    assert render_bytes.flags["C_CONTIGUOUS"]
    return render_bytes
