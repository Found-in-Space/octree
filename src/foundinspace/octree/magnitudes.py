from __future__ import annotations

import numpy as np

RENDER_MAG_SCALE = 100.0
RENDER_MAG_MIN_TICKS = -32768
RENDER_MAG_MAX_TICKS = 32767
RENDER_MAG_NONFINITE = 99.0
RENDER_MAG_CODEC_IDENTITY = "centimag-round-clip/v1"


def encode_render_magnitude_ticks(mag_abs: np.ndarray) -> np.ndarray:
    """Quantize absolute magnitudes exactly as the 16-byte render codec does."""
    values = np.asarray(mag_abs, dtype=np.float64)
    normalized = np.where(np.isfinite(values), values, RENDER_MAG_NONFINITE)
    return np.clip(
        np.round(normalized * RENDER_MAG_SCALE),
        RENDER_MAG_MIN_TICKS,
        RENDER_MAG_MAX_TICKS,
    ).astype(np.int16)


def quantize_render_magnitudes(mag_abs: np.ndarray) -> np.ndarray:
    """Return the absolute magnitudes the runtime will decode from a payload."""
    return encode_render_magnitude_ticks(mag_abs).astype(np.float64) / RENDER_MAG_SCALE
