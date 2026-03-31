import numpy as np

# Teff / log_g encoding (used for payload serialization)
TEFF_LO = 2000.0
TEFF_HI = 50000.0
TEFF_SENTINEL = 255  # Unknown Teff → use default in shader
LOG_TEFF_LO = np.log10(TEFF_LO)
LOG_TEFF_HI = np.log10(TEFF_HI)
LOG_TEFF_RANGE = LOG_TEFF_HI - LOG_TEFF_LO


def encode_teff(teff_k: np.ndarray) -> np.ndarray:
    """Encode Teff (K) to 8-bit log scale. Invalid/NaN → TEFF_SENTINEL."""
    valid = np.isfinite(teff_k) & (teff_k >= TEFF_LO) & (teff_k <= TEFF_HI)
    log_t = np.log10(np.clip(teff_k, TEFF_LO, TEFF_HI))
    v_t = np.floor(255 * (log_t - LOG_TEFF_LO) / LOG_TEFF_RANGE).astype(np.uint8)
    return np.where(valid, np.clip(v_t, 0, 254), TEFF_SENTINEL)
