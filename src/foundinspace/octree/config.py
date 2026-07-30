import numpy as np

WORLD_CENTER = np.array([0.0, 0.0, 0.0], dtype=np.float64)
WORLD_HALF_SIZE_PC = 200_000.0  # pc; root half-width H0
# Bits per axis for 3D Morton grid (63-bit code).
MORTON_BITS = 21
DEFAULT_CLASSIC_MAX_LEVEL = 14
# Above practical staged depths so Stage 01 uses one shard per level unless overridden.
DEFAULT_DEEP_SHARD_FROM_LEVEL = 99
DEFAULT_MAG_VIS = 6.5
