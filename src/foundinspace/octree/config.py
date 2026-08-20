import numpy as np

WORLD_CENTER = np.array([0.0, 0.0, 0.0], dtype=np.float64)
WORLD_HALF_SIZE_PC = 200_000.0  # pc; root half-width H0
# Bits per axis for 3D Morton grid (63-bit code).
MORTON_BITS = 21
DEFAULT_CLASSIC_MAX_LEVEL = 14
DEFAULT_CLASSIC_PARTITION_FROM_LEVEL = 8
DEFAULT_CLASSIC_PARTITION_PREFIX_BITS = 6
DEFAULT_STAR_FORMAT_VERSION = 2
DEFAULT_TERMINAL_WATERLINE = 1_000
# Above practical preparation depths so one shard is used per level unless overridden.
DEFAULT_DEEP_SHARD_FROM_LEVEL = 99
DEFAULT_MAG_VIS = 6.5
