import numpy as np

# Octree build defaults (coordinate-system.md; must match across shard-parquet, octree*, scripts)
from foundinspace.octree.mag_levels import MagLevelConfig

WORLD_CENTER = np.array([0.0, 0.0, 0.0], dtype=np.float64)
WORLD_HALF_SIZE_PC = 200_000.0  # pc; root half-width H0
# Bits per axis for 3D Morton grid (63-bit code). Not the same as DEFAULT_MAX_LEVEL.
MORTON_BITS = 21
DEFAULT_MAX_LEVEL = 13  # default --max-level for all build CLIs
# Above max_level so Stage 01 uses one shard per level unless overridden.
DEFAULT_DEEP_SHARD_FROM_LEVEL = 99
DEFAULT_MAG_VIS = 6.5

LEVEL_CONFIG = MagLevelConfig(
    v_mag=DEFAULT_MAG_VIS,
    world_half_size=WORLD_HALF_SIZE_PC,
    max_level=DEFAULT_MAX_LEVEL,
)
