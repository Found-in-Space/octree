from __future__ import annotations

import math
from dataclasses import dataclass

from ..config import DEFAULT_MAG_VIS
from .types import ShardKey


@dataclass(frozen=True, slots=True)
class BuildPlan:
    max_level: int
    deep_shard_from_level: int
    deep_prefix_bits: int
    batch_size: int
    mag_limit: float = DEFAULT_MAG_VIS

    def validate(self) -> None:
        if self.max_level < 0:
            raise ValueError(f"max_level must be >= 0, got {self.max_level}")
        if self.deep_shard_from_level < 0:
            raise ValueError(
                f"deep_shard_from_level must be >= 0, got {self.deep_shard_from_level}"
            )
        if self.deep_prefix_bits < 0:
            raise ValueError(
                f"deep_prefix_bits must be >= 0, got {self.deep_prefix_bits}"
            )
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be > 0, got {self.batch_size}")
        if not math.isfinite(self.mag_limit):
            raise ValueError("mag_limit must be finite")
        for level in range(self.deep_shard_from_level, self.max_level + 1):
            if level > 0 and self.deep_prefix_bits > 3 * level:
                raise ValueError(
                    f"deep_prefix_bits ({self.deep_prefix_bits}) exceeds "
                    f"3 * level ({3 * level}) at level {level}"
                )

    def shard_keys_for_level(self, level: int) -> list[ShardKey]:
        if level == 0 or level < self.deep_shard_from_level:
            return [ShardKey(level=level, prefix_bits=0, prefix=0)]
        n_shards = 1 << self.deep_prefix_bits
        return [
            ShardKey(level=level, prefix_bits=self.deep_prefix_bits, prefix=p)
            for p in range(n_shards)
        ]
