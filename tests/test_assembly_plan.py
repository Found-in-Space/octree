from __future__ import annotations

import pytest

from foundinspace.octree.assembly.plan import BuildPlan
from foundinspace.octree.assembly.types import ShardKey


class TestBuildPlanValidation:
    def test_valid_plan(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=8,
            deep_prefix_bits=3,
            batch_size=100_000,
        )
        plan.validate()

    def test_negative_max_level(self):
        plan = BuildPlan(
            max_level=-1,
            deep_shard_from_level=0,
            deep_prefix_bits=3,
            batch_size=1000,
        )
        with pytest.raises(ValueError, match="max_level"):
            plan.validate()

    def test_negative_deep_shard_from_level(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=-1,
            deep_prefix_bits=3,
            batch_size=100_000,
        )
        with pytest.raises(ValueError, match="deep_shard_from_level"):
            plan.validate()

    def test_negative_deep_prefix_bits(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=8,
            deep_prefix_bits=-1,
            batch_size=100_000,
        )
        with pytest.raises(ValueError, match="deep_prefix_bits"):
            plan.validate()

    def test_zero_batch_size(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=8,
            deep_prefix_bits=3,
            batch_size=0,
        )
        with pytest.raises(ValueError, match="batch_size"):
            plan.validate()

    def test_prefix_bits_exceeds_3_times_level(self):
        plan = BuildPlan(
            max_level=2,
            deep_shard_from_level=1,
            deep_prefix_bits=4,
            batch_size=100_000,
        )
        with pytest.raises(ValueError, match="exceeds"):
            plan.validate()

    def test_deep_shard_from_level_beyond_max_level(self):
        plan = BuildPlan(
            max_level=5,
            deep_shard_from_level=10,
            deep_prefix_bits=3,
            batch_size=1000,
        )
        plan.validate()


class TestShardKeysForLevel:
    def test_level_zero_always_unsharded(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=0,
            deep_prefix_bits=3,
            batch_size=1000,
        )
        keys = plan.shard_keys_for_level(0)
        assert keys == [ShardKey(level=0, prefix_bits=0, prefix=0)]

    def test_shallow_level_single_shard(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=8,
            deep_prefix_bits=3,
            batch_size=1000,
        )
        keys = plan.shard_keys_for_level(5)
        assert keys == [ShardKey(level=5, prefix_bits=0, prefix=0)]

    def test_deep_level_octant_sharding(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=8,
            deep_prefix_bits=3,
            batch_size=1000,
        )
        keys = plan.shard_keys_for_level(10)
        assert len(keys) == 8
        assert keys[0] == ShardKey(level=10, prefix_bits=3, prefix=0)
        assert keys[7] == ShardKey(level=10, prefix_bits=3, prefix=7)

    def test_boundary_level(self):
        plan = BuildPlan(
            max_level=13,
            deep_shard_from_level=8,
            deep_prefix_bits=3,
            batch_size=1000,
        )
        shallow = plan.shard_keys_for_level(7)
        assert len(shallow) == 1
        deep = plan.shard_keys_for_level(8)
        assert len(deep) == 8

    def test_prefix_bits_1(self):
        plan = BuildPlan(
            max_level=5,
            deep_shard_from_level=2,
            deep_prefix_bits=1,
            batch_size=1000,
        )
        keys = plan.shard_keys_for_level(3)
        assert len(keys) == 2
        assert keys[0].prefix == 0
        assert keys[1].prefix == 1
