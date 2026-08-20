"""Output-profile-neutral materialization primitives."""

from .runs import (
    RunMergeBounds,
    SortedRunLayout,
    iter_merged_batches,
    reduce_sorted_runs,
    write_merged_run,
)

__all__ = [
    "RunMergeBounds",
    "SortedRunLayout",
    "iter_merged_batches",
    "reduce_sorted_runs",
    "write_merged_run",
]
