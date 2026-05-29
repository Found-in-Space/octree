from __future__ import annotations

from pathlib import Path

import pytest

from foundinspace.octree.s3_resume.state import (
    load_state,
    make_new_state,
    save_state,
    validate_source_unchanged,
)


def test_state_atomic_write_and_load(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"x" * 1024)
    state = make_new_state(
        source_path=source,
        bucket="bucket",
        key="key",
        region="eu-west-1",
        profile="profile",
        part_size=512,
        checksum_algorithm="none",
        object_params={},
    )
    path = tmp_path / "state.json"
    save_state(path, state)
    loaded = load_state(path)
    assert loaded["source"]["path"] == str(source)
    assert loaded["target"]["bucket"] == "bucket"


def test_source_identity_mismatch_requires_force(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"a" * 64)
    state = make_new_state(
        source_path=source,
        bucket="bucket",
        key="key",
        region=None,
        profile="profile",
        part_size=64,
        checksum_algorithm="none",
        object_params={},
    )
    source.write_bytes(b"b" * 64)
    with pytest.raises(ValueError):
        validate_source_unchanged(state, force_source_changed=False)
    validate_source_unchanged(state, force_source_changed=True)
