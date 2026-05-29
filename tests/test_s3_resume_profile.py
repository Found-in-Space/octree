from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from foundinspace.octree.s3_resume.aws import make_s3_client
from foundinspace.octree.s3_resume.cli import main


def test_put_uses_aws_profile_env_in_state(tmp_path: Path) -> None:
    source = tmp_path / "input.bin"
    source.write_bytes(b"x" * 1024)
    state_path = tmp_path / ".input.bin.s3mpu.json"

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["put", str(source), "s3://bucket/key", "--dry-run-init"],
        env={"AWS_PROFILE": "env-profile"},
    )

    assert result.exit_code == 0, result.output
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["aws"]["profile"] == "env-profile"


def test_make_s3_client_uses_default_chain_when_profile_is_none(
    monkeypatch: Any,
) -> None:
    calls: list[dict[str, Any]] = []

    class _FakeSession:
        def __init__(self, **kwargs: Any) -> None:
            calls.append(kwargs)

        def client(self, *_: Any, **__: Any) -> object:
            return object()

    import foundinspace.octree.s3_resume.aws as aws_mod

    monkeypatch.setattr(aws_mod.boto3, "Session", _FakeSession)
    make_s3_client(
        profile=None, region="eu-west-1", retry_mode="standard", max_attempts=3
    )
    assert calls == [{"region_name": "eu-west-1"}]
