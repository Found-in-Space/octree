from __future__ import annotations

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


def make_s3_client(
    *,
    profile: str | None,
    region: str | None,
    retry_mode: str,
    max_attempts: int,
):
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    config = Config(retries={"mode": retry_mode, "max_attempts": max_attempts})
    return session.client("s3", config=config)


def normalize_aws_error(exc: Exception) -> str:
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        msg = exc.response.get("Error", {}).get("Message", str(exc))
        if code in {"ExpiredToken", "UnauthorizedException", "InvalidClientTokenId"}:
            return f"AWS auth/session failure ({code}): {msg}"
        return f"AWS client error ({code}): {msg}"
    if isinstance(exc, BotoCoreError):
        return f"AWS SDK error: {exc}"
    return str(exc)
