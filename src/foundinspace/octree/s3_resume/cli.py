from __future__ import annotations

import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import click
from botocore.exceptions import ClientError

from foundinspace.octree.s3_resume.aws import make_s3_client, normalize_aws_error
from foundinspace.octree.s3_resume.multipart import (
    abort_multipart_upload,
    complete_multipart_upload,
    create_multipart_upload,
    list_all_parts,
    upload_part,
)
from foundinspace.octree.s3_resume.progress import Progress
from foundinspace.octree.s3_resume.state import (
    load_state,
    make_new_state,
    merge_remote_parts,
    save_state,
    uploaded_parts_for_completion,
    validate_source_unchanged,
)
from foundinspace.octree.s3_resume.util import (
    ChecksumMode,
    checksum_for_part,
    choose_part_size_bytes,
    default_state_path,
    parse_s3_uri,
)


def _object_params(
    *,
    storage_class: str | None,
    content_type: str | None,
    cache_control: str | None,
    metadata: tuple[str, ...],
    sse: str | None,
    sse_kms_key_id: str | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if storage_class:
        params["StorageClass"] = storage_class
    if content_type:
        params["ContentType"] = content_type
    if cache_control:
        params["CacheControl"] = cache_control
    if metadata:
        out: dict[str, str] = {}
        for pair in metadata:
            if "=" not in pair:
                raise click.ClickException(
                    f"invalid --metadata '{pair}' (expected key=value)"
                )
            k, v = pair.split("=", 1)
            out[k] = v
        params["Metadata"] = out
    if sse:
        params["ServerSideEncryption"] = sse
    if sse_kms_key_id:
        params["SSEKMSKeyId"] = sse_kms_key_id
    return params


def _retryable(exc: Exception) -> bool:
    if isinstance(exc, ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        code = exc.response.get("Error", {}).get("Code", "")
        return status >= 500 or code in {
            "RequestTimeout",
            "Throttling",
            "SlowDown",
            "InternalError",
            "ServiceUnavailable",
        }
    return True


def _upload_one_part(
    *,
    s3: Any,
    state: dict[str, Any],
    part_number: int,
    max_attempts: int,
    checksum_mode: ChecksumMode,
) -> dict[str, Any]:
    part = state["parts"][str(part_number)]
    source_path = Path(state["source"]["path"])
    offset = int(part["offset"])
    length = int(part["length"])
    bucket = state["target"]["bucket"]
    key = state["target"]["key"]
    upload_id = state["multipart"]["upload_id"]

    for attempt in range(1, max_attempts + 1):
        part["attempts"] = attempt
        part["status"] = "uploading"
        try:
            with source_path.open("rb") as f:
                f.seek(offset)
                data = f.read(length)
            checksum_key, checksum_value = checksum_for_part(data, checksum_mode)
            args: dict[str, Any] = {
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
                "Body": data,
            }
            if checksum_key and checksum_value:
                args[checksum_key] = checksum_value
            response = upload_part(s3, **args)
            part["etag"] = response["ETag"]
            if checksum_value:
                part["checksum"] = checksum_value
            part["status"] = "uploaded"
            return {"part_number": part_number, "length": length}
        except Exception as exc:
            part["status"] = "failed"
            if attempt == max_attempts or not _retryable(exc):
                raise
            delay = min(30.0, (2 ** (attempt - 1)) + random.random())
            time.sleep(delay)
    raise RuntimeError(f"failed to upload part {part_number}")


def _missing_parts(state: dict[str, Any]) -> list[int]:
    return [
        int(k)
        for k, p in state["parts"].items()
        if p.get("status") != "uploaded" or "etag" not in p
    ]


def _reconcile_remote(s3: Any, state: dict[str, Any]) -> None:
    upload_id = state["multipart"]["upload_id"]
    if not upload_id:
        return
    remote_parts = list_all_parts(
        s3,
        bucket=state["target"]["bucket"],
        key=state["target"]["key"],
        upload_id=upload_id,
    )
    merge_remote_parts(state, remote_parts)


@click.group()
def main() -> None:
    """Resumable S3 multipart uploader."""


@main.command("put")
@click.argument(
    "local_file", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.argument("destination")
@click.option("--profile", envvar="AWS_PROFILE", default=None, type=str)
@click.option("--region", default=None, type=str)
@click.option("--part-size-mib", default=64, show_default=True, type=int)
@click.option("--state", "state_path", default=None, type=click.Path(path_type=Path))
@click.option(
    "--checksum",
    "checksum_mode",
    default="none",
    type=click.Choice(["none", "crc32c", "sha256"], case_sensitive=False),
)
@click.option("--max-workers", default=1, show_default=True, type=int)
@click.option("--max-attempts", default=5, show_default=True, type=int)
@click.option(
    "--retry-mode",
    default="standard",
    show_default=True,
    type=click.Choice(["standard", "adaptive"], case_sensitive=False),
)
@click.option("--storage-class", default=None, type=str)
@click.option("--content-type", default=None, type=str)
@click.option("--cache-control", default=None, type=str)
@click.option("--metadata", "metadata", multiple=True)
@click.option("--sse", default=None, type=str)
@click.option("--sse-kms-key-id", default=None, type=str)
@click.option("--if-no-state-create", is_flag=True, default=False)
@click.option("--no-progress", is_flag=True, default=False)
@click.option("--dry-run-init", is_flag=True, default=False)
@click.option("--abort-on-error", is_flag=True, default=False)
@click.option("--force-source-changed", is_flag=True, default=False)
def put(
    local_file: Path,
    destination: str,
    profile: str | None,
    region: str | None,
    part_size_mib: int,
    state_path: Path | None,
    checksum_mode: str,
    max_workers: int,
    max_attempts: int,
    retry_mode: str,
    storage_class: str | None,
    content_type: str | None,
    cache_control: str | None,
    metadata: tuple[str, ...],
    sse: str | None,
    sse_kms_key_id: str | None,
    if_no_state_create: bool,
    no_progress: bool,
    dry_run_init: bool,
    abort_on_error: bool,
    force_source_changed: bool,
) -> None:
    target = parse_s3_uri(destination)
    state_path = state_path or default_state_path(local_file)
    object_params = _object_params(
        storage_class=storage_class,
        content_type=content_type,
        cache_control=cache_control,
        metadata=metadata,
        sse=sse,
        sse_kms_key_id=sse_kms_key_id,
    )

    if state_path.exists():
        state = load_state(state_path)
        validate_source_unchanged(state, force_source_changed=force_source_changed)
    else:
        if if_no_state_create:
            raise click.ClickException(f"state file does not exist: {state_path}")
        part_size = choose_part_size_bytes(local_file.stat().st_size, part_size_mib)
        state = make_new_state(
            source_path=local_file,
            bucket=target.bucket,
            key=target.key,
            region=region,
            profile=profile,
            part_size=part_size,
            checksum_algorithm=checksum_mode,
            object_params=object_params,
        )
        save_state(state_path, state)

    resolved_profile = profile or state.get("aws", {}).get("profile")

    click.echo(
        f"source={state['source']['path']} destination=s3://{state['target']['bucket']}/{state['target']['key']}"
    )

    if dry_run_init:
        click.echo(f"initialized state file: {state_path}")
        return

    try:
        s3 = make_s3_client(
            profile=resolved_profile,
            region=region,
            retry_mode=retry_mode,
            max_attempts=max_attempts,
        )
        if not state["multipart"]["upload_id"]:
            state["upload_state"] = "INITIATED"
            state["multipart"]["upload_id"] = create_multipart_upload(
                s3,
                bucket=state["target"]["bucket"],
                key=state["target"]["key"],
                checksum_algorithm=checksum_mode,
                object_params=state["object_params"],
            )
            state["multipart"]["initiated_at"] = state["updated_at"]
            save_state(state_path, state)

        state["upload_state"] = "VERIFYING_REMOTE"
        _reconcile_remote(s3, state)
        save_state(state_path, state)

        missing = _missing_parts(state)
        state["upload_state"] = "IN_PROGRESS"
        save_state(state_path, state)
        progress = Progress(
            total_bytes=int(state["source"]["size"]), enabled=not no_progress
        )
        uploaded_now = 0
        if max_workers <= 1:
            for part_number in missing:
                result = _upload_one_part(
                    s3=s3,
                    state=state,
                    part_number=part_number,
                    max_attempts=max_attempts,
                    checksum_mode=checksum_mode,  # type: ignore[arg-type]
                )
                uploaded_now += result["length"]
                progress.add(result["length"])
                save_state(state_path, state)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        _upload_one_part,
                        s3=s3,
                        state=state,
                        part_number=part_number,
                        max_attempts=max_attempts,
                        checksum_mode=checksum_mode,  # type: ignore[arg-type]
                    ): part_number
                    for part_number in missing
                }
                for future in as_completed(futures):
                    result = future.result()
                    uploaded_now += result["length"]
                    progress.add(result["length"])
                    save_state(state_path, state)
        progress.finish()
        click.echo(f"uploaded bytes this run: {uploaded_now}")

        missing = _missing_parts(state)
        if missing:
            raise click.ClickException(f"missing parts after upload: {missing[:10]}")
        state["upload_state"] = "COMPLETING"
        save_state(state_path, state)
        complete_multipart_upload(
            s3,
            bucket=state["target"]["bucket"],
            key=state["target"]["key"],
            upload_id=state["multipart"]["upload_id"],
            parts=uploaded_parts_for_completion(state),
        )
        state["complete"] = True
        state["upload_state"] = "COMPLETED"
        save_state(state_path, state)
        click.echo("upload complete")
    except Exception as exc:
        state["upload_state"] = "FAILED"
        save_state(state_path, state)
        if abort_on_error and state["multipart"]["upload_id"]:
            try:
                abort_multipart_upload(
                    s3,
                    bucket=state["target"]["bucket"],
                    key=state["target"]["key"],
                    upload_id=state["multipart"]["upload_id"],
                )
                state["upload_state"] = "ABORTED"
                save_state(state_path, state)
            except Exception:
                pass
        raise click.ClickException(normalize_aws_error(exc)) from exc


@main.command("resume")
@click.option("--state", "state_path", required=True, type=click.Path(path_type=Path))
@click.option("--max-workers", default=1, show_default=True, type=int)
@click.option("--max-attempts", default=5, show_default=True, type=int)
@click.option(
    "--retry-mode",
    default="standard",
    show_default=True,
    type=click.Choice(["standard", "adaptive"], case_sensitive=False),
)
@click.option("--region", default=None, type=str)
@click.option("--force-source-changed", is_flag=True, default=False)
def resume_cmd(
    state_path: Path,
    max_workers: int,
    max_attempts: int,
    retry_mode: str,
    region: str | None,
    force_source_changed: bool,
) -> None:
    state = load_state(state_path)
    profile = state.get("aws", {}).get("profile")
    source_path = Path(state["source"]["path"])
    destination = f"s3://{state['target']['bucket']}/{state['target']['key']}"
    validate_source_unchanged(state, force_source_changed=force_source_changed)
    ctx = click.get_current_context()
    ctx.invoke(
        put,
        local_file=source_path,
        destination=destination,
        profile=profile,
        region=region or state["target"].get("region"),
        part_size_mib=max(5, int(state["multipart"]["part_size"]) // (1024 * 1024)),
        state_path=state_path,
        checksum_mode=state["multipart"]["checksum_algorithm"].lower(),
        max_workers=max_workers,
        max_attempts=max_attempts,
        retry_mode=retry_mode,
        storage_class=state.get("object_params", {}).get("StorageClass"),
        content_type=state.get("object_params", {}).get("ContentType"),
        cache_control=state.get("object_params", {}).get("CacheControl"),
        metadata=tuple(
            f"{k}={v}"
            for k, v in state.get("object_params", {}).get("Metadata", {}).items()
        ),
        sse=state.get("object_params", {}).get("ServerSideEncryption"),
        sse_kms_key_id=state.get("object_params", {}).get("SSEKMSKeyId"),
        if_no_state_create=True,
        no_progress=False,
        dry_run_init=False,
        abort_on_error=False,
        force_source_changed=force_source_changed,
    )


@main.command("status")
@click.option("--state", "state_path", required=True, type=click.Path(path_type=Path))
@click.option("--json", "as_json", is_flag=True, default=False)
def status_cmd(state_path: Path, as_json: bool) -> None:
    state = load_state(state_path)
    pending = len(_missing_parts(state))
    summary = {
        "state_path": str(state_path),
        "upload_state": state["upload_state"],
        "upload_id": state["multipart"]["upload_id"],
        "complete": state["complete"],
        "pending_parts": pending,
        "total_parts": len(state["parts"]),
    }
    if as_json:
        click.echo(json.dumps(summary, indent=2))
    else:
        click.echo(
            f"state={summary['upload_state']} complete={summary['complete']} "
            f"parts={summary['total_parts'] - summary['pending_parts']}/{summary['total_parts']}"
        )


@main.command("abort")
@click.option("--state", "state_path", required=True, type=click.Path(path_type=Path))
@click.option("--region", default=None, type=str)
def abort_cmd(state_path: Path, region: str | None) -> None:
    state = load_state(state_path)
    upload_id = state["multipart"]["upload_id"]
    if not upload_id:
        raise click.ClickException("state has no active upload_id")
    s3 = make_s3_client(
        profile=state.get("aws", {}).get("profile"),
        region=region or state["target"].get("region"),
        retry_mode="standard",
        max_attempts=5,
    )
    abort_multipart_upload(
        s3,
        bucket=state["target"]["bucket"],
        key=state["target"]["key"],
        upload_id=upload_id,
    )
    state["upload_state"] = "ABORTED"
    save_state(state_path, state)
    click.echo("multipart upload aborted")


@main.command("verify")
@click.option("--state", "state_path", required=True, type=click.Path(path_type=Path))
@click.option("--region", default=None, type=str)
def verify_cmd(state_path: Path, region: str | None) -> None:
    from foundinspace.octree.s3_resume.verify import verify_remote_object

    state = load_state(state_path)
    s3 = make_s3_client(
        profile=state.get("aws", {}).get("profile"),
        region=region or state["target"].get("region"),
        retry_mode="standard",
        max_attempts=5,
    )
    ok = verify_remote_object(
        s3,
        bucket=state["target"]["bucket"],
        key=state["target"]["key"],
        local_path=Path(state["source"]["path"]),
    )
    if not ok:
        raise click.ClickException("verification failed")
    click.echo("verification passed")


if __name__ == "__main__":
    main()
