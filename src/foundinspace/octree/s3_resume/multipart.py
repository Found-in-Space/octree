from __future__ import annotations

from typing import Any


def create_multipart_upload(
    s3: Any,
    *,
    bucket: str,
    key: str,
    checksum_algorithm: str,
    object_params: dict[str, Any],
) -> str:
    params: dict[str, Any] = {"Bucket": bucket, "Key": key, **object_params}
    if checksum_algorithm != "none":
        params["ChecksumAlgorithm"] = checksum_algorithm.upper()
    response = s3.create_multipart_upload(**params)
    return response["UploadId"]


def list_all_parts(
    s3: Any,
    *,
    bucket: str,
    key: str,
    upload_id: str,
) -> dict[int, dict[str, Any]]:
    marker = 0
    out: dict[int, dict[str, Any]] = {}
    while True:
        response = s3.list_parts(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumberMarker=marker,
            MaxParts=1000,
        )
        for part in response.get("Parts", []):
            checksum = (
                part.get("ChecksumSHA256")
                or part.get("ChecksumCRC32C")
                or part.get("ChecksumCRC32")
                or part.get("ChecksumCRC64NVME")
            )
            out[part["PartNumber"]] = {"ETag": part["ETag"], "Checksum": checksum}
        if not response.get("IsTruncated"):
            break
        marker = response["NextPartNumberMarker"]
    return out


def upload_part(s3: Any, **kwargs: Any) -> dict[str, Any]:
    return s3.upload_part(**kwargs)


def complete_multipart_upload(
    s3: Any,
    *,
    bucket: str,
    key: str,
    upload_id: str,
    parts: list[dict[str, Any]],
) -> dict[str, Any]:
    return s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )


def abort_multipart_upload(
    s3: Any,
    *,
    bucket: str,
    key: str,
    upload_id: str,
) -> None:
    s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
