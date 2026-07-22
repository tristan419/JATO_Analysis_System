#!/usr/bin/env python3
"""Upload and attest immutable production release archives in Tencent COS.

The upload command is intentionally GitHub Actions-specific: it requires the
main branch, the production environment OIDC subject, and short-lived Tencent
STS credentials.  Long-lived COS credentials and pre-signed URLs are never
accepted by this tool.
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import datetime as dt
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any, BinaryIO, Mapping, Protocol
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from frontend_release_artifact import SHA256_PATTERN, SHA_PATTERN, sha256_file


RECEIPT_SCHEMA = "jato.cos-release-receipt.v1"
EXPECTED_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
EXPECTED_GITHUB_REF = "refs/heads/main"
DEFAULT_RELEASE_PREFIX = "releases"
DEFAULT_PART_SIZE_MIB = 8
DEFAULT_THREADS = 4
MAX_PART_SIZE_MIB = 64
MAX_THREADS = 4
MAX_PART_UPLOAD_ATTEMPTS = 3
PART_UPLOAD_BACKOFF_SECONDS = 1.0
CRC64_MASK = (1 << 64) - 1
CRC64_REVERSED_POLYNOMIAL = 0xC96C5795D7870F42
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,49}-[1-9][0-9]{4,}$")
REGION_PATTERN = re.compile(r"^[a-z][a-z0-9-]{1,31}$")
PREFIX_PATTERN = re.compile(r"^[a-z0-9][a-z0-9/_-]{0,62}$")
ROLE_ARN_PATTERN = re.compile(
    r"^qcs::cam::uin/[1-9][0-9]*:roleName/[A-Za-z0-9+=,.@_-]{1,64}$"
)
PROVIDER_PATTERN = re.compile(r"^[A-Za-z0-9+=,.@_-]{1,64}$")
SAFE_EXTERNAL_FIELD_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/=+-]{0,255}$")


class COSReleaseError(RuntimeError):
    """Raised when the immutable COS release contract is violated."""


class COSClient(Protocol):
    def head_object(self, *, Bucket: str, Key: str) -> Mapping[str, Any]: ...

    def create_multipart_upload(
        self, *, Bucket: str, Key: str, **kwargs: Any
    ) -> Mapping[str, Any]: ...

    def upload_part(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        PartNumber: int,
        UploadId: str,
        EnableMD5: bool,
    ) -> Mapping[str, Any]: ...

    def complete_multipart_upload(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: Mapping[str, Any],
        **kwargs: Any,
    ) -> Mapping[str, Any]: ...

    def abort_multipart_upload(
        self, *, Bucket: str, Key: str, UploadId: str
    ) -> None: ...


@dataclass(frozen=True)
class ArchiveIdentity:
    path: Path
    git_sha: str
    size: int
    sha256: str
    crc64ecma: str


@dataclass(frozen=True)
class TemporaryCredentials:
    secret_id: str
    secret_key: str
    token: str
    expires_at: str


def _crc64_table() -> tuple[int, ...]:
    values: list[int] = []
    for byte in range(256):
        value = byte
        for _ in range(8):
            value = (
                (value >> 1) ^ CRC64_REVERSED_POLYNOMIAL
                if value & 1
                else value >> 1
            )
        values.append(value & CRC64_MASK)
    return tuple(values)


CRC64_TABLE = _crc64_table()


def crc64_ecma_bytes(data: bytes, *, current: int = CRC64_MASK) -> int:
    """Update a reflected CRC64-ECMA/XZ accumulator without final XOR."""

    value = current
    for byte in data:
        value = CRC64_TABLE[(value ^ byte) & 0xFF] ^ (value >> 8)
    return value & CRC64_MASK


def crc64_ecma_stream(handle: BinaryIO) -> str:
    value = CRC64_MASK
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        value = crc64_ecma_bytes(block, current=value)
    return str(value ^ CRC64_MASK)


def crc64_ecma_file(path: Path) -> str:
    if not path.is_file():
        raise COSReleaseError(f"release archive is missing: {path}")
    with path.open("rb") as handle:
        return crc64_ecma_stream(handle)


def require_git_sha(value: str, context: str = "git SHA") -> str:
    normalized = value.strip().lower()
    if not SHA_PATTERN.fullmatch(normalized):
        raise COSReleaseError(f"{context} must be a 40-character lowercase SHA")
    return normalized


def require_sha256(value: str, context: str = "SHA-256") -> str:
    normalized = value.strip().lower()
    if not SHA256_PATTERN.fullmatch(normalized):
        raise COSReleaseError(f"{context} must be a 64-character lowercase SHA-256")
    return normalized


def require_bucket(value: str) -> str:
    normalized = value.strip().lower()
    if not BUCKET_PATTERN.fullmatch(normalized):
        raise COSReleaseError(
            "COS bucket must be a lowercase bucket name including its numeric APPID suffix"
        )
    return normalized


def require_region(value: str) -> str:
    normalized = value.strip().lower()
    if not REGION_PATTERN.fullmatch(normalized):
        raise COSReleaseError("COS region has an invalid format")
    return normalized


def require_prefix(value: str) -> str:
    normalized = value.strip().strip("/").lower()
    if not PREFIX_PATTERN.fullmatch(normalized) or "//" in normalized:
        raise COSReleaseError("COS release prefix has an invalid format")
    if any(part in {".", ".."} for part in normalized.split("/")):
        raise COSReleaseError("COS release prefix may not contain dot path segments")
    return normalized


def object_key(prefix: str, git_sha: str, archive_sha256: str) -> str:
    return (
        f"{require_prefix(prefix)}/{require_git_sha(git_sha)}/"
        f"{require_sha256(archive_sha256, 'archive SHA-256')}.tar.gz"
    )


def accelerate_endpoint(bucket: str) -> str:
    return f"{require_bucket(bucket)}.cos.accelerate.tencentcos.cn"


def internal_endpoint(bucket: str, region: str) -> str:
    return f"{require_bucket(bucket)}.cos-internal.{require_region(region)}.tencentcos.cn"


def archive_identity(path: Path, git_sha: str) -> ArchiveIdentity:
    resolved = path.resolve()
    if not resolved.is_file():
        raise COSReleaseError(f"release archive is missing: {resolved}")
    size = resolved.stat().st_size
    if size <= 0:
        raise COSReleaseError("release archive must not be empty")
    return ArchiveIdentity(
        path=resolved,
        git_sha=require_git_sha(git_sha),
        size=size,
        sha256=sha256_file(resolved),
        crc64ecma=crc64_ecma_file(resolved),
    )


def _decode_jwt_claims(token: str) -> Mapping[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise COSReleaseError("GitHub OIDC response is not a JWT")
    try:
        encoded = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise COSReleaseError("GitHub OIDC JWT claims are malformed") from exc
    if not isinstance(claims, Mapping):
        raise COSReleaseError("GitHub OIDC JWT claims must be an object")
    return claims


def validate_github_oidc_claims(
    claims: Mapping[str, Any],
    *,
    audience: str,
    repository: str,
    environment: str,
) -> None:
    expected_subject = f"repo:{repository}:environment:{environment}"
    required = {
        "iss": EXPECTED_OIDC_ISSUER,
        "sub": expected_subject,
        "repository": repository,
        "ref": EXPECTED_GITHUB_REF,
    }
    mismatches = [
        name
        for name, expected in required.items()
        if str(claims.get(name) or "") != expected
    ]
    actual_audience = claims.get("aud")
    audience_matches = (
        actual_audience == audience
        if isinstance(actual_audience, str)
        else isinstance(actual_audience, list) and audience in actual_audience
    )
    if not audience_matches:
        mismatches.append("aud")
    if mismatches:
        raise COSReleaseError(
            "GitHub OIDC token is outside the approved main/production scope: "
            + ", ".join(sorted(mismatches))
        )


def _urlopen_json(request: urllib_request.Request, *, timeout: int = 30) -> Mapping[str, Any]:
    try:
        with urllib_request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        raise COSReleaseError(
            f"identity provider request failed with HTTP {exc.code}"
        ) from exc
    except (urllib_error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise COSReleaseError("identity provider request failed") from exc
    if not isinstance(payload, Mapping):
        raise COSReleaseError("identity provider returned a non-object response")
    return payload


def request_github_oidc_token(audience: str) -> str:
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "").strip()
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "").strip()
    if not request_url or not request_token:
        raise COSReleaseError(
            "GitHub OIDC request variables are unavailable; id-token: write is required"
        )
    separator = "&" if "?" in request_url else "?"
    request = urllib_request.Request(
        f"{request_url}{separator}audience={urllib_parse.quote(audience, safe='')}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {request_token}",
        },
        method="GET",
    )
    payload = _urlopen_json(request)
    token = str(payload.get("value") or "").strip()
    if not token:
        raise COSReleaseError("GitHub OIDC response is missing the token")
    return token


def assume_tencent_role_with_web_identity(
    *,
    oidc_token: str,
    role_arn: str,
    provider_id: str,
    region: str,
    run_id: str,
    run_attempt: str,
) -> TemporaryCredentials:
    if not ROLE_ARN_PATTERN.fullmatch(role_arn):
        raise COSReleaseError("Tencent COS upload role ARN has an invalid format")
    if not PROVIDER_PATTERN.fullmatch(provider_id):
        raise COSReleaseError("Tencent OIDC provider id has an invalid format")
    if not run_id.isdigit() or not run_attempt.isdigit():
        raise COSReleaseError("GitHub run identity must be numeric")
    session_name = f"github-{run_id}-{run_attempt}"
    body = json.dumps(
        {
            "DurationSeconds": 3600,
            "ProviderId": provider_id,
            "RoleArn": role_arn,
            "RoleSessionName": session_name[:64],
            "WebIdentityToken": oidc_token,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib_request.Request(
        "https://sts.tencentcloudapi.com/",
        data=body,
        headers={
            "Authorization": "SKIP",
            "Content-Type": "application/json; charset=utf-8",
            "X-TC-Action": "AssumeRoleWithWebIdentity",
            "X-TC-Region": require_region(region),
            "X-TC-Timestamp": str(int(time.time())),
            "X-TC-Version": "2018-08-13",
        },
        method="POST",
    )
    payload = _urlopen_json(request)
    response = payload.get("Response")
    if not isinstance(response, Mapping):
        raise COSReleaseError("Tencent STS response is missing Response")
    if isinstance(response.get("Error"), Mapping):
        error_payload = response["Error"]
        code = str(error_payload.get("Code") or "unknown")
        raise COSReleaseError(f"Tencent STS rejected the OIDC role request: {code}")
    credentials = response.get("Credentials")
    if not isinstance(credentials, Mapping):
        raise COSReleaseError("Tencent STS response is missing temporary credentials")
    values = {
        "secret_id": str(credentials.get("TmpSecretId") or "").strip(),
        "secret_key": str(credentials.get("TmpSecretKey") or "").strip(),
        "token": str(credentials.get("Token") or "").strip(),
        "expires_at": str(response.get("Expiration") or "").strip(),
    }
    if not all(values.values()):
        raise COSReleaseError("Tencent STS returned incomplete temporary credentials")
    return TemporaryCredentials(**values)


def _header_map(payload: Mapping[str, Any]) -> dict[str, str]:
    return {str(key).lower(): str(value).strip() for key, value in payload.items()}


def _response_status_code(exc: BaseException) -> int | None:
    getter = getattr(exc, "get_status_code", None)
    if callable(getter):
        try:
            return int(getter())
        except (TypeError, ValueError):
            return None
    value = getattr(exc, "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_exception_value(exc: BaseException, getter_name: str) -> str:
    getter = getattr(exc, getter_name, None)
    if not callable(getter):
        return ""
    try:
        value = str(getter() or "").strip()
    except Exception:
        return ""
    return value if SAFE_EXTERNAL_FIELD_PATTERN.fullmatch(value) else ""


def sanitized_external_error(
    exc: BaseException,
    *,
    operation: str = "COS release operation",
) -> COSReleaseError:
    """Create a log-safe boundary error without rendering external exceptions."""

    code = _safe_exception_value(exc, "get_error_code")
    if not code:
        status_code = _response_status_code(exc)
        code = f"HTTP_{status_code}" if status_code is not None else "ExternalError"
    request_id = _safe_exception_value(exc, "get_request_id")
    fields = [f"code={code}"]
    if request_id:
        fields.append(f"request-id={request_id}")
    return COSReleaseError(f"{operation} failed ({', '.join(fields)})")


def head_object_or_none(client: COSClient, *, bucket: str, key: str) -> Mapping[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _response_status_code(exc) == 404:
            return None
        raise


def verify_head(
    head: Mapping[str, Any],
    *,
    identity: ArchiveIdentity,
) -> dict[str, str]:
    headers = _header_map(head)
    expected = {
        "content-length": str(identity.size),
        "x-cos-hash-crc64ecma": identity.crc64ecma,
        "x-cos-meta-artifact-sha256": identity.sha256,
        "x-cos-meta-git-sha": identity.git_sha,
        "x-cos-meta-size": str(identity.size),
        "x-cos-server-side-encryption": "AES256",
    }
    mismatches = [
        name for name, expected_value in expected.items() if headers.get(name) != expected_value
    ]
    if mismatches:
        raise COSReleaseError(
            "COS immutable object HEAD mismatch: " + ", ".join(sorted(mismatches))
        )
    if headers.get("x-cos-version-id"):
        raise COSReleaseError(
            "COS object returned a version id; this transport requires a never-versioned bucket"
        )
    return headers


def _read_part(path: Path, offset: int, length: int) -> bytes:
    with path.open("rb") as handle:
        handle.seek(offset)
        payload = handle.read(length)
    if len(payload) != length:
        raise COSReleaseError(
            f"release archive changed while reading part: expected={length} actual={len(payload)}"
        )
    return payload


def _upload_part(
    client: COSClient,
    *,
    identity: ArchiveIdentity,
    bucket: str,
    key: str,
    upload_id: str,
    part_number: int,
    offset: int,
    length: int,
) -> dict[str, Any]:
    last_error: BaseException | None = None
    for attempt in range(1, MAX_PART_UPLOAD_ATTEMPTS + 1):
        try:
            payload = _read_part(identity.path, offset, length)
            expected_crc64 = str(crc64_ecma_bytes(payload) ^ CRC64_MASK)
            response = client.upload_part(
                Bucket=bucket,
                Key=key,
                Body=payload,
                PartNumber=part_number,
                UploadId=upload_id,
                EnableMD5=True,
            )
            headers = _header_map(response)
            if headers.get("x-cos-hash-crc64ecma") != expected_crc64:
                raise COSReleaseError(
                    f"COS CRC64 mismatch for multipart part {part_number}"
                )
            etag = headers.get("etag", "").strip()
            if not etag:
                raise COSReleaseError(
                    f"COS response is missing ETag for multipart part {part_number}"
                )
            return {"PartNumber": part_number, "ETag": etag}
        except Exception as exc:
            last_error = exc
            if attempt == MAX_PART_UPLOAD_ATTEMPTS:
                break
            print(
                "COS multipart retry: "
                f"part={part_number} attempt={attempt}/{MAX_PART_UPLOAD_ATTEMPTS}"
            )
            time.sleep(PART_UPLOAD_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise COSReleaseError(
        f"COS multipart part {part_number} failed after "
        f"{MAX_PART_UPLOAD_ATTEMPTS} attempts"
    ) from last_error


def multipart_upload(
    client: COSClient,
    *,
    identity: ArchiveIdentity,
    bucket: str,
    key: str,
    part_size_mib: int,
    threads: int,
) -> Mapping[str, Any]:
    if not 1 <= part_size_mib <= MAX_PART_SIZE_MIB:
        raise COSReleaseError(
            f"multipart part size must be between 1 and {MAX_PART_SIZE_MIB} MiB"
        )
    if not 1 <= threads <= MAX_THREADS:
        raise COSReleaseError(f"multipart thread count must be between 1 and {MAX_THREADS}")
    part_size = part_size_mib * 1024 * 1024
    part_count = (identity.size + part_size - 1) // part_size
    if part_count > 10_000:
        raise COSReleaseError("release archive would exceed COS's 10,000-part limit")

    response = client.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType="application/gzip",
        ForbidOverwrite="true",
        ServerSideEncryption="AES256",
        Metadata={
            "x-cos-meta-artifact-sha256": identity.sha256,
            "x-cos-meta-git-sha": identity.git_sha,
            "x-cos-meta-size": str(identity.size),
        },
    )
    upload_id = str(response.get("UploadId") or "").strip()
    if not upload_id:
        raise COSReleaseError("COS did not return a multipart upload id")

    try:
        completed_parts: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = []
            for part_number in range(1, part_count + 1):
                offset = (part_number - 1) * part_size
                length = min(part_size, identity.size - offset)
                futures.append(
                    pool.submit(
                        _upload_part,
                        client,
                        identity=identity,
                        bucket=bucket,
                        key=key,
                        upload_id=upload_id,
                        part_number=part_number,
                        offset=offset,
                        length=length,
                    )
                )
            for future in as_completed(futures):
                completed_parts.append(future.result())
                print(
                    "COS multipart progress: "
                    f"{len(completed_parts)}/{part_count} verified parts"
                )
        completed_parts.sort(key=lambda item: int(item["PartNumber"]))
        complete = client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Part": completed_parts},
            ForbidOverwrite="true",
        )
        complete_headers = _header_map(complete)
        if complete_headers.get("x-cos-hash-crc64ecma") != identity.crc64ecma:
            raise COSReleaseError("COS complete-multipart CRC64 does not match the archive")
        return complete
    except Exception:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise


def upload_or_reuse(
    client: COSClient,
    *,
    identity: ArchiveIdentity,
    bucket: str,
    prefix: str,
    part_size_mib: int = DEFAULT_PART_SIZE_MIB,
    threads: int = DEFAULT_THREADS,
) -> tuple[str, Mapping[str, Any], bool]:
    normalized_bucket = require_bucket(bucket)
    key = object_key(prefix, identity.git_sha, identity.sha256)
    existing = head_object_or_none(client, bucket=normalized_bucket, key=key)
    if existing is not None:
        verify_head(existing, identity=identity)
        return key, existing, True

    try:
        multipart_upload(
            client,
            identity=identity,
            bucket=normalized_bucket,
            key=key,
            part_size_mib=part_size_mib,
            threads=threads,
        )
    except Exception:
        raced = head_object_or_none(client, bucket=normalized_bucket, key=key)
        if raced is None:
            raise
        verify_head(raced, identity=identity)
        return key, raced, True
    head = head_object_or_none(client, bucket=normalized_bucket, key=key)
    if head is None:
        raise COSReleaseError("COS object is missing immediately after multipart completion")
    verify_head(head, identity=identity)
    return key, head, False


def create_cos_client(
    *,
    credentials: TemporaryCredentials,
    region: str,
) -> COSClient:
    try:
        from qcloud_cos import CosConfig, CosS3Client
        from qcloud_cos import cos_comm
    except ImportError as exc:
        raise COSReleaseError(
            "cos-python-sdk-v5 is required for COS release upload"
        ) from exc

    # SDK 1.9.44 supports arbitrary headers through this mapping but does not
    # yet expose Tencent's x-cos-forbid-overwrite convenience name.
    cos_comm.maplist.setdefault("ForbidOverwrite", "x-cos-forbid-overwrite")
    config = CosConfig(
        Region=require_region(region),
        SecretId=credentials.secret_id,
        SecretKey=credentials.secret_key,
        Token=credentials.token,
        Scheme="https",
        Endpoint="cos.accelerate.tencentcos.cn",
        EnableOldDomain=False,
        EnableInternalDomain=False,
        VerifySSL=True,
    )
    return CosS3Client(config)


def make_receipt(
    *,
    identity: ArchiveIdentity,
    bucket: str,
    region: str,
    key: str,
    head: Mapping[str, Any],
    reused: bool,
    repository: str,
    run_id: str,
    run_attempt: str,
) -> dict[str, Any]:
    headers = _header_map(head)
    now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    return {
        "schema": RECEIPT_SCHEMA,
        "status": "candidate",
        "transport": "tencent-cos",
        "bucket": require_bucket(bucket),
        "region": require_region(region),
        "objectKey": key,
        "archive": {
            "bytes": identity.size,
            "sha256": identity.sha256,
            "crc64ecma": identity.crc64ecma,
        },
        "git": {
            "commitSha": identity.git_sha,
            "repository": repository,
            "ref": EXPECTED_GITHUB_REF,
        },
        "github": {"runId": run_id, "runAttempt": run_attempt},
        "cos": {
            "endpoint": accelerate_endpoint(bucket),
            "etag": headers.get("etag", ""),
            "versionId": headers.get("x-cos-version-id", ""),
            "reusedExistingObject": reused,
            "retention": "releases-prefix-30-days",
        },
        "uploadedAt": now,
    }


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_github_outputs(path: Path, receipt: Mapping[str, Any]) -> None:
    archive = receipt["archive"]
    if not isinstance(archive, Mapping):
        raise COSReleaseError("receipt archive field is invalid")
    values = {
        "bucket": receipt["bucket"],
        "region": receipt["region"],
        "object-key": receipt["objectKey"],
        "archive-bytes": archive["bytes"],
        "archive-sha256": archive["sha256"],
        "archive-crc64": archive["crc64ecma"],
    }
    with path.open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def seal_receipt(
    receipt: Mapping[str, Any],
    *,
    git_sha: str,
    repository: str,
) -> dict[str, Any]:
    payload = json.loads(json.dumps(receipt))
    if payload.get("schema") != RECEIPT_SCHEMA or payload.get("status") != "candidate":
        raise COSReleaseError("only a candidate COS release receipt can be sealed")
    if payload.get("transport") != "tencent-cos":
        raise COSReleaseError("candidate receipt transport is invalid")
    bucket = require_bucket(str(payload.get("bucket") or ""))
    require_region(str(payload.get("region") or ""))
    git = payload.get("git")
    expected_git_sha = require_git_sha(git_sha)
    if not isinstance(git, Mapping) or git.get("commitSha") != expected_git_sha:
        raise COSReleaseError("receipt git SHA does not match the verified production release")
    if git.get("ref") != EXPECTED_GITHUB_REF or git.get("repository") != repository:
        raise COSReleaseError("candidate receipt git scope is invalid")
    archive = payload.get("archive")
    if not isinstance(archive, Mapping):
        raise COSReleaseError("candidate receipt archive field is invalid")
    archive_sha256 = require_sha256(
        str(archive.get("sha256") or ""),
        "candidate archive SHA-256",
    )
    archive_bytes = archive.get("bytes")
    if type(archive_bytes) is not int or archive_bytes <= 0:
        raise COSReleaseError("candidate receipt archive bytes must be a positive integer")
    archive_crc64 = str(archive.get("crc64ecma") or "")
    if not archive_crc64.isdigit() or int(archive_crc64) > CRC64_MASK:
        raise COSReleaseError("candidate receipt CRC64 is invalid")
    expected_key = object_key(DEFAULT_RELEASE_PREFIX, expected_git_sha, archive_sha256)
    if payload.get("objectKey") != expected_key:
        raise COSReleaseError("candidate receipt object key is outside the release namespace")
    github = payload.get("github")
    if not isinstance(github, Mapping) or not all(
        str(github.get(name) or "").isdigit() for name in ("runId", "runAttempt")
    ):
        raise COSReleaseError("candidate receipt GitHub run identity is invalid")
    cos = payload.get("cos")
    if not isinstance(cos, Mapping):
        raise COSReleaseError("candidate receipt COS field is invalid")
    if cos.get("endpoint") != accelerate_endpoint(bucket):
        raise COSReleaseError("candidate receipt COS endpoint is invalid")
    if cos.get("versionId"):
        raise COSReleaseError("candidate receipt refers to a versioned COS object")
    if cos.get("retention") != "releases-prefix-30-days":
        raise COSReleaseError("candidate receipt retention contract is invalid")
    payload["status"] = "verified-production"
    payload["verifiedAt"] = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    return payload


def _require_main_production_environment(repository: str, environment: str) -> None:
    if os.environ.get("GITHUB_REF", "") != EXPECTED_GITHUB_REF:
        raise COSReleaseError("COS production upload only accepts refs/heads/main")
    if os.environ.get("GITHUB_REPOSITORY", "") != repository:
        raise COSReleaseError("GitHub repository identity does not match the upload request")
    if environment != "production":
        raise COSReleaseError("COS production upload requires environment=production")


def command_upload(args: argparse.Namespace) -> int:
    repository = args.repository.strip()
    environment = args.environment.strip()
    _require_main_production_environment(repository, environment)
    identity = archive_identity(args.archive, args.github_sha)
    bucket = require_bucket(args.bucket)
    region = require_region(args.region)
    prefix = require_prefix(args.prefix)
    if prefix != DEFAULT_RELEASE_PREFIX:
        raise COSReleaseError("production COS upload only accepts the releases prefix")
    oidc_token = request_github_oidc_token(args.audience)
    claims = _decode_jwt_claims(oidc_token)
    validate_github_oidc_claims(
        claims,
        audience=args.audience,
        repository=repository,
        environment=environment,
    )
    print("GitHub OIDC token scope verified for main/production")
    credentials = assume_tencent_role_with_web_identity(
        oidc_token=oidc_token,
        role_arn=args.role_arn.strip(),
        provider_id=args.provider_id.strip(),
        region=region,
        run_id=args.run_id,
        run_attempt=args.run_attempt,
    )
    print("Tencent STS issued a short-lived COS upload credential")
    try:
        client = create_cos_client(credentials=credentials, region=region)
        key, head, reused = upload_or_reuse(
            client,
            identity=identity,
            bucket=bucket,
            prefix=prefix,
            part_size_mib=args.part_size_mib,
            threads=args.threads,
        )
    except COSReleaseError:
        raise
    except Exception as exc:
        raise sanitized_external_error(exc, operation="COS release upload") from None
    receipt = make_receipt(
        identity=identity,
        bucket=bucket,
        region=region,
        key=key,
        head=head,
        reused=reused,
        repository=repository,
        run_id=args.run_id,
        run_attempt=args.run_attempt,
    )
    write_json(args.receipt, receipt)
    write_github_outputs(args.github_output, receipt)
    print(
        "COS release upload verified: "
        f"key={key} bytes={identity.size} sha256={identity.sha256} "
        f"crc64ecma={identity.crc64ecma} reused={str(reused).lower()}"
    )
    return 0


def command_seal(args: argparse.Namespace) -> int:
    repository = args.repository.strip()
    _require_main_production_environment(repository, "production")
    try:
        candidate = json.loads(args.receipt.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise COSReleaseError(f"candidate receipt is unreadable: {args.receipt}") from exc
    if not isinstance(candidate, Mapping):
        raise COSReleaseError("candidate receipt must be a JSON object")
    sealed = seal_receipt(
        candidate,
        git_sha=args.github_sha,
        repository=repository,
    )
    write_json(args.output, sealed)
    print(f"Sealed verified production COS receipt: {args.output}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    upload = subparsers.add_parser("upload", help="OIDC upload and HEAD-verify a release")
    upload.add_argument("--archive", type=Path, required=True)
    upload.add_argument("--github-sha", required=True)
    upload.add_argument("--bucket", required=True)
    upload.add_argument("--region", required=True)
    upload.add_argument("--prefix", default=DEFAULT_RELEASE_PREFIX)
    upload.add_argument("--role-arn", required=True)
    upload.add_argument("--provider-id", required=True)
    upload.add_argument("--audience", required=True)
    upload.add_argument("--repository", required=True)
    upload.add_argument("--environment", default="production")
    upload.add_argument("--run-id", required=True)
    upload.add_argument("--run-attempt", required=True)
    upload.add_argument("--receipt", type=Path, required=True)
    upload.add_argument("--github-output", type=Path, required=True)
    upload.add_argument("--part-size-mib", type=int, default=DEFAULT_PART_SIZE_MIB)
    upload.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    upload.set_defaults(handler=command_upload)

    seal = subparsers.add_parser("seal", help="mark a fully verified receipt as production")
    seal.add_argument("--receipt", type=Path, required=True)
    seal.add_argument("--github-sha", required=True)
    seal.add_argument("--repository", required=True)
    seal.add_argument("--output", type=Path, required=True)
    seal.set_defaults(handler=command_seal)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except COSReleaseError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        redacted = sanitized_external_error(exc)
        print(f"[ERROR] {redacted}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
