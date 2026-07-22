from __future__ import annotations

import io
import json
from pathlib import Path
import types
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest import mock


DEPLOY_DIR = Path(__file__).resolve().parents[1] / "deploy"
sys.path.insert(0, str(DEPLOY_DIR))

import cos_release_transport as transport  # noqa: E402


class MissingObjectError(RuntimeError):
    def get_status_code(self) -> int:
        return 404


class FakeCOSClient:
    def __init__(self, identity: transport.ArchiveIdentity) -> None:
        self.identity = identity
        self.head: dict[str, str] | None = None
        self.created: dict[str, object] | None = None
        self.completed: dict[str, object] | None = None
        self.uploaded_parts: list[tuple[int, bytes]] = []
        self.aborted = False

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, str]:
        del Bucket, Key
        if self.head is None:
            raise MissingObjectError("not found")
        return dict(self.head)

    def create_multipart_upload(
        self, *, Bucket: str, Key: str, **kwargs: object
    ) -> dict[str, str]:
        self.created = {"Bucket": Bucket, "Key": Key, **kwargs}
        return {"UploadId": "upload-1"}

    def upload_part(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        PartNumber: int,
        UploadId: str,
        EnableMD5: bool,
    ) -> dict[str, str]:
        del Bucket, Key, UploadId
        self.assertions = EnableMD5
        self.uploaded_parts.append((PartNumber, Body))
        crc = transport.crc64_ecma_bytes(Body) ^ transport.CRC64_MASK
        return {"ETag": f'"part-{PartNumber}"', "x-cos-hash-crc64ecma": str(crc)}

    def complete_multipart_upload(
        self,
        *,
        Bucket: str,
        Key: str,
        UploadId: str,
        MultipartUpload: dict[str, object],
        **kwargs: object,
    ) -> dict[str, str]:
        self.completed = {
            "Bucket": Bucket,
            "Key": Key,
            "UploadId": UploadId,
            **kwargs,
        }
        parts = MultipartUpload["Part"]
        assert isinstance(parts, list)
        assert [part["PartNumber"] for part in parts] == list(range(1, len(parts) + 1))
        self.head = matching_head(self.identity)
        return {"x-cos-hash-crc64ecma": self.identity.crc64ecma}

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str) -> None:
        del Bucket, Key, UploadId
        self.aborted = True


class BadPartCRCFakeCOSClient(FakeCOSClient):
    def upload_part(self, **kwargs: object) -> dict[str, str]:
        response = super().upload_part(**kwargs)
        response["x-cos-hash-crc64ecma"] = "0"
        return response


class FlakyPartCRCFakeCOSClient(FakeCOSClient):
    def __init__(self, identity: transport.ArchiveIdentity) -> None:
        super().__init__(identity)
        self.attempts = 0
        self.attempt_bodies: list[bytes] = []

    def upload_part(self, **kwargs: object) -> dict[str, str]:
        self.attempts += 1
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        self.attempt_bodies.append(body)
        if self.attempts == 1:
            raise LeakySDKError()
        response = super().upload_part(**kwargs)
        if self.attempts < transport.MAX_PART_UPLOAD_ATTEMPTS:
            response["x-cos-hash-crc64ecma"] = "0"
        return response


class LeakySDKError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("credential-marker session-marker authorization-marker")

    def get_error_code(self) -> str:
        return "AccessDenied"

    def get_request_id(self) -> str:
        return "NjA-safe-request-id"

    def get_status_code(self) -> int:
        return 403


def matching_head(identity: transport.ArchiveIdentity) -> dict[str, str]:
    return {
        "Content-Length": str(identity.size),
        "ETag": '"multipart-etag"',
        "x-cos-hash-crc64ecma": identity.crc64ecma,
        "x-cos-meta-artifact-sha256": identity.sha256,
        "x-cos-meta-git-sha": identity.git_sha,
        "x-cos-meta-size": str(identity.size),
        "x-cos-server-side-encryption": "AES256",
    }


class COSReleaseTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.archive = self.root / "release.tar.gz"
        self.archive.write_bytes((b"immutable-release\n" * 150_000) + b"end")
        self.git_sha = "a" * 40
        self.identity = transport.archive_identity(self.archive, self.git_sha)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_crc64_matches_cos_known_vector(self) -> None:
        value = transport.crc64_ecma_bytes(b"123456789") ^ transport.CRC64_MASK
        self.assertEqual(value, 11_051_210_869_376_104_954)

    def test_object_key_and_endpoints_are_derived_not_arbitrary(self) -> None:
        key = transport.object_key("releases", self.git_sha, self.identity.sha256)
        self.assertEqual(key, f"releases/{self.git_sha}/{self.identity.sha256}.tar.gz")
        bucket = "jato-release-1250000000"
        self.assertEqual(
            transport.accelerate_endpoint(bucket),
            f"{bucket}.cos.accelerate.tencentcos.cn",
        )
        self.assertEqual(
            transport.internal_endpoint(bucket, "ap-guangzhou"),
            f"{bucket}.cos-internal.ap-guangzhou.tencentcos.cn",
        )
        with self.assertRaises(transport.COSReleaseError):
            transport.object_key("../release", self.git_sha, self.identity.sha256)

    def test_oidc_claims_are_exactly_main_production_scoped(self) -> None:
        repository = "tristan419/JATO_Analysis_System"
        claims = {
            "iss": transport.EXPECTED_OIDC_ISSUER,
            "aud": "jato-production-cos",
            "sub": f"repo:{repository}:environment:production",
            "repository": repository,
            "ref": transport.EXPECTED_GITHUB_REF,
        }
        transport.validate_github_oidc_claims(
            claims,
            audience="jato-production-cos",
            repository=repository,
            environment="production",
        )
        claims["sub"] = f"repo:{repository}:ref:refs/heads/feature"
        with self.assertRaisesRegex(transport.COSReleaseError, "production scope"):
            transport.validate_github_oidc_claims(
                claims,
                audience="jato-production-cos",
                repository=repository,
                environment="production",
            )

    def test_production_upload_rejects_non_release_prefix_before_oidc(self) -> None:
        args = SimpleNamespace(
            archive=self.archive,
            github_sha=self.git_sha,
            bucket="jato-release-1250000000",
            region="ap-guangzhou",
            prefix="smoke",
            repository="tristan419/JATO_Analysis_System",
            environment="production",
        )
        environment = {
            "GITHUB_REF": transport.EXPECTED_GITHUB_REF,
            "GITHUB_REPOSITORY": args.repository,
        }
        with mock.patch.dict(transport.os.environ, environment, clear=False):
            with self.assertRaisesRegex(transport.COSReleaseError, "releases prefix"):
                transport.command_upload(args)

    def test_github_oidc_request_uses_encoded_audience_and_bearer(self) -> None:
        captured: list[object] = []

        def fake_open(request: object) -> dict[str, str]:
            captured.append(request)
            return {"value": "header.payload.signature"}

        environment = {
            "ACTIONS_ID_TOKEN_REQUEST_URL": "https://example.invalid/oidc?api=1",
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "runner-token",
        }
        with mock.patch.dict(transport.os.environ, environment, clear=False), mock.patch.object(
            transport, "_urlopen_json", side_effect=fake_open
        ):
            token = transport.request_github_oidc_token("jato production/cos")
        self.assertEqual(token, "header.payload.signature")
        request = captured[0]
        self.assertIn("audience=jato%20production%2Fcos", request.full_url)
        self.assertEqual(request.headers["Authorization"], "Bearer runner-token")

    def test_tencent_sts_exchange_uses_skip_and_returns_only_temp_credentials(self) -> None:
        captured: list[object] = []

        def fake_open(request: object) -> dict[str, object]:
            captured.append(request)
            return {
                "Response": {
                    "Credentials": {
                        "TmpSecretId": "temp-id",
                        "TmpSecretKey": "temp-key",
                        "Token": "temp-token",
                    },
                    "Expiration": "2099-01-01T00:00:00Z",
                }
            }

        with mock.patch.object(transport, "_urlopen_json", side_effect=fake_open):
            credentials = transport.assume_tencent_role_with_web_identity(
                oidc_token="oidc-token",
                role_arn="qcs::cam::uin/1250000000:roleName/JatoReleaseUpload",
                provider_id="GitHubActions",
                region="ap-guangzhou",
                run_id="123",
                run_attempt="2",
            )
        self.assertEqual(credentials.secret_id, "temp-id")
        request = captured[0]
        self.assertEqual(request.headers["Authorization"], "SKIP")
        self.assertEqual(request.headers["X-tc-action"], "AssumeRoleWithWebIdentity")
        body = json.loads(request.data.decode("utf-8"))
        self.assertEqual(body["WebIdentityToken"], "oidc-token")
        self.assertEqual(body["RoleSessionName"], "github-123-2")

    def test_cos_client_uses_https_acceleration_and_custom_overwrite_header(self) -> None:
        captured: dict[str, object] = {}

        class FakeConfig:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

        class FakeClient:
            def __init__(self, config: object) -> None:
                self.config = config

        fake_module = types.ModuleType("qcloud_cos")
        fake_module.CosConfig = FakeConfig
        fake_module.CosS3Client = FakeClient
        fake_module.cos_comm = SimpleNamespace(maplist={})
        credentials = transport.TemporaryCredentials(
            secret_id="temp-id",
            secret_key="temp-key",
            token="temp-token",
            expires_at="2099-01-01T00:00:00Z",
        )
        with mock.patch.dict(sys.modules, {"qcloud_cos": fake_module}):
            client = transport.create_cos_client(
                credentials=credentials,
                region="ap-guangzhou",
            )
        self.assertIsInstance(client, FakeClient)
        self.assertEqual(captured["Scheme"], "https")
        self.assertEqual(captured["Endpoint"], "cos.accelerate.tencentcos.cn")
        self.assertTrue(captured["VerifySSL"])
        self.assertEqual(
            fake_module.cos_comm.maplist["ForbidOverwrite"],
            "x-cos-forbid-overwrite",
        )

    def test_hash_locked_real_sdk_accepts_transport_adapter(self) -> None:
        try:
            from qcloud_cos import cos_comm
        except ImportError as exc:
            self.fail(f"hash-locked cos-python-sdk-v5 must be installed for CI: {exc}")
        credentials = transport.TemporaryCredentials(
            secret_id="temp-id",
            secret_key="temp-key",
            token="temp-token",
            expires_at="2099-01-01T00:00:00Z",
        )
        client = transport.create_cos_client(
            credentials=credentials,
            region="ap-guangzhou",
        )
        self.assertEqual(
            cos_comm.maplist["ForbidOverwrite"],
            "x-cos-forbid-overwrite",
        )
        self.assertEqual(client._conf._endpoint, "cos.accelerate.tencentcos.cn")
        self.assertTrue(
            client._conf.uri(
                bucket="jato-release-1250000000",
                path="releases/test.tar.gz",
            ).startswith(
                "https://jato-release-1250000000.cos.accelerate.tencentcos.cn/"
            )
        )

    def test_matching_existing_object_is_reused_without_upload(self) -> None:
        client = FakeCOSClient(self.identity)
        client.head = matching_head(self.identity)
        key, head, reused = transport.upload_or_reuse(
            client,
            identity=self.identity,
            bucket="jato-release-1250000000",
            prefix="releases",
        )
        self.assertTrue(reused)
        self.assertEqual(head["Content-Length"], str(self.identity.size))
        self.assertIn(self.identity.sha256, key)
        self.assertIsNone(client.created)

    def test_existing_object_metadata_collision_fails_closed(self) -> None:
        client = FakeCOSClient(self.identity)
        client.head = matching_head(self.identity)
        client.head["x-cos-meta-artifact-sha256"] = "b" * 64
        with self.assertRaisesRegex(transport.COSReleaseError, "HEAD mismatch"):
            transport.upload_or_reuse(
                client,
                identity=self.identity,
                bucket="jato-release-1250000000",
                prefix="releases",
            )
        self.assertIsNone(client.created)

    def test_versioned_object_fails_closed(self) -> None:
        client = FakeCOSClient(self.identity)
        client.head = matching_head(self.identity)
        client.head["x-cos-version-id"] = "version-1"
        with self.assertRaisesRegex(transport.COSReleaseError, "never-versioned"):
            transport.upload_or_reuse(
                client,
                identity=self.identity,
                bucket="jato-release-1250000000",
                prefix="releases",
            )
        self.assertIsNone(client.created)

    def test_multipart_upload_checks_each_crc_and_final_head(self) -> None:
        client = FakeCOSClient(self.identity)
        key, _, reused = transport.upload_or_reuse(
            client,
            identity=self.identity,
            bucket="jato-release-1250000000",
            prefix="releases",
            part_size_mib=1,
            threads=2,
        )
        self.assertFalse(reused)
        self.assertEqual(len(client.uploaded_parts), 3)
        self.assertEqual(client.created["ForbidOverwrite"], "true")
        self.assertEqual(client.created["ServerSideEncryption"], "AES256")
        self.assertNotIn("Tagging", client.created)
        self.assertEqual(client.completed["ForbidOverwrite"], "true")
        self.assertEqual(
            client.created["Metadata"]["x-cos-meta-artifact-sha256"],
            self.identity.sha256,
        )
        self.assertIn(self.git_sha, key)
        self.assertFalse(client.aborted)

    def test_bad_part_crc_aborts_and_never_completes(self) -> None:
        client = BadPartCRCFakeCOSClient(self.identity)
        with mock.patch.object(transport.time, "sleep"):
            with self.assertRaisesRegex(transport.COSReleaseError, "multipart part"):
                transport.multipart_upload(
                    client,
                    identity=self.identity,
                    bucket="jato-release-1250000000",
                    key=transport.object_key(
                        "releases", self.git_sha, self.identity.sha256
                    ),
                    part_size_mib=1,
                    threads=2,
                )
        self.assertTrue(client.aborted)
        self.assertIsNone(client.head)

    def test_part_crc_failure_retries_three_times_with_exponential_backoff(self) -> None:
        client = FlakyPartCRCFakeCOSClient(self.identity)
        with mock.patch.object(transport.time, "sleep") as sleep:
            result = transport._upload_part(
                client,
                identity=self.identity,
                bucket="jato-release-1250000000",
                key=transport.object_key("releases", self.git_sha, self.identity.sha256),
                upload_id="upload-1",
                part_number=1,
                offset=0,
                length=1024,
            )
        self.assertEqual(result["PartNumber"], 1)
        self.assertEqual(client.attempts, transport.MAX_PART_UPLOAD_ATTEMPTS)
        self.assertEqual(len(client.attempt_bodies), transport.MAX_PART_UPLOAD_ATTEMPTS)
        self.assertTrue(
            all(body == client.attempt_bodies[0] for body in client.attempt_bodies)
        )
        self.assertEqual(
            sleep.call_args_list,
            [
                mock.call(transport.PART_UPLOAD_BACKOFF_SECONDS),
                mock.call(transport.PART_UPLOAD_BACKOFF_SECONDS * 2),
            ],
        )

    def test_command_boundary_redacts_external_exception_details(self) -> None:
        handler_args = mock.Mock()
        handler_args.handler.side_effect = LeakySDKError()
        parser = mock.Mock()
        parser.parse_args.return_value = handler_args
        stderr = io.StringIO()
        with mock.patch.object(transport, "build_parser", return_value=parser), mock.patch(
            "sys.stderr", stderr
        ):
            result = transport.main([])
        output = stderr.getvalue()
        self.assertEqual(result, 1)
        self.assertIn("code=AccessDenied", output)
        self.assertIn("request-id=NjA-safe-request-id", output)
        self.assertNotIn("credential-marker", output)
        self.assertNotIn("session-marker", output)
        self.assertNotIn("authorization-marker", output)

    def test_multipart_memory_bounds_fail_closed(self) -> None:
        client = FakeCOSClient(self.identity)
        with self.assertRaisesRegex(transport.COSReleaseError, "part size"):
            transport.multipart_upload(
                client,
                identity=self.identity,
                bucket="jato-release-1250000000",
                key=transport.object_key("releases", self.git_sha, self.identity.sha256),
                part_size_mib=transport.MAX_PART_SIZE_MIB + 1,
                threads=1,
            )
        with self.assertRaisesRegex(transport.COSReleaseError, "thread count"):
            transport.multipart_upload(
                client,
                identity=self.identity,
                bucket="jato-release-1250000000",
                key=transport.object_key("releases", self.git_sha, self.identity.sha256),
                part_size_mib=1,
                threads=transport.MAX_THREADS + 1,
            )
        self.assertIsNone(client.created)

    def test_receipt_can_only_be_sealed_for_matching_commit(self) -> None:
        receipt = transport.make_receipt(
            identity=self.identity,
            bucket="jato-release-1250000000",
            region="ap-guangzhou",
            key=transport.object_key("releases", self.git_sha, self.identity.sha256),
            head=matching_head(self.identity),
            reused=False,
            repository="tristan419/JATO_Analysis_System",
            run_id="123",
            run_attempt="1",
        )
        repository = "tristan419/JATO_Analysis_System"
        sealed = transport.seal_receipt(
            receipt,
            git_sha=self.git_sha,
            repository=repository,
        )
        self.assertEqual(sealed["status"], "verified-production")
        self.assertEqual(receipt["status"], "candidate")
        with self.assertRaises(transport.COSReleaseError):
            transport.seal_receipt(
                receipt,
                git_sha="b" * 40,
                repository=repository,
            )

        tampered = json.loads(json.dumps(receipt))
        tampered["objectKey"] = f"releases/{self.git_sha}/{'b' * 64}.tar.gz"
        with self.assertRaisesRegex(transport.COSReleaseError, "object key"):
            transport.seal_receipt(
                tampered,
                git_sha=self.git_sha,
                repository=repository,
            )

        versioned = json.loads(json.dumps(receipt))
        versioned["cos"]["versionId"] = "version-1"
        with self.assertRaisesRegex(transport.COSReleaseError, "versioned"):
            transport.seal_receipt(
                versioned,
                git_sha=self.git_sha,
                repository=repository,
            )

    def test_github_outputs_never_contain_credentials(self) -> None:
        receipt = transport.make_receipt(
            identity=self.identity,
            bucket="jato-release-1250000000",
            region="ap-guangzhou",
            key=transport.object_key("releases", self.git_sha, self.identity.sha256),
            head=matching_head(self.identity),
            reused=False,
            repository="tristan419/JATO_Analysis_System",
            run_id="123",
            run_attempt="1",
        )
        output = self.root / "github-output"
        transport.write_github_outputs(output, receipt)
        content = output.read_text(encoding="utf-8")
        self.assertNotIn("secret", content.lower())
        self.assertNotIn("token", content.lower())
        self.assertNotIn("authorization", content.lower())
        self.assertEqual(len(content.splitlines()), 6)


if __name__ == "__main__":
    unittest.main()
