from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import frontend_release_artifact as release_artifact  # noqa: E402


GITHUB_SHA = "2" * 40
APP_COMMIT = "5" * 40
RUN_ID = "123456"
RUN_ATTEMPT = "2"
REPOSITORY = "example/JATO_Analysis_System"
WORKFLOW = "production-release"
ARTIFACT_NAME = f"frontend-dist-{GITHUB_SHA}"
ARTIFACT_IDENTITY = release_artifact.artifact_identity(
    REPOSITORY,
    RUN_ID,
    RUN_ATTEMPT,
    ARTIFACT_NAME,
)
GITHUB_ARTIFACT_ID = "987654"
GITHUB_ARTIFACT_DIGEST = "a" * 64
NODE_VERSION = "v20.19.0"


class FrontendReleaseArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.dist_dir = self.root / "dist"
        self.functions_dir = self.root / "functions"
        self.release_dir = self.root / "release"
        self.dist_dir.mkdir()
        (self.functions_dir / "v1").mkdir(parents=True)
        (self.functions_dir / "oauth-relay").mkdir(parents=True)
        (self.dist_dir / "index.html").write_text("<main>release</main>\n", encoding="utf-8")
        assets = self.dist_dir / "assets"
        assets.mkdir()
        (assets / "app.js").write_text("console.log('immutable');\n", encoding="utf-8")
        (self.functions_dir / "healthz.js").write_text(
            "export const onRequest = () => Response.json({status: 'ok'});\n",
            encoding="utf-8",
        )
        (self.functions_dir / "v1" / "[[path]].js").write_text(
            "export const onRequest = () => Response.json({route: 'v1'});\n",
            encoding="utf-8",
        )
        (self.functions_dir / "oauth-relay" / "[[path]].js").write_text(
            "export const onRequest = () => Response.json({route: 'oauth'});\n",
            encoding="utf-8",
        )
        build_id = release_artifact.frontend_build_id(self.dist_dir)
        (self.dist_dir / release_artifact.BUILD_META_NAME).write_text(
            json.dumps(
                {
                    "commit": APP_COMMIT,
                    "deployCommit": GITHUB_SHA,
                    "commitMode": "application",
                    "builtAt": "2026-07-15T01:02:03.000Z",
                    "nodeVersion": NODE_VERSION,
                    "frontendBuildId": build_id,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        self.manifest = release_artifact.create_release(
            dist_dir=self.dist_dir,
            functions_dir=self.functions_dir,
            release_dir=self.release_dir,
            github_sha=GITHUB_SHA,
            artifact_name=ARTIFACT_NAME,
            repository=REPOSITORY,
            workflow=WORKFLOW,
            run_id=RUN_ID,
            run_attempt=RUN_ATTEMPT,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def verify(self, **overrides: object) -> dict[str, object]:
        artifact = self.manifest["artifact"]
        frontend = self.manifest["frontend"]
        arguments: dict[str, object] = {
            "release_dir": self.release_dir,
            "expected_github_sha": GITHUB_SHA,
            "expected_artifact_name": ARTIFACT_NAME,
            "expected_artifact_identity": ARTIFACT_IDENTITY,
            "expected_artifact_checksum": artifact["checksum"],
            "expected_build_id": frontend["buildId"],
            "expected_node_version": NODE_VERSION,
            "expected_run_id": RUN_ID,
            "expected_run_attempt": RUN_ATTEMPT,
            "github_artifact_id": GITHUB_ARTIFACT_ID,
            "github_artifact_digest": GITHUB_ARTIFACT_DIGEST,
        }
        arguments.update(overrides)
        return release_artifact.verify_release(**arguments)

    def test_create_and_verify_materializes_public_provenance(self) -> None:
        materialized = self.root / "materialized"
        materialized_functions = self.root / "materialized-functions"
        provenance = self.verify(
            materialize_dir=materialized,
            materialize_functions_dir=materialized_functions,
        )

        self.assertTrue((materialized / "index.html").is_file())
        self.assertTrue((materialized_functions / "healthz.js").is_file())
        self.assertTrue((materialized_functions / "v1" / "[[path]].js").is_file())
        public_provenance = json.loads(
            (materialized / release_artifact.PUBLIC_PROVENANCE_NAME).read_text(
                encoding="utf-8"
            )
        )
        build_meta = json.loads(
            (materialized / release_artifact.BUILD_META_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual(public_provenance, provenance)
        self.assertEqual(build_meta["artifactId"], ARTIFACT_IDENTITY)
        self.assertEqual(build_meta["githubArtifactId"], GITHUB_ARTIFACT_ID)
        self.assertEqual(build_meta["appCommit"], APP_COMMIT)
        self.assertEqual(build_meta["githubSha"], GITHUB_SHA)
        self.assertEqual(build_meta["nodeVersion"], NODE_VERSION)
        self.assertEqual(
            build_meta["edgeFunctionsTreeId"],
            provenance["edgeFunctions"]["treeId"],
        )

    def test_payload_is_deterministic(self) -> None:
        second_release = self.root / "second-release"
        second_manifest = release_artifact.create_release(
            dist_dir=self.dist_dir,
            functions_dir=self.functions_dir,
            release_dir=second_release,
            github_sha=GITHUB_SHA,
            artifact_name=ARTIFACT_NAME,
            repository=REPOSITORY,
            workflow=WORKFLOW,
            run_id=RUN_ID,
            run_attempt=RUN_ATTEMPT,
        )

        self.assertEqual(
            self.manifest["artifact"]["checksum"],
            second_manifest["artifact"]["checksum"],
        )
        self.assertEqual(
            (self.release_dir / release_artifact.PAYLOAD_NAME).read_bytes(),
            (second_release / release_artifact.PAYLOAD_NAME).read_bytes(),
        )

    def test_missing_artifact_fails_closed(self) -> None:
        (self.release_dir / release_artifact.PAYLOAD_NAME).unlink()
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "artifact payload is missing",
        ):
            self.verify()

    def test_tampered_checksum_fails_closed(self) -> None:
        with (self.release_dir / release_artifact.PAYLOAD_NAME).open("ab") as payload:
            payload.write(b"tampered")
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "payload checksum mismatch",
        ):
            self.verify()

    def test_wrong_sha_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "manifest github SHA mismatch",
        ):
            self.verify(expected_github_sha="3" * 40)

    def test_incomplete_manifest_fails_closed(self) -> None:
        manifest_path = self.release_dir / release_artifact.MANIFEST_NAME
        incomplete = copy.deepcopy(self.manifest)
        del incomplete["frontend"]["nodeVersion"]
        manifest_path.write_text(
            json.dumps(incomplete, indent=2) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "frontend.nodeVersion must be a non-empty string",
        ):
            self.verify()

    def test_tampered_edge_functions_manifest_fails_closed(self) -> None:
        manifest_path = self.release_dir / release_artifact.MANIFEST_NAME
        tampered = copy.deepcopy(self.manifest)
        tampered["edgeFunctions"]["treeId"] = "f" * 64
        manifest_path.write_text(
            json.dumps(tampered, indent=2) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "Functions tree does not match",
        ):
            self.verify()

    def test_missing_required_edge_function_fails_closed(self) -> None:
        (self.functions_dir / "healthz.js").unlink()
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "required Cloudflare Pages Functions are missing",
        ):
            release_artifact.create_release(
                dist_dir=self.dist_dir,
                functions_dir=self.functions_dir,
                release_dir=self.root / "missing-function-release",
                github_sha=GITHUB_SHA,
                artifact_name=ARTIFACT_NAME,
                repository=REPOSITORY,
                workflow=WORKFLOW,
                run_id=RUN_ID,
                run_attempt=RUN_ATTEMPT,
            )

    def test_edge_function_directory_symlink_fails_closed(self) -> None:
        (self.functions_dir / "linked-v1").symlink_to(
            self.functions_dir / "v1",
            target_is_directory=True,
        )
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "content directory contains a symlink",
        ):
            release_artifact.create_release(
                dist_dir=self.dist_dir,
                functions_dir=self.functions_dir,
                release_dir=self.root / "symlink-release",
                github_sha=GITHUB_SHA,
                artifact_name=ARTIFACT_NAME,
                repository=REPOSITORY,
                workflow=WORKFLOW,
                run_id=RUN_ID,
                run_attempt=RUN_ATTEMPT,
            )

    def test_wrong_artifact_identity_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "artifact identity does not match",
        ):
            self.verify(expected_artifact_identity="gha://wrong/release")

    def test_public_parity_rejects_platform_specific_metadata(self) -> None:
        materialized = self.root / "public"
        provenance = self.verify(materialize_dir=materialized)
        build_meta = json.loads(
            (materialized / release_artifact.BUILD_META_NAME).read_text(encoding="utf-8")
        )
        altered_provenance = copy.deepcopy(provenance)
        altered_provenance["artifact"]["checksum"] = "b" * 64

        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "public release provenance does not match",
        ):
            release_artifact.validate_public_documents(
                build_meta,
                altered_provenance,
                provenance,
            )

        altered_build_meta = copy.deepcopy(build_meta)
        altered_build_meta["frontendBuildId"] = "c" * 64
        with self.assertRaisesRegex(
            release_artifact.ReleaseValidationError,
            "frontendBuildId.*does not match",
        ):
            release_artifact.validate_public_documents(
                altered_build_meta,
                provenance,
                provenance,
            )


if __name__ == "__main__":
    unittest.main()
