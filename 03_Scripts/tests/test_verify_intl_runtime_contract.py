from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest
from unittest import mock
from urllib.error import HTTPError, URLError


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "03_Scripts" / "deploy"))

import verify_intl_runtime_contract as runtime_contract  # noqa: E402


ORIGIN = "https://intl.example.test"


class FakeResponse:
    def __init__(
        self,
        payload: object = None,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> None:
        self.status = status
        self.headers = headers or {"content-type": "application/json"}
        self.body = (
            body
            if body is not None
            else json.dumps(payload).encode("utf-8")
        )

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self.body if size < 0 else self.body[:size]


def health_response(
    *,
    marker: str = "healthz",
    cache_control: str = "no-store",
) -> FakeResponse:
    return FakeResponse(
        {"status": "ok"},
        headers={
            "content-type": "application/json; charset=utf-8",
            "cache-control": cache_control,
            "x-jato-edge-proxy": marker,
        },
    )


def freshness_response(
    *,
    endpoint: str = runtime_contract.FRESHNESS_PATH,
    cache_state: str = "MISS",
) -> FakeResponse:
    return FakeResponse(
        {"items": [{"country": "Hungary", "latest_month": "2026-06"}]},
        headers={
            "content-type": "application/json",
            "x-jato-edge-cache-endpoint": endpoint,
            "x-jato-edge-cache": cache_state,
        },
    )


def oauth_health_response(*, cache_control: str = "no-store") -> FakeResponse:
    return FakeResponse(
        {"status": "ok"},
        headers={
            "content-type": "application/json",
            "cache-control": cache_control,
        },
    )


def successful_attempt() -> list[FakeResponse]:
    return [health_response(), freshness_response(), oauth_health_response()]


class IntlRuntimeContractTests(unittest.TestCase):
    @mock.patch.object(runtime_contract, "urlopen")
    def test_success(self, mocked_urlopen: mock.Mock) -> None:
        mocked_urlopen.side_effect = successful_attempt()

        responses = runtime_contract.verify_runtime_contract(
            ORIGIN,
            attempts=1,
            delay_seconds=0,
            timeout_seconds=2,
        )

        self.assertEqual(
            [response.path for response in responses],
            [path for path, _validator in runtime_contract.CHECKS],
        )
        requested_paths = [call.args[0].full_url for call in mocked_urlopen.call_args_list]
        self.assertEqual(
            requested_paths,
            [f"{ORIGIN}{path}" for path, _validator in runtime_contract.CHECKS],
        )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_spa_html_fake_200_fails_closed(self, mocked_urlopen: mock.Mock) -> None:
        html_response = FakeResponse(
            headers={"content-type": "text/html; charset=utf-8"},
            body=b"<!doctype html><title>JATO</title>",
        )
        mocked_urlopen.side_effect = [
            html_response,
            freshness_response(),
            oauth_health_response(),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"/healthz: expected application/json, got text/html",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_oauth_relay_html_fake_200_fails_closed(
        self,
        mocked_urlopen: mock.Mock,
    ) -> None:
        mocked_urlopen.side_effect = [
            health_response(),
            freshness_response(),
            FakeResponse(
                headers={"content-type": "text/html"},
                body=b"<html><body>SPA fallback</body></html>",
            ),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"/oauth-relay/healthz: expected application/json",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_retries_then_succeeds(self, mocked_urlopen: mock.Mock) -> None:
        mocked_urlopen.side_effect = [
            URLError("temporary edge propagation failure"),
            freshness_response(),
            oauth_health_response(),
            *successful_attempt(),
        ]
        sleeper = mock.Mock()

        responses = runtime_contract.verify_runtime_contract(
            ORIGIN,
            attempts=2,
            delay_seconds=3,
            timeout_seconds=2,
            sleeper=sleeper,
        )

        self.assertEqual(len(responses), 3)
        sleeper.assert_called_once_with(3)
        self.assertEqual(mocked_urlopen.call_count, 6)

    @mock.patch.object(runtime_contract, "urlopen")
    def test_retry_exhaustion_reports_network_errors(
        self,
        mocked_urlopen: mock.Mock,
    ) -> None:
        mocked_urlopen.side_effect = URLError("edge unreachable")
        sleeper = mock.Mock()

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"failed after 2 attempt\(s\).*attempt 2/2.*request failed",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=2,
                delay_seconds=1,
                timeout_seconds=2,
                sleeper=sleeper,
            )

        sleeper.assert_called_once_with(1)
        self.assertEqual(mocked_urlopen.call_count, 6)

    @mock.patch.object(runtime_contract, "urlopen")
    def test_wrong_edge_markers_fail_closed(self, mocked_urlopen: mock.Mock) -> None:
        mocked_urlopen.side_effect = [
            health_response(marker="wrong"),
            freshness_response(endpoint="/wrong", cache_state="UNKNOWN"),
            oauth_health_response(),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"x-jato-edge-proxy=healthz.*x-jato-edge-cache-endpoint",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_health_proxy_requires_no_store(self, mocked_urlopen: mock.Mock) -> None:
        mocked_urlopen.side_effect = [
            health_response(cache_control="public, max-age=300"),
            freshness_response(),
            oauth_health_response(),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"/healthz: expected Cache-Control to include no-store",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_invalid_json_fails_closed(self, mocked_urlopen: mock.Mock) -> None:
        mocked_urlopen.side_effect = [
            FakeResponse(body=b"not-json"),
            freshness_response(),
            oauth_health_response(),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"/healthz: invalid JSON",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_oauth_relay_requires_no_store(self, mocked_urlopen: mock.Mock) -> None:
        mocked_urlopen.side_effect = [
            health_response(),
            freshness_response(),
            oauth_health_response(cache_control="public, max-age=300"),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"/oauth-relay/healthz: expected Cache-Control to include no-store",
        ):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_www_profile_accepts_backend_json_without_edge_headers(
        self,
        mocked_urlopen: mock.Mock,
    ) -> None:
        mocked_urlopen.side_effect = [
            FakeResponse({"status": "ok"}),
            FakeResponse({"items": [{"country": "Hungary"}]}),
        ]

        responses = runtime_contract.verify_runtime_contract(
            "https://www.example.test",
            attempts=1,
            delay_seconds=0,
            timeout_seconds=2,
            profile="www",
        )

        self.assertEqual(
            [response.path for response in responses],
            ["/healthz", runtime_contract.FRESHNESS_PATH],
        )
        self.assertEqual(mocked_urlopen.call_count, 2)

    @mock.patch.object(runtime_contract, "urlopen")
    def test_www_profile_rejects_empty_freshness_json(
        self,
        mocked_urlopen: mock.Mock,
    ) -> None:
        mocked_urlopen.side_effect = [
            FakeResponse({"status": "ok"}),
            FakeResponse({}),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            "expected payload items to be a non-empty list",
        ):
            runtime_contract.verify_runtime_contract(
                "https://www.example.test",
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
                profile="www",
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_www_profile_rejects_error_shaped_freshness_json(
        self,
        mocked_urlopen: mock.Mock,
    ) -> None:
        mocked_urlopen.side_effect = [
            FakeResponse({"status": "ok"}),
            FakeResponse({"detail": "upstream unavailable"}),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            "expected payload items to be a non-empty list",
        ):
            runtime_contract.verify_runtime_contract(
                "https://www.example.test",
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
                profile="www",
            )

    @mock.patch.object(runtime_contract, "urlopen")
    def test_www_profile_rejects_html_and_502(
        self,
        mocked_urlopen: mock.Mock,
    ) -> None:
        mocked_urlopen.side_effect = [
            FakeResponse(
                headers={"content-type": "text/html"},
                body=b"<html>bad gateway</html>",
            ),
            HTTPError(
                f"{ORIGIN}{runtime_contract.FRESHNESS_PATH}",
                502,
                "Bad Gateway",
                hdrs=None,
                fp=None,
            ),
        ]

        with self.assertRaisesRegex(
            runtime_contract.ContractValidationError,
            r"/healthz: expected application/json.*data-freshness: HTTP 502",
        ):
            runtime_contract.verify_runtime_contract(
                "https://www.example.test",
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
                profile="www",
            )

    def test_unknown_profile_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported runtime contract profile"):
            runtime_contract.verify_runtime_contract(
                ORIGIN,
                attempts=1,
                delay_seconds=0,
                timeout_seconds=2,
                profile="unknown",
            )


if __name__ == "__main__":
    unittest.main()
