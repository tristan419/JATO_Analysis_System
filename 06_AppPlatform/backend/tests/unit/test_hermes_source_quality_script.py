import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
SCRIPT_PATH = SCRIPT_DIR / "hermes_source_quality.py"

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

spec = importlib.util.spec_from_file_location("hermes_source_quality_script", SCRIPT_PATH)
source_quality = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_quality)


def test_compute_quality_score_uses_runtime_status() -> None:
    source = {
        "sourceId": "source.news.batch_a",
        "sourceType": "news",
        "status": "active",
        "governanceStatus": "registered",
        "knownIssues": ["Google News RSS blocked from Tencent Cloud"],
        "lastObserved": {
            "successCount": 21,
            "failedCount": None,
            "lastSuccessAt": "2026-05-13T15:18:03Z",
        },
        "quality": {
            "successRate": None,
            "timeoutRate": 0.0,
            "extractionQualityScore": 0.55,
        },
    }
    status_data = {
        "news": {
            "status": "failure",
            "successCount": 0,
            "failedCount": 0,
            "lastRunAt": "2026-05-19T22:15:26Z",
            "lastError": "news sync exited with code 1",
        }
    }

    result = source_quality._compute_quality_score(source, status_data)

    assert result["sourceId"] == "source.news.batch_a"
    assert result["lastFailureAt"] == "2026-05-19T22:15:26Z"
    assert result["lastFailureReason"] == "news sync exited with code 1"
    assert "runtime status=failure" in result["reasons"]


def test_registry_quality_from_score_derives_rates() -> None:
    scored = {
        "qualityScore": 50,
        "status": "degraded",
        "risk": "medium",
        "successCount": 0,
        "failedCount": 8,
        "reasons": ["Single source timeout/403 can fail entire batch"],
    }

    result = source_quality._registry_quality_from_score(
        scored,
        "2026-05-20T18:43:52Z",
    )

    assert result["successRate"] == 0.0
    assert result["failureRate"] == 1.0
    assert result["timeoutRate"] == 1.0
    assert result["http403Rate"] == 1.0
    assert result["extractionQualityScore"] == 0.5
