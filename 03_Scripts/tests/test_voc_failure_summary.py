import importlib.util
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "voc" / "summarize_voc_failures.py"

spec = importlib.util.spec_from_file_location("summarize_voc_failures", SCRIPT_PATH)
summarize_voc_failures = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(summarize_voc_failures)


def test_build_failed_sources_reads_current_voc_summary_shape(tmp_path: Path) -> None:
    raw_payload = {
        "errors": [
            {"url": "https://example.com/thread-1", "error": "403 Client Error"},
            {"url": "https://example.com/thread-2", "error": "Read timed out"},
        ],
    }
    raw_path = tmp_path / "04_Processed_data" / "voc" / "se" / "raw" / "se_forum.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_text('{"errors":[]}', encoding="utf-8")

    def load_raw_payload(path: str | None, repo_root: Path) -> dict:
        assert path == str(raw_path)
        assert repo_root == tmp_path
        return raw_payload

    summary = [
        {
            "batch_code": "voc_batch_a",
            "countries": [
                {
                    "country_code": "SE",
                    "sources": [
                        {
                            "source_code": "se_forum",
                            "site_name": "SE Forum",
                            "document_count": 0,
                            "error_count": 2,
                            "output_path": str(raw_path),
                        },
                        {
                            "source_code": "se_ok",
                            "site_name": "SE OK",
                            "document_count": 4,
                            "error_count": 0,
                        },
                    ],
                }
            ],
        }
    ]

    result = summarize_voc_failures.build_failed_sources(
        summary,
        repo_root=tmp_path,
        raw_payload_loader=load_raw_payload,
    )

    assert result == [
        {
            "batchCode": "voc_batch_a",
            "country": "SE",
            "source": "se_forum",
            "siteName": "SE Forum",
            "documentCount": 0,
            "errorCount": 2,
            "errors": raw_payload["errors"],
            "outputPath": str(raw_path),
        }
    ]


def test_build_failed_sources_keeps_legacy_flat_summary() -> None:
    result = summarize_voc_failures.build_failed_sources(
        [{"source": "old_source", "country": "SE", "status": "failed", "error": "timeout"}],
        repo_root=Path("/tmp"),
    )

    assert result[0]["source"] == "old_source"
    assert result[0]["country"] == "SE"
    assert result[0]["errorCount"] == 1
