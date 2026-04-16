import pytest

from app.services import local_wiki_service


def test_query_local_wiki_documents_returns_empty_when_chromadb_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(local_wiki_service, "chromadb", None)
    monkeypatch.setattr(
        local_wiki_service,
        "_CHROMADB_IMPORT_ERROR",
        ModuleNotFoundError("No module named 'chromadb'"),
    )
    local_wiki_service.clear_local_wiki_caches()

    assert local_wiki_service.query_local_wiki_documents("XC60 price") == []
