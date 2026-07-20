import pandas as pd
import pytest

from app.services import local_wiki_service


def test_build_vehicle_documents_uses_normalized_msrp_in_eur() -> None:
    frame = pd.DataFrame(
        [
            {
                "Countries": "Czech Republic",
                "Make": "SKODA",
                "Model": "ENYAQ",
                "Trim level": "85",
                "Powertrain type": "BEV",
                "Version name": "ENYAQ 85",
                "Currency": "CZK",
                "MSRP规整": 50384.4,
                "MSRP including delivery charge": 1259610,
                "Base price": 1200000,
                "Retail price": 1259610,
            }
        ]
    )

    documents, _, _ = local_wiki_service.build_vehicle_documents(frame)

    assert len(documents) == 1
    assert "MSRP 50384.4 EUR (normalized)" in documents[0]
    assert "Base 1200000 CZK" in documents[0]
    assert "Retail 1259610 CZK" in documents[0]
    assert "MSRP 1259610" not in documents[0]


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
