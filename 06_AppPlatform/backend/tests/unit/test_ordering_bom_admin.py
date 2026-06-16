from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from app.db.models import CountryPaymentTermMaster, CountrySkuFobResolved
from app.infra import order_genius_repository as repo
from app.services.ordering_normalization import normalize_brand, normalize_brand_text


class _ScalarResult:
    def __init__(self, values: list[object]):
        self._values = values

    def all(self) -> list[object]:
        return self._values


class _ExecuteResult:
    def __init__(self, values: list[object]):
        self._values = values

    def scalars(self) -> _ScalarResult:
        return _ScalarResult(self._values)


class _FakeSession:
    def __init__(self, execute_values: list[object] | None = None):
        self.execute_values = execute_values or []
        self.added: list[object] = []

    def execute(self, _stmt: object) -> _ExecuteResult:
        return _ExecuteResult(self.execute_values)

    def add(self, row: object) -> None:
        self.added.append(row)


def test_normalize_jaecoo_brand_variants() -> None:
    assert normalize_brand("JEACOO") == "JAECOO"
    assert normalize_brand("jecoo") == "JAECOO"
    assert normalize_brand_text("JEACOO JAECOO7") == "JAECOO JAECOO7"


def test_list_bom_with_fob_empty_keeps_tuple_shape(monkeypatch) -> None:
    monkeypatch.setattr(repo, "list_all_material_skus_for_admin", lambda *_, **__: [])
    monkeypatch.setattr(
        repo,
        "list_ordering_country_options",
        lambda _session: [{"countryCode": "LV"}],
    )

    assert repo.list_bom_with_fob(_FakeSession()) == ([], ["LV"])


def test_list_ordering_country_options_includes_fob_only_country(monkeypatch) -> None:
    monkeypatch.setattr(
        repo,
        "list_country_payment_terms",
        lambda _session: [
            SimpleNamespace(
                country_code="LV",
                country_name="Latvia",
                payment_term_code="LC90",
                payment_method="LC",
                lc_days=90,
            )
        ],
    )

    result = repo.list_ordering_country_options(_FakeSession(["SK", "LV"]))

    assert result == [
        {
            "countryCode": "LV",
            "countryName": "Latvia",
            "paymentTermCode": "LC90",
            "paymentMethod": "LC",
            "lcDays": 90,
        },
        {
            "countryCode": "SK",
            "countryName": "Slovakia",
            "paymentTermCode": None,
            "paymentMethod": None,
            "lcDays": None,
        },
    ]


def test_copy_country_fobs_creates_target_country_rows(monkeypatch) -> None:
    baseline_id = uuid4()
    source_row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="CZ",
        material_code="T7000SE**MY0001",
        payment_term_code="LC90",
        uploaded_fob_eur=14900,
        final_fob_eur=14900,
        fob_source_mode="explicit_price_by_payment_term",
        is_active=True,
    )
    target_term = CountryPaymentTermMaster(
        country_payment_term_id=uuid4(),
        country_code="SK",
        country_name="Slovakia",
        payment_term_code="LC90",
        payment_method="LC",
        lc_days=90,
        is_active=True,
    )
    fake_session = _FakeSession()

    monkeypatch.setattr(
        repo,
        "list_fob_by_country",
        lambda _session, country_code, payment_term_code=None: [source_row]
        if country_code == "CZ"
        else [],
    )
    monkeypatch.setattr(
        repo,
        "get_country_payment_term",
        lambda _session, country_code: target_term if country_code == "SK" else None,
    )
    monkeypatch.setattr(
        repo,
        "get_fob_for_country_sku",
        lambda _session, country_code, material_code: None,
    )

    result = repo.copy_country_fobs(fake_session, "CZ", "SK")

    assert result["copied"] == 1
    assert result["updated"] == 0
    assert result["skipped"] == 0
    created = fake_session.added[0]
    assert isinstance(created, CountrySkuFobResolved)
    assert created.country_code == "SK"
    assert created.payment_term_code == "LC90"
    assert created.final_fob_eur == 14900
    assert created.fob_source_country_code == "CZ"
