from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.db.models import (
    BrandColourSurchargeRule,
    CountryPaymentTermMaster,
    CountrySkuFobResolved,
    FobResolvedHistory,
)
from app.infra import order_genius_repository as repo
from app.services import order_genius_service
from app.services.ordering_normalization import (
    infer_colour_tier,
    normalize_brand,
    normalize_brand_text,
)


class _ScalarResult:
    def __init__(self, values: list[object]):
        self._values = values

    def all(self) -> list[object]:
        return self._values

    def first(self) -> object | None:
        return self._values[0] if self._values else None


class _ExecuteResult:
    def __init__(self, values: list[object]):
        self._values = values

    def scalars(self) -> _ScalarResult:
        return _ScalarResult(self._values)

    def all(self) -> list[object]:
        return self._values


class _FakeSession:
    def __init__(self, execute_values: list[object] | None = None):
        self.execute_values = execute_values or []
        self.added: list[object] = []

    def execute(self, _stmt: object) -> _ExecuteResult:
        return _ExecuteResult(self.execute_values)

    def add(self, row: object) -> None:
        self.added.append(row)


class _QueryResult:
    def __init__(self, values: list[object]):
        self.values = values

    def filter(self, *_criteria: object) -> "_QueryResult":
        return self

    def all(self) -> list[object]:
        return self.values


class _QueryFakeSession(_FakeSession):
    def __init__(self, query_values: list[object]):
        super().__init__()
        self.query_values = query_values
        self.flushed = False

    def query(self, _model: object) -> _QueryResult:
        return _QueryResult(self.query_values)

    def flush(self) -> None:
        self.flushed = True


def test_normalize_jaecoo_brand_variants() -> None:
    assert normalize_brand("JEACOO") == "JAECOO"
    assert normalize_brand("jecoo") == "JAECOO"
    assert normalize_brand_text("JEACOO JAECOO7") == "JAECOO JAECOO7"


def test_infer_colour_tier_handles_dual_swatch_and_special_finish() -> None:
    assert infer_colour_tier("Carbon black / khaki white") == "dual"
    assert infer_colour_tier("Carbon black + grey roof") == "dual"
    assert infer_colour_tier("Aviation silver", colour_hex="#C8C0B8|#111111") == "dual"
    assert infer_colour_tier("Matte black (Black Edition)") == "special"


def test_fob_based_tier_ignores_cleared_zero_fob() -> None:
    zero = _legacy_jaecoo_sku()
    zero.material_code = "T7000Z5BWMY0000"
    zero.bom_template = "T7000Z5BWMY0000"
    base = _legacy_jaecoo_sku()
    base.material_code = "T7000Z5BWMY0001"
    base.bom_template = "T7000Z5BWMY0001"
    dual = _legacy_jaecoo_sku()
    dual.material_code = "T7000Z5ZEMY0002"
    dual.bom_template = "T7000Z5ZEMY0002"
    dual.exterior_color_code = "ZE"

    session = _QueryFakeSession([
        SimpleNamespace(material_code=zero.material_code, country_code="NL", final_fob_eur=0),
        SimpleNamespace(material_code=base.material_code, country_code="NL", final_fob_eur=1000),
        SimpleNamespace(material_code=dual.material_code, country_code="NL", final_fob_eur=1200),
    ])

    updated = order_genius_service._assign_fob_based_tiers(session, [zero, base, dual])

    assert updated == 1
    assert base.colour_tier == "single"
    assert dual.colour_tier == "dual"
    assert session.flushed is True


def _legacy_jaecoo_sku() -> SimpleNamespace:
    return SimpleNamespace(
        material_code="T7000Z5**MY0026",
        bom_template="T7000Z5**MY0026",
        brand="JEACOO",
        model_name="JEACOO5 HEV",
        version="Exclusive-FWD",
        powertrain=None,
        exterior_color_name="Khaki white",
        exterior_color_code="BW",
        exterior_color_type="single",
        colour_tier="single",
        colour_hex=None,
        interior_color_name="Black-Black",
        interior_colour_code="R19",
        interior_package=None,
        edition_tag=None,
        remark=None,
        effective_from_month=None,
        effective_to_month=None,
    )


def test_build_matrix_normalizes_legacy_jaecoo_and_model_powertrain(monkeypatch) -> None:
    sku = _legacy_jaecoo_sku()
    fob = SimpleNamespace(final_fob_eur=15300)

    monkeypatch.setattr(
        order_genius_service.repo,
        "get_country_payment_term",
        lambda _session, _country_code, _order_month_hint=None: SimpleNamespace(
            payment_term_code="LC90",
            country_name="Slovakia",
        ),
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_active_skus",
        lambda *_args, **_kwargs: [sku],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_fobs_for_country_material_codes",
        lambda _session, _country_code, material_codes, _payment_term_code=None: {
            sku.material_code: fob,
        } if sku.material_code in material_codes else {},
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_quantities_for_country_year",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_historical_skus_with_quantity",
        lambda *_args, **_kwargs: [],
    )

    result = order_genius_service.build_matrix(
        _FakeSession(),
        "SK",
        2026,
        brand="JAECOO",
        model_name="JAECOO5 HEV",
        powertrain="HEV",
    )

    assert result["totalRows"] == 1
    row = result["rows"][0]
    assert row["brand"] == "JAECOO"
    assert row["modelName"] == "JAECOO5 HEV"
    assert row["powertrain"] == "HEV"
    assert row["fobEur"] == 15300


def test_build_matrix_backfills_interior_and_preserves_paint_tier(monkeypatch) -> None:
    blank = _legacy_jaecoo_sku()
    blank.material_code = "T7000Z5CPMY0026"
    blank.exterior_color_name = "Matte black (Black Edition)"
    blank.exterior_color_code = "CP"
    blank.exterior_color_type = "single"
    blank.colour_tier = "single"
    blank.interior_color_name = None

    donor = _legacy_jaecoo_sku()
    donor.material_code = "T7000Z5BWMY0026"
    donor.exterior_color_code = "BW"
    donor.interior_color_name = "Black-Black"

    monkeypatch.setattr(
        order_genius_service.repo,
        "get_country_payment_term",
        lambda _session, _country_code, _order_month_hint=None: SimpleNamespace(
            payment_term_code="LC90",
            country_name="Slovakia",
        ),
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_active_skus",
        lambda *_args, **_kwargs: [blank, donor],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_fobs_for_country_material_codes",
        lambda _session, _country_code, material_codes, _payment_term_code=None: {
            code: SimpleNamespace(final_fob_eur=15300)
            for code in material_codes
        },
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_quantities_for_country_year",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_historical_skus_with_quantity",
        lambda *_args, **_kwargs: [],
    )

    result = order_genius_service.build_matrix(_FakeSession(), "SK", 2026)
    by_code = {row["materialCode"]: row for row in result["rows"]}

    assert by_code["T7000Z5CPMY0026"]["interiorColorName"] == "Black-Black"
    assert by_code["T7000Z5CPMY0026"]["colourTier"] == "special"


def test_build_matrix_excludes_cleared_zero_fob(monkeypatch) -> None:
    sku = _legacy_jaecoo_sku()

    monkeypatch.setattr(
        order_genius_service.repo,
        "get_country_payment_term",
        lambda _session, _country_code, _order_month_hint=None: SimpleNamespace(
            payment_term_code="LC90",
            country_name="Slovakia",
        ),
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_active_skus",
        lambda *_args, **_kwargs: [sku],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_fobs_for_country_material_codes",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_quantities_for_country_year",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_historical_skus_with_quantity",
        lambda *_args, **_kwargs: [],
    )

    result = order_genius_service.build_matrix(_FakeSession(), "SK", 2026)

    assert result["rows"] == []
    assert result["totalRows"] == 0


def test_list_bom_with_fob_backfills_interior_and_effective_colour_tier(monkeypatch) -> None:
    blank = _legacy_jaecoo_sku()
    blank.material_code = "T7000Z5CPMY0026"
    blank.exterior_color_name = "Matte black (Black Edition)"
    blank.exterior_color_code = "CP"
    blank.exterior_color_type = "single"
    blank.colour_tier = "single"
    blank.interior_color_name = None
    blank.interior_colour_code = None
    blank.interior_package = None
    blank.baseline_version_id = None
    blank.lifecycle_status = "active"
    blank.is_active = True
    blank.effective_from_month = None
    blank.effective_to_month = None
    blank.row_version = 1
    blank.source_sheet_name = None
    blank.source_row_number = None
    blank.raw_payload_json = None
    blank.colour_hex = None
    blank.colour_code_confirmed = True

    donor = _legacy_jaecoo_sku()
    donor.material_code = "T7000Z5BWMY0026"
    donor.interior_color_name = "Black-Black"
    donor.interior_colour_code = "R19"
    donor.interior_package = "Black-Black"
    donor.baseline_version_id = None
    donor.lifecycle_status = "active"
    donor.is_active = True
    donor.effective_from_month = None
    donor.effective_to_month = None
    donor.row_version = 1
    donor.source_sheet_name = None
    donor.source_row_number = None
    donor.raw_payload_json = None
    donor.colour_hex = None
    donor.colour_code_confirmed = True

    monkeypatch.setattr(repo, "list_bom_admin_country_columns", lambda _session: ["NL"])
    monkeypatch.setattr(
        repo,
        "list_all_material_skus_for_admin",
        lambda *_args, **_kwargs: [blank, donor],
    )

    rows, _countries = repo.list_bom_with_fob(_FakeSession())
    by_code = {row["materialCode"]: row for row in rows}

    assert by_code["T7000Z5CPMY0026"]["interiorColorName"] == "Black-Black"
    assert by_code["T7000Z5CPMY0026"]["interiorColourCode"] == "R19"
    assert by_code["T7000Z5CPMY0026"]["colourTier"] == "special"


def test_build_options_normalizes_legacy_jaecoo_filter_values(monkeypatch) -> None:
    sku = _legacy_jaecoo_sku()

    monkeypatch.setattr(
        order_genius_service.repo,
        "get_country_payment_term",
        lambda *_args, **_kwargs: SimpleNamespace(payment_term_code="LC90"),
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_active_fob_material_codes",
        lambda *_args, **_kwargs: [sku.material_code],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_active_skus",
        lambda *_args, **_kwargs: [sku],
    )

    result = order_genius_service.build_options(
        _FakeSession(),
        "SK",
        brand="JAECOO",
        model_name="JAECOO5 HEV",
    )

    assert result["brands"] == ["JAECOO"]
    assert result["models"] == ["JAECOO5 HEV"]
    assert result["powertrains"] == ["HEV"]
    assert result["versions"] == ["Exclusive-FWD"]
    assert result["materialCodes"] == ["T7000Z5**MY0026"]


def test_list_bom_with_fob_empty_keeps_tuple_shape(monkeypatch) -> None:
    monkeypatch.setattr(repo, "list_all_material_skus_for_admin", lambda *_, **__: [])
    monkeypatch.setattr(
        repo,
        "list_active_fob_country_codes",
        lambda _session: ["LV"],
    )

    assert repo.list_bom_with_fob(_FakeSession()) == ([], ["NL", "LV"])


def test_list_bom_admin_country_columns_keeps_nl_first(monkeypatch) -> None:
    monkeypatch.setattr(
        repo,
        "list_active_fob_country_codes",
        lambda _session: ["SK", "CZ"],
    )

    assert repo.list_bom_admin_country_columns(_FakeSession()) == ["NL", "CZ", "SK"]


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
    by_code = {item["countryCode"]: item for item in result}

    assert "DE" in by_code
    assert "PT" in by_code
    assert by_code["LV"] == {
        "countryCode": "LV",
        "countryName": "Latvia",
        "paymentTermCode": "LC90",
        "paymentMethod": "LC",
        "lcDays": 90,
    }
    assert by_code["SK"] == {
        "countryCode": "SK",
        "countryName": "Slovakia",
        "paymentTermCode": None,
        "paymentMethod": None,
        "lcDays": None,
    }


def test_list_ordering_country_options_skips_unsupported_country_codes(monkeypatch) -> None:
    monkeypatch.setattr(
        repo,
        "list_country_payment_terms",
        lambda _session: [
            SimpleNamespace(
                country_code="PU",
                country_name="Portugal",
                payment_term_code="TT",
                payment_method="TT",
                lc_days=0,
            )
        ],
    )

    result = repo.list_ordering_country_options(_FakeSession(["PU", "SK"]))
    by_code = {item["countryCode"]: item for item in result}

    assert "PU" not in by_code
    assert by_code["PT"]["countryName"] == "Portugal"
    assert by_code["SK"]["countryName"] == "Slovakia"


def test_upsert_colour_surcharge_creates_normalized_special_rule() -> None:
    fake_session = _FakeSession()

    rule = repo.upsert_colour_surcharge(fake_session, "JEACOO", "Special", 350)

    assert fake_session.added == [rule]
    assert rule.brand == "JAECOO"
    assert rule.colour_type == "special"
    assert rule.surcharge_eur == 350
    assert rule.is_active is True


def test_upsert_colour_surcharge_updates_existing_rule() -> None:
    existing = BrandColourSurchargeRule(
        colour_surcharge_rule_id=uuid4(),
        brand="JAECOO",
        colour_type="dual",
        surcharge_eur=300,
        is_active=True,
    )
    fake_session = _FakeSession([existing])

    rule = repo.upsert_colour_surcharge(fake_session, "JAECOO", "dual", 320)

    assert rule is existing
    assert fake_session.added == []
    assert existing.surcharge_eur == 320


def test_upsert_colour_surcharge_rejects_unknown_type() -> None:
    with pytest.raises(ValueError, match="colourType must be dual or special"):
        repo.upsert_colour_surcharge(_FakeSession(), "OMODA", "single", 0)


def test_colour_hex_rules_group_by_brand_code_and_normalized_name() -> None:
    skus = [
        SimpleNamespace(
            material_code="A",
            brand="JEACOO",
            exterior_color_code="bw",
            exterior_color_name="Khaki   White",
            colour_hex="#f0ece0",
        ),
        SimpleNamespace(
            material_code="B",
            brand="JAECOO",
            exterior_color_code="BW",
            exterior_color_name="khaki white",
            colour_hex="#F0ECE0",
        ),
        SimpleNamespace(
            material_code="C",
            brand="JAECOO",
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#FFFFFF",
        ),
    ]

    rules = repo.build_colour_hex_rules_from_skus(skus)

    assert len(rules) == 1
    rule = rules[0]
    assert rule["brand"] == "JAECOO"
    assert rule["colourCode"] == "BW"
    assert rule["normalizedColourName"] == "khaki white"
    assert rule["status"] == "conflict"
    assert rule["hexOptions"] == [
        {"colourHex": "#F0ECE0", "skuCount": 2},
        {"colourHex": "#FFFFFF", "skuCount": 1},
    ]


def test_find_reusable_colour_hex_requires_no_conflict() -> None:
    standard = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="JAECOO",
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#F0ECE0",
        )
    ])
    conflict = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="JAECOO",
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#F0ECE0",
        ),
        SimpleNamespace(
            material_code="B",
            brand="JAECOO",
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#FFFFFF",
        ),
    ])

    assert repo.find_reusable_colour_hex(
        standard, "JAECOO", "BW", "khaki white",
    ) == "#F0ECE0"
    assert repo.find_reusable_colour_hex(
        conflict, "JAECOO", "BW", "khaki white",
    ) is None


def test_set_standard_colour_hex_for_rule_updates_matching_skus_only() -> None:
    matching = SimpleNamespace(
        material_code="A",
        brand="JAECOO",
        exterior_color_code="BW",
        exterior_color_name="Khaki White",
        colour_hex="#F0ECE0",
        updated_at_utc=None,
    )
    same_code_other_name = SimpleNamespace(
        material_code="B",
        brand="JAECOO",
        exterior_color_code="BW",
        exterior_color_name="Carbon Black",
        colour_hex="#000000",
        updated_at_utc=None,
    )
    fake_session = _FakeSession([matching, same_code_other_name])

    result = repo.set_standard_colour_hex_for_rule(
        fake_session, "JAECOO", "BW", "khaki white", "#ffffff",
    )

    assert result["updated"] == 1
    assert result["materialCodes"] == ["A"]
    assert matching.colour_hex == "#FFFFFF"
    assert matching.updated_at_utc is not None
    assert same_code_other_name.colour_hex == "#000000"


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


def test_adjust_country_fobs_updates_rows_and_writes_history(monkeypatch) -> None:
    baseline_id = uuid4()
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="SK",
        material_code="T7000SE**MY0001",
        payment_term_code="LC90",
        uploaded_fob_eur=14900,
        final_fob_eur=14900,
        fob_source_mode="copied_from_country",
        is_active=True,
    )
    fake_session = _FakeSession()

    monkeypatch.setattr(
        repo,
        "list_fob_by_country",
        lambda _session, country_code, payment_term_code=None: [row]
        if country_code == "SK"
        else [],
    )

    result = repo.adjust_country_fobs(fake_session, "SK", 200, changed_by="admin")

    assert result == {
        "countryCode": "SK",
        "deltaEur": 200,
        "rows": 1,
        "adjusted": 1,
        "skippedNegative": 0,
        "unchanged": 0,
    }
    assert row.final_fob_eur == 15100
    assert row.fob_source_mode == "manual_country_adjust"
    assert row.updated_at_utc is not None
    history = fake_session.added[0]
    assert isinstance(history, FobResolvedHistory)
    assert history.country_code == "SK"
    assert history.material_code == "T7000SE**MY0001"
    assert history.old_final_fob_eur == 14900
    assert history.new_final_fob_eur == 15100
    assert history.changed_by == "admin"
