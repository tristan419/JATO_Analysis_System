from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.routes import order_genius as order_genius_routes
from app.db.models import (
    BrandColourSurchargeRule,
    BrandColourSwatchRule,
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
    resolve_material_brand,
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


class _QueuedExecuteSession(_FakeSession):
    def __init__(self, execute_batches: list[list[object]]):
        super().__init__()
        self.execute_batches = list(execute_batches)

    def execute(self, _stmt: object) -> _ExecuteResult:
        if not self.execute_batches:
            return _ExecuteResult([])
        return _ExecuteResult(self.execute_batches.pop(0))


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


class _CreateMaterialSession(_FakeSession):
    def __init__(self) -> None:
        super().__init__()
        self.flushed = False
        self.committed = False
        self.rolled_back = False

    def flush(self) -> None:
        self.flushed = True

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_normalize_jaecoo_brand_variants() -> None:
    assert normalize_brand("JEACOO") == "JAECOO"
    assert normalize_brand("jecoo") == "JAECOO"
    assert normalize_brand_text("JEACOO JAECOO7") == "JAECOO JAECOO7"


def test_resolve_material_brand_infers_only_known_brand_from_identity() -> None:
    assert resolve_material_brand("", "JAECOO5 ICE", "T71611C") == "JAECOO"
    assert resolve_material_brand("", "OMODA5 EV", "T7000") == "OMODA"
    assert resolve_material_brand("", "J5 ICE", "T71611C") == ""
    assert resolve_material_brand("JAECOO", "J5 ICE", "T71611C") == "JAECOO"


def test_create_material_sku_canonicalizes_jaecoo_and_creates_manual_baseline(
    monkeypatch,
) -> None:
    session = _CreateMaterialSession()
    baseline_id = uuid4()
    reusable_colour_args: dict[str, object] = {}
    baseline_publishers: list[str] = []

    monkeypatch.setattr(
        order_genius_routes.repo,
        "get_sku_by_material_code_any_status",
        lambda *_: None,
    )
    monkeypatch.setattr(order_genius_routes.repo, "get_latest_baseline", lambda *_: None)

    def create_baseline_version(*, published_by: str, **_kwargs: object) -> SimpleNamespace:
        baseline_publishers.append(published_by)
        return SimpleNamespace(baseline_version_id=baseline_id)

    def resolve_colour_attributes(
        _session: object,
        brand: str,
        colour_code: str,
        *,
        colour_name: str,
        colour_hex: str | None,
        colour_hex_supplied: bool,
    ) -> dict:
        reusable_colour_args.update(
            brand=brand,
            colour_code=colour_code,
            colour_name=colour_name,
            colour_hex=colour_hex,
            colour_hex_supplied=colour_hex_supplied,
        )
        return {"colourName": colour_name, "colourHex": "#FFFFFF"}

    monkeypatch.setattr(
        order_genius_routes.repo,
        "create_baseline_version",
        create_baseline_version,
    )
    monkeypatch.setattr(
        order_genius_routes.repo,
        "resolve_colour_attributes",
        resolve_colour_attributes,
    )
    monkeypatch.setattr(
        order_genius_routes.repo,
        "copy_country_material_finance_template",
        lambda *_args, **_kwargs: 0,
    )

    result = order_genius_routes.create_material_sku(
        {
            "materialCode": "T7000Z5BWMY0026",
            "brand": "JEACOO",
            "modelName": "JEACOO5 HEV",
            "version": "Exclusive-FWD",
            "colour": "Khaki white",
            "colourCode": "bw",
        },
        session=session,
        user=SimpleNamespace(name="admin@example.com"),
    )

    created_sku = session.added[0]
    assert created_sku.brand == "JAECOO"
    assert created_sku.model_name == "JAECOO5 HEV"
    assert created_sku.baseline_version_id == baseline_id
    assert created_sku.colour_hex == "#FFFFFF"
    assert reusable_colour_args == {
        "brand": "JAECOO",
        "colour_code": "BW",
        "colour_name": "Khaki white",
        "colour_hex": None,
        "colour_hex_supplied": False,
    }
    assert baseline_publishers == ["admin@example.com"]
    assert session.flushed is True
    assert session.committed is True
    assert session.rolled_back is False
    assert result["materialCode"] == "T7000Z5BWMY0026"


def test_create_material_sku_rejects_duplicate_material_code(monkeypatch) -> None:
    session = _CreateMaterialSession()
    monkeypatch.setattr(
        order_genius_routes.repo,
        "get_sku_by_material_code_any_status",
        lambda *_: SimpleNamespace(material_code="T7000Z5BWMY0026"),
    )

    with pytest.raises(HTTPException) as exc_info:
        order_genius_routes.create_material_sku(
            {
                "materialCode": "T7000Z5BWMY0026",
                "brand": "JAECOO",
                "modelName": "JAECOO5 HEV",
                "version": "Exclusive-FWD",
                "colour": "Khaki white",
                "colourCode": "BW",
            },
            session=session,
            user=SimpleNamespace(name="admin@example.com"),
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Material code already exists: T7000Z5BWMY0026"
    assert session.added == []
    assert session.committed is False


def test_create_material_sku_initializes_automatic_fobs_from_source(monkeypatch) -> None:
    session = _CreateMaterialSession()
    baseline_id = uuid4()
    calls: dict[str, object] = {}
    monkeypatch.setattr(order_genius_routes.repo, "get_sku_by_material_code_any_status", lambda *_: None)
    monkeypatch.setattr(order_genius_routes.repo, "get_latest_baseline", lambda *_: SimpleNamespace(baseline_version_id=baseline_id))
    monkeypatch.setattr(
        order_genius_routes.repo,
        "resolve_colour_attributes",
        lambda *_args, **_kwargs: {"colourName": "Matte silver", "colourHex": "#AAAAAA"},
    )
    monkeypatch.setattr(order_genius_routes.repo, "copy_country_material_finance_template", lambda *_args, **_kwargs: 0)

    def initialize(_session: object, target: str, source: str, **kwargs: object) -> dict[str, object]:
        calls.update(target=target, source=source, kwargs=kwargs)
        return {"sourceMaterialCode": source, "materialCode": target, "rows": 1, "created": 1, "skippedNoBase": 0, "details": []}

    monkeypatch.setattr(order_genius_routes.repo, "initialize_sku_fobs_from_source", initialize)
    result = order_genius_routes.create_material_sku(
        {
            "materialCode": "T7000UEMY0001",
            "brand": "OMODA",
            "modelName": "OMODA7",
            "version": "Premium-FWD",
            "colour": "Matte silver",
            "colourCode": "UE",
            "colourTier": "special",
            "sourceMaterialCode": "T7000BWMY0001",
            "automaticFobs": True,
        },
        session=session,
        user=SimpleNamespace(name="admin@example.com"),
    )

    assert calls["target"] == "T7000UEMY0001"
    assert calls["source"] == "T7000BWMY0001"
    assert result["automaticFobs"]["created"] == 1
    assert session.committed is True


def test_initialize_new_dual_uses_single_base_and_resolved_surcharge(monkeypatch) -> None:
    target = SimpleNamespace(
        material_code="TARGET-ZE",
        colour_tier="dual",
        bom_template="T7000**MY0001",
        brand="OMODA",
        model_name="OMODA7 SHS",
        powertrain="PHEV",
        exterior_color_code="ZE",
    )
    source = SimpleNamespace(material_code="SOURCE-BW")
    source_fob = SimpleNamespace(
        baseline_version_id=uuid4(),
        country_code="SE",
        payment_term_code="TT",
        final_fob_eur=15000,
        remark="copied note",
    )
    session = _FakeSession([source_fob])
    rows = iter([target, source])
    monkeypatch.setattr(repo, "get_sku_by_material_code_any_status", lambda *_: next(rows))
    monkeypatch.setattr(repo, "_find_colour_surcharge_base_fob", lambda *_args: 15000.0)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_args: 200.0)

    result = repo.initialize_sku_fobs_from_source(
        session,
        target.material_code,
        source.material_code,
    )

    created = session.added[0]
    assert result["created"] == 1
    assert result["skippedNoBase"] == 0
    assert created.base_fob_eur == 15000.0
    assert created.colour_surcharge_eur == 200.0
    assert created.final_fob_eur == 15200.0
    assert created.fob_source_mode == "uploaded_base_plus_colour"
    assert created.material_code == target.material_code


def test_special_colour_rule_precedes_brand_special_default(monkeypatch) -> None:
    sku = SimpleNamespace(
        brand="OMODA",
        model_name="OMODA7",
        bom_template="T7000",
        exterior_color_code="UE",
    )
    special_rule = SimpleNamespace(surcharge_eur=200)
    brand_rule = SimpleNamespace(surcharge_eur=300)
    monkeypatch.setattr(repo, "get_special_colour_surcharge_for_sku", lambda *_: special_rule)
    monkeypatch.setattr(repo, "get_brand_colour_surcharge", lambda *_: brand_rule)

    assert repo.get_colour_surcharge_amount_for_sku(_FakeSession(), sku, "special") == 200

    monkeypatch.setattr(repo, "get_special_colour_surcharge_for_sku", lambda *_: None)
    assert repo.get_colour_surcharge_amount_for_sku(_FakeSession(), sku, "special") == 300


def test_new_dual_base_write_derives_final_from_shared_rule(monkeypatch) -> None:
    sku = SimpleNamespace(
        material_code="T7000ZEMY0001",
        brand="OMODA",
        model_name="OMODA7 SHS",
        bom_template="T7000**MY0001",
        exterior_color_code="ZE",
        colour_tier="dual",
        exterior_color_type="dual",
    )
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=uuid4(),
        country_code="SE",
        material_code=sku.material_code,
        payment_term_code="TT",
        uploaded_fob_eur=15000,
        final_fob_eur=15200,
        fob_source_mode="uploaded_base_plus_colour",
        is_active=True,
    )
    monkeypatch.setattr(repo, "get_sku_by_material_code_any_status", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 200.0)

    result = repo.update_sku_fob_for_country(
        _FakeSession([row]), sku.material_code, "SE", 15500, "TT"
    )

    assert result is row
    assert row.base_fob_eur == 15500
    assert row.colour_surcharge_eur == 200
    assert row.final_fob_eur == 15700


def test_missing_tier_and_missing_rule_never_become_zero(monkeypatch) -> None:
    sku = SimpleNamespace(
        brand="OMODA",
        model_name="OMODA7 SHS",
        bom_template="T7000**MY0001",
        exterior_color_code="ZE",
        colour_tier=None,
        exterior_color_type=None,
    )
    assert repo.resolve_colour_surcharge_for_sku(_FakeSession(), sku, None)["status"] == "missing_tier"
    monkeypatch.setattr(repo, "get_special_colour_surcharge_for_sku", lambda *_: None)
    monkeypatch.setattr(repo, "get_brand_colour_surcharge", lambda *_: None)
    assert repo.resolve_colour_surcharge_for_sku(_FakeSession(), sku, "dual")["status"] == "missing_rule"


def test_colour_override_is_bound_to_the_requested_tier(monkeypatch) -> None:
    sku = SimpleNamespace(
        brand="OMODA",
        model_name="OMODA9 SHS",
        bom_template="T6480J1**LX0017",
        exterior_color_code="UE",
    )
    special_rule = SimpleNamespace(surcharge_eur=300)
    dual_rule = SimpleNamespace(surcharge_eur=250)
    defaults = {
        "dual": SimpleNamespace(surcharge_eur=200),
        "special": SimpleNamespace(surcharge_eur=200),
    }
    monkeypatch.setattr(
        repo,
        "get_special_colour_surcharge_for_sku",
        lambda _session, _sku, tier="special": (
            dual_rule if tier == "dual" else special_rule
        ),
    )
    monkeypatch.setattr(
        repo,
        "get_brand_colour_surcharge",
        lambda _session, _brand, tier: defaults[tier],
    )

    assert repo.get_colour_surcharge_amount_for_sku(_FakeSession(), sku, "special") == 300
    assert repo.get_colour_surcharge_amount_for_sku(_FakeSession(), sku, "dual") == 250


def test_upsert_special_colour_surcharge_normalizes_tier() -> None:
    rule = repo.upsert_special_colour_surcharge(
        _FakeSession(),
        "omoda",
        "UE",
        300,
        model_name="OMODA9 SHS",
        colour_tier="DUAL",
    )

    assert rule.brand == "OMODA"
    assert rule.colour_tier == "dual"
    assert rule.surcharge_eur == 300


def test_uploaded_base_plus_colour_uses_unified_special_rule(monkeypatch) -> None:
    sku = SimpleNamespace(
        material_code="T7000UEMY0001",
        brand="OMODA",
        model_name="OMODA7",
        exterior_color_type="special",
        colour_tier="special",
        raw_payload_json={"country_fobs": {"SE": 15000}},
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "get_country_payment_term",
        lambda *_args, **_kwargs: SimpleNamespace(payment_term_code="TT", country_name="SE"),
    )
    monkeypatch.setattr(order_genius_service.repo, "get_colour_surcharge_amount_for_sku", lambda *_args: 200.0)
    monkeypatch.setattr(order_genius_service.repo, "upsert_fob_resolved", lambda _session, fob: fob)

    resolved = order_genius_service._resolve_fob_for_sku(
        _FakeSession(),
        "SE",
        sku,
        uuid4(),
        "uploaded_base_plus_colour",
    )

    assert resolved.base_fob_eur == 15000
    assert resolved.colour_surcharge_eur == 200
    assert resolved.final_fob_eur == 15200

    final = order_genius_service._resolve_fob_for_sku(
        _FakeSession(),
        "SE",
        sku,
        uuid4(),
        "uploaded_final_fob",
    )
    assert final.base_fob_eur is None
    assert final.colour_surcharge_eur is None
    assert final.final_fob_eur == 15000


def test_template_base_save_derives_colours_and_keeps_base_after_single_moves(monkeypatch) -> None:
    baseline_id = uuid4()
    single = SimpleNamespace(
        material_code="T7000BWMY0001",
        bom_template="T7000**MY0001",
        colour_tier="single",
        exterior_color_type="single",
        is_active=True,
    )
    dual = SimpleNamespace(
        material_code="T7000ZEMY0001",
        bom_template="T7000**MY0001",
        colour_tier="dual",
        exterior_color_type="dual",
        is_active=True,
    )
    single_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(), baseline_version_id=baseline_id,
        country_code="SE", material_code=single.material_code, payment_term_code="TT",
        final_fob_eur=15000, is_active=True,
    )
    dual_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(), baseline_version_id=baseline_id,
        country_code="SE", material_code=dual.material_code, payment_term_code="TT",
        final_fob_eur=15200, is_active=True,
    )
    session = _QueuedExecuteSession([[single, dual], [single_fob, dual_fob]])
    monkeypatch.setattr(repo, "get_country_payment_term", lambda *_: SimpleNamespace(payment_term_code="TT"))
    monkeypatch.setattr(repo, "get_latest_baseline", lambda *_: SimpleNamespace(baseline_version_id=baseline_id))
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda _session, sku, tier: 200.0 if tier == "dual" else 0.0)

    result = repo.update_bom_template_base_fob(
        session,
        "T7000**MY0001",
        [single.material_code, dual.material_code],
        "SE",
        15500,
        changed_by="admin",
    )

    assert result["updated"] == 2
    assert single_fob.base_fob_eur == 15500
    assert single_fob.final_fob_eur == 15500
    assert dual_fob.base_fob_eur == 15500
    assert dual_fob.colour_surcharge_eur == 200
    assert dual_fob.final_fob_eur == 15700
    assert single_fob.fob_source_mode == "template_base"

    session = _QueuedExecuteSession([[dual], [dual_fob]])
    result = repo.update_bom_template_base_fob(
        session,
        "T7000**MY0001",
        [dual.material_code],
        "SE",
        16000,
        changed_by="admin",
    )
    assert result["updated"] == 1
    assert dual_fob.base_fob_eur == 16000
    assert dual_fob.final_fob_eur == 16200


@pytest.mark.parametrize("scenario", ["known", "unknown", "conflict"])
def test_create_material_sku_resolves_known_unknown_and_conflict_rules(
    monkeypatch,
    scenario: str,
) -> None:
    def rule_row(code: str, name: str, colour_hex: str) -> SimpleNamespace:
        return SimpleNamespace(
            material_code=f"RULE-{name}",
            brand="OMODA",
            exterior_color_code=code,
            exterior_color_name=name,
            colour_hex=colour_hex,
        )

    rows = {
        "known": [rule_row("W3", "water blue", "#B6D3FB")],
        "unknown": [],
        "conflict": [
            rule_row("ZF", "Red black", "#19191A|#8B0000"),
            rule_row("ZF", "Ruby black", "#1A1A1A|#8B0000"),
        ],
    }[scenario]
    code, supplied_name = {
        "known": ("W3", "W3"),
        "unknown": ("XY", "Ocean blue"),
        "conflict": ("ZF", "ZF"),
    }[scenario]
    body = {
        "materialCode": f"NEW-{scenario}",
        "brand": "OMODA",
        "modelName": "OMODA5",
        "version": "Premium",
        "colour": supplied_name,
        "colourCode": code,
    }
    if scenario == "unknown":
        body["colourHex"] = "#123456"
    session = _CreateMaterialSession()
    session.execute_values = rows
    monkeypatch.setattr(order_genius_routes.repo, "get_sku_by_material_code_any_status", lambda *_: None)
    monkeypatch.setattr(
        order_genius_routes.repo,
        "get_latest_baseline",
        lambda *_: SimpleNamespace(baseline_version_id=uuid4()),
    )
    monkeypatch.setattr(order_genius_routes.repo, "copy_country_material_finance_template", lambda *_a, **_k: 0)

    result = order_genius_routes.create_material_sku(
        body,
        session=session,
        user=SimpleNamespace(name="admin"),
    )
    created = session.added[0]

    expected = {
        "known": ("water blue", "#B6D3FB"),
        "unknown": ("Ocean blue", "#123456"),
        "conflict": ("ZF", None),
    }[scenario]
    assert (created.exterior_color_name, created.colour_hex) == expected
    assert (result["colourName"], result["colourHex"]) == expected


def test_infer_colour_tier_handles_dual_swatch_and_special_finish() -> None:
    assert infer_colour_tier("Carbon black / khaki white") == "dual"
    assert infer_colour_tier("Carbon black + grey roof") == "dual"
    assert infer_colour_tier("Aviation silver", colour_hex="#C8C0B8|#111111") == "dual"
    assert infer_colour_tier("Matte black (Black Edition)") == "special"


def test_effective_colour_tier_legacy_fallback_ignores_fillable_name_and_swatch() -> None:
    sku = SimpleNamespace(
        colour_tier=None,
        exterior_color_type="single",
        exterior_color_name="Matte black",
        colour_hex="#111111|#FFFFFF",
    )

    assert order_genius_service._effective_colour_tier(sku) == "single"


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
    sku.colour_hex = "#F0ECE0"
    historical = _legacy_jaecoo_sku()
    historical.material_code = "T7000Z5ZEMY0025"
    historical.exterior_color_code = "ZE"
    historical.colour_tier = "dual"
    historical.colour_hex = "#1A1A1A|#F0ECE0"
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
            code: fob for code in material_codes
        },
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_quantities_for_country_year",
        lambda *_args, **_kwargs: [SimpleNamespace(
            material_code=historical.material_code,
            order_month=1,
            quantity=2,
            row_version=1,
        )],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "list_historical_skus_with_quantity",
        lambda *_args, **_kwargs: [historical.material_code],
    )
    monkeypatch.setattr(
        order_genius_service.repo,
        "get_skus_by_material_codes_any_status",
        lambda *_args, **_kwargs: {historical.material_code: historical},
    )

    result = order_genius_service.build_matrix(
        _FakeSession(),
        "SK",
        2026,
        brand="JAECOO",
        model_name="JAECOO5 HEV",
        powertrain="HEV",
    )

    assert result["totalRows"] == 2
    rows = {row["materialCode"]: row for row in result["rows"]}
    row = rows[sku.material_code]
    assert row["brand"] == "JAECOO"
    assert row["modelName"] == "JAECOO5 HEV"
    assert row["powertrain"] == "HEV"
    assert row["fobEur"] == 15300
    assert row["colourCode"] == "BW"
    assert row["colourTier"] == "single"
    assert row["colourHex"] == "#F0ECE0"
    historical_row = rows[historical.material_code]
    assert historical_row["lifecycleStatus"] == "historical"
    assert historical_row["colourCode"] == "ZE"
    assert historical_row["colourTier"] == "dual"
    assert historical_row["colourHex"] == "#1A1A1A|#F0ECE0"


def test_build_matrix_backfills_interior_and_preserves_paint_tier(monkeypatch) -> None:
    blank = _legacy_jaecoo_sku()
    blank.material_code = "T7000Z5CPMY0026"
    blank.exterior_color_name = "Matte black (Black Edition)"
    blank.exterior_color_code = "CP"
    blank.exterior_color_type = "single"
    blank.colour_tier = "single"
    blank.colour_hex = "#19191A|#8B0000"
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
    assert by_code["T7000Z5CPMY0026"]["colourTier"] == "single"
    assert by_code["T7000Z5CPMY0026"]["colourHex"] == "#19191A|#8B0000"


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


def test_colour_rules_group_by_normalized_brand_code_and_fill_placeholder() -> None:
    skus = [
        SimpleNamespace(
            material_code="A",
            brand="JEACOO",
            exterior_color_code="w3",
            exterior_color_name="water blue",
            colour_hex="#b6d3fb",
        ),
        SimpleNamespace(
            material_code="B",
            brand="JAECOO",
            exterior_color_code="W3",
            exterior_color_name="W3",
            colour_hex=None,
        ),
    ]

    rule = repo.build_colour_hex_rules_from_skus(skus)[0]
    assert rule["brand"] == "JAECOO"
    assert rule["colourCode"] == "W3"
    assert rule["status"] == "fillable"
    assert rule["standardColourName"] == "water blue"
    assert rule["standardColourHex"] == "#B6D3FB"
    assert rule["placeholderNameSkuCount"] == 1
    assert rule["previewChanges"] == [
        {
            "materialCode": "B",
            "brand": "JAECOO",
            "colourCode": "W3",
            "oldColourName": "W3",
            "newColourName": "water blue",
            "oldColourHex": None,
            "newColourHex": "#B6D3FB",
        }
    ]


def test_colour_rules_include_known_brand_when_legacy_brand_is_empty() -> None:
    rule = repo.build_colour_hex_rules_from_skus([
        SimpleNamespace(
            material_code="J5-ICE-W3",
            brand="",
            model_name="JAECOO5 ICE",
            bom_template="T71611C",
            exterior_color_code="W3",
            exterior_color_name="Water blue",
            colour_hex="#B6D3FB",
        )
    ])[0]

    assert rule["brand"] == "JAECOO"
    assert rule["colourCode"] == "W3"
    assert rule["status"] == "complete"


def test_colour_rules_report_name_and_swatch_conflicts_independently() -> None:
    skus = [
        SimpleNamespace(
            material_code="A",
            brand="OMODA",
            exterior_color_code="ZF",
            exterior_color_name="Red black",
            colour_hex="#19191A|#8B0000",
        ),
        SimpleNamespace(
            material_code="B",
            brand="OMODA",
            exterior_color_code="ZF",
            exterior_color_name="Ruby black",
            colour_hex="#1A1A1A|#8B0000",
        ),
    ]

    rule = repo.build_colour_hex_rules_from_skus(skus)[0]
    summary = repo.summarize_colour_hex_rules([rule])

    assert rule["status"] == "name_conflict"
    assert rule["hasNameConflict"] is True
    assert rule["hasSwatchConflict"] is True
    assert summary["nameConflict"] == 1
    assert summary["swatchConflict"] == 1
    assert rule["previewChanges"] == []


def test_colour_rules_report_unknown_placeholder_as_missing() -> None:
    rule = repo.build_colour_hex_rules_from_skus([
        SimpleNamespace(
            material_code="A",
            brand="OMODA",
            exterior_color_code="XY",
            exterior_color_name="XY",
            colour_hex=None,
        )
    ])[0]

    assert rule["status"] == "missing"
    assert rule["standardColourName"] is None
    assert rule["standardColourHex"] is None
    assert rule["placeholderNameSkuCount"] == 1
    assert rule["missingSwatchSkuCount"] == 1


def test_summarize_invalid_colour_rule_identities_exposes_sample_without_mutation() -> None:
    session = _FakeSession([
        SimpleNamespace(
            material_code="J5-ICE-W3",
            brand="",
            model_name="JAECOO5 ICE",
            bom_template="T71611C",
            exterior_color_code="W3",
            is_active=True,
        ),
        SimpleNamespace(
            material_code="UNKNOWN-1",
            brand="",
            model_name="J5 ICE",
            bom_template="T71611C",
            exterior_color_code="W3",
            is_active=True,
        ),
    ])

    assert repo.summarize_invalid_colour_rule_identities(session) == {
        "invalidIdentitySkuCount": 1,
        "invalidIdentitySampleMaterialCodes": ["UNKNOWN-1"],
    }


def test_resolve_colour_attributes_reuses_only_unambiguous_rule() -> None:
    standard = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="JAECOO",
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#F0ECE0",
        ),
    ])

    result = repo.resolve_colour_attributes(
        standard, "JEACOO", "bw", colour_name="BW",
    )

    assert result["colourName"] == "Khaki White"
    assert result["colourHex"] == "#F0ECE0"
    assert result["source"] == "brand_code_rule"


def test_lookup_colour_rule_uses_unique_name_alias_after_code_miss(monkeypatch) -> None:
    session = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="JAECOO",
            model_name="JAECOO5 ICE",
            bom_template="T71611C",
            is_active=True,
            exterior_color_code="BW",
            exterior_color_name="Khaki White (BW)",
            colour_hex="#F0ECE0",
        ),
    ])
    monkeypatch.setattr(repo, "_list_colour_rule_candidate_skus", lambda *_args: [])

    result = repo.lookup_colour_rule(
        session,
        "JEACOO",
        "ZZ",
        colour_name="khaki white",
    )

    assert result["source"] == "name_candidate"
    assert result["colourCode"] == "ZZ"
    assert result["colourName"] == "Khaki White (BW)"
    assert result["colourHex"] == "#F0ECE0"
    assert len(result["nameCandidates"]) == 1


def test_lookup_colour_rule_keeps_multiple_name_candidates_for_user_choice(monkeypatch) -> None:
    session = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="OMODA",
            model_name="OMODA5 EV",
            bom_template="T7000",
            is_active=True,
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#F0ECE0",
        ),
        SimpleNamespace(
            material_code="B",
            brand="OMODA",
            model_name="OMODA9 SHS",
            bom_template="T7001",
            is_active=True,
            exterior_color_code="KW",
            exterior_color_name="Khaki White",
            colour_hex="#F5F0E8",
        ),
    ])
    monkeypatch.setattr(repo, "_list_colour_rule_candidate_skus", lambda *_args: [])

    result = repo.lookup_colour_rule(
        session,
        "OMODA",
        "ZZ",
        colour_name="Khaki White",
    )

    assert result["source"] == "name_candidates"
    assert result["colourHex"] is None
    assert [item["colourCode"] for item in result["nameCandidates"]] == ["BW", "KW"]


def test_resolve_colour_attributes_canonicalizes_unique_name_alias(monkeypatch) -> None:
    session = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="JAECOO",
            model_name="JAECOO5 ICE",
            bom_template="T71611C",
            is_active=True,
            exterior_color_code="BW",
            exterior_color_name="Khaki White",
            colour_hex="#F0ECE0",
        ),
    ])
    monkeypatch.setattr(repo, "_list_colour_rule_candidate_skus", lambda *_args: [])

    result = repo.resolve_colour_attributes(
        session,
        "JAECOO",
        "ZZ",
        colour_name="khaki white",
    )

    assert result["colourName"] == "Khaki White"
    assert result["colourHex"] == "#F0ECE0"
    assert result["source"] == "name_candidate"


def test_resolve_colour_attributes_preserves_explicit_hex_clear() -> None:
    session = _FakeSession([
        SimpleNamespace(
            material_code="A",
            brand="OMODA",
            exterior_color_code="W3",
            exterior_color_name="water blue",
            colour_hex="#B6D3FB",
        ),
    ])

    result = repo.resolve_colour_attributes(
        session,
        "OMODA",
        "W3",
        colour_name="water blue",
        colour_hex=None,
        colour_hex_supplied=True,
    )

    assert result["colourHex"] is None


def test_set_standard_colour_hex_for_rule_resolves_whole_brand_code() -> None:
    matching = SimpleNamespace(
        material_code="A",
        brand="JAECOO",
        exterior_color_code="BW",
        exterior_color_name="Khaki White",
        colour_hex="#F0ECE0",
        colour_tier="single",
        is_published=True,
        final_fob_eur=14000,
        updated_at_utc=None,
    )
    same_code_other_name = SimpleNamespace(
        material_code="B",
        brand="JAECOO",
        exterior_color_code="BW",
        exterior_color_name="Carbon Black",
        colour_hex="#000000",
        colour_tier="dual",
        is_published=False,
        final_fob_eur=14300,
        updated_at_utc=None,
    )
    fake_session = _FakeSession([matching, same_code_other_name])

    result = repo.set_standard_colour_hex_for_rule(
        fake_session, "JAECOO", "BW", "khaki white", "#ffffff",
    )

    assert result["updated"] == 2
    assert result["materialCodes"] == ["A", "B"]
    assert matching.colour_hex == "#FFFFFF"
    assert matching.updated_at_utc is not None
    assert same_code_other_name.exterior_color_name == "khaki white"
    assert same_code_other_name.colour_hex == "#FFFFFF"
    assert (matching.colour_tier, matching.is_published, matching.final_fob_eur) == (
        "single", True, 14000,
    )
    assert (
        same_code_other_name.colour_tier,
        same_code_other_name.is_published,
        same_code_other_name.final_fob_eur,
    ) == ("dual", False, 14300)
    assert len(fake_session.added) == 1
    standard = fake_session.added[0]
    assert isinstance(standard, BrandColourSwatchRule)
    assert (standard.brand, standard.colour_code, standard.colour_hex) == (
        "JAECOO",
        "BW",
        "#FFFFFF",
    )


def test_persistent_colour_standard_is_authoritative_without_active_skus() -> None:
    standard = BrandColourSwatchRule(
        brand_colour_swatch_rule_id=uuid4(),
        brand="OMODA",
        colour_code="TE",
        colour_name="Matte gray",
        colour_hex="#8A8A8A",
        is_active=True,
    )
    session = _FakeSession([standard])

    result = repo.lookup_colour_rule(session, "OMODA", "TE")

    assert result == {
        "brand": "OMODA",
        "colourCode": "TE",
        "status": "complete",
        "colourName": "Matte gray",
        "colourHex": "#8A8A8A",
        "source": "persistent_rule",
        "hasNameConflict": False,
        "hasSwatchConflict": False,
        "nameCandidates": [],
    }


def test_shared_colour_display_resolves_same_standard_for_bom_and_matrix() -> None:
    standard = BrandColourSwatchRule(
        brand_colour_swatch_rule_id=uuid4(),
        brand="OMODA",
        colour_code="TE",
        colour_name="Matte gray",
        colour_hex="#8A8A8A",
        is_active=True,
    )
    sku = SimpleNamespace(
        brand="OMODA",
        model_name="OMODA9 SHS",
        bom_template="T7160**MH0001",
        exterior_color_code="TE",
        exterior_color_name="Matte gray",
        colour_hex=None,
    )

    assert repo.resolve_colour_display_values(
        sku,
        {("OMODA", "TE"): standard},
    ) == ("Matte gray", "#8A8A8A")


def test_preview_and_apply_colour_rule_fills_use_same_material_codes() -> None:
    donor = SimpleNamespace(
        material_code="A",
        brand="OMODA",
        exterior_color_code="W3",
        exterior_color_name="water blue",
        colour_hex="#B6D3FB",
        colour_tier="single",
        is_published=True,
        final_fob_eur=14000,
        updated_at_utc=None,
    )
    target = SimpleNamespace(
        material_code="B",
        brand="OMODA",
        exterior_color_code="W3",
        exterior_color_name="W3",
        colour_hex=None,
        colour_tier="dual",
        is_published=False,
        final_fob_eur=14200,
        updated_at_utc=None,
    )
    session = _FakeSession([donor, target])

    preview = repo.preview_colour_rule_fills(session)
    result = repo.apply_colour_rule_fills(
        session,
        [item["materialCode"] for item in preview["items"]],
        preview["fingerprint"],
    )

    assert result["materialCodes"] == ["B"]
    assert target.exterior_color_name == "water blue"
    assert target.colour_hex == "#B6D3FB"
    assert (target.colour_tier, target.is_published, target.final_fob_eur) == (
        "dual", False, 14200,
    )
    assert (donor.colour_tier, donor.is_published, donor.final_fob_eur) == (
        "single", True, 14000,
    )
    assert session.added == []


def test_preview_and_apply_exclude_inactive_skus() -> None:
    def sku(code: str, name: str, colour_hex: str | None, is_active: bool):
        return SimpleNamespace(
            material_code=code,
            brand="OMODA",
            exterior_color_code="W3",
            exterior_color_name=name,
            colour_hex=colour_hex,
            is_active=is_active,
            updated_at_utc=None,
        )

    donor = sku("A", "water blue", "#B6D3FB", True)
    target = sku("B", "W3", None, True)
    inactive = sku("C", "W3", None, False)

    class ActiveOnlySession(_FakeSession):
        def execute(self, stmt: object) -> _ExecuteResult:
            statement_sql = str(stmt).lower()
            assert (
                "material_sku_master.is_active = true" in statement_sql
                or "brand_colour_swatch_rule.is_active = true" in statement_sql
            )
            return _ExecuteResult([row for row in self.execute_values if row.is_active])

    session = ActiveOnlySession([donor, target, inactive])
    preview = repo.preview_colour_rule_fills(session)
    assert [item["materialCode"] for item in preview["items"]] == ["B"]

    repo.apply_colour_rule_fills(session, ["B"], preview["fingerprint"])

    assert target.exterior_color_name == "water blue"
    assert inactive.exterior_color_name == "W3"
    assert inactive.colour_hex is None


@pytest.mark.parametrize("stale_kind", ["source_changed", "missing_code", "extra_code"])
def test_apply_colour_rule_fills_rejects_stale_preview_without_writes(
    stale_kind: str,
) -> None:
    donor = SimpleNamespace(
        material_code="A",
        brand="OMODA",
        exterior_color_code="W3",
        exterior_color_name="water blue",
        colour_hex="#B6D3FB",
        updated_at_utc=None,
    )
    target = SimpleNamespace(
        material_code="B",
        brand="OMODA",
        exterior_color_code="W3",
        exterior_color_name="W3",
        colour_hex=None,
        updated_at_utc=None,
    )
    session = _FakeSession([donor, target])
    preview = repo.preview_colour_rule_fills(session)
    material_codes = [item["materialCode"] for item in preview["items"]]
    if stale_kind == "source_changed":
        donor.colour_hex = "#FFFFFF"
    elif stale_kind == "missing_code":
        material_codes = []
    else:
        material_codes.append("EXTRA")

    with pytest.raises(ValueError, match="preview is stale"):
        repo.apply_colour_rule_fills(
            session,
            material_codes,
            preview["fingerprint"],
        )

    assert target.exterior_color_name == "W3"
    assert target.colour_hex is None
    assert target.updated_at_utc is None
    assert session.added == []


def test_apply_colour_rule_route_maps_stale_preview_to_conflict(monkeypatch) -> None:
    session = _CreateMaterialSession()
    monkeypatch.setattr(
        order_genius_routes.repo,
        "apply_colour_rule_fills",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("stale")),
    )

    with pytest.raises(HTTPException) as exc_info:
        order_genius_routes.apply_colour_rule_fills(
            {"materialCodes": ["A"], "previewFingerprint": "old"},
            session=session,
        )

    assert exc_info.value.status_code == 409
    assert session.rolled_back is True
    assert session.committed is False


def test_patch_colour_code_saves_name_and_hex_without_rekey(monkeypatch) -> None:
    session = _CreateMaterialSession()
    sku = SimpleNamespace(
        material_code="T7000NHX4MY0002",
        brand="OMODA",
        exterior_color_code="X4",
        exterior_color_name="X4",
        colour_hex="#94A3B8",
        colour_code_confirmed=True,
        bom_template="T7000NH**MY0002",
    )
    monkeypatch.setattr(order_genius_routes.repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(
        order_genius_routes.repo,
        "update_bom_template_material_codes",
        lambda *_args, **_kwargs: pytest.fail("name/swatch edit must not rekey"),
    )

    result = order_genius_routes.patch_colour_code(
        sku.material_code,
        {
            "colourCode": "X4",
            "colourName": "Carbon crystal black / aviation silver",
            "colourHex": "#1A1A1A|#C8C0B8",
        },
        session=session,
    )

    assert result["materialCode"] == sku.material_code
    assert result["colourName"] == "Carbon crystal black / aviation silver"
    assert result["colourHex"] == "#1A1A1A|#C8C0B8"
    cleared = order_genius_routes.patch_colour_code(
        sku.material_code,
        {"colourCode": "X4", "colourName": result["colourName"], "colourHex": None},
        session=session,
    )
    assert cleared["colourHex"] is None
    assert sku.colour_hex is None
    assert session.committed is True


def test_patch_colour_code_changed_code_rekeys_and_saves_name_hex(monkeypatch) -> None:
    session = _CreateMaterialSession()
    sku = SimpleNamespace(
        material_code="T7000NHX4MY0002",
        brand="OMODA",
        exterior_color_code="X4",
        exterior_color_name="Old dual",
        colour_hex="#111111|#CCCCCC",
        colour_code_confirmed=True,
        bom_template="T7000NH**MY0002",
    )
    calls: list[tuple[list[str], str]] = []
    monkeypatch.setattr(order_genius_routes.repo, "get_sku_by_material_code", lambda *_: sku)

    def rekey(_session, material_codes: list[str], template: str) -> dict[str, str]:
        calls.append((material_codes, template))
        return {sku.material_code: "T7000NHZUMY0002"}

    monkeypatch.setattr(order_genius_routes.repo, "update_bom_template_material_codes", rekey)

    result = order_genius_routes.patch_colour_code(
        sku.material_code,
        {
            "colourCode": "ZU",
            "colourName": "Carbon black / aquatic green",
            "colourHex": "#1A1A1A|#1ABC9C",
        },
        session=session,
    )

    assert calls == [(["T7000NHX4MY0002"], "T7000NH**MY0002")]
    assert result["materialCode"] == "T7000NHZUMY0002"
    assert result["colourName"] == "Carbon black / aquatic green"
    assert result["colourHex"] == "#1A1A1A|#1ABC9C"


def test_patch_colour_code_rejects_unknown_code_without_name(monkeypatch) -> None:
    session = _CreateMaterialSession()
    sku = SimpleNamespace(
        material_code="T7000NHX4MY0002",
        brand="OMODA",
        exterior_color_code="X4",
        exterior_color_name="Old dual",
        colour_hex="#111111|#CCCCCC",
        colour_code_confirmed=True,
        bom_template="T7000NH**MY0002",
    )
    monkeypatch.setattr(order_genius_routes.repo, "get_sku_by_material_code", lambda *_: sku)

    with pytest.raises(HTTPException) as exc_info:
        order_genius_routes.patch_colour_code(
            sku.material_code,
            {"colourCode": "ZZ"},
            session=session,
        )

    assert exc_info.value.status_code == 400
    assert "requires a non-placeholder colourName" in str(exc_info.value.detail)
    assert sku.exterior_color_code == "X4"
    assert sku.exterior_color_name == "Old dual"
    assert sku.colour_hex == "#111111|#CCCCCC"
    assert session.committed is False


def test_colour_tier_reprice_reports_each_country_without_overwriting_manual(
    monkeypatch,
) -> None:
    baseline_id = uuid4()
    sku = SimpleNamespace(
        material_code="T7000NHX4MY0002",
        brand="OMODA",
        exterior_color_code="X4",
        colour_tier="dual",
    )

    def fob(country: str, final: float, source: str, base=None, surcharge=None):
        return CountrySkuFobResolved(
            country_sku_fob_id=uuid4(),
            baseline_version_id=baseline_id,
            country_code=country,
            material_code=sku.material_code,
            payment_term_code="LC90",
            uploaded_fob_eur=final,
            base_fob_eur=base,
            colour_surcharge_eur=surcharge,
            final_fob_eur=final,
            fob_source_mode=source,
            is_active=True,
        )

    manual = fob("NL", 1300, "manual_edit")
    manual_with_base = fob("SE", 1300, "manual_edit", 1000, 200)
    no_base = fob("FI", 1100, "uploaded_base_plus_colour")
    updated = fob("AT", 1000, "uploaded_base_plus_colour", 1000, None)
    unchanged = fob("CZ", 1200, "uploaded_base_plus_colour", 1000, 200)
    session = _FakeSession([manual, manual_with_base, no_base, updated, unchanged])
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 200.0)
    monkeypatch.setattr(
        repo,
        "_find_colour_surcharge_base_resolution",
        lambda _session, _sku, country, *_args: (
            {"status": "missing", "baseFobEur": None, "candidates": []}
            if country in {"NL", "FI"}
            else {"status": "resolved", "baseFobEur": 1000.0, "candidates": [1000.0]}
        ),
    )

    result = repo.reprice_sku_colour_surcharge_fobs(session, sku.material_code)
    details = {item["countryCode"]: item for item in result["details"]}

    assert result["updated"] == 2
    assert result["unchanged"] == 1
    assert result["skippedManual"] == 1
    assert result["skippedNoBase"] == 1
    assert details["NL"]["reason"] == "manual_fob"
    assert details["FI"]["reason"] == "missing_single_base"
    assert details["AT"]["newFinalFobEur"] == 1200
    assert details["AT"]["colourSurchargeEur"] == 200
    assert manual_with_base.final_fob_eur == 1200
    assert manual_with_base.uploaded_fob_eur == 1000
    assert manual_with_base.fob_source_mode == "template_base"
    assert details["CZ"]["status"] == "unchanged"
    assert manual.final_fob_eur == 1300


def test_colour_tier_reprice_uses_single_base_for_existing_dual_row(
    monkeypatch,
) -> None:
    baseline_id = uuid4()
    sku = SimpleNamespace(
        material_code="T7160RGZKMH0001",
        bom_template="T7160RG**MH0001",
        brand="OMODA",
        model_name="OMODA9 SHS",
        exterior_color_code="ZK",
        colour_tier="dual",
    )
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="CH",
        material_code=sku.material_code,
        payment_term_code="TT",
        uploaded_fob_eur=19350,
        base_fob_eur=20150,
        colour_surcharge_eur=300,
        final_fob_eur=19350,
        fob_source_mode="copied_from_country",
        fob_source_country_code="CZ",
        is_active=True,
    )
    session = _FakeSession([row])
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)
    monkeypatch.setattr(
        repo,
        "_find_colour_surcharge_base_resolution",
        lambda *_: {"status": "resolved", "baseFobEur": 19350.0, "candidates": [19350.0]},
    )

    result = repo.reprice_sku_colour_surcharge_fobs(session, sku.material_code)

    assert result["updated"] == 1
    assert row.base_fob_eur == 19350
    assert row.colour_surcharge_eur == 300
    assert row.final_fob_eur == 19650
    assert row.fob_source_mode == "copied_from_country"


def test_colour_tier_reprice_recalculates_template_base_without_freezing_it(
    monkeypatch,
) -> None:
    baseline_id = uuid4()
    sku = SimpleNamespace(
        material_code="T7000ZEMY0001",
        bom_template="T7000**MY0001",
        brand="OMODA",
        exterior_color_code="ZE",
        colour_tier="dual",
    )
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="SE",
        material_code=sku.material_code,
        payment_term_code="TT",
        uploaded_fob_eur=15500,
        base_fob_eur=15500,
        colour_surcharge_eur=200,
        final_fob_eur=15700,
        fob_source_mode="template_base",
        is_active=True,
    )
    session = _FakeSession([row])
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)
    monkeypatch.setattr(
        repo,
        "_find_colour_surcharge_base_resolution",
        lambda *_: {"status": "missing", "baseFobEur": None, "candidates": []},
    )

    result = repo.reprice_sku_colour_surcharge_fobs(session, sku.material_code, changed_by="admin")

    assert result["updated"] == 1
    assert row.base_fob_eur == 15500
    assert row.colour_surcharge_eur == 300
    assert row.final_fob_eur == 15800
    assert row.fob_source_mode == "template_base"


def test_colour_tier_reprice_uses_legacy_exterior_type_when_tier_is_blank(
    monkeypatch,
) -> None:
    sku = SimpleNamespace(
        material_code="T7160LEGACYDUAL",
        bom_template="T7160**MH0001",
        brand="JAECOO",
        exterior_color_code="ZK",
        colour_tier=None,
        exterior_color_type="dual",
    )
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=uuid4(),
        country_code="CH",
        material_code=sku.material_code,
        payment_term_code="TT",
        uploaded_fob_eur=19350,
        base_fob_eur=19350,
        final_fob_eur=19350,
        fob_source_mode="copied_from_country",
        is_active=True,
    )
    session = _FakeSession([row])
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)
    monkeypatch.setattr(
        repo,
        "_find_colour_surcharge_base_resolution",
        lambda *_: {"status": "resolved", "baseFobEur": 19350.0, "candidates": [19350.0]},
    )

    result = repo.reprice_sku_colour_surcharge_fobs(session, sku.material_code)

    assert result["updated"] == 1
    assert row.final_fob_eur == 19650
    assert row.colour_surcharge_eur == 300


def test_colour_tier_reprice_recalculates_manual_base_with_special_override(
    monkeypatch,
) -> None:
    baseline_id = uuid4()
    sku = SimpleNamespace(
        material_code="T6480J1UELX0017",
        bom_template="T6480J1**LX0017",
        brand="OMODA",
        model_name="OMODA9 SHS",
        exterior_color_code="UE",
        colour_tier="special",
    )
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="CH",
        material_code=sku.material_code,
        payment_term_code="TT",
        uploaded_fob_eur=25200,
        base_fob_eur=25200,
        colour_surcharge_eur=200,
        final_fob_eur=25400,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    session = _FakeSession([row])
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)
    monkeypatch.setattr(
        repo,
        "_find_colour_surcharge_base_resolution",
        lambda *_: {"status": "missing", "baseFobEur": None, "candidates": []},
    )

    result = repo.reprice_sku_colour_surcharge_fobs(session, sku.material_code)

    assert result["updated"] == 1
    assert result["skippedManual"] == 0
    assert row.base_fob_eur == 25200
    assert row.colour_surcharge_eur == 300
    assert row.final_fob_eur == 25500
    assert row.fob_source_mode == "template_base"


def test_colour_surcharge_reprice_audit_classifies_generic_rows_without_writing(monkeypatch) -> None:
    dual = SimpleNamespace(
        material_code="T7160RGZKMH0001",
        brand="JAECOO",
        model_name="JAECOO7 SHS",
        version="Exclusive-FWD",
        powertrain="PHEV",
        bom_template="T7160RG**MH0001",
        exterior_color_code="ZK",
        exterior_color_name="Carbon crystal black&Olive gray",
        exterior_color_type="dual",
        colour_tier="dual",
    )
    explicit = SimpleNamespace(**{**dual.__dict__, "material_code": "T7160RGZNMH0001"})
    ambiguous = SimpleNamespace(**{**dual.__dict__, "material_code": "T7160RGZMMH0001"})

    def row(material_code: str, source: str, final: float, base=None, surcharge=None):
        return SimpleNamespace(
            material_code=material_code,
            country_code="CH",
            payment_term_code="TT",
            final_fob_eur=final,
            base_fob_eur=base,
            colour_surcharge_eur=surcharge,
            fob_source_mode=source,
        )

    rows = [
        row(dual.material_code, "copied_from_country", 19350),
        row(explicit.material_code, "explicit_price_by_payment_term", 19350),
        row(ambiguous.material_code, "copied_from_country", 19350),
    ]
    session = _QueuedExecuteSession([
        [dual, explicit, ambiguous],
        rows,
        [19350],
        [19350],
        [19350, 19400],
    ])
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)

    result = repo.audit_colour_surcharge_reprice(session)
    by_code = {item["materialCode"]: item for item in result["items"]}

    assert result["summary"] == {
        "rows": 3,
        "autoReprice": 2,
        "alreadyCorrect": 0,
        "missingBase": 0,
        "ambiguousBase": 1,
        "explicitFinal": 0,
        "missingTier": 0,
        "missingRule": 0,
        "notApplicable": 0,
    }
    assert by_code[dual.material_code]["expectedFinalFobEur"] == 19650
    assert by_code[dual.material_code]["category"] == "auto_reprice"
    assert by_code[explicit.material_code]["category"] == "auto_reprice"
    assert by_code[ambiguous.material_code]["category"] == "ambiguous_base"
    assert result["fingerprint"]
    assert session.added == []


def test_colour_surcharge_reprice_audit_uses_stored_manual_base_only_when_single_is_absent(
    monkeypatch,
) -> None:
    sku = SimpleNamespace(
        material_code="T6480J1UELX0017",
        brand="OMODA",
        model_name="OMODA9 SHS",
        version="Comfort-FWD",
        powertrain="PHEV",
        bom_template="T6480J1**LX0017",
        exterior_color_code="UE",
        exterior_color_name="Matte gray",
        exterior_color_type="special",
        colour_tier="special",
    )
    row = SimpleNamespace(
        material_code=sku.material_code,
        country_code="CH",
        payment_term_code="TT",
        final_fob_eur=25400,
        base_fob_eur=25200,
        colour_surcharge_eur=200,
        fob_source_mode="manual_edit",
    )
    session = _QueuedExecuteSession([[sku], [row], []])
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)

    result = repo.audit_colour_surcharge_reprice(session)

    assert result["summary"]["autoReprice"] == 1
    assert result["items"][0]["trustedSingleBaseFobEur"] == 25200
    assert result["items"][0]["expectedFinalFobEur"] == 25500


def test_colour_surcharge_reprice_audit_keeps_manual_row_ambiguous(
    monkeypatch,
) -> None:
    sku = SimpleNamespace(
        material_code="T6480J1UELX0017",
        brand="OMODA",
        model_name="OMODA9 SHS",
        bom_template="T6480J1**LX0017",
        exterior_color_code="UE",
        exterior_color_type="special",
        colour_tier="special",
    )
    row = SimpleNamespace(
        material_code=sku.material_code,
        country_code="CH",
        payment_term_code="TT",
        final_fob_eur=25400,
        base_fob_eur=25200,
        colour_surcharge_eur=200,
        fob_source_mode="manual_edit",
    )
    session = _QueuedExecuteSession([[sku], [row], [1000, 1200]])
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)

    result = repo.audit_colour_surcharge_reprice(session)

    assert result["summary"] == {
        "rows": 1,
        "autoReprice": 0,
        "alreadyCorrect": 0,
        "missingBase": 0,
        "ambiguousBase": 1,
        "explicitFinal": 0,
        "missingTier": 0,
        "missingRule": 0,
        "notApplicable": 0,
    }
    assert result["items"][0]["trustedSingleBaseFobEur"] is None
    assert result["items"][0]["reason"] == "multiple_single_bases_for_template_country"


def test_colour_surcharge_reprice_audit_keeps_explicit_final_without_base_separate(
    monkeypatch,
) -> None:
    sku = SimpleNamespace(
        material_code="T6480J1UELX0017",
        brand="OMODA",
        model_name="OMODA9 SHS",
        bom_template="T6480J1**LX0017",
        exterior_color_code="UE",
        exterior_color_type="special",
        colour_tier="special",
    )
    row = SimpleNamespace(
        material_code=sku.material_code,
        country_code="CH",
        payment_term_code="TT",
        final_fob_eur=25400,
        base_fob_eur=None,
        colour_surcharge_eur=None,
        fob_source_mode="uploaded_final_fob",
    )
    session = _QueuedExecuteSession([[sku], [row], []])
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)

    result = repo.audit_colour_surcharge_reprice(session)

    assert result["summary"]["explicitFinal"] == 1
    assert result["summary"]["missingBase"] == 0
    assert result["items"][0]["reason"] == "explicit_final_without_single_base"


def test_colour_surcharge_reprice_writer_counts_ambiguous_without_writing(
    monkeypatch,
) -> None:
    sku = SimpleNamespace(
        material_code="T7160RGZKMH0001",
        bom_template="T7160RG**MH0001",
        brand="JAECOO",
        exterior_color_code="ZK",
        colour_tier="dual",
    )
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=uuid4(),
        country_code="CH",
        material_code=sku.material_code,
        payment_term_code="TT",
        uploaded_fob_eur=19350,
        base_fob_eur=19350,
        colour_surcharge_eur=300,
        final_fob_eur=19350,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    session = _FakeSession([row])
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_: sku)
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 300.0)
    monkeypatch.setattr(
        repo,
        "_find_colour_surcharge_base_resolution",
        lambda *_: {"status": "ambiguous", "baseFobEur": None, "candidates": [19350.0, 19400.0]},
    )

    result = repo.reprice_sku_colour_surcharge_fobs(session, sku.material_code)

    assert result["updated"] == 0
    assert result["skippedAmbiguous"] == 1
    assert result["skippedManual"] == 0
    assert row.final_fob_eur == 19350


def test_colour_surcharge_reprice_apply_deduplicates_payment_term_rows(
    monkeypatch,
) -> None:
    audit_items = [
        {
            "materialCode": "T7160RGZKMH0001",
            "countryCode": "CH",
            "paymentTermCode": "TT",
            "category": "auto_reprice",
        },
        {
            "materialCode": "T7160RGZKMH0001",
            "countryCode": "CH",
            "paymentTermCode": "LC90",
            "category": "auto_reprice",
        },
    ]
    monkeypatch.setattr(
        repo,
        "audit_colour_surcharge_reprice",
        lambda *_args, **_kwargs: {"fingerprint": "fp", "items": audit_items},
    )
    calls: list[dict[str, object]] = []

    def reprice(_session: object, material_code: str, **kwargs: object) -> dict[str, int]:
        calls.append({"materialCode": material_code, **kwargs})
        return {
            "updated": 2,
            "unchanged": 0,
            "skippedManual": 0,
            "skippedNoBase": 0,
            "skippedAmbiguous": 0,
        }

    monkeypatch.setattr(repo, "reprice_sku_colour_surcharge_fobs", reprice)

    result = repo.apply_colour_surcharge_reprice_audit(
        _FakeSession(), "fp", changed_by="admin"
    )

    assert calls == [
        {
            "materialCode": "T7160RGZKMH0001",
            "country_code": "CH",
            "changed_by": "admin",
        }
    ]
    assert result["totals"] == {
        "requested": 1,
        "updated": 2,
        "unchanged": 0,
        "skipped": 0,
    }


def test_copy_country_fobs_creates_target_country_rows(monkeypatch) -> None:
    baseline_id = uuid4()
    source_row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="CZ",
        material_code="T7000SE**MY0001",
        payment_term_code="LC90",
        uploaded_fob_eur=15000,
        base_fob_eur=15000,
        colour_surcharge_eur=200,
        final_fob_eur=15200,
        fob_source_mode="template_base",
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
    assert created.base_fob_eur == 15000
    assert created.colour_surcharge_eur == 200
    assert created.final_fob_eur == 15200
    assert created.fob_source_country_code == "CZ"


def test_copy_country_fobs_reprices_copied_dual_against_target_country(monkeypatch) -> None:
    baseline_id = uuid4()
    source_row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="CZ",
        material_code="T7160RGZKMH0001",
        payment_term_code="TT",
        uploaded_fob_eur=20450,
        base_fob_eur=20150,
        colour_surcharge_eur=300,
        final_fob_eur=20450,
        fob_source_mode="template_base",
        is_active=True,
    )
    dual = SimpleNamespace(
        material_code=source_row.material_code,
        colour_tier="dual",
        exterior_color_type="dual",
    )
    target_term = CountryPaymentTermMaster(
        country_payment_term_id=uuid4(),
        country_code="CH",
        country_name="Switzerland",
        payment_term_code="TT",
        payment_method="TT",
        lc_days=0,
        is_active=True,
    )
    fake_session = _FakeSession()
    repriced: list[dict[str, object]] = []
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
        lambda _session, country_code: target_term if country_code == "CH" else None,
    )
    monkeypatch.setattr(repo, "get_fob_for_country_sku", lambda *_args: None)
    monkeypatch.setattr(repo, "get_sku_by_material_code", lambda *_args: dual)

    def reprice(_session, material_code, **kwargs):
        repriced.append({"materialCode": material_code, **kwargs})
        return {"updated": 1}

    monkeypatch.setattr(repo, "reprice_sku_colour_surcharge_fobs", reprice)

    result = repo.copy_country_fobs(fake_session, "CZ", "CH")

    assert result["repriced"] == 1
    assert repriced == [{
        "materialCode": source_row.material_code,
        "country_code": "CH",
        "changed_by": "copy_country_fobs",
    }]


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
    monkeypatch.setattr(
        repo,
        "get_sku_by_material_code_any_status",
        lambda *_: SimpleNamespace(
            brand="OMODA",
            model_name="OMODA7 SHS",
            bom_template="T7000SE**MY0001",
            exterior_color_code="BW",
            colour_tier="single",
        ),
    )

    result = repo.adjust_country_fobs(fake_session, "SK", 200, changed_by="admin")

    assert result == {
        "countryCode": "SK",
        "deltaEur": 200,
        "rows": 1,
        "adjusted": 1,
        "skippedNegative": 0,
        "unchanged": 0,
        "skippedNoBase": 0,
        "skippedAmbiguous": 0,
        "skippedMissingTier": 0,
        "skippedMissingRule": 0,
    }
    assert row.final_fob_eur == 15100
    assert row.fob_source_mode == "template_base_country_adjust"
    assert row.updated_at_utc is not None
    history = fake_session.added[0]
    assert isinstance(history, FobResolvedHistory)
    assert history.country_code == "SK"
    assert history.material_code == "T7000SE**MY0001"
    assert history.old_final_fob_eur == 14900
    assert history.new_final_fob_eur == 15100
    assert history.changed_by == "admin"


def test_adjust_country_fobs_moves_template_base_and_preserves_surcharge(monkeypatch) -> None:
    baseline_id = uuid4()
    row = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="SE",
        material_code="T7000ZE**MY0001",
        payment_term_code="TT",
        uploaded_fob_eur=15500,
        base_fob_eur=15500,
        colour_surcharge_eur=200,
        final_fob_eur=15700,
        fob_source_mode="template_base",
        is_active=True,
    )
    fake_session = _FakeSession()
    monkeypatch.setattr(
        repo,
        "list_fob_by_country",
        lambda _session, country_code, payment_term_code=None: [row]
        if country_code == "SE"
        else [],
    )
    monkeypatch.setattr(
        repo,
        "get_sku_by_material_code_any_status",
        lambda *_: SimpleNamespace(
            brand="OMODA",
            model_name="OMODA7 SHS",
            bom_template="T7000**MY0001",
            exterior_color_code="ZE",
            colour_tier="dual",
        ),
    )
    monkeypatch.setattr(repo, "get_colour_surcharge_amount_for_sku", lambda *_: 200.0)

    result = repo.adjust_country_fobs(fake_session, "SE", 500, changed_by="admin")

    assert result["adjusted"] == 1
    assert row.base_fob_eur == 16000
    assert row.uploaded_fob_eur == 16000
    assert row.colour_surcharge_eur == 200
    assert row.final_fob_eur == 16200
    assert row.fob_source_mode == "template_base_country_adjust"
    history = fake_session.added[0]
    assert isinstance(history, FobResolvedHistory)
    assert history.new_uploaded_fob_eur == 16000
    assert history.new_final_fob_eur == 16200


def test_sync_missing_template_fobs_backfills_new_colour_rows_only() -> None:
    baseline_id = uuid4()
    template = "T6481QN**LX0002"
    base = SimpleNamespace(
        material_code="T6481QNBWLX0002",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Khaki white",
        exterior_color_code="BW",
        exterior_color_type="single",
        colour_tier="single",
        colour_hex=None,
        edition_tag=None,
    )
    cleared = SimpleNamespace(
        material_code="T6481QNKYLX0002",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Gray",
        exterior_color_code="KY",
        exterior_color_type="single",
        colour_tier="single",
        colour_hex=None,
        edition_tag=None,
    )
    dual = SimpleNamespace(
        material_code="T6481QNZELX0002",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Black & White",
        exterior_color_code="ZE",
        exterior_color_type="dual",
        colour_tier="dual",
        colour_hex="#111111|#FFFFFF",
        edition_tag=None,
    )
    special = SimpleNamespace(
        material_code="T6481QNUELX0002",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Matte gray",
        exterior_color_code="UE",
        exterior_color_type="special",
        colour_tier="special",
        colour_hex="#777777",
        edition_tag=None,
    )
    base_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="AT",
        material_code=base.material_code,
        payment_term_code="LC90",
        uploaded_fob_eur=28000,
        final_fob_eur=28000,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    cleared_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="AT",
        material_code=cleared.material_code,
        payment_term_code="LC90",
        uploaded_fob_eur=0,
        final_fob_eur=0,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    dual_rule = BrandColourSurchargeRule(
        colour_surcharge_rule_id=uuid4(),
        brand="JAECOO",
        colour_type="dual",
        surcharge_eur=300,
        is_active=True,
    )
    special_rule = BrandColourSurchargeRule(
        colour_surcharge_rule_id=uuid4(),
        brand="JAECOO",
        colour_type="special",
        surcharge_eur=300,
        is_active=True,
    )
    fake_session = _QueuedExecuteSession([
        [base, cleared, dual, special],
        [base_fob, cleared_fob],
        [dual_rule],
        [special_rule],
    ])

    result = repo.sync_missing_template_fobs(
        fake_session,
        bom_template=template,
        changed_by="admin",
    )

    assert result["created"] == 2
    assert result["skippedExisting"] == 1
    assert result["skippedCleared"] == 1
    created_by_code = {row.material_code: row for row in fake_session.added}
    assert created_by_code[dual.material_code].final_fob_eur == 28300
    assert created_by_code[dual.material_code].fob_source_mode == "derived_from_template_colour"
    assert created_by_code[special.material_code].final_fob_eur == 28300
    assert cleared.material_code not in created_by_code


def test_sync_template_fobs_can_reprice_existing_colour_surcharges() -> None:
    baseline_id = uuid4()
    template = "T6481QN**LX0004"
    base = SimpleNamespace(
        material_code="T6481QNBWLX0004",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Khaki white",
        exterior_color_code="BW",
        exterior_color_type="single",
        colour_tier="single",
        colour_hex=None,
        edition_tag=None,
    )
    dual = SimpleNamespace(
        material_code="T6481QNZELX0004",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Black & White",
        exterior_color_code="ZE",
        exterior_color_type="dual",
        colour_tier="dual",
        colour_hex="#111111|#FFFFFF",
        edition_tag=None,
    )
    special = SimpleNamespace(
        material_code="T6481QNUELX0004",
        bom_template=template,
        brand="JAECOO",
        exterior_color_name="Matte gray",
        exterior_color_code="UE",
        exterior_color_type="special",
        colour_tier="special",
        colour_hex="#777777",
        edition_tag=None,
    )
    base_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="AT",
        material_code=base.material_code,
        payment_term_code="LC90",
        final_fob_eur=28650,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    dual_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="AT",
        material_code=dual.material_code,
        payment_term_code="LC90",
        final_fob_eur=28650,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    special_fob = CountrySkuFobResolved(
        country_sku_fob_id=uuid4(),
        baseline_version_id=baseline_id,
        country_code="AT",
        material_code=special.material_code,
        payment_term_code="LC90",
        final_fob_eur=28650,
        fob_source_mode="manual_edit",
        is_active=True,
    )
    dual_rule = BrandColourSurchargeRule(
        colour_surcharge_rule_id=uuid4(),
        brand="JAECOO",
        colour_type="dual",
        surcharge_eur=300,
        is_active=True,
    )
    special_rule = BrandColourSurchargeRule(
        colour_surcharge_rule_id=uuid4(),
        brand="JAECOO",
        colour_type="special",
        surcharge_eur=300,
        is_active=True,
    )
    fake_session = _QueuedExecuteSession([
        [base, dual, special],
        [base_fob, dual_fob, special_fob],
        [dual_rule],
        [special_rule],
    ])

    result = repo.sync_missing_template_fobs(
        fake_session,
        bom_template=template,
        changed_by="admin",
        reprice_existing_colour_surcharges=True,
    )

    assert result["created"] == 0
    assert result["repriced"] == 2
    assert result["skippedExisting"] == 1
    assert dual_fob.final_fob_eur == 28950
    assert dual_fob.base_fob_eur == 28650
    assert dual_fob.colour_surcharge_eur == 300
    assert dual_fob.fob_source_mode == "colour_surcharge_repriced"
    assert special_fob.final_fob_eur == 28950
    histories = [row for row in fake_session.added if isinstance(row, FobResolvedHistory)]
    assert len(histories) == 2
    assert {history.material_code for history in histories} == {
        dual.material_code,
        special.material_code,
    }
