from app.services.config_text_normalization import config_feature_semantic_keys


def test_product_evidence_semantic_keys_align_multilingual_high_confidence_features() -> None:
    equivalent_groups = {
        "feature.sunroof": [
            "Panoramic skylight / 全景天窗",
            "Panoramadach (Ausrüstungscode – 006)",
        ],
        "feature.display.head_up": [
            "HUD",
            "Head-up Display",
        ],
        "feature.tailgate.power": [
            "Induction electric tailgate (key induction) / 感应电动尾门（钥匙感应）",
            "Heckklappe elektrisch",
        ],
        "feature.seat.front_ventilation": [
            "Ventilated front seats (cushions + backrests) / 前排座椅通风（坐垫+靠背）",
            "Sitzbelüftung vorne",
        ],
        "feature.seat.driver_memory": [
            "Driver seat with memory function / 座椅记忆",
            "Fahrersitz mit Memory",
        ],
    }

    for expected_key, labels in equivalent_groups.items():
        for label in labels:
            assert expected_key in config_feature_semantic_keys(label)


def test_product_evidence_semantic_keys_do_not_merge_related_but_distinct_features() -> None:
    assert "feature.seat.driver_memory" not in config_feature_semantic_keys(
        "Exterior mirror memory / 外后视镜记忆"
    )
    assert "feature.tailgate.power" not in config_feature_semantic_keys("Manual tailgate")
    assert "feature.sunroof" not in config_feature_semantic_keys("Without panoramic roof")
