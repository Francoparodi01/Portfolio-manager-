from src.analysis.feature_snapshot import build_feature_snapshot_from_layers


def test_feature_snapshot_hash_is_stable_for_key_order_changes():
    first = build_feature_snapshot_from_layers(
        {
            "macro": {"weight": 0.3, "raw": 0.1},
            "technical": {"raw": 0.2, "weight": 0.3},
            "source": "execution_plan",
            "amount_ars": 100_000,
        }
    )
    second = build_feature_snapshot_from_layers(
        {
            "amount_ars": 999_999,
            "source": "execution_plan",
            "technical": {"weight": 0.3, "raw": 0.2},
            "macro": {"raw": 0.1, "weight": 0.3},
        }
    )

    assert first.feature_snapshot_id == second.feature_snapshot_id
    assert "amount_ars" not in first.payload
    assert "source" not in first.payload


def test_feature_snapshot_hash_changes_when_feature_changes():
    baseline = build_feature_snapshot_from_layers(
        {"technical": {"raw": 0.2, "weight": 0.3}}
    )
    changed = build_feature_snapshot_from_layers(
        {"technical": {"raw": 0.25, "weight": 0.3}}
    )

    assert baseline.feature_snapshot_id != changed.feature_snapshot_id
