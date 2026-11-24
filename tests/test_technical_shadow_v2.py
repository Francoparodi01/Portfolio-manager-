from src.analysis.technical_shadow_v2 import build_technical_shadow_v2


def test_strong_uptrend_keeps_negative_reversion_as_extension_risk():
    result = build_technical_shadow_v2(
        regime="STRONG_UPTREND",
        trend_score=0.80,
        reversion_score=-0.80,
    )

    assert result.score == 0.68
    assert result.bias == "POSITIVE"
    assert result.rule == "trend_continuation"
    assert result.reversion_contribution == 0.0
    assert result.extension_risk == 0.8
    assert result.affects_analysis is False
    assert result.affects_execution is False


def test_range_prioritizes_reversion_signal():
    result = build_technical_shadow_v2(
        regime="RANGE",
        trend_score=0.20,
        reversion_score=0.80,
    )

    assert result.score == 0.65
    assert result.rule == "range_reversion"
    assert result.trend_contribution == 0.05
    assert result.reversion_contribution == 0.60


def test_downtrend_does_not_turn_oversold_reading_into_positive_bias():
    result = build_technical_shadow_v2(
        regime="DOWNTREND",
        trend_score=-0.70,
        reversion_score=0.90,
    )

    assert result.score == -0.595
    assert result.bias == "NEGATIVE"
    assert result.reversion_contribution == 0.0
    assert result.reversal_confirmation_required is True


def test_structural_break_blocks_positive_shadow_score():
    result = build_technical_shadow_v2(
        regime="TRANSITIONAL",
        trend_score=0.60,
        reversion_score=0.60,
        structural_break_confirmed=True,
    )

    assert result.score == 0.0
    assert result.bias == "NEUTRAL"
    assert result.structural_break_gate is True


def test_payload_declares_fixed_horizons_and_unvalidated_state():
    payload = build_technical_shadow_v2(
        regime="RANGE",
        trend_score=0.0,
        reversion_score=0.0,
    ).to_dict()

    assert payload["version"] == "technical-shadow-v2"
    assert payload["calibration_status"] == "UNVALIDATED"
    assert payload["evaluation_horizons"] == [5, 10, 20, 40]
    assert payload["affects_analysis"] is False
    assert payload["affects_execution"] is False
