from src.collector.data.normalizer import ConfidenceResult


def _high_score_result(*, expected_positions, positions_parsed):
    checks = [
        ("rows_found", True, 2.0),
        ("parse_success", True, 3.0),
        ("min_positions", True, 2.0),
        ("prices_positive", True, 3.0),
    ]
    return ConfidenceResult.compute(
        checks,
        expected_positions=expected_positions,
        positions_parsed=positions_parsed,
    )


def test_scraper_rejects_partial_parse():
    result = _high_score_result(expected_positions=8, positions_parsed=3)

    assert result.score == 1.0
    assert result.parsed_ratio == 3 / 8
    assert result.is_acceptable(0.5) is False


def test_scraper_accepts_full_parse():
    result = _high_score_result(expected_positions=8, positions_parsed=8)

    assert result.score == 1.0
    assert result.parsed_ratio == 1.0
    assert result.is_acceptable(0.5) is True


def test_scraper_rejects_unknown_expected():
    result = _high_score_result(expected_positions=None, positions_parsed=8)

    assert result.parsed_ratio is None
    assert result.is_acceptable(0.5) is False


def test_confidence_score_does_not_override_ratio():
    result = _high_score_result(expected_positions=8, positions_parsed=7)

    assert result.score == 1.0
    assert result.parsed_ratio == 7 / 8
    assert result.is_acceptable(0.5) is False
