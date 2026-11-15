from datetime import datetime, timezone

from src.collector.portfolio_quality import (
    PRICE_STATUS_FRESH,
    PRICE_STATUS_MISSING,
    PRICE_STATUS_STALE,
    enrich_positions_with_market_metadata,
    normalize_positions_with_fresh_market_prices,
)


def test_enrich_positions_uses_market_asset_type_and_marks_fresh():
    positions = [{"ticker": "PAMP", "asset_type": "CEDEAR", "current_price": 4890}]
    market_rows = [
        {
            "ticker": "PAMP",
            "asset_type": "ACCION",
            "last_price": 4905,
            "ts": datetime(2026, 5, 18, 20, 0, tzinfo=timezone.utc),
        }
    ]

    enriched = enrich_positions_with_market_metadata(positions, market_rows)

    assert enriched[0]["asset_type"] == "ACCION"
    assert enriched[0]["asset_type_source"] == "market_prices"
    assert enriched[0]["market_data_status"] == PRICE_STATUS_FRESH
    assert enriched[0]["is_operable"] is True


def test_enrich_positions_marks_old_quotes_as_stale():
    positions = [{"ticker": "DOW", "asset_type": "CEDEAR"}]
    market_rows = [
        {
            "ticker": "DOW",
            "asset_type": "CEDEAR",
            "last_price": 10500,
            "ts": datetime(2026, 5, 15, 20, 0, tzinfo=timezone.utc),
        },
        {
            "ticker": "NVDA",
            "asset_type": "CEDEAR",
            "last_price": 13710,
            "ts": datetime(2026, 5, 18, 20, 0, tzinfo=timezone.utc),
        },
    ]

    enriched = enrich_positions_with_market_metadata(positions, market_rows)

    assert enriched[0]["market_data_status"] == PRICE_STATUS_STALE
    assert enriched[0]["is_operable"] is False
    assert "desactualizado" in enriched[0]["market_data_reason"]


def test_enrich_positions_marks_missing_market_rows():
    enriched = enrich_positions_with_market_metadata(
        [{"ticker": "DOW", "asset_type": "CEDEAR"}],
        [],
    )

    assert enriched[0]["market_data_status"] == PRICE_STATUS_MISSING
    assert enriched[0]["is_operable"] is False


def test_normalize_rejects_previous_session_price_for_current_snapshot():
    positions = [
        {
            "ticker": "MU",
            "quantity": 1,
            "current_price": 288950,
            "market_value": 288950,
        }
    ]

    normalized = normalize_positions_with_fresh_market_prices(
        positions,
        [
            {
                "ticker": "MU",
                "last_price": 274125,
                "ts": datetime(2026, 8, 11, 20, 2, tzinfo=timezone.utc),
            }
        ],
        reference_at=datetime(2026, 8, 12, 13, 32, tzinfo=timezone.utc),
    )

    assert normalized[0]["market_data_status"] == PRICE_STATUS_STALE
    assert normalized[0]["price_normalized"] is False
    assert normalized[0]["current_price"] == 288950
    assert normalized[0]["market_value"] == 288950
