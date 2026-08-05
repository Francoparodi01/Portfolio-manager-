from datetime import date, datetime, timezone

from src.analysis.issuer_events import IssuerRegistryEntry, confidence_for, payload_hash
from src.collector.issuer_event_sources import (
    PortfolioInstrumentSeed,
    build_registry_from_portfolio,
    cnv_relevant_fact_observations,
    finnhub_earnings_observations,
    fmp_split_observations,
    parse_sec_company_directory,
    sec_submission_observations,
    yahoo_earnings_calendar_observations,
    yahoo_split_calendar_observations,
)
from src.collector.db import _json_payload


def _sec_entry() -> IssuerRegistryEntry:
    return IssuerRegistryEntry(
        issuer_id="SEC:0004962",
        issuer_name="American Express Company",
        source_market="US",
        primary_symbol="AXP",
        sec_cik="4962",
    ).normalized()


def test_sec_company_directory_supports_current_tabular_payload():
    parsed = parse_sec_company_directory(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[4962, "American Express Company", "AXP", "NYSE"]],
        }
    )

    assert parsed == {
        "AXP": {
            "cik": "0000004962",
            "name": "American Express Company",
            "exchange": "NYSE",
        }
    }


def test_registry_maps_cedear_to_us_issuer_and_local_instrument():
    entries, instruments = build_registry_from_portfolio(
        [PortfolioInstrumentSeed("AXP", "CEDEAR", "ARS")],
        sec_companies={
            "AXP": {"cik": "4962", "name": "American Express Company", "exchange": "NYSE"}
        },
    )

    assert entries[0].issuer_id == "SEC:0000004962"
    assert entries[0].source_market == "US"
    assert instruments[0].instrument_id == "BYMA:CEDEAR:AXP:ARS"
    assert instruments[0].source_ticker == "AXP"


def test_registry_keeps_known_local_issuer_as_argentina_source():
    entries, instruments = build_registry_from_portfolio(
        [PortfolioInstrumentSeed("YPFD", "ACCION", "ARS", issuer_hint="YPF")]
    )

    assert entries[0].issuer_id == "AR:YPF"
    assert entries[0].cnv_entity_name == "YPF"
    assert entries[0].metadata["issuer_symbol"] == "YPF"
    assert entries[0].metadata["local_symbol"] == "YPFD"
    assert instruments[0].instrument_id == "BYMA:ACCION:YPFD:ARS"


def test_yahoo_local_split_maps_to_argentina_instrument():
    entries, _ = build_registry_from_portfolio(
        [PortfolioInstrumentSeed("YPFD", "ACCION", "ARS", issuer_hint="YPF")]
    )

    observations = yahoo_split_calendar_observations(
        [
            {
                "Symbol": "YPFD.BA",
                "Company": "YPF Sociedad Anonima",
                "Payable On": "2026-08-03T04:00:00Z",
                "Old Share Worth": 1,
                "Share Worth": 10,
            }
        ],
        entries,
        today=date(2026, 8, 4),
    )

    assert len(observations) == 1
    observed = observations[0]
    assert observed.issuer_id == "AR:YPF"
    assert observed.ticker == "YPFD"
    assert observed.source == "YAHOO"
    assert observed.event_type == "SPLIT"
    assert observed.lifecycle_status == "DISCOVERED"
    assert observed.event_date == date(2026, 8, 3)
    assert observed.confidence == 0.75
    assert observed.actionable is False
    assert observed.raw_payload["event_scope"] == "local_instrument"
    assert observed.raw_payload["quantity_factor"] == 10.0


def test_yahoo_split_ratio_supports_reverse_splits():
    observations = yahoo_split_calendar_observations(
        [
            {
                "Symbol": "AXP",
                "Payable On": "2026-08-20T04:00:00Z",
                "Old Share Worth": 5,
                "Share Worth": 1,
            }
        ],
        [_sec_entry()],
        today=date(2026, 8, 4),
    )

    assert len(observations) == 1
    assert observations[0].event_type == "REVERSE_SPLIT"
    assert observations[0].lifecycle_status == "ANNOUNCED"
    assert observations[0].raw_payload["quantity_factor"] == 0.2


def test_yahoo_earnings_maps_issuer_symbol_and_market_timing():
    entries, _ = build_registry_from_portfolio(
        [PortfolioInstrumentSeed("YPFD", "ACCION", "ARS", issuer_hint="YPF")]
    )

    observations = yahoo_earnings_calendar_observations(
        [
            {
                "Symbol": "YPF",
                "Event Name": "Q2 2026 Earnings Call",
                "Event Start Date": "2026-08-10T20:00:00Z",
                "Timing": "AMC",
                "Fiscal Period End": "2026-06-30",
                "EPS Estimate": 2.0,
                "Reported EPS": None,
                "Surprise(%)": None,
            }
        ],
        entries,
    )

    assert len(observations) == 1
    observed = observations[0]
    assert observed.issuer_id == "AR:YPF"
    assert observed.ticker == "YPFD"
    assert observed.event_time_hint == "after_close"
    assert observed.fiscal_year == 2026
    assert observed.fiscal_quarter == 2
    assert observed.fiscal_period_end == date(2026, 6, 30)
    assert observed.raw_payload["event_scope"] == "issuer"
    assert observed.raw_payload["earnings_phase"] == "scheduled"
    assert observed.raw_payload["eps_estimate"] == 2.0
    assert observed.raw_payload["fiscal_period_end"] == "2026-06-30"


def test_yahoo_earnings_deduplicates_local_row_and_keeps_reported_result():
    observations = yahoo_earnings_calendar_observations(
        [
            {
                "Symbol": "AXP.BA",
                "Event Name": "Q3 Earnings",
                "Event Start Date": "2026-10-15T12:00:00Z",
                "Timing": "BMO",
                "EPS Estimate": 3.1,
            },
            {
                "Symbol": "AXP",
                "Event Name": "Q3 Earnings",
                "Event Start Date": "2026-10-15T12:00:00Z",
                "Timing": "BMO",
                "EPS Estimate": 3.1,
                "Reported EPS": 3.4,
                "Surprise(%)": 9.68,
            },
        ],
        [_sec_entry()],
    )

    assert len(observations) == 1
    observed = observations[0]
    assert observed.raw_payload["event_scope"] == "issuer"
    assert observed.raw_payload["earnings_phase"] == "post_reported"
    assert observed.raw_payload["reported_eps"] == 3.4
    assert observed.raw_payload["surprise_pct"] == 9.68


def test_sec_submissions_keep_only_relevant_forms_after_lookback():
    entry = _sec_entry()
    observations = sec_submission_observations(
        entry,
        {
            "filings": {
                "recent": {
                    "accessionNumber": ["0000004962-26-000001", "0000004962-25-000002", "0000004962-26-000003"],
                    "form": ["8-K", "10-Q", "3"],
                    "filingDate": ["2026-08-04", "2025-01-01", "2026-08-04"],
                    "acceptanceDateTime": ["2026-08-04T13:12:00.000Z", "2025-01-01T10:00:00.000Z", "2026-08-04T14:00:00.000Z"],
                    "primaryDocument": ["form8k.htm", "form10q.htm", "form3.htm"],
                    "reportDate": ["2026-08-04", "2024-12-31", ""],
                    "items": ["2.02", "", ""],
                }
            }
        },
        since=date(2026, 8, 1),
    )

    assert len(observations) == 1
    observed = observations[0]
    assert observed.observation_key == "SEC:0000004962:0000004962-26-000001"
    assert observed.event_type == "FILING"
    assert observed.confidence == 1.0
    assert observed.raw_payload["confidence_basis"] == "primary_official"
    assert observed.raw_payload["requires_structured_extraction"] is True
    assert observed.source_published_at == datetime(2026, 8, 4, 13, 12, tzinfo=timezone.utc)


def test_fmp_split_is_announcement_not_effective_corporate_action():
    observations = fmp_split_observations(
        [{"symbol": "AXP", "date": "2026-08-20", "numerator": 5, "denominator": 1}],
        {"AXP": _sec_entry()},
        today=date(2026, 8, 4),
    )

    assert len(observations) == 1
    observed = observations[0]
    assert observed.event_type == "SPLIT"
    assert observed.lifecycle_status == "ANNOUNCED"
    assert observed.actionable is False
    assert observed.raw_payload["quantity_factor"] == 5.0


def test_finnhub_earnings_preserves_market_time_hint():
    observations = finnhub_earnings_observations(
        {
            "earningsCalendar": [
                {"symbol": "AXP", "date": "2026-08-06", "hour": "amc", "year": 2026, "quarter": 3}
            ]
        },
        {"AXP": _sec_entry()},
    )

    assert len(observations) == 1
    assert observations[0].event_type == "EARNINGS"
    assert observations[0].event_time_hint == "after_close"
    assert observations[0].lifecycle_status == "ANNOUNCED"
    assert observations[0].fiscal_year == 2026
    assert observations[0].fiscal_quarter == 3


def test_cnv_parser_filters_issuer_and_classifies_split():
    html = """
    <table><tbody>
      <tr><td>04 ago. 2026 09:15</td><td>YPF S.A.</td>
          <td>INFORMACION RELEVANTE - DESDOBLAMIENTO DE ACCIONES</td>
          <td><a href='/SitioWeb/HechosRelevantes/3549001'>Documento</a></td></tr>
      <tr><td>04 ago. 2026 09:16</td><td>OTRA SOCIEDAD S.A.</td>
          <td>RESULTADOS</td><td><a href='/x/3549002'>Documento</a></td></tr>
    </tbody></table>
    """
    entries = [
        IssuerRegistryEntry(
            issuer_id="AR:YPF",
            issuer_name="YPF",
            source_market="AR",
            primary_symbol="YPFD",
            cnv_entity_name="YPF",
        )
    ]

    observations = cnv_relevant_fact_observations(html, entries)

    assert len(observations) == 1
    observed = observations[0]
    assert observed.observation_key == "CNV:AR:YPF:3549001"
    assert observed.ticker == "YPFD"
    assert observed.event_type == "SPLIT"
    assert observed.event_date == date(2026, 8, 4)


def test_cnv_parser_does_not_match_issuer_mentioned_only_in_description():
    html = """
    <table><tbody>
      <tr><td>04 ago. 2026 09:15</td><td>OTRA SOCIEDAD S.A.</td>
          <td>ACUERDO COMERCIAL CON YPF</td>
          <td><a href='/SitioWeb/HechosRelevantes/3549003'>Documento</a></td></tr>
    </tbody></table>
    """
    entries = [
        IssuerRegistryEntry(
            issuer_id="AR:YPF",
            issuer_name="YPF",
            source_market="AR",
            primary_symbol="YPFD",
            cnv_entity_name="YPF",
        )
    ]

    assert cnv_relevant_fact_observations(html, entries) == []


def test_cnv_parser_classifies_reverse_split_before_generic_split():
    html = """
    <table><tbody>
      <tr><td>04 ago. 2026 09:15</td><td>YPF S.A.</td>
          <td>REVERSE SPLIT DE ACCIONES</td>
          <td><a href='/SitioWeb/HechosRelevantes/3549004'>Documento</a></td></tr>
    </tbody></table>
    """
    entries = [
        IssuerRegistryEntry(
            issuer_id="AR:YPF",
            issuer_name="YPF",
            source_market="AR",
            primary_symbol="YPFD",
            cnv_entity_name="YPF",
        )
    ]

    observations = cnv_relevant_fact_observations(html, entries)

    assert len(observations) == 1
    assert observations[0].event_type == "REVERSE_SPLIT"


def test_cnv_parser_maps_custodian_notice_to_each_mentioned_cedear():
    html = """
    <table><tbody>
      <tr><td>04 ago. 2026 17:53</td><td>BANCO COMAFI S.A.</td>
          <td>INFORMACION RELEVANTE - CEDEARS - ANUNCIO DE DIVIDENDO AXP-VST</td>
          <td>3553783</td>
          <td><a href='https://aif2.cnv.gov.ar/Presentations/publicview/example'>Documento</a></td>
      </tr>
    </tbody></table>
    """
    entries = [
        _sec_entry(),
        IssuerRegistryEntry(
            issuer_id="SEC:0001711269",
            issuer_name="Vistra Corp.",
            source_market="US",
            primary_symbol="VST",
            sec_cik="1711269",
        ),
        IssuerRegistryEntry(
            issuer_id="SEC:0001691493",
            issuer_name="Nu Holdings Ltd.",
            source_market="US",
            primary_symbol="NU",
            sec_cik="1691493",
        ),
    ]

    observations = cnv_relevant_fact_observations(html, entries)

    assert {observation.ticker for observation in observations} == {"AXP", "VST"}
    assert {observation.event_type for observation in observations} == {"DIVIDEND"}
    assert {observation.confidence for observation in observations} == {0.85}
    assert {observation.raw_payload["match_basis"] for observation in observations} == {
        "cedear_ticker"
    }


def test_payload_hash_is_stable_for_equivalent_mapping_order():
    assert payload_hash({"b": 2, "a": 1}) == payload_hash({"a": 1, "b": 2})


def test_confidence_scale_is_named_and_rejects_unknown_basis():
    assert confidence_for("regulator_instrument_match") == 0.85

    try:
        confidence_for("guess")
    except ValueError as exc:
        assert "unsupported confidence basis" in str(exc)
    else:
        raise AssertionError("unknown confidence basis should fail")


def test_jsonb_text_from_asyncpg_is_decoded_for_audit_reads():
    assert _json_payload('{"registry_basis":"latest_portfolio_snapshot"}') == {
        "registry_basis": "latest_portfolio_snapshot"
    }
