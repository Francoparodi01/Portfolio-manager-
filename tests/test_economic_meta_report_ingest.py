from __future__ import annotations

from src.analysis.economic_meta_report_ingest import ingest_analysis_report, parse_analysis_report
from src.analysis.economic_meta_store import EconomicMetaShadowStore


REPORT = """
🧠 Análisis de cartera
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
24/09/2026 21:39 ART

💼 Portfolio: $2.892.597 ARS | Cash libre: $3.832 ARS

Plan operativo
   Plan ventas: $709.585 ARS | Plan compras: $0 ARS | Cash post-plan: $708.095 ARS
   Fees estimados: $5.322 ARS

Lectura de cartera
🔴 MU → SELL_PARTIAL → -$348.400 ARS | score -0.110 | NEGATIVA OPERABLE | peso 12.0% → 0.0%
   Régimen técnico: RANGE | trend shadow +0.600
🔴 YPFD → SELL_PARTIAL → -$69.080 ARS | score -0.082 | NEGATIVA OPERABLE | peso 4.5% → 2.1%
   Régimen técnico: TRANSITIONAL | trend shadow -0.344
🔴 IREN → SELL_PARTIAL → -$292.105 ARS | score -0.051 | NEGATIVA DÉBIL | peso 12.0% → 1.9%
   Régimen técnico: RANGE | trend shadow +0.319
🔵 NVDA → WATCH | score +0.040 | NEUTRAL / RUIDO | peso 16.7% → 33.6%
   Régimen técnico: RANGE | trend shadow +0.600
🔵 GDX → WATCH | score +0.032 | NEUTRAL / RUIDO | peso 12.9% → 17.4%
   Régimen técnico: RANGE | trend shadow +0.331
🔵 NVS → WATCH | score +0.016 | NEUTRAL / RUIDO | peso 6.0% → 35.0%
   Régimen técnico: TRANSITIONAL | trend shadow +0.105
🟡 SPCX → HOLD | score -0.144 | NEGATIVA OPERABLE | peso 2.3% → 2.0%
   Régimen técnico: TRANSITIONAL | trend shadow -0.100
🟡 SNDK → HOLD | score -0.058 | NEGATIVA DÉBIL | peso 6.9% → 2.0%
   Régimen técnico: RANGE | trend shadow +0.100
🟡 AMD → HOLD | score -0.034 | NEUTRAL / RUIDO | peso 14.1% → 2.0%
   Régimen técnico: TRANSITIONAL | trend shadow +0.695
🟡 TQQQ → HOLD | score -0.026 | NEUTRAL / RUIDO | peso 12.4% → 2.0%
   Régimen técnico: RANGE | trend shadow +0.600
"""


COMPACT_REPORT = """
🧠 **CIERRE DE RUEDA — 24/09 22:06 ART**
💼 **$2.892.597 ARS** | Cash **$3.832 ARS** | Régimen: **NORMAL**

━━━ **SIMULACIÓN CONTEXTUAL** ━━━
🔴 REVALIDAR SELL IREN -$292.105 ARS score -0.050
🔴 REVALIDAR SELL MU -$348.400 ARS score -0.110
🔴 REVALIDAR SELL YPFD -$69.080 ARS score -0.085
Ventas: **$709.585 ARS** | Compras: **$0 ARS** | Fees: **$5.322 ARS** | Cash post: **$708.095 ARS**

━━━ **CARTERA** ━━━
🔴 **MU** `-0.110` T`+0.060` M`-0.070` S`+0.001` R=**RANGE** trend=`+0.600` 12.0%→0.0% SELL_PARTIAL
🔴 **YPFD** `-0.085` T`+0.016` M`+0.001` S`-0.002` R=**TRANSITIONAL** trend=`-0.344` 4.5%→2.1% SELL_PARTIAL
🔴 **IREN** `-0.050` T`+0.118` M`-0.066` S`-0.002` R=**RANGE** trend=`+0.319` 12.0%→1.9% SELL_PARTIAL
🔵 **NVDA** `+0.040` T`+0.110` M`-0.070` S`+0.000` R=**RANGE** trend=`+0.600` 16.7%→33.6% watch — Optimizer sugería aumentar
🔵 **GDX** `+0.032` T`+0.100` M`-0.066` S`-0.002` R=**RANGE** trend=`+0.331` 12.9%→17.4% watch — Optimizer sugería aumentar
🔵 **NVS** `+0.016` T`+0.084` M`-0.066` S`-0.002` R=**TRANSITIONAL** trend=`+0.105` 6.0%→35.0% watch — Optimizer sugería aumentar
🟡 **SPCX** `-0.144` T`+0.024` M`-0.066` S`-0.002` R=**TRANSITIONAL** trend=`-0.100` 2.3%→2.0% HOLD
🟡 **SNDK** `-0.058` T`+0.110` M`-0.066` S`-0.002` R=**RANGE** trend=`+0.100` 6.9%→2.0% hold — costo no justifica
🟡 **AMD** `-0.034` T`+0.049` M`-0.070` S`-0.000` R=**TRANSITIONAL** trend=`+0.695` 14.1%→2.0% hold — neutral
🟡 **TQQQ** `-0.025` T`+0.042` M`-0.066` S`-0.002` R=**RANGE** trend=`+0.600` 12.4%→2.0% hold — neutral
"""


def _assert_expected_tickers(rows):
    assert {row.ticker for row in rows} == {
        "MU", "YPFD", "IREN", "NVDA", "GDX", "NVS", "SPCX", "SNDK", "AMD", "TQQQ"
    }


def test_parse_full_analysis_has_all_ten_positions_and_operational_turnover():
    as_of, rows, metrics = parse_analysis_report(REPORT)

    assert as_of.isoformat() == "2026-09-25T00:39:00+00:00"
    assert len(rows) == 10
    _assert_expected_tickers(rows)
    by_ticker = {row.ticker: row for row in rows}
    assert by_ticker["MU"].candidate_action == "REDUCE"
    assert by_ticker["NVDA"].candidate_action == "BUY"
    assert by_ticker["NVS"].candidate_action == "BUY"
    assert by_ticker["AMD"].candidate_action == "HOLD"
    assert by_ticker["SPCX"].regime == "TRANSITIONAL"

    assert 0.24 < metrics["portfolio_turnover"] < 0.25
    assert 74.9 < metrics["estimated_cost_bps"] < 75.1


def test_parse_compact_analysis_matches_visible_analysis_and_operational_turnover():
    as_of, rows, metrics = parse_analysis_report(COMPACT_REPORT)

    # Compact reports omit the year; the parser attaches the current ART year.
    assert (as_of.month, as_of.day, as_of.hour, as_of.minute) == (9, 25, 1, 6)
    assert len(rows) == 10
    _assert_expected_tickers(rows)
    by_ticker = {row.ticker: row for row in rows}
    assert by_ticker["MU"].candidate_action == "REDUCE"
    assert by_ticker["MU"].score == -0.110
    assert by_ticker["YPFD"].score == -0.085
    assert by_ticker["IREN"].score == -0.050
    assert by_ticker["NVDA"].candidate_action == "BUY"
    assert by_ticker["NVS"].candidate_action == "BUY"
    assert by_ticker["TQQQ"].candidate_action == "HOLD"
    assert by_ticker["TQQQ"].score == -0.025
    assert by_ticker["SPCX"].regime == "TRANSITIONAL"

    assert 0.24 < metrics["portfolio_turnover"] < 0.25
    assert 74.9 < metrics["estimated_cost_bps"] < 75.1


def test_ingest_full_analysis_persists_three_shadow_policies_per_ticker(tmp_path):
    path = tmp_path / "shadow.jsonl"
    summary = ingest_analysis_report(REPORT, store_path=path)

    assert summary["candidate_count"] == 10
    assert summary["records_written"] == 30
    assert summary["capital_effect"] is False
    assert summary["portfolio_turnover"] < 0.35

    rows = EconomicMetaShadowStore(path).read_all()
    assert len(rows) == 30
    assert {row["ticker"] for row in rows} == {
        "MU", "YPFD", "IREN", "NVDA", "GDX", "NVS", "SPCX", "SNDK", "AMD", "TQQQ"
    }
    assert {row["policy_name"] for row in rows} == {"META-A", "META-B", "META-C"}
    assert all(str(row["opportunity_id"]).startswith("analysis:report:") for row in rows)
    assert all(row["capital_effect"] is False for row in rows)

    second = ingest_analysis_report(REPORT, store_path=path)
    assert second["records_written"] == 0
    assert len(EconomicMetaShadowStore(path).read_all()) == 30


def test_ingest_compact_analysis_persists_latest_visible_scores(tmp_path):
    path = tmp_path / "shadow.jsonl"
    summary = ingest_analysis_report(COMPACT_REPORT, store_path=path)

    assert summary["candidate_count"] == 10
    assert summary["records_written"] == 30
    assert summary["portfolio_turnover"] < 0.35
    rows = EconomicMetaShadowStore(path).read_all()
    assert len(rows) == 30
    i = [row for row in rows if row["ticker"] == "IREN"]
    assert {row["candidate_score"] for row in i} == {-0.050}
    t = [row for row in rows if row["ticker"] == "TQQQ"]
    assert {row["candidate_score"] for row in t} == {-0.025}
