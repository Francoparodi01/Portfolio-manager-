"""Reproducible offline thesis-desk run, optionally capturing public market tables.

python -m scripts.backtest_thesis_desk --start 2026-09-01 --end 2026-10-31 \
    --db-container cocos_db --output outputs/thesis-desk-sep-oct-2026
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess

from src.backtesting.thesis_desk import ART, DeskPolicy, run_desk, sessions_between
from src.thesis_desk.historical_market import BASIS, recorded_daily_references

ROOT = Path(__file__).resolve().parents[1]
SOURCES = [
    {"url": "https://x.com/MatiasScalbi/status/2097105195415097524",
     "published_at": "2026-09-07T23:30:15+00:00", "role": "BASE_DESK_INSPIRATION"},
    {"url": "https://x.com/MatiasScalbi/status/2118142082275577874",
     "published_at": "2026-11-05T00:43:20+00:00", "role": "POST_PERIOD_RISK_VARIANT_INSPIRATION"},
]


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def capture_sql(start, end, recorded_closes=False):
    # Dates are parsed before interpolation; there are no user-provided SQL names.
    days = sessions_between(start, end)
    begin = datetime.fromisoformat(start).replace(tzinfo=ART)
    until = datetime.combine(days[-1], datetime.min.time(), ART).replace(hour=17)
    history = (begin - timedelta(days=120)).isoformat()
    facts_start = (begin - timedelta(days=14)).isoformat()
    begin, until = begin.isoformat(), until.isoformat()
    queries = {
        "daily_candles": f"""SELECT ts,ticker,asset_type,currency,source,close_price,interval,
            bar_end,available_at,scraped_at,is_closed,adjustment_policy
            FROM market_candles WHERE ts >= '{history}' AND ts <= '{until}'
            AND currency='ARS' AND interval='1d' ORDER BY ticker,ts,source""",
        "intraday_points": f"""SELECT ts,ticker,asset_type,currency,last_price FROM market_prices
            WHERE ts >= '{begin}' AND ts < '{until}' AND currency='ARS'
            ORDER BY ts,ticker,asset_type""",
        "issuer_events": f"""SELECT id,ticker,source,event_type,event_date,source_published_at,
            source_url,source_hash,title,created_at,updated_at FROM issuer_event_observations
            WHERE event_date >= '{facts_start}'::timestamptz::date
            AND event_date <= '{until}'::timestamptz::date ORDER BY event_date,id""",
        "corporate_effects": f"""SELECT i.id,i.ticker,i.asset_type,i.currency,i.quantity_factor,i.price_factor,
            i.cost_basis_factor,i.is_active,e.lifecycle_status,
            e.effective_at,e.created_at AS event_created_at,e.updated_at AS event_updated_at,
            i.created_at AS effect_created_at,i.updated_at AS effect_updated_at
            FROM corporate_events e JOIN corporate_event_instrument_effects i ON i.event_id=e.id
            WHERE e.effective_at >= '{history}' AND e.effective_at <= '{until}' ORDER BY e.effective_at,i.id""",
    }
    if recorded_closes:
        queries["recorded_close_quotes"] = f"""SELECT DISTINCT ON
            (ticker,asset_type,currency,(ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date)
            ts,ticker,asset_type,currency,last_price FROM market_prices
            WHERE ts >= '{history}' AND ts < '{until}' AND currency='ARS'
            AND last_price > 0
            AND (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= '16:30'
            AND (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < '17:00'
            ORDER BY ticker,asset_type,currency,(ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,ts DESC"""
    statements = ["BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;",
                  "SET LOCAL statement_timeout='120s';", "SET LOCAL timezone='UTC';"]
    for name, query in queries.items():
        statements.append("SELECT jsonb_build_object('name','" + name + "','rows',COALESCE(jsonb_agg(q),'[]'::jsonb)) FROM (" + query + ") q;")
    statements.append("COMMIT;")
    return "\n".join(statements), queries


def capture_database(container, user, database, start, end, directory, recorded_closes=False):
    sql, queries = capture_sql(start, end, recorded_closes)
    (directory / "capture.sql").write_text(sql, encoding="utf-8")
    temporary = directory / "capture.jsonl.tmp"
    command = ["docker", "exec", "-i", container, "psql", "-U", user, "-d", database,
               "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"]
    try:
        with temporary.open("wb") as stream:
            completed = subprocess.run(command, input=sql.encode(), stdout=stream, stderr=subprocess.PIPE, timeout=600)
        if completed.returncode:
            raise RuntimeError("Read-only database capture failed: " + completed.stderr.decode(errors="replace")[-1000:])
        observations = [json.loads(line, parse_float=Decimal) for line in temporary.read_text(encoding="utf-8").splitlines() if line.strip()]
        if {o["name"] for o in observations} != set(queries):
            raise ValueError("Incomplete capture")
        for observation in observations:
            observation["row_count"] = len(observation["rows"])
            observation["query_sha256"] = hashlib.sha256(queries[observation["name"]].encode()).hexdigest()
        capture = dict(schema="thesis-desk-capture-v1", transaction_read_only=True,
                       source="CURRENT_DATABASE_CAPTURE_NOT_ORIGINAL_VINTAGE", captured_at=datetime.now(timezone.utc).isoformat(),
                       observations=observations)
        write(directory / "capture.json", capture)
        return capture
    finally:
        temporary.unlink(missing_ok=True)


def source_audit(rows):
    events = rows.get("issuer_events", [])
    bars = rows.get("daily_candles", [])
    return dict(row_counts={k: len(v) for k, v in rows.items()},
                issuer_event_types=dict(Counter(r["source"] + ":" + r["event_type"] for r in events)),
                candles_with_temporal_lineage=sum(bool(r.get("bar_end") and r.get("available_at") and r.get("scraped_at") and r.get("is_closed")) for r in bars),
                candles_with_explicit_raw_adjustment=sum(r.get("adjustment_policy") in {"RAW", "UNADJUSTED"} for r in bars),
                facts_conversion="NONE: generic filing, dividend, capital-limit and earnings-calendar records are not verified guidance/deal theses",
                universe_conversion="NONE: recorded ticker availability does not establish historical IVV membership or SMID market cap")


def summary(results, audit):
    lines = [f"# Backtest de desk por tesis: {results[0]['requested_start']} a {results[0]['requested_end']}", "",
             "Simulación retrospectiva en ARS sobre datos locales. No representa operaciones reales ni la flota del autor.", "",
             "| Variante | Estado | Ruedas / cortes | Fills | Retorno neto modelado |",
             "|---|---|---:|---:|---:|"]
    for result in results:
        metrics = result["metrics"]
        value = f"{float(metrics['return_pct']):.3f}%" if metrics else "NO EVALUABLE"
        lines.append(f"| {result['policy']['variant']} | {result['status']} | {result['sessions']} / {result['cuts']} | {len(result['fills'])} | {value} |")
    lines += ["", f"Período solicitado: {results[0]['requested_start']} a {results[0]['requested_end']}. Última rueda: {results[0]['actual_last_session']}.",
              f"Precios utilizables: {results[0]['quote_rows']} filas en {results[0]['quote_sessions']} ruedas.", "",
              "## Cobertura", "", "Los faltantes no son señales negativas ni prueban que no hubiera oportunidades."]
    for result in results:
        gaps = ", ".join(f"{domain}: {len(days)} ruedas" for domain, days in result["coverage_gaps"].items()) or "sin huecos declarados"
        lines.append(f"- {result['policy']['variant']}: {gaps}. Calidad: {', '.join(result['quality_flags']) or 'sin flags'}.")
    lines.append(f"- Ruedas sin cotizaciones dentro del horario: {', '.join(results[0]['missing_quote_sessions']) or 'ninguna'}.")
    lines.append(f"- Velas con linaje temporal completo: {audit['candles_with_temporal_lineage']}/{audit['row_counts'].get('daily_candles', 0)}; con política raw explícita: {audit['candles_with_explicit_raw_adjustment']}.")
    lines += ["", f"Eventos originales por fuente/tipo: `{json.dumps(audit['issuer_event_types'], ensure_ascii=False)}`.",
              "No se convierten titulares genéricos, calendarios de balances o cambios de programas CEDEAR en tesis oficiales.",
              "Los registros originales y los SQL están en capture.json y capture.sql; source-audit.json conserva los conteos.", "",
              "## Interpretación", "",
              "BASE_V1 adapta la guía publicada el 07/09/2026; su aplicación al inicio de septiembre también es retrospectiva.",
              "RISK_V2 incorpora reglas publicadas el 04/11/2026 ART, después del período. No es una validación fuera de muestra.",
              "Las reglas que el autor no cuantificó se fijaron como supuestos explícitos antes de la corrida, sin buscar la variante ganadora.",
              "CASH es una referencia nominal sin interés. HOLD queda no evaluable hasta contar con una cartera de referencia fechada y financiada.",
              "No se acredita rentabilidad real, alpha, ni independencia estadística de los ciclos. No hubo órdenes ni cambios operativos.", "",
              "## Fuentes y reproducción", ""]
    for source in SOURCES:
        lines.append(f"- [{source['role']}]({source['url']}) — {source['published_at']}.")
    lines += ["- policy.json: reglas congeladas antes de capturar/correr. manifest.json: hashes de inputs, código y resultados.",
              "- docs/thesis-desk-backtest.md: contrato de evidencia, decisiones metodológicas y comando de reproducción.", ""]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2026-09-01")
    parser.add_argument("--end", default="2026-10-31")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--capture", type=Path)
    source.add_argument("--db-container")
    parser.add_argument("--db-user", default="quantia")
    parser.add_argument("--db-name", default="quantia")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--initial-cash", default="3000000")
    parser.add_argument("--recorded-closes", action="store_true",
        help="Use the PAPER desk's last 16:30-17:00 recorded quote as a daily reference; not an official closing price")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    sessions_between(args.start, args.end)
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("Use a new/empty output directory; historical runs are not overwritten")
    args.output.mkdir(parents=True, exist_ok=True)
    policies = [DeskPolicy(variant=v, initial_cash=args.initial_cash) for v in ("BASE_V1", "RISK_V2")]
    # Written before data inspection: these are fixed adaptations, not optimized winners.
    write(args.output / "policy.json", dict(created_at=datetime.now(timezone.utc).isoformat(), start=args.start, end=args.end,
          sources=SOURCES, policies=[p.__dict__ for p in policies], selection="FIXED_RULES_NO_PARAMETER_SEARCH",
          technical_price_basis=BASIS if args.recorded_closes else "EXPLICIT_DAILY_CANDLE_LINEAGE",
          scope="ARS_QUANTIA_INSTRUMENTS_WITH_EXPLICIT_US_COMMON_STOCK_MAPPING"))
    if args.capture:
        capture_path = args.capture.resolve()
        capture = json.loads(capture_path.read_text(encoding="utf-8"), parse_float=Decimal)
    else:
        capture = capture_database(args.db_container, args.db_user, args.db_name, args.start, args.end, args.output, args.recorded_closes)
        capture_path = (args.output / "capture.json").resolve()
    rows = {o["name"]: o["rows"] for o in capture["observations"]}
    if args.recorded_closes:
        if "recorded_close_quotes" not in rows:
            raise ValueError("Capture has no recorded_close_quotes; recapture with --recorded-closes")
        references, recovery = recorded_daily_references(rows["recorded_close_quotes"], capture["captured_at"])
        rows["daily_candles"] = list(rows.get("daily_candles", [])) + references
        write(args.output / "recorded-daily-references.json", references)
        write(args.output / "price-recovery.json", recovery)
    evidence = json.loads(args.evidence.read_text(encoding="utf-8"), parse_float=Decimal) if args.evidence else dict(
        schema="thesis-desk-evidence-v1", facts=[], universe=[], risk=[], blackouts=[], coverage=[],
        missing_reason="NO_VERIFIED_HISTORICAL_DESK_EVIDENCE_SUPPLIED_OR_FOUND_IN_LOCAL_ISSUER_TABLE")
    write(args.output / "evidence.json", evidence)
    audit = source_audit(rows)
    write(args.output / "source-audit.json", audit)
    results = [run_desk(rows, evidence, args.start, args.end, policy) for policy in policies]
    for result in results:
        result["technical_price_basis"] = BASIS if args.recorded_closes else "EXPLICIT_DAILY_CANDLE_LINEAGE"
        write(args.output / (result["policy"]["variant"] + ".json"), result)
    (args.output / "result.md").write_text(summary(results, audit), encoding="utf-8")
    files = [p for p in args.output.iterdir() if p.is_file()]
    code = [ROOT / p for p in ("src/backtesting/thesis_desk.py", "scripts/backtest_thesis_desk.py", "src/thesis_desk/historical_market.py", "src/core/market_calendar.py", "config/market_holidays_ar.json")]
    write(args.output / "manifest.json", dict(capture_path=str(capture_path), capture_sha256=sha(capture_path),
          artifacts={p.name: sha(p) for p in files}, code_sha256={str(p.relative_to(ROOT)): sha(p) for p in code},
          source_capture_status=capture.get("source", "UNKNOWN"),
          completed_at=datetime.now(timezone.utc).isoformat()))
    print(json.dumps([{k: r[k] for k in ("status", "sessions", "cuts", "quote_rows", "evidence_counts", "metrics")} for r in results]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
