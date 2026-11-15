"""Auditable USD price capture and paired event outcomes; never account PnL."""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import math
from pathlib import Path
import re
import statistics
import time

import httpx

from src.backtesting.thesis_desk import instant
from src.thesis_desk.contracts import digest
from src.thesis_desk.usd_pilot import NY, read, save, utcnow, validate_extension


def positive(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def parse_chart(body, symbol, calendar, asof):
    chart = body.get("chart", {})
    if chart.get("error") or not chart.get("result") or len(chart["result"]) != 1:
        raise ValueError("Yahoo chart result missing or ambiguous")
    result = chart["result"][0]
    meta = result["meta"]
    if meta.get("currency") != "USD" or meta.get("symbol") != symbol:
        raise ValueError("Unexpected price currency or symbol")
    expected_type = "ETF" if symbol == "IVV" else "EQUITY"
    if meta.get("instrumentType") != expected_type:
        raise ValueError("Unexpected underlying instrument type")
    schedule = {s["date"]: s for s in calendar}
    indicators = result["indicators"]["quote"]
    if len(indicators) != 1:
        raise ValueError("Ambiguous quote arrays")
    quote = indicators[0]
    timestamps = result.get("timestamp", [])
    if any(len(quote.get(k, [])) != len(timestamps) for k in ("open", "high", "low", "close", "volume")):
        raise ValueError("Inconsistent quote array lengths")
    bars, rejected = {}, []
    for n, stamp in enumerate(timestamps):
        day = datetime.fromtimestamp(stamp, timezone.utc).astimezone(NY).date().isoformat()
        if day not in schedule or instant(schedule[day]["close"]) > instant(asof):
            rejected.append(dict(date=day, reason="NOT_CLOSED_REGULAR_SESSION"))
            continue
        values = {k: quote[k][n] for k in ("open", "high", "low", "close")}
        if not all(positive(x) for x in values.values()):
            rejected.append(dict(date=day, reason="INVALID_OR_MISSING_OHLC"))
            continue
        if values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
            rejected.append(dict(date=day, reason="INCONSISTENT_OHLC"))
            continue
        if day in bars:
            raise ValueError("Duplicate daily price")
        bars[day] = dict(**values, volume=quote["volume"][n], timestamp=stamp)
    dividends, splits = {}, []
    for event in result.get("events", {}).get("dividends", {}).values():
        day = datetime.fromtimestamp(event["date"], timezone.utc).astimezone(NY).date().isoformat()
        amount = event.get("amount")
        if not positive(amount) or day not in schedule:
            raise ValueError("Invalid dividend or ex-date")
        if instant(schedule[day]["close"]) <= instant(asof):
            dividends[day] = dividends.get(day, 0) + amount
    for event in result.get("events", {}).get("splits", {}).values():
        day = datetime.fromtimestamp(event["date"], timezone.utc).astimezone(NY).date().isoformat()
        splits.append(dict(date=day, numerator=event.get("numerator"), denominator=event.get("denominator")))
    return dict(symbol=symbol, currency="USD", instrument_type=meta["instrumentType"],
                exchange=meta.get("exchangeName"), bars=bars, dividends=dividends,
                splits=splits, rejected=rejected,
                price_basis="YAHOO_SPLIT_ADJUSTED_OHLC_CURRENT_VINTAGE_NOT_DIVIDEND_ADJUSTED")


def capture_market(output, *, refresh=False, reuse_run=None):
    output = Path(output)
    protocol = read(output / "protocol.json")
    previous = None
    if reuse_run is not None:
        if refresh:
            raise ValueError("Cannot refresh and reuse an earlier price cutoff together")
        reuse_run = Path(reuse_run)
        old_protocol = read(reuse_run / "protocol.json")
        validate_extension(old_protocol, protocol)
        previous = read(reuse_run / "market.json")
        validate_market(reuse_run, old_protocol, previous)
    target = output / "market.json"
    if target.exists():
        market = read(target)
        validate_market(output, protocol, market)
        if previous and market["asof"] != previous["asof"]:
            raise ValueError("Existing market capture has a different comparison cutoff")
        save(output / "market_snapshots" / (digest(market) + ".json"), market)
        if not refresh:
            return market
    asof = previous["asof"] if previous else utcnow()
    period1 = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp())
    period2 = int(instant(asof).timestamp())
    symbols = [c["symbol"] for c in protocol["cohort"]] + ["IVV"]
    raw = output / "market_raw"
    raw.mkdir(exist_ok=True)

    def fetch(symbol):
        failures = []
        for attempt in range(3):
            host = "query1.finance.yahoo.com" if attempt != 1 else "query2.finance.yahoo.com"
            try:
                with httpx.Client(timeout=35, headers={"User-Agent": "Mozilla/5.0"}) as client:
                    response = client.get(f"https://{host}/v8/finance/chart/{symbol}",
                                          params=dict(period1=period1, period2=period2, interval="1d",
                                                      events="div,splits", includeAdjustedClose="true"))
                response.raise_for_status()
                data = parse_chart(response.json(), symbol, protocol["calendar"], asof)
                relative = f"market_raw/{digest(response.content)}.json"
                (output / relative).write_bytes(response.content)
                receipt = dict(raw_path=relative, sha256=digest(response.content),
                               url=str(response.url), captured_at=utcnow(), status=response.status_code)
                return symbol, data, receipt, None
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                failures.append(type(exc).__name__)
                if attempt < 2:
                    time.sleep(attempt+1)
        return symbol, None, None, dict(status="SOURCE_FAILED", attempts=failures)

    market = dict(schema="quantia-usd-market-v1", asof=asof, captured_at=utcnow(),
                  protocol_hash=digest(protocol), symbols={}, receipts={}, failures={})
    if previous:
        for symbol, data in previous["symbols"].items():
            receipt = dict(previous["receipts"][symbol])
            original = reuse_run / receipt["raw_path"]
            receipt["raw_path"] = f"market_raw/{receipt['sha256']}.json"
            (output / receipt["raw_path"]).write_bytes(original.read_bytes())
            market["symbols"][symbol], market["receipts"][symbol] = data, receipt
        market["reused_from"] = dict(run=str(reuse_run.resolve()), market_hash=digest(previous),
                                    symbols=len(market["symbols"]))
    with ThreadPoolExecutor(max_workers=3) as pool:
        for symbol, data, receipt, error in pool.map(fetch, [s for s in symbols if s not in market["symbols"]]):
            if error:
                market["failures"][symbol] = error
            else:
                market["symbols"][symbol], market["receipts"][symbol] = data, receipt
    market["captured_at"] = utcnow()
    save(output / "market_snapshots" / (digest(market) + ".json"), market)
    save(target, market)
    return market


def long_return(entry_open, exit_close, dividends, fee=0.006, slip=0.0015):
    if not positive(entry_open) or not positive(exit_close) or not math.isfinite(dividends) or dividends < 0:
        raise ValueError("Invalid return inputs")
    buy = entry_open * (1 + slip) * (1 + fee)
    sell = exit_close * (1 - slip) * (1 - fee)
    return dict(gross=(exit_close + dividends) / entry_open - 1,
                net=(sell + dividends) / buy - 1, dividends_per_share=dividends)


def validate_market(output, protocol, market):
    """Verify normalized bars against the archived response, not only its receipt hash."""
    if market["protocol_hash"] != digest(protocol):
        raise ValueError("Price capture belongs to a different protocol")
    expected = {c["symbol"] for c in protocol["cohort"]} | {"IVV"}
    if set(market["symbols"]) | set(market["failures"]) != expected:
        raise ValueError("Market capture does not account for the complete frozen cohort")
    if set(market["symbols"]) & set(market["failures"]) or set(market["symbols"]) != set(market["receipts"]):
        raise ValueError("Conflicting market capture status")
    for symbol, receipt in market["receipts"].items():
        path = Path(output) / receipt["raw_path"]
        if digest(path.read_bytes()) != receipt["sha256"]:
            raise ValueError("Raw market source changed")
        restored = parse_chart(read(path), symbol, protocol["calendar"], market["asof"])
        if restored != market["symbols"][symbol]:
            raise ValueError("Normalized market bars differ from archived source")


def audit_decision(decision):
    """Diagnostic only: preserve the original agent decision and its measured outcome."""
    flags = []
    if not decision or decision["action"] != "BUY":
        return flags
    at = instant(decision["decision_cutoff"])
    for finding in decision.get("findings", []):
        quote = finding["source"]["quote"]
        # Explicit older-year transaction statements are clearly historical. Absence of
        # this pattern does NOT certify freshness, and financial period labels are excluded.
        dated = re.search(
            r"\b(?:in|on)\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)"
            r"\s+(?:\d{1,2},?\s+)?(20\d{2})\b.{0,65}\b(?:signed|entered into|closed|completed|acquired)\b",
            quote, re.I)
        if finding["kind"].startswith("DEAL_") and dated and int(dated[1]) < at.year:
            flags.append(dict(code="HISTORICAL_TRANSACTION_AS_NEW", finding_id=finding["id"],
                              quote=quote, source_document_id=finding["source"]["document_id"],
                              explanation="Selected transaction clause explicitly refers to an older year"))
    supplied = " ".join(f["source"]["context"] for f in decision.get("findings", []))
    pm_text = (decision.get("pm") or {}).get("reason", "")
    numbers = re.findall(r"(?<![\w])\d+(?:[.,]\d+)*(?![\w])", pm_text)
    missing = sorted({n for n in numbers if not re.search(r"(?<!\d)" + re.escape(n) + r"(?!\d)", supplied)})
    if missing:
        flags.append(dict(code="PM_NUMBERS_NOT_IN_SUPPLIED_CONTEXT", values=missing,
                          explanation="PM explanation includes numeric details absent from its supplied source context"))
    return flags


def event_outcome(event, horizon, decision, market, protocol):
    entry, end = event["entry"], event["exit_sessions"][str(horizon)]
    action = decision["action"] if decision else "UNKNOWN"
    row = dict(event_id=event["id"], symbol=event["symbol"], accession=event["accession"],
               published_at=event["published_at"], entry_date=entry["date"], exit_date=end["date"],
               horizon=horizon, primary_nonoverlap=event["primary_nonoverlap"], action=action,
               decision_basis=decision.get("basis") if decision else "PENDING",
               status=None, hold_gross=None, hold_net=None, agent_net=None, baseline_net=None,
               benchmark_net=None, agent_minus_hold=None, agent_minus_baseline=None,
               buy_minus_benchmark=None, prior_5=None, prior_20=None, announcement_to_entry=None,
               baseline_action=None, entry_open=None, exit_close=None, dividends_per_share=None)
    if instant(end["close"]) > instant(market["asof"]):
        row["status"] = "IMMATURE"
        return row
    data = market["symbols"].get(event["symbol"])
    if not data:
        row["status"] = "PRICE_SOURCE_FAILED"
        return row
    if data["splits"]:
        row["status"] = "SPLIT_REQUIRES_VERIFICATION"
        return row
    bars = data["bars"]
    if entry["date"] not in bars or end["date"] not in bars:
        row["status"] = "MISSING_ENTRY_OR_EXIT"
        return row
    # Reject a hole anywhere in a holding window; never silently shorten N sessions.
    window = [s["date"] for s in protocol["calendar"] if entry["date"] <= s["date"] <= end["date"]]
    if len(window) != horizon or any(d not in bars for d in window):
        row["status"] = "MISSING_INTERMEDIATE_SESSION"
        return row
    dividend = sum(amount for day, amount in data["dividends"].items() if entry["date"] < day <= end["date"])
    fee, slip = protocol["rules"]["fee_each_side"], protocol["rules"]["slippage_each_side"]
    row["entry_open"], row["exit_close"] = bars[entry["date"]]["open"], bars[end["date"]]["close"]
    outcome = long_return(row["entry_open"], row["exit_close"], dividend, fee, slip)
    row.update(status="EVALUABLE", hold_gross=outcome["gross"], hold_net=outcome["net"],
               dividends_per_share=dividend)
    if action in {"BUY", "CASH"}:
        row["agent_net"] = row["hold_net"] if action == "BUY" else 0.0
        row["agent_minus_hold"] = row["agent_net"] - row["hold_net"]
    # Features must precede the source/decision cutoff, not just the next executable open.
    past = [s["date"] for s in protocol["calendar"] if instant(s["close"]) < instant(event["decision_cutoff"])]
    if past and past[-1] in bars:
        last = bars[past[-1]]["close"]
        row["announcement_to_entry"] = row["entry_open"] / last - 1
        for h in (5, 20):
            if len(past) >= h+1 and all(d in bars for d in past[-h-1:]):
                row[f"prior_{h}"] = last / bars[past[-h-1]]["close"] - 1
        if len(past) >= 20 and all(d in bars for d in past[-20:]):
            sma = statistics.mean(bars[d]["close"] for d in past[-20:])
            row["baseline_action"] = "BUY" if last > sma else "CASH"
            row["baseline_net"] = row["hold_net"] if row["baseline_action"] == "BUY" else 0.0
            if row["agent_net"] is not None:
                row["agent_minus_baseline"] = row["agent_net"] - row["baseline_net"]
    benchmark = market["symbols"].get("IVV")
    if (benchmark and not benchmark["splits"] and all(d in benchmark["bars"] for d in window)):
        div = sum(a for d, a in benchmark["dividends"].items() if entry["date"] < d <= end["date"])
        row["benchmark_net"] = long_return(benchmark["bars"][entry["date"]]["open"],
                                           benchmark["bars"][end["date"]]["close"], div, fee, slip)["net"]
        if action == "BUY":
            row["buy_minus_benchmark"] = row["hold_net"] - row["benchmark_net"]
    return row


def mean(rows, key):
    values = [r[key] for r in rows if r[key] is not None]
    return statistics.mean(values) if values else None


def summarize(rows):
    summaries = {}
    for label, subset in (("primary_nonoverlap", [r for r in rows if r["primary_nonoverlap"]]),
                          ("all_events_descriptive", rows)):
        summaries[label] = {}
        for horizon in (5, 10, 20):
            own = [r for r in subset if r["horizon"] == horizon]
            eligible = [r for r in own if r["status"] == "EVALUABLE"]
            paired = [r for r in eligible if r["agent_net"] is not None and r["baseline_net"] is not None]
            buys = [r for r in paired if r["action"] == "BUY"]
            summaries[label][str(horizon)] = dict(
                total=len(own), statuses=dict(Counter(r["status"] for r in own)),
                price_evaluable=len(eligible), paired=len(paired), buy=len(buys),
                cash=sum(r["action"] == "CASH" for r in paired),
                unknown_decisions=sum(r["action"] == "UNKNOWN" for r in own),
                agent_mean_net=mean(paired, "agent_net"), hold_mean_net=mean(paired, "hold_net"),
                baseline_mean_net=mean(paired, "baseline_net"),
                agent_minus_hold=mean(paired, "agent_minus_hold"),
                agent_minus_baseline=mean(paired, "agent_minus_baseline"),
                buys_mean_net=mean(buys, "hold_net"),
                buys_median_net=statistics.median(r["hold_net"] for r in buys) if buys else None,
                buys_positive=sum(r["hold_net"] > 0 for r in buys),
                buys_mean_excess_ivv=mean(buys, "buy_minus_benchmark"),
                buys_with_benchmark=sum(r["buy_minus_benchmark"] is not None for r in buys),
                buys_prior5_positive=sum(r["prior_5"] is not None and r["prior_5"] > 0 for r in buys),
                buys_with_prior5=sum(r["prior_5"] is not None for r in buys),
                buys_prior5_mean=mean(buys, "prior_5"),
                buys_prior20_mean=mean(buys, "prior_20"),
                buys_announcement_to_entry_mean=mean(buys, "announcement_to_entry"))
    return summaries


def evaluate(output):
    output = Path(output)
    protocol, events, market = (read(output / f"{name}.json") for name in ("protocol", "events", "market"))
    if digest(events) != protocol["events_hash"] or market["protocol_hash"] != digest(protocol):
        raise ValueError("Frozen inputs changed")
    validate_market(output, protocol, market)
    rows, actions, quality = [], {}, []
    for event in events:
        path = output / "decisions" / (event["id"] + ".json")
        decision = read(path) if path.exists() else None
        if decision and (decision["event_hash"] != digest(event) or decision["model_digest"] != protocol["model_digest"]):
            raise ValueError("Decision provenance differs")
        actions[event["id"]] = decision["action"] if decision else "PENDING"
        flags = audit_decision(decision)
        if flags:
            quality.append(dict(event_id=event["id"], symbol=event["symbol"],
                                published_at=event["published_at"], flags=flags))
        rows.extend(event_outcome(event, h, decision, market, protocol) for h in (5, 10, 20))
    result = dict(schema="quantia-usd-pilot-result-v1", generated_at=utcnow(),
                  market_asof=market["asof"], protocol_hash=digest(protocol),
                  market_hash=digest(market), evaluation_code_sha256=digest(Path(__file__).read_bytes()),
                  decision_statuses=dict(Counter(actions.values())),
                  cohort_size=len(protocol["cohort"]), symbols_with_events=len({e["symbol"] for e in events}),
                  no_event_symbols=[c["symbol"] for c in protocol["cohort"] if not any(e["symbol"] == c["symbol"] for e in events)],
                  source_failures=market["failures"], summaries=summarize(rows), decision_quality_flags=quality,
                  limitations=protocol["limitations"])
    result["status"] = ("COMPLETE_RETROSPECTIVE_PILOT" if all(a in {"BUY", "CASH"} for a in actions.values())
                        else "PARTIAL_DECISIONS")
    if result["status"] == "COMPLETE_RETROSPECTIVE_PILOT" and any(r["status"] not in {"EVALUABLE", "IMMATURE"} for r in rows):
        result["status"] = "COMPLETE_DECISIONS_WITH_PRICE_GAPS"
    save(output / "result.json", result)
    save(output / "outcomes.json", rows)
    with (output / "outcomes.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    write_report(output, protocol, result, rows)
    # Retain each evaluated input/code version when a later price capture matures windows.
    version = digest([result["market_hash"], result["evaluation_code_sha256"], rows])
    archive = output / "evaluations" / version
    if not archive.exists():
        archive.mkdir(parents=True)
        for name in ("protocol.json", "events.json", "market.json", "result.json",
                     "outcomes.json", "outcomes.csv", "report.md"):
            (archive / name).write_bytes((output / name).read_bytes())
    return result


def write_report(output, protocol, result, rows):
    def pct(x):
        return "N/D" if x is None else f"{100*x:+.2f}%"
    def points(x):
        return "N/D" if x is None else f"{100*x:+.2f} pp"
    main = result["summaries"]["primary_nonoverlap"]
    lines = ["# Piloto de tesis en USD — septiembre/octubre 2026", "",
             f"Estado: **{result['status']}**. Precios cerrados al corte **{result['market_asof']}**.",
             f"Universo fijado: {result['cohort_size']} acciones; {result['symbols_with_events']} con publicaciones SEC.",
             f"Sin publicaciones en el archivo: {', '.join(result['no_event_symbols']) or 'ninguna'}.",
             f"Decisiones: {result['decision_statuses']}.", "",
             "## Comparación principal", "",
             "Cada fila compara exactamente los mismos eventos. Se excluyen ventanas superpuestas de 20 ruedas",
             "por acción antes de ver las decisiones y los resultados. Las medias son retornos por evento,",
             "**no rentabilidad de una cartera financiada ni PnL de la cuenta**.", "",
             "| Ruedas | Pares | BUY / CASH | Agente neto | HOLD neto | SMA20 neto | Agente − HOLD | Agente − SMA20 | Inmaduros |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for h, s in main.items():
        lines.append(f"| {h} | {s['paired']}/{s['total']} | {s['buy']} / {s['cash']} | "
                     f"{pct(s['agent_mean_net'])} | {pct(s['hold_mean_net'])} | {pct(s['baseline_mean_net'])} | "
                     f"{points(s['agent_minus_hold'])} | {points(s['agent_minus_baseline'])} | {s['statuses'].get('IMMATURE', 0)} |")
    lines += ["", "## ¿Anticipación o reacción?", "",
              "| Ruedas | Compras maduras | Positivas netas | Retorno neto compras | Exceso frente a IVV | Subida previa 5 ruedas | Movimiento hasta entrada |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for h, s in main.items():
        lines.append(f"| {h} | {s['buy']} | {s['buys_positive']}/{s['buy']} | {pct(s['buys_mean_net'])} | "
                     f"{points(s['buys_mean_excess_ivv'])} | {pct(s['buys_prior5_mean'])} | {pct(s['buys_announcement_to_entry_mean'])} |")
    lines += ["", "## Calidad de las decisiones", "",
              "Estos controles diagnósticos no eliminan decisiones ni cambian los retornos. La evaluación conserva",
              "lo que respondió el agente, incluidos sus errores. Las explicaciones generadas no son evidencia",
              "independiente: deben contrastarse con las citas y el contexto archivados.", ""]
    quality_labels = {
        "HISTORICAL_TRANSACTION_AS_NEW": "acuerdo de un año anterior tratado como una tesis nueva",
        "PM_NUMBERS_NOT_IN_SUPPLIED_CONTEXT": "el PM agregó cifras ausentes del contexto que recibió",
    }
    for item in result["decision_quality_flags"]:
        lines.append(f"- **{item['symbol']} ({item['published_at'][:10]})**: " +
                     "; ".join(quality_labels.get(flag["code"], flag["code"]) for flag in item["flags"]) + ".")
    if not result["decision_quality_flags"]:
        lines.append("- Sin alertas de estos controles acotados; esto no certifica la calidad de todas las decisiones.")
    lines += ["", "La subida previa termina en el último cierre anterior a la aceptación SEC. El movimiento",
              "hasta entrada incluye el lapso entre ese cierre y la primera apertura ejecutable; con datos diarios",
              "no puede atribuirse causalmente al anuncio. Una compra positiva no demuestra información no incorporada:",
              "debe superar a los comparadores y repetirse en una muestra prospectiva mayor.", "",
              "## Reglas fijadas antes de calcular resultados", "",
              "- Acciones subyacentes en USD, elegidas por peso IVV del 31/08, sin seleccionar por retornos.",
              "- Research → Contrarian → PM con qwen3.5:9b local. Sin API paga, herramientas ni precios futuros en los prompts.",
              "- Una publicación SEC por evento; documentos y anexos del mismo accession se agrupan.",
              "- Entrada: primera apertura regular posterior a aceptación SEC + 60 minutos.",
              "- Salida: cierre de la quinta, décima o vigésima rueda, incluyendo la rueda de entrada.",
              "- Comisión hipotética 0,60% y deslizamiento 0,15% por lado. No son costos reales de un broker estadounidense.",
              "- Dividendos en efectivo: ex-fechas posteriores al día de entrada y hasta el día de salida; sin reinversión.",
              "- HOLD compra la misma acción en las mismas fechas para todos los eventos, incluso cuando el agente elige CASH.",
              "- SMA20 usa solo cierres anteriores al corte de decisión. CASH rinde 0; un error del modelo queda UNKNOWN.",
              "- Los tres roles usan el mismo modelo: son llamadas separadas, no verificadores independientes.",
              "- Un split en el intervalo de captura bloquea la medición del símbolo hasta verificar sus ajustes.",
              "- Retornos faltantes e inmaduros se conservan como ausentes, nunca como 0%.",
              "", "## Límites de interpretación", ""]
    lines.extend("- " + item for item in result["limitations"])
    lines += ["", "## Evidencia reproducible", "",
              "- [Protocolo y costos](protocol.json), [eventos y citas](events.json), [captura de precios](market.json).",
              "- [Resultado agregado](result.json), [todos los resultados CSV](outcomes.csv), [resultados JSON](outcomes.json).",
              "- Directorios calls/ y decisions/: prompts, respuestas, hashes, fuentes y fechas reales de procesamiento.",
              "- En una copia de evaluations/, las carpetas calls/, decisions/ y market_raw/ se consultan en la raíz de la corrida.",
              "- Calendario: [NYSE](https://www.nyse.com/trade/hours-calendars). Precios: endpoints públicos Yahoo Chart archivados en market_raw/.",
              "", "## Todas las compras seleccionadas", "",
              "| Acción | Publicación SEC | Entrada | Principal sin solapamiento | 5 ruedas neto | 10 ruedas neto | 20 ruedas neto |",
              "|---|---|---|---|---:|---:|---:|"]
    buys = {}
    for row in rows:
        if row["action"] == "BUY":
            buys.setdefault(row["event_id"], {})[row["horizon"]] = row
    for by_horizon in buys.values():
        r = by_horizon[5]
        values = [pct(by_horizon[h]["hold_net"]) if by_horizon[h]["status"] == "EVALUABLE"
                  else by_horizon[h]["status"] for h in (5, 10, 20)]
        lines.append(f"| {r['symbol']} | {r['published_at']} | {r['entry_date']} | "
                     f"{'Sí' if r['primary_nonoverlap'] else 'No'} | {' | '.join(values)} |")
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
