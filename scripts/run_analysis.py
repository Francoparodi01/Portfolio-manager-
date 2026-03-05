"""
scripts/run_analysis.py — Pipeline cuantitativo completo

COMANDOS:
    python scripts/run_analysis.py
    python scripts/run_analysis.py --tickers CVX NVDA
    python scripts/run_analysis.py --period 1y
    python scripts/run_analysis.py --no-llm
    python scripts/run_analysis.py --no-sentiment
    python scripts/run_analysis.py --no-telegram
    python scripts/run_analysis.py --no-llm --no-sentiment
"""
import argparse, asyncio, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.technical import analyze_portfolio, fetch_history
from src.analysis.macro     import fetch_macro, score_macro_for_ticker, get_macro_regime
from src.analysis.sentiment import fetch_sentiment
from src.analysis.risk      import build_portfolio_risk_report
from src.analysis.synthesis import SynthesisResult, LayerScore, build_full_report

import requests
import numpy as np

logger = get_logger(__name__)

LAYER_WEIGHTS   = {"technical": 0.30, "macro": 0.30, "risk": 0.25, "sentiment": 0.15}
ANTHROPIC_API   = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL    = "claude-sonnet-4-20250514"


def _map_tech(signal: str, strength: float, score_raw: float = 0.0) -> float:
    """BUY/SELL usan strength con signo. HOLD usa score_raw normalizado."""
    if signal == "BUY":
        return min(strength, 1.0)
    elif signal == "SELL":
        return -min(strength, 1.0)
    else:
        return float(max(min(score_raw / 9.0, 1.0), -1.0))


def blend_scores_local(ticker, technical_signal, technical_strength, macro_score,
                       risk_position, sentiment_score, technical_score_raw=0.0):
    layers = []

    tech_raw = _map_tech(technical_signal, technical_strength, technical_score_raw)
    layers.append(LayerScore("technical", tech_raw, LAYER_WEIGHTS["technical"],
                             tech_raw * LAYER_WEIGHTS["technical"]))

    layers.append(LayerScore("macro", macro_score, LAYER_WEIGHTS["macro"],
                             macro_score * LAYER_WEIGHTS["macro"]))

    rl = risk_position.get("risk_level", "NORMAL")
    rp = {"LOW": 0.05, "NORMAL": 0.00, "ELEVATED": 0.00,
          "HIGH": -0.10, "EXTREME": -0.40}.get(rl, 0.0)
    warnings = risk_position.get("warnings", [])
    if any("drawdown" in w.lower() for w in warnings):
        rp -= 0.10
    layers.append(LayerScore("risk", rp, LAYER_WEIGHTS["risk"],
                             rp * LAYER_WEIGHTS["risk"], reasons=warnings))

    layers.append(LayerScore("sentiment", sentiment_score, LAYER_WEIGHTS["sentiment"],
                             sentiment_score * LAYER_WEIGHTS["sentiment"]))

    final = float(np.clip(sum(l.weighted for l in layers), -1.0, 1.0))

    if   final >=  0.40: decision = "BUY"
    elif final >=  0.15: decision = "ACCUMULATE"
    elif final <= -0.40: decision = "SELL"
    elif final <= -0.15: decision = "REDUCE"
    else:                decision = "HOLD"

    signs     = [1 if l.weighted > 0.01 else (-1 if l.weighted < -0.01 else 0) for l in layers]
    consensus = abs(sum(signs)) / len(signs) if signs else 0.0
    conf      = min(abs(final) * 0.6 + consensus * 0.4, 0.95)

    sug = risk_position.get("suggested_pct_adj", 0.05)
    if decision in ("BUY", "ACCUMULATE"):
        ps = sug * (0.5 + conf * 0.5)
    elif decision in ("SELL", "REDUCE"):
        ps = sug * max(0.1, 0.5 - conf * 0.4)
    else:
        ps = sug

    return SynthesisResult(
        ticker=ticker, decision=decision,
        confidence=round(conf, 4), final_score=round(final, 4),
        position_size=round(float(np.clip(ps, 0, 0.25)), 4),
        layers=layers,
    )


def synthesize_with_llm_local(result, macro_snap, macro_reasons, technical_reasons,
                               sentiment_headlines, risk_position, portfolio_context):
    try:
        macro_dict = macro_snap.to_dict() if hasattr(macro_snap, "to_dict") else {}
        headlines_str = "\n".join(
            f"  [{h.get('source','')}] {h.get('title','')} ({h.get('score',0):+.1f})"
            for h in sentiment_headlines[:5]
        ) or "  Sin noticias"

        # Acceder a layers por nombre, no por índice
        layers_by_name = {l.name: l for l in result.layers}
        tech_layer  = layers_by_name.get("technical")
        macro_layer = layers_by_name.get("macro")
        sent_layer  = layers_by_name.get("sentiment")

        prompt = f"""Eres un analista cuantitativo senior. Activo: {result.ticker}.
Decision del sistema: {result.decision} (score={result.final_score:+.2f}, confianza={result.confidence:.0%}).

Capas:
- Tecnico (30%): {tech_layer.raw_score:+.2f} — {'; '.join(technical_reasons[:3]) or 'N/A'}
- Macro (30%): {macro_layer.raw_score:+.2f} — {'; '.join(macro_reasons[:3]) or 'N/A'}
- Riesgo (25%): vol={risk_position.get('volatility_annual',0):.0%}, sharpe={risk_position.get('sharpe',0):.2f}
- Sentiment (15%): {sent_layer.raw_score:+.2f if sent_layer else 'N/A'}

Macro: WTI ${macro_dict.get('wti','?')} ({macro_dict.get('wti_chg',0):+.1f}%), VIX {macro_dict.get('vix','?')} ({macro_dict.get('vix_chg',0):+.1f}%), DXY {macro_dict.get('dxy','?')}, SP500 {macro_dict.get('sp500_chg',0):+.1f}%

Noticias:
{headlines_str}

Portfolio: posicion actual {risk_position.get('current_pct',0):.1%}, total ${portfolio_context.get('total_ars',0):,.0f} ARS.

Responde en 3 oraciones en español:
1. Valida o cuestiona la decision considerando el contexto macro.
2. El factor mas importante que el modelo puede subestimar.
3. Accion concreta recomendada."""

        resp = requests.post(
            ANTHROPIC_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 300,
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            text = ""
            for block in data.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block["text"].strip()
                    break
            if text:
                result.reasoning = text
                result.llm_used  = True
                logger.info(f"LLM {result.ticker}: ok")
        else:
            logger.warning(f"Claude API {resp.status_code} para {result.ticker}")
    except Exception as e:
        logger.warning(f"LLM fallo {result.ticker}: {e}")
    return result


async def get_portfolio_data(cfg):
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        snap = await db.get_latest_snapshot()
        if not snap:
            logger.error("Sin snapshots — correr run_once.py primero")
            sys.exit(1)
        positions = snap.get("positions", [])
        total_ars = float(snap.get("total_value_ars", 0))
        cash_ars  = float(snap.get("cash_ars", 0))
        history   = []
        if hasattr(db, "get_portfolio_history"):
            history = await db.get_portfolio_history(limit=60)
        return positions, total_ars, cash_ars, history
    finally:
        await db.close()


async def main(tickers_override, period, no_telegram, no_llm, no_sentiment):
    cfg      = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)

    # 1. Posiciones
    if tickers_override:
        positions = [{"ticker": t, "market_value": 0} for t in tickers_override]
        total_ars = cash_ars = 0.0
        history   = []
    else:
        positions, total_ars, cash_ars, history = await get_portfolio_data(cfg)

    tickers = [p["ticker"] for p in positions]
    logger.info(f"Pipeline: {tickers} | periodo={period}")

    # 2. Macro
    logger.info("Descargando macro...")
    macro_snap   = fetch_macro()
    macro_regime = get_macro_regime(macro_snap)
    logger.info(f"Regimen: {macro_regime}")

    # 3. Tecnico + precios
    logger.info("Calculando tecnico...")
    tech_signals = analyze_portfolio(tickers, period=period)
    tech_map     = {s.ticker: s for s in tech_signals}
    prices_map   = {}
    for ticker in tickers:
        df = fetch_history(ticker, period=period)
        if df is not None and "Close" in df.columns:
            prices_map[ticker] = df["Close"].squeeze()

    # 4. Risk
    logger.info("Calculando riesgo...")
    portfolio_risk = build_portfolio_risk_report(
        positions=positions, prices_map=prices_map,
        total_ars=total_ars, cash_ars=cash_ars,
        history=history, vix=macro_snap.vix,
    )
    risk_map = {p["ticker"]: p for p in portfolio_risk.positions}

    # 5. Sentiment
    sentiment_map = {}
    if not no_sentiment:
        logger.info("Analizando sentiment...")
        for ticker in tickers:
            sentiment_map[ticker] = fetch_sentiment(ticker)
    else:
        logger.info("Sentiment omitido (--no-sentiment)")

    # 6. Sintesis
    logger.info("Sintetizando...")
    results = []
    for ticker in tickers:
        tech   = tech_map.get(ticker)
        risk_p = risk_map.get(ticker, {
            "risk_level": "NORMAL", "warnings": [],
            "suggested_pct_adj": 0.10, "current_pct": 0.25,
            "volatility_annual": 0.0, "sharpe": 0.0, "action": "MANTENER",
        })
        sent         = sentiment_map.get(ticker)
        macro_score, macro_reasons = score_macro_for_ticker(ticker, macro_snap)

        if not tech:
            logger.warning(f"Sin datos tecnicos para {ticker}")
            continue

        score_raw = getattr(tech, "score_raw", 0.0)

        result = blend_scores_local(
            ticker=ticker,
            technical_signal=tech.signal,
            technical_strength=tech.strength,
            macro_score=macro_score,
            risk_position=risk_p,
            sentiment_score=sent.score if sent else 0.0,
            technical_score_raw=score_raw,
        )

        if not no_llm:
            result = synthesize_with_llm_local(
                result=result,
                macro_snap=macro_snap,
                macro_reasons=macro_reasons,
                technical_reasons=tech.reasons,
                sentiment_headlines=sent.top_headlines if sent else [],
                risk_position=risk_p,
                portfolio_context={"total_ars": total_ars, "cash_ars": cash_ars,
                                   "regime": macro_regime},
            )

        results.append(result)

    # 7. Consola
    print("\n" + "=" * 70)
    for r in results:
        icon = {"BUY":"🟢🟢","ACCUMULATE":"🟢","HOLD":"🟡","REDUCE":"🔴","SELL":"🔴🔴"}.get(r.decision,"⚪")
        print(f"\n{icon}  {r.ticker:6s}  {r.decision:10s}  score={r.final_score:+.4f}  conf={r.confidence:.0%}  size={r.position_size:.1%}  llm={'si' if r.llm_used else 'no'}")
        for layer in r.layers:
            sign = "+" if layer.weighted >= 0 else ""
            print(f"   {layer.name:10s}  raw={layer.raw_score:+.3f} × {layer.weight:.0%} = {sign}{layer.weighted:.4f}")
        if r.reasoning:
            print(f"   🧠 {r.reasoning[:300]}")
        for w in r.warnings:
            print(f"   ⚠️  {w}")

    print(f"\n{'─'*70}")
    print(f"  RISK  drawdown={portfolio_risk.drawdown_current:.1%} ({portfolio_risk.drawdown_status})  vix={portfolio_risk.vix_level}")
    for p in portfolio_risk.positions:
        print(f"  {p['ticker']:6s}  vol={p['volatility_annual']:.0%}  sharpe={p['sharpe']:.2f}  "
              f"actual={p['current_pct']:.1%}  sugerido={p['suggested_pct_adj']:.1%}  → {p['action']}")
    print("=" * 70)

    # 8. Telegram
    if not no_telegram and cfg.scraper.telegram_enabled:
        logger.info("Enviando a Telegram...")
        report = build_full_report(results, macro_snap, total_ars, portfolio_risk)
        notifier.send_raw(report)
        logger.info("Reporte enviado")
    else:
        logger.info("Telegram omitido")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--tickers",      nargs="+", default=[])
    p.add_argument("--period",       default="6mo", choices=["1mo","3mo","6mo","1y","2y"])
    p.add_argument("--no-telegram",  action="store_true")
    p.add_argument("--no-llm",       action="store_true")
    p.add_argument("--no-sentiment", action="store_true")
    args = p.parse_args()
    asyncio.run(main(args.tickers, args.period, args.no_telegram, args.no_llm, args.no_sentiment))