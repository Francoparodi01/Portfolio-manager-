"""
Orquestador del pipeline cuantitativo completo:
Tecnico → Macro → Sentiment → Riesgo → Sintesis → Telegram

Uso:
    python scripts/run_analysis.py
    python scripts/run_analysis.py --tickers CVX NVDA
    python scripts/run_analysis.py --period 1y
    python scripts/run_analysis.py --no-llm        # sin Claude API
    python scripts/run_analysis.py --no-sentiment  # sin noticias RSS
    python scripts/run_analysis.py --no-telegram
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
from src.analysis.synthesis import blend_scores, synthesize_with_llm, build_full_report

logger = get_logger(__name__)


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
        # get_portfolio_history debe retornar lista de dicts con total_value_ars
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
        total_ars, cash_ars, history = 0.0, 0.0, []
    else:
        positions, total_ars, cash_ars, history = await get_portfolio_data(cfg)

    tickers = [p["ticker"] for p in positions]
    logger.info(f"Pipeline: {tickers} | periodo={period}")

    # 2. Macro
    logger.info("Descargando macro...")
    macro_snap   = fetch_macro()
    macro_regime = get_macro_regime(macro_snap)
    logger.info(f"Regimen: {macro_regime}")

    # 3. Tecnico + precios historicos para risk engine
    logger.info("Calculando tecnico...")
    tech_signals = analyze_portfolio(tickers, period=period)
    tech_map     = {s.ticker: s for s in tech_signals}
    prices_map   = {}
    for ticker in tickers:
        df = fetch_history(ticker, period=period)
        if df is not None and "Close" in df.columns:
            prices_map[ticker] = df["Close"].squeeze()

    # 4. Riesgo
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

    # 6. Sintesis
    logger.info("Sintetizando...")
    results = []
    for ticker in tickers:
        tech   = tech_map.get(ticker)
        risk_p = risk_map.get(ticker, {})
        sent   = sentiment_map.get(ticker)
        macro_score, macro_reasons = score_macro_for_ticker(ticker, macro_snap)

        if not tech:
            continue

        result = blend_scores(
            ticker=ticker,
            technical_signal=tech.signal,
            technical_strength=tech.strength,
            macro_score=macro_score,
            risk_position=risk_p,
            sentiment_score=sent.score if sent else 0.0,
        )

        if not no_llm:
            result = synthesize_with_llm(
                result=result,
                macro_snap=macro_snap,
                macro_reasons=macro_reasons,
                technical_reasons=tech.reasons,
                sentiment_headlines=sent.top_headlines if sent else [],
                risk_position=risk_p,
                portfolio_context={"total_ars": total_ars, "cash_ars": cash_ars, "regime": macro_regime},
            )
        results.append(result)

    # 7. Consola
    print("\n" + "="*70)
    for r in results:
        icon = {"BUY":"🟢🟢","ACCUMULATE":"🟢","HOLD":"🟡","REDUCE":"🔴","SELL":"🔴🔴"}.get(r.decision,"⚪")
        print(f"\n{icon} {r.ticker} → {r.decision}  score={r.final_score:+.3f}  conf={r.confidence:.0%}  size={r.position_size:.1%}")
        for layer in r.layers:
            print(f"   {layer.name:10s} raw={layer.raw_score:+.3f} × {layer.weight:.0%} = {layer.weighted:+.4f}")
        if r.reasoning:
            print(f"   LLM: {r.reasoning[:250]}")

    print(f"\n--- RISK ENGINE | drawdown={portfolio_risk.drawdown_current:.1%} ({portfolio_risk.drawdown_status})")
    for p in portfolio_risk.positions:
        print(f"  {p['ticker']} vol={p['volatility_annual']:.0%} sharpe={p['sharpe']:.2f} "
              f"actual={p['current_pct']:.1%} → sugerido={p['suggested_pct_adj']:.1%} ({p['action']})")
    print("="*70)

    # 8. Telegram
    if not no_telegram and cfg.scraper.telegram_enabled:
        notifier.send_raw(build_full_report(results, macro_snap, total_ars))
        notifier.send_raw(portfolio_risk.to_telegram())
        logger.info("Reportes enviados a Telegram")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--tickers",      nargs="+", default=[])
    p.add_argument("--period",       default="6mo", choices=["1mo","3mo","6mo","1y","2y"])
    p.add_argument("--no-telegram",  action="store_true")
    p.add_argument("--no-llm",       action="store_true")
    p.add_argument("--no-sentiment", action="store_true")
    args = p.parse_args()
    asyncio.run(main(args.tickers, args.period, args.no_telegram, args.no_llm, args.no_sentiment))