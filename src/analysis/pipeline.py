async def run_quant_pipeline(snapshot):

    tickers = [p.ticker for p in snapshot.positions]

    tech = analyze_portfolio(tickers)
    macro = get_macro_context()
    sent = get_sentiment_batch(tickers)
    risk = compute_portfolio_risk(snapshot)

    decisions = synthesize_portfolio(
        snapshot=snapshot,
        technical=tech,
        macro=macro,
        sentiment=sent,
        risk=risk,
    )

    return decisions