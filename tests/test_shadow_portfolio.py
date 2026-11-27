from datetime import datetime, timedelta, timezone

from src.analysis.shadow_portfolio import max_drawdown_pct, render_shadow, simulate_shadow
from src.core.telegram_format import validate_telegram_html

UTC = timezone.utc


def _at(day, hour, minute=0):
    # 13:00 UTC = 10:00 ART
    return datetime(2026, 9, day, hour + 3, minute, tzinfo=UTC)


def _quotes(ticker, prices):
    rows = []
    for day, price in prices.items():
        for hour in (10, 11, 12, 13, 14, 15, 16):
            rows.append({"ts": _at(day, hour, 30), "ticker": ticker, "asset_type": "CEDEAR",
                         "currency": "ARS", "last_price": price})
    return rows


def _inputs(orders, *, positions=None, cash=1_000_000):
    prices_mu = {1: 100.0, 2: 100.0, 3: 80.0, 4: 70.0, 7: 70.0}
    prices_spy = {1: 1000.0, 2: 1010.0, 3: 1020.0, 4: 1030.0, 7: 1040.0}
    prices_aapl = {1: 50.0, 2: 50.0, 3: 55.0, 4: 60.0, 7: 66.0}
    return {
        "snapshot": {"scraped_at": _at(1, 10, 45), "cash": cash, "total": 1_500_000},
        "positions": positions if positions is not None else [
            {"ticker": "MU", "asset_type": "CEDEAR", "currency": "ARS", "quantity": 10_000, "market_value": 1_000_000},
        ],
        "orders": orders,
        "navs": [{"scraped_at": _at(1, 10, 45), "nav": 2_000_000.0}],
        "intraday_points": sorted(
            _quotes("MU", prices_mu) + _quotes("SPY", prices_spy) + _quotes("AAPL", prices_aapl),
            key=lambda r: (r["ts"], r["ticker"]),
        ),
        "daily_candles": [],
        "until": _at(7, 16, 59),
        "benchmark": "SPY",
    }


def _sell_mu(plan_id, at):
    # Real account: MU is 50% of a 2M NAV; the bot sells 600k -> target weight 20%.
    return {"plan_id": plan_id, "created_at": at, "ticker": "MU", "side": "SELL", "amount_ars": 600_000.0,
            "quantity_est": 6000.0, "reason": "recorte", "current_weight": 0.5, "target_weight": 0.2}


def test_shadow_follows_a_sell_and_beats_holding_a_falling_stock():
    report = simulate_shadow(_inputs([_sell_mu("p1", _at(2, 11))]))
    hold, bot = report.paths["hold"].final, report.paths["all"].final
    assert report.plans_used == 1
    assert len(report.paths["all"].fills) == 1 and report.paths["all"].fills[0]["side"] == "SELL"
    # MU falls from 100 to 70; selling 60% at ~100 beats holding.
    assert bot["return_pct"] > hold["return_pct"]
    assert hold["return_pct"] < 0


def test_repeated_plans_are_idempotent_in_the_shadow():
    once = simulate_shadow(_inputs([_sell_mu("p1", _at(2, 11))]))
    thrice = simulate_shadow(_inputs([_sell_mu("p1", _at(2, 11)), _sell_mu("p2", _at(2, 13)), _sell_mu("p3", _at(2, 16))]))
    sold_once = sum(f["quantity"] for f in once.paths["all"].fills)
    sold_thrice = sum(f["quantity"] for f in thrice.paths["all"].fills)
    # The same target weight three times must not sell three times.
    assert sold_thrice == sold_once


def test_sells_only_variant_ignores_buys_and_benchmark_is_measured():
    buy = {"plan_id": "p2", "created_at": _at(3, 11), "ticker": "AAPL", "side": "BUY", "amount_ars": 400_000.0,
           "quantity_est": 8000.0, "reason": "abrir", "current_weight": 0.0, "target_weight": 0.2}
    report = simulate_shadow(_inputs([_sell_mu("p1", _at(2, 11)), buy]))
    assert any(f["side"] == "BUY" for f in report.paths["all"].fills)
    assert not any(f["side"] == "BUY" for f in report.paths["sells_only"].fills)
    assert report.benchmark[-1]["return_pct"] > 3.9
    text = render_shadow(report)
    assert validate_telegram_html(text)[0] is True
    assert "Solo sus ventas" in text and "Bot menos no operar" in text and "SPY" in text


def test_positions_without_ars_price_are_excluded_and_reported():
    positions = [
        {"ticker": "MU", "asset_type": "CEDEAR", "currency": "ARS", "quantity": 10_000, "market_value": 1_000_000},
        {"ticker": "AL30D", "asset_type": "BONO", "currency": "USD", "quantity": 100, "market_value": 50_000},
    ]
    report = simulate_shadow(_inputs([], positions=positions))
    assert report.excluded_positions == ["AL30D"]
    assert any("AL30D" in w for w in report.warnings)


def test_max_drawdown_on_daily_nav():
    daily = [{"nav_ars": v} for v in (100, 120, 90, 130)]
    assert abs(max_drawdown_pct(daily) - (-25.0)) < 1e-9


def test_telegram_handler_parses_the_start_date(monkeypatch):
    import asyncio
    from types import SimpleNamespace

    import scripts.telegram_bot as bot

    calls, sent = [], []

    async def allowed(update, context):
        return True

    async def loading(update, text):
        return None

    async def run_cmd(cmd, timeout):
        calls.append(cmd)
        return 0, "<b>ok</b>", "", 0.1

    async def send_text(context, chat_id, text):
        sent.append(text)

    monkeypatch.setattr(bot, "ensure_allowed_chat", allowed)
    monkeypatch.setattr(bot, "answer_loading", loading)
    monkeypatch.setattr(bot, "run_cmd", run_cmd)
    monkeypatch.setattr(bot, "send_text", send_text)
    update = SimpleNamespace(effective_chat=SimpleNamespace(id=1))

    asyncio.run(bot.shadow_portfolio_handler(update, SimpleNamespace(args=["desde=01/07/2026"])))
    assert calls[-1][-2:] == ["--desde", "01/07/2026"]
    asyncio.run(bot.shadow_portfolio_handler(update, SimpleNamespace(args=[])))
    assert "--desde" not in calls[-1]
    asyncio.run(bot.shadow_portfolio_handler(update, SimpleNamespace(args=["ayer"])))
    assert len(calls) == 2 and "desde=01/07/2026" in sent[-1]


def test_orders_on_positions_the_shadow_lacks_are_counted_and_overlap_reported():
    from src.analysis.shadow_portfolio import overlap, real_weights_by_day

    sell_aapl = {"plan_id": "p9", "created_at": _at(3, 11), "ticker": "AAPL", "side": "SELL", "amount_ars": 100_000.0,
                 "quantity_est": 2000.0, "reason": "x", "current_weight": 0.1, "target_weight": 0.05}
    inputs = _inputs([_sell_mu("p1", _at(2, 11)), sell_aapl])
    inputs["real_weights"] = real_weights_by_day([
        {"scraped_at": _at(d, 16), "cash": 100_000, "ticker": "AAPL", "market_value": 900_000} for d in (1, 2, 3, 4, 7)
    ])
    report = simulate_shadow(inputs)
    assert report.order_outcomes["applied"] == 1
    assert report.order_outcomes["no_position"] == 1  # the real account sells AAPL; the shadow never had it
    # Shadow holds MU + cash; real holds AAPL + 10% cash: they only share that cash.
    assert abs(report.paths["all"].daily[-1]["overlap"] - 0.1) < 1e-9
    text = render_shadow(report)
    assert "sin la posición en la sombra 1" in text and "comparten menos de la mitad" in text
    assert overlap({"A": 0.6, "B": 0.4}, {"A": 0.5, "C": 0.5}) == 0.5


def test_identical_books_overlap_fully():
    from src.analysis.shadow_portfolio import overlap

    assert overlap({"MU": 0.5, "_CASH": 0.5}, {"MU": 0.5, "_CASH": 0.5}) == 1.0
