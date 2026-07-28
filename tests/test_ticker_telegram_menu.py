from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_main_menu_exposes_ticker_analysis_button_and_command():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")

    assert '("ticker", "Analisis tecnico por accion")' in source
    assert 'callback_data="ticker_analysis"' in source
    assert '"ticker_analysis": action_ticker_prompt' in source
    assert 'CommandHandler("ticker",' in source
    assert 'CommandHandler("tecnico",' in source
    assert '"scripts/run_ticker_analysis.py"' in source


def test_ticker_button_is_read_only_prompt():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    action_start = source.index("async def action_ticker_prompt")
    action_end = source.index("async def action_ticker_analysis", action_start)
    action = source[action_start:action_end]

    assert "/ticker NVDA" in action
    assert "no guarda decision_log" in action
    assert "no cambia thresholds" in action


def test_ticker_action_sends_multiple_chart_paths():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    action_start = source.index("async def action_ticker_analysis")
    action_end = source.index("async def action_performance", action_start)
    action = source[action_start:action_end]

    assert "_ticker_chart_paths(out, chart_path)" in action
    assert "for index, path in enumerate(chart_paths" in action
    assert "send_photo" in action
    assert "line.startswith(\"[chart]\")" in action
