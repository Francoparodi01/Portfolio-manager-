import asyncio
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


def _load():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_confidence_audit.py"
    spec = importlib.util.spec_from_file_location("run_confidence_audit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Conn:
    def __init__(self, decisions):
        self.decisions = decisions
        self.sql = []

    async def fetchrow(self, sql, *args):
        self.sql.append(sql)
        now = datetime.now(tz=timezone.utc)
        if "FROM portfolio_snapshots\n            ORDER BY" in sql:
            return {"scraped_at": now, "total_value_ars": 1.0, "cash_ars": 0.0, "confidence_score": 1.0}
        if "FROM portfolio_snapshots" in sql:
            return {"last_24h": 1, "last_7d": 5}
        if "latest_price_day" in sql:
            return {"business_day": now.date(), "latest_candle_day": now.date(), "price_assets": 10,
                    "internal_candles": 10, "latest_candle_assets": 10, "missing_internal": 0}
        if "FROM market_prices" in sql:
            return {"latest_ts": now, "rows_24h": 1, "rows_7d": 10, "tickers_7d": 10}
        if "FROM decision_log" in sql:
            return self.decisions
        return {"total": 0, "reconciled": 0, "unreconciled": 0, "latest_executed_at": now - timedelta(days=1)}

    async def fetchval(self, sql, *args):
        return False

    async def close(self):
        return None


def test_confidence_verdict_counts_only_executed_canonical_outcomes(monkeypatch):
    module = _load()
    decisions = {
        "total": 500, "execution_plan": 100, "approved_executable": 10, "executed": 20,
        "executed_manual": 5, "blocked": 30, "optimizer": 200, "radar": 150,
        # Plenty of outcomes overall (radar/optimizer), but only 8 from executed decisions.
        "closed_5d": 400, "closed_10d": 300, "closed_20d": 200, "closed_5d_real": 8,
    }
    conn = _Conn(decisions)

    async def connect(_dsn):
        return conn

    monkeypatch.setattr(module.asyncpg, "connect", connect)
    monkeypatch.setattr(
        module, "get_config", lambda: SimpleNamespace(database=SimpleNamespace(url="postgresql://x"))
    )
    text = asyncio.run(module.build_confidence_audit(days=180))
    assert "CONFIABLE" not in text
    assert "Outcomes 5d ejecutados (canonicos): <b>8</b>" in text
    assert "faltan al menos 12 outcomes 5d de operaciones ejecutadas" in text
    decision_sql = next(sql for sql in conn.sql if "FROM decision_log" in sql)
    assert "legacy_external" in decision_sql and "closed_5d_real" in decision_sql
