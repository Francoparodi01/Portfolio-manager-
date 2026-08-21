from __future__ import annotations

import asyncio
import inspect

import pytest

from src.analysis.radar_exploratory import (
    RADAR_EXPLORATORY_SCHEMA_SQL,
    RadarExploratoryStore,
    exploratory_callback,
    parse_compact_radar_candidates,
    parse_exploratory_callback,
)
from src.scheduler import runner


COMPACT_REPORT = """\
🔭 <b>Radar de oportunidades — compacto</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Universo: 249
Gate: <b>OPERABLE</b> | VIX 15.0

<b>Top ideas</b>
1. 🆕 <b>FSLR</b> | score <code>+0.211</code> | edge <code>+0.044</code> | R/R 5.7x
   💰 Sizing aprox: <b>$219.485 ARS</b>
   Compra V3: <b>A</b> · compra primaria · 20d · shadow
   ↩️ Reversión: sin extremo claro (+0.000)
   Idea radar: evaluar entrada
2. 🔄 <b>AMZN</b> | score <code>+0.199</code> | edge <code>+0.159</code> | R/R 1.4x
   Compra V3: <b>C</b> · esperar setup · 20d · shadow
   Idea radar: esperar confirmación
"""

MODERN_COMPACT_REPORT = """\
🔭 <b>Radar · próxima apertura</b>
250 analizados · 2 ideas detectadas · estado <b>NORMAL</b>

1. 🟡 <b>FSLR</b> · V3 <b>A</b> (compra primaria)
   Señal <code>+0.211</code> · R/R <code>5.7x</code>
   A favor: RS fuerte vs SPY (+8.9% en 20d)
   Próximo paso: evaluar entrada

2. ⛔ <b>GLOB</b> · V3 <b>rechazada</b> (no elegible para compra)
   Señal <code>+0.155</code> · R/R <code>2.8x</code>
   Motivo: no mejora la cartera actual
"""


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_args):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


def test_compact_report_parser_preserves_visible_v3_evidence():
    candidates = parse_compact_radar_candidates(COMPACT_REPORT)

    assert [candidate.ticker for candidate in candidates] == ["FSLR", "AMZN"]
    assert candidates[0].rank_position == 1
    assert candidates[0].radar_score == pytest.approx(0.211)
    assert candidates[0].edge == pytest.approx(0.044)
    assert candidates[0].risk_reward == pytest.approx(5.7)
    assert candidates[0].v3_tier == "A"
    assert candidates[0].v3_classification == "compra primaria"
    assert candidates[0].action_text == "evaluar entrada"
    assert "FSLR" in candidates[0].evidence["rendered_block"]


def test_modern_compact_report_parser_normalizes_rejected_tier():
    candidates = parse_compact_radar_candidates(MODERN_COMPACT_REPORT)

    assert [candidate.ticker for candidate in candidates] == ["FSLR", "GLOB"]
    assert candidates[0].v3_tier == "A"
    assert candidates[0].risk_reward == pytest.approx(5.7)
    assert candidates[0].edge is None
    assert candidates[0].action_text == "evaluar entrada"
    assert candidates[1].v3_tier == "REJECTED"


def test_callbacks_have_a_separate_bounded_namespace():
    assert exploratory_callback("follow", 91) == "re:follow:91"
    assert exploratory_callback("dismiss", 91) == "re:dismiss:91"
    assert parse_exploratory_callback("re:follow:91") == ("follow", 91)
    assert parse_exploratory_callback("rs:follow:91") is None
    assert parse_exploratory_callback("re:follow:not-an-id") is None


def test_schema_is_exploratory_and_does_not_touch_official_sources():
    assert "radar_exploratory_runs" in RADAR_EXPLORATORY_SCHEMA_SQL
    assert "radar_exploratory_candidates" in RADAR_EXPLORATORY_SCHEMA_SQL
    assert "CHECK (metric_scope = 'exploratory')" in RADAR_EXPLORATORY_SCHEMA_SQL
    assert "CHECK (is_primary_metric = FALSE)" in RADAR_EXPLORATORY_SCHEMA_SQL
    assert "decision_log" not in RADAR_EXPLORATORY_SCHEMA_SQL
    assert "radar_discovery_runs" not in RADAR_EXPLORATORY_SCHEMA_SQL
    assert "radar_discovery_snapshots" not in RADAR_EXPLORATORY_SCHEMA_SQL


def test_report_deduplication_is_scoped_to_scoring_version():
    save_source = inspect.getsource(RadarExploratoryStore.save_report)
    assert "ON CONFLICT (owner_chat_id, report_hash, scoring_version)" in save_source
    assert "AND scoring_version=$3" in save_source


@pytest.mark.parametrize(
    ("requested", "stored", "conflict"),
    [("FOLLOW", "FOLLOW", False), ("DISMISS", "FOLLOW", True)],
)
def test_first_manual_user_action_is_immutable(requested, stored, conflict):
    class _Connection:
        def __init__(self):
            self.fetchrow_calls = []

        async def execute(self, *_args):
            return "OK"

        async def fetchrow(self, statement, *_args):
            self.fetchrow_calls.append(statement)
            if "UPDATE radar_exploratory_candidates" in statement:
                return None
            return {"id": 91, "owner_chat_id": 123, "user_action": stored}

    conn = _Connection()
    result = asyncio.run(
        RadarExploratoryStore(_Pool(conn)).record_user_action(
            91,
            owner_chat_id=123,
            action=requested,
        )
    )

    assert result is not None
    assert result["action_recorded"] is False
    assert result["action_conflict"] is conflict
    update_sql = next(sql for sql in conn.fetchrow_calls if "UPDATE" in sql)
    assert "AND user_action IS NULL" in update_sql


def test_follow_reconciliation_is_strict_and_does_not_double_attribute():
    class _Connection:
        def __init__(self):
            self.calls = []

        async def fetchval(self, statement):
            assert "to_regclass('public.radar_setup_alerts')" in statement
            return True

        async def execute(self, statement, *args):
            self.calls.append((statement, args))
            if "WITH eligible_pairs" in statement:
                return "UPDATE 1"
            return "OK"

    conn = _Connection()
    count = asyncio.run(
        RadarExploratoryStore(_Pool(conn)).reconcile_followed_fills(
            owner_chat_id=123,
        )
    )

    assert count == 1
    statement, args = next(call for call in conn.calls if "WITH eligible_pairs" in call[0])
    assert "f.owner_chat_id=c.owner_chat_id" in statement
    assert "UPPER(f.ticker)=UPPER(c.ticker)" in statement
    assert "f.side='BUY'" in statement
    assert "f.created_at >= c.user_action_at" in statement
    assert "f.executed_at >= c.user_action_at" in statement
    date_only_guard = statement.split(
        "WHEN f.executed_at_precision='date_only'",
        maxsplit=1,
    )[1].split("ELSE", maxsplit=1)[0]
    assert ">" in date_only_guard
    assert ">=" not in date_only_guard
    assert "c.user_action='FOLLOW'" in statement
    assert "FROM radar_setup_alerts setup" in statement
    assert "decision_log" not in statement
    assert args == (123, 16)


def test_scheduler_reconciles_manual_evidence_without_official_radar(monkeypatch):
    calls = []

    class _Db:
        async def get_pool(self):
            return object()

    async def _reconcile(self, *, owner_chat_id):
        calls.append((self.pool, owner_chat_id))
        return 2

    monkeypatch.setattr(runner, "RADAR_DISCOVERY_LEDGER_ENABLED", False)
    monkeypatch.setattr(runner, "RADAR_INTRADAY_SETUP_ALERTS_ENABLED", False)
    monkeypatch.setattr(runner, "RADAR_MANUAL_EXPLORATORY_ENABLED", True)
    monkeypatch.setattr(
        RadarExploratoryStore,
        "reconcile_followed_fills",
        _reconcile,
    )

    result = asyncio.run(
        runner._reconcile_radar_setup_followed_fills(
            _Db(),
            owner_chat_id=123,
        )
    )

    assert result == 2
    assert len(calls) == 1
    assert calls[0][1] == 123
