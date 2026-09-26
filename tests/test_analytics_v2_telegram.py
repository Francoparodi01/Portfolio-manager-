import asyncio
import copy
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.analysis import analytics_v2_live as live
from src.core.telegram_format import validate_telegram_html


def recommendation(identity=1, **changes):
    return {"id": identity, "owner_chat_id": 123, "ticker": "MU", "decision": "BUY",
            "decided_at": "2026-07-01T15:00:00+00:00", "source": "execution_plan",
            "status": "EXECUTED", "metric_scope": "planner_audit", "is_primary_metric": True,
            "outcome_basis": "canonical_cocos_v1", "outcome_5d": .1,
            "outcome_filled_at": "2026-08-01T15:00:00+00:00", **changes}


def snapshot(rows=None, attrs=None):
    return {"source": "LIVE_DB_READ_ONLY", "version": live.VERSION, "owner_chat_id": 123,
            "captured_at": "2026-09-22T15:00:00+00:00", "days": 180,
            "legacy_null_included": False, "decisions": rows or [], "attributions": attrs or [],
            "fill_coverage": {}, "schema": {}}


def base_row(report, cohort="BOT_ONLY", horizon=5):
    return next(r for r in report["metrics"] if r["cohort"] == cohort and
                r["horizon_days"] == horizon and r["cost_scenario"] == "RESEARCH_BASE")


def test_repetitions_anchor_without_double_count_or_sell_inversion():
    report = live.summarize(snapshot([
        recommendation(), recommendation(2, decided_at="2026-07-02T15:00:00+00:00", outcome_5d=.99),
        recommendation(3, decided_at="2026-07-03T15:00:00+00:00", decision="SELL", outcome_5d=.2),
    ]))
    row = base_row(report)
    assert row["n_episodes"] == row["n_observed"] == 2
    assert row["n_recommendations"] == 3
    assert row["ev_net"] == pytest.approx(.135)
    assert row["n_effective"] is None and row["gate"] == "OBSERVE"
    assert row["ev_lower"] is None
    assert row["ev_reason_code"] == "INSUFFICIENT_DATE_BLOCKS_OR_VARIATION"
    assert len(report["episode_links"]) == 3


@pytest.mark.parametrize("changes,reason", [
    ({"outcome_5d": None}, "OUTCOME_NOT_RECORDED_MATURITY_UNKNOWN"),
    ({"outcome_basis": None}, "UNVERIFIED_PRICE_BASIS"),
    ({"outcome_filled_at": "2026-10-01T00:00:00+00:00"}, "OUTCOME_AVAILABLE_AFTER_CAPTURE"),
    ({"outcome_5d": float("nan")}, "OUTCOME_NOT_RECORDED_MATURITY_UNKNOWN"),
])
def test_missing_and_future_data_do_not_become_zero(changes, reason):
    # A DB capture normalizes nonfinite values before hashing.
    report = live.summarize(live.normalize(snapshot([recommendation(**changes)])))
    row = base_row(report)
    assert row["ev_net"] is None and row["n_observed"] == 0
    assert row["n_unavailable"] == 1
    assert report["outcomes"][0]["reason_code"] == reason


def test_costs_are_scenarios_and_executable_outcome_takes_precedence():
    report = live.summarize(snapshot([recommendation(executable_outcome_5d=.2)]))
    rows = [r for r in report["metrics"] if r["horizon_days"] == 5]
    assert [r["cost_bps"] for r in rows] == [0, 75, 150, 250, 400]
    assert [r["ev_net"] for r in rows] == pytest.approx([.2, .1925, .185, .175, .16])
    assert report["economic_pnl"]["economic_pnl_net"] is None
    assert report["matching"]["status"] == "UNAVAILABLE"
    assert report["swaps"]["status"] == "DISABLED_SHADOW"


def test_followed_manual_radar_and_ambiguous_are_separate():
    attrs = [{"id": 9, "owner_chat_id": 123, "ticker": "MU", "side": "BUY",
              "executed_at": "2026-07-01T16:00:00+00:00", "eligible_for_viability": False,
              "outcome_basis": "canonical_cocos_v1", "outcome_5d": .5}]
    report = live.summarize(snapshot([
        recommendation(1, source="broker_fill", attributed_followed=False),
        recommendation(2, source="broker_fill", attributed_followed=True),
        recommendation(3, source="broker_fill", attributed_followed=None),
        recommendation(4, source="radar", metric_scope="radar_audit"),
        recommendation(5, decision="NEUTRAL"),
    ], attrs))
    assert base_row(report, "MANUAL_ONLY")["n_recommendations"] == 1
    assert base_row(report, "RADAR_ALL")["n_recommendations"] == 1
    assert base_row(report, "FOLLOWED")["n_ambiguous"] == 1
    assert base_row(report, "FOLLOWED")["ev_net"] is None
    assert report["excluded_rows"]["OUTSIDE_COHORT_OR_ATTRIBUTED"] == 3


@pytest.mark.parametrize("owner", [456, None])
def test_cross_owner_capture_fails_closed(owner):
    with pytest.raises(ValueError, match="owner mismatch"):
        live.summarize(snapshot([recommendation(owner_chat_id=owner)]))


def test_replay_manifest_and_archive_are_verifiable(tmp_path):
    data = snapshot([recommendation()])
    report = live.summarize(data)
    assert report == live.summarize(copy.deepcopy(data))
    text = live.render_telegram(report)
    assert validate_telegram_html(text)[0]
    assert len(text) < 3900 and "PnL neto real: <b>N/D</b>" in text
    assert "OBSERVE" in text and "Intervalo 95% EV" in text
    path = live.write_archive(report, tmp_path / "report.zip")
    with zipfile.ZipFile(path) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        for name, expected in manifest["output_hashes"].items():
            assert hashlib.sha256(archive.read(name)).hexdigest() == expected
        assert json.loads(archive.read("capture.json")) == data
    with pytest.raises(FileExistsError):
        live.write_archive(report, path)


def test_synthetic_package_cannot_be_presented_as_live():
    data = snapshot()
    data["source"] = "SYNTHETIC"
    with pytest.raises(ValueError, match="live capture"):
        live.summarize(data)


def test_numeric_runtime_is_part_of_run_identity(monkeypatch):
    data = snapshot([recommendation()])
    before = live.summarize(data)
    monkeypatch.setattr(live.platform, "python_version", lambda: "DIFFERENT_RUNTIME")
    after = live.summarize(data)
    assert before["manifest"]["analysis_run_id"] != after["manifest"]["analysis_run_id"]
    assert before["manifest"]["input_hash"] == after["manifest"]["input_hash"]


class CaptureConnection:
    def __init__(self, other_owners=False):
        self.calls = []
        self.other_owners = other_owners
        self.schema = {
            "decision_log": set(live.DECISION_COLUMNS) | {"source", "layers", "superseded_by_id"},
            "broker_fills": {"id", "owner_chat_id", "executed_at", "fees_ars", "raw_payload", "decision_log_id", "external_fill_id"},
            "plan_execution_attributions": set(live.ATTRIBUTION_COLUMNS),
            "plan_execution_attribution_movements": {"attribution_id", "broker_movement_id"},
            "broker_movements": {"id", "external_movement_id", "owner_chat_id"},
        }

    def transaction(self, **kwargs):
        self.transaction_options = kwargs
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return datetime(2026, 9, 22, tzinfo=timezone.utc) if "CURRENT_TIMESTAMP" in sql else self.other_owners

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        if "information_schema" in sql:
            return [{"table_name": table, "column_name": col} for table, cols in self.schema.items() for col in cols]
        return []

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return {"n_fills": 0, "n_fees_observed": 0, "observed_fees_sum": None}


@pytest.mark.parametrize("requested,other,expected", [(True, False, True), (True, True, False), (False, False, False)])
def test_capture_read_only_scope_and_single_owner_legacy(requested, other, expected):
    conn = CaptureConnection(other)
    data = asyncio.run(live.capture(conn, owner_chat_id=123, allow_legacy_null=requested))
    assert conn.transaction_options == {"readonly": True, "isolation": "repeatable_read"}
    assert data["legacy_null_included"] is expected
    for sql, args in conn.calls:
        assert sql.lstrip().startswith("SELECT")
        if "$4" in sql:
            assert args[0] == 123 and args[2] is expected
            assert "owner_chat_id=$1" in sql and "<= $4" in sql


def test_capture_schema_failure_and_bounds():
    conn = CaptureConnection()
    conn.schema["decision_log"].remove("owner_chat_id")
    with pytest.raises(ValueError, match="required"):
        asyncio.run(live.capture(conn, owner_chat_id=123))
    with pytest.raises(ValueError, match="bounded"):
        asyncio.run(live.capture(conn, owner_chat_id=0))


def test_legacy_movement_links_scope_both_endpoints_even_without_movement_owner():
    conn = CaptureConnection()
    conn.schema["broker_movements"].remove("owner_chat_id")
    asyncio.run(live.capture(conn, owner_chat_id=123))
    sql = next(sql for sql, _ in conn.calls if "AS attributed_followed" in sql)
    assert "bf.owner_chat_id=$1" in sql and "a.owner_chat_id=$1" in sql and "dl.owner_chat_id=$1" in sql
    assert "external_fill_ids" in sql and "NULL::boolean AS attributed_followed" not in sql
    # Ambiguous links also exclude manual-only classification.
    assert "eligible_for_viability=TRUE" not in sql


@pytest.mark.parametrize("multiuser,caller,expected", [(False, 123, True), (True, 123, False), (False, 456, False)])
def test_cli_legacy_inclusion_requires_configured_single_user(monkeypatch, multiuser, caller, expected):
    from scripts import run_analytics_v2 as cli
    conn = SimpleNamespace(close=AsyncMock())
    cfg = SimpleNamespace(multiuser_enabled=multiuser, scraper=SimpleNamespace(telegram_chat_id="123"),
                          database=SimpleNamespace(url="postgresql://test"))
    capture = AsyncMock(return_value=snapshot())
    monkeypatch.setattr(cli, "get_config", lambda: cfg)
    monkeypatch.setattr(cli.asyncpg, "connect", AsyncMock(return_value=conn))
    monkeypatch.setattr(cli, "capture", capture)
    asyncio.run(cli.async_main(SimpleNamespace(owner_chat_id=caller, days=180, archive_out=None)))
    assert capture.call_args.kwargs["allow_legacy_null"] is expected
    assert capture.call_args.kwargs["owner_chat_id"] == caller
    conn.close.assert_awaited_once()


@pytest.mark.parametrize("failure", [None, "process", "no_archive", "send"])
def test_telegram_action_scopes_owner_and_cleans_temp_files(monkeypatch, failure):
    from scripts import telegram_bot as bot
    paths = []

    async def fake_run(args, timeout):
        assert args[args.index("--owner-chat-id") + 1] == "123"
        assert timeout == 240 and "scripts/run_analytics_v2.py" in args
        path = Path(args[args.index("--archive-out") + 1])
        paths.append(path)
        if failure != "no_archive":
            path.write_bytes(b"test archive")
        return (1 if failure == "process" else 0), "<b>Report</b>", "PRIVATE_ERROR", 1.0

    sent = AsyncMock()
    document = AsyncMock(side_effect=RuntimeError("send failed") if failure == "send" else None)
    monkeypatch.setattr(bot, "run_cmd", fake_run)
    monkeypatch.setattr(bot, "send_text", sent)
    context = SimpleNamespace(bot=SimpleNamespace(send_document=document))
    if failure == "send":
        with pytest.raises(RuntimeError, match="send failed"):
            asyncio.run(bot.action_analytics_v2(context, 123))
    else:
        asyncio.run(bot.run_action("analytics_v2", context, 123))
    assert paths and not paths[0].parent.exists()
    assert "PRIVATE_ERROR" not in str(sent.call_args)
    assert document.await_count == (0 if failure in {"process", "no_archive"} else 1)


def test_command_alias_button_loading_and_authorization(monkeypatch):
    from scripts import telegram_bot as bot
    assert dict(bot.BOT_COMMAND_SPECS)["analytics"]
    assert bot.CALLBACK_ALIASES["analytics"] == bot.CALLBACK_ALIASES["analytics_v2"] == "analytics_v2"
    assert bot.ACTION_LOADING_TEXT["analytics_v2"]
    assert "/analytics" in bot.help_text()
    source = Path(bot.__file__).read_text(encoding="utf-8")
    assert 'CommandHandler("analytics",' in source and 'CommandHandler("analytics_v2",' in source
    assert 'callback_data="analytics_v2"' in source
    monkeypatch.setattr(bot, "ensure_allowed_chat", AsyncMock(return_value=False))
    run = AsyncMock()
    monkeypatch.setattr(bot, "run_action", run)
    update = SimpleNamespace(message=SimpleNamespace(chat_id=123), effective_chat=SimpleNamespace(id=123))
    asyncio.run(bot.analytics_v2_handler(update, SimpleNamespace()))
    run.assert_not_awaited()
