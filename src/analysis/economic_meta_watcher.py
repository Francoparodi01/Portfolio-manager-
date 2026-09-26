"""Automatic shadow ingestion of complete Quantia analysis runs.

This module is deliberately isolated from the production decision path. It only
reads completed/stable analysis rows from PostgreSQL and appends Economic Meta
Policy records to the local shadow JSONL store.

Coverage per run:
- BUY/SELL/blocked planner decisions from ``decision_log`` where source is
  ``execution_plan``.
- final planner HOLD decisions from ``position_hold_observations``.

It never updates ``decision_log``, execution plans, portfolio weights, orders or
broker state.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import asyncpg

from src.analysis.economic_meta_policy import evaluate_all_preregistered
from src.analysis.economic_meta_store import (
    DEFAULT_SHADOW_PATH,
    EconomicMetaShadowStore,
)
from src.analysis.historical_edge import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_RESEARCH_COST_BPS,
    load_historical_rows,
    match_historical_edge,
    normalize_action,
)
from src.core.config import get_config

logger = logging.getLogger(__name__)

DEFAULT_STATE_PATH = Path("outputs/economic_meta_policy/watcher_state.json")
DB_INPUT_VERSION = "meta-db-input-v3"


def _dsn(value: str) -> str:
    return str(value).replace("postgresql+asyncpg://", "postgresql://")


def _aware(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        result = float(value)
        return result
    except (TypeError, ValueError):
        return default


def _legacy_single_owner_chat_id() -> int | None:
    """Resolve the configured legacy owner only when multiuser mode is disabled."""
    try:
        cfg = get_config()
    except Exception:
        return None
    if bool(getattr(cfg, "multiuser_enabled", False)):
        return None
    raw = str(getattr(getattr(cfg, "scraper", None), "telegram_chat_id", "") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _action(value: Any) -> str:
    """Normalize DB actions with the same semantics as report ingestion."""
    raw = str(value or "").upper().strip()
    if raw in {"SELL_PARTIAL", "REDUCE", "TRIM"}:
        return "REDUCE"
    if raw in {"SELL", "SELL_FULL", "EXIT", "CLOSE"}:
        return "SELL"
    if raw in {"BUY", "BUY_FULL", "BUY_PARTIAL", "ADD"}:
        return "BUY"
    return raw


def _portfolio_turnover(rows: list[dict[str, Any]]) -> float:
    """Mirror the legacy report-ingest turnover definition.

    Only executable directional deltas count. HOLD/WATCH rows are excluded and
    opposite sides are not added together; turnover is the larger of aggregate
    buys and aggregate sells, exactly like ``economic_meta_report_ingest``.
    """
    buy_delta = 0.0
    sell_delta = 0.0
    for row in rows:
        action = _action(row.get("action"))
        delta = _float(row.get("delta_weight"), 0.0)
        if action == "BUY":
            buy_delta += max(0.0, delta)
        elif action in {"SELL", "REDUCE"}:
            sell_delta += max(0.0, -delta)
    return max(0.0, min(max(buy_delta, sell_delta), 1.0))


@dataclass(frozen=True, slots=True)
class WatcherRunSummary:
    runs_seen: int = 0
    candidates_seen: int = 0
    records_written: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "runs_seen": self.runs_seen,
            "candidates_seen": self.candidates_seen,
            "records_written": self.records_written,
        }


class EconomicMetaAnalysisWatcher:
    """Read stable analysis runs and mirror them into shadow meta-policy records."""

    def __init__(
        self,
        database_url: str,
        *,
        store_path: str | Path = DEFAULT_SHADOW_PATH,
        state_path: str | Path = DEFAULT_STATE_PATH,
        settle_seconds: int = 45,
        max_runs_per_poll: int = 20,
    ) -> None:
        self.database_url = _dsn(database_url)
        self.store = EconomicMetaShadowStore(store_path)
        self.state_path = Path(state_path)
        self.settle_seconds = max(10, int(settle_seconds))
        self.max_runs_per_poll = max(1, min(int(max_runs_per_poll), 100))
        self.legacy_single_owner_chat_id = _legacy_single_owner_chat_id()
        self.activated_at = self._load_or_create_activation()

    def _load_or_create_activation(self) -> datetime:
        if self.state_path.exists():
            try:
                payload = json.loads(self.state_path.read_text(encoding="utf-8"))
                return _aware(payload.get("activated_at"))
            except Exception:
                logger.warning("[META][WATCHER] estado inválido; se recrea", exc_info=True)

        # Small grace window allows an analysis that is finishing while the bot is
        # being rebuilt to be captured without retrospectively backfilling history.
        activated_at = datetime.now(timezone.utc) - timedelta(seconds=self.settle_seconds)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(
                {
                    "activated_at": activated_at.isoformat(),
                    "mode": "SHADOW_ONLY",
                    "capital_effect": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return activated_at

    def _existing_keys(self) -> set[tuple[str, str]]:
        keys: set[tuple[str, str]] = set()
        try:
            rows = self.store.read_all()
        except Exception:
            logger.warning("[META][WATCHER] no pude leer store para deduplicar", exc_info=True)
            return keys
        for row in rows:
            opportunity = str(row.get("opportunity_id") or "").strip()
            policy = str(row.get("policy_name") or "").strip()
            if opportunity and policy:
                keys.add((opportunity, policy))
        return keys

    async def _stable_runs(self, conn: asyncpg.Connection) -> list[str]:
        merged: dict[str, datetime] = {}
        rows = await conn.fetch(
            """
            SELECT run_id::text AS run_id, MAX(decided_at) AS last_seen
            FROM decision_log
            WHERE run_id IS NOT NULL
              AND decided_at >= $1
              AND COALESCE(source, layers->>'source') = 'execution_plan'
            GROUP BY run_id
            """,
            self.activated_at,
        )
        for row in rows:
            merged[str(row["run_id"])] = _aware(row["last_seen"])

        try:
            hold_rows = await conn.fetch(
                """
                SELECT run_id::text AS run_id, MAX(observed_at) AS last_seen
                FROM position_hold_observations
                WHERE observed_at >= $1
                GROUP BY run_id
                """,
                self.activated_at,
            )
        except asyncpg.UndefinedTableError:
            hold_rows = []

        for row in hold_rows:
            run_id = str(row["run_id"])
            seen = _aware(row["last_seen"])
            if run_id not in merged or seen > merged[run_id]:
                merged[run_id] = seen

        # Bootstrap/backfill exactly the most recent formal analysis candidates from
        # the last 24h even when they predate watcher activation. This handles the
        # common deployment case where Telegram is rebuilt after the latest analysis
        # and /analisis subsequently serves a cached report instead of creating a new
        # run. Deduplication by opportunity_id+policy keeps this idempotent.
        latest_rows = await conn.fetch(
            """
            SELECT run_id::text AS run_id, MAX(decided_at) AS last_seen
            FROM decision_log
            WHERE run_id IS NOT NULL
              AND decided_at >= NOW() - INTERVAL '24 hours'
              AND COALESCE(source, layers->>'source') = 'execution_plan'
            GROUP BY run_id
            ORDER BY last_seen DESC
            LIMIT 1
            """
        )
        for row in latest_rows:
            run_id = str(row["run_id"])
            seen = _aware(row["last_seen"])
            if run_id not in merged or seen > merged[run_id]:
                merged[run_id] = seen

        try:
            latest_hold_rows = await conn.fetch(
                """
                SELECT run_id::text AS run_id, MAX(observed_at) AS last_seen
                FROM position_hold_observations
                WHERE observed_at >= NOW() - INTERVAL '24 hours'
                GROUP BY run_id
                ORDER BY last_seen DESC
                LIMIT 1
                """
            )
        except asyncpg.UndefinedTableError:
            latest_hold_rows = []

        for row in latest_hold_rows:
            run_id = str(row["run_id"])
            seen = _aware(row["last_seen"])
            if run_id not in merged or seen > merged[run_id]:
                merged[run_id] = seen

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.settle_seconds)
        stable = [
            (run_id, last_seen)
            for run_id, last_seen in merged.items()
            if last_seen <= cutoff
        ]
        stable.sort(key=lambda item: item[1])
        return [run_id for run_id, _ in stable[: self.max_runs_per_poll]]

    async def _candidate_rows(
        self,
        conn: asyncpg.Connection,
        run_id: str,
    ) -> list[dict[str, Any]]:
        by_ticker: dict[str, dict[str, Any]] = {}

        decision_rows = await conn.fetch(
            """
            SELECT
                id,
                owner_chat_id,
                run_id::text AS run_id,
                decided_at AS as_of,
                ticker,
                decision AS action,
                final_score,
                regime,
                delta_weight,
                status,
                decision_type
            FROM decision_log
            WHERE run_id = $1::uuid
              AND COALESCE(source, layers->>'source') = 'execution_plan'
            ORDER BY decided_at, id
            """,
            run_id,
        )
        for raw in decision_rows:
            row = dict(raw)
            if row.get("owner_chat_id") is None and self.legacy_single_owner_chat_id is not None:
                row["owner_chat_id"] = self.legacy_single_owner_chat_id
            ticker = str(row.get("ticker") or "").upper().strip()
            if not ticker:
                continue
            row["source_kind"] = "decision_log"
            by_ticker[ticker] = row

        try:
            hold_rows = await conn.fetch(
                """
                SELECT
                    id,
                    owner_chat_id,
                    run_id::text AS run_id,
                    observed_at AS as_of,
                    ticker,
                    action,
                    final_score,
                    regime,
                    delta_weight,
                    status,
                    'hold_observation'::text AS decision_type
                FROM position_hold_observations
                WHERE run_id = $1::uuid
                ORDER BY observed_at, id
                """,
                run_id,
            )
        except asyncpg.UndefinedTableError:
            hold_rows = []

        for raw in hold_rows:
            row = dict(raw)
            if row.get("owner_chat_id") is None and self.legacy_single_owner_chat_id is not None:
                row["owner_chat_id"] = self.legacy_single_owner_chat_id
            ticker = str(row.get("ticker") or "").upper().strip()
            if not ticker or ticker in by_ticker:
                continue
            row["source_kind"] = "position_hold_observations"
            by_ticker[ticker] = row

        return [by_ticker[ticker] for ticker in sorted(by_ticker)]

    async def _estimated_cost_bps(self, conn: asyncpg.Connection, run_id: str) -> float:
        try:
            row = await conn.fetchrow(
                """
                SELECT gross_sell_ars, fee_sell_ars, gross_buy_ars, fee_buy_ars
                FROM execution_plans
                WHERE run_id = $1::uuid
                ORDER BY created_at DESC
                LIMIT 1
                """,
                run_id,
            )
        except asyncpg.UndefinedTableError:
            return 0.0
        if not row:
            return 0.0
        gross = abs(_float(row["gross_sell_ars"])) + abs(_float(row["gross_buy_ars"]))
        fees = abs(_float(row["fee_sell_ars"])) + abs(_float(row["fee_buy_ars"]))
        if gross <= 0:
            return 0.0
        return max(0.0, fees / gross * 10_000.0)

    async def _history_by_owner(
        self,
        conn: asyncpg.Connection,
        rows: list[dict[str, Any]],
    ) -> dict[int, list[dict[str, Any]]]:
        if not rows:
            return {}
        run_as_of = max(_aware(row.get("as_of")) for row in rows)
        cutoff = run_as_of - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        owners = {
            int(row["owner_chat_id"])
            for row in rows
            if row.get("owner_chat_id") is not None
        }
        history: dict[int, list[dict[str, Any]]] = {}
        for owner in owners:
            try:
                owner_history = await load_historical_rows(
                    conn,
                    owner_chat_id=owner,
                    as_of=run_as_of,
                    lookback_days=DEFAULT_LOOKBACK_DAYS,
                )
                allow_legacy_null = self.legacy_single_owner_chat_id == owner
                if allow_legacy_null:
                    legacy_rows = await conn.fetch(
                        """
                        SELECT
                            id,
                            run_id::text AS run_id,
                            decided_at,
                            ticker,
                            decision,
                            final_score,
                            regime,
                            outcome_20d,
                            outcome_basis,
                            outcome_filled_at,
                            status,
                            metric_scope,
                            COALESCE(source, layers->>'source') AS source
                        FROM decision_log
                        WHERE owner_chat_id IS NULL
                          AND decided_at >= $1
                          AND decided_at < $2
                          AND COALESCE(source, layers->>'source') = 'execution_plan'
                          AND COALESCE(metric_scope, 'planner_audit') <> 'debug'
                        ORDER BY decided_at, id
                        """,
                        cutoff,
                        run_as_of,
                    )
                    owner_history.extend(dict(row) for row in legacy_rows)

                # A run with only HOLD observations may have no execution_plan row.
                # Include HOLDs as chronology/break markers only: historical_edge
                # never scores them as profitable samples because they are not
                # formal BUY/SELL candidates.
                try:
                    hold_markers = await conn.fetch(
                        """
                        SELECT
                            id,
                            run_id::text AS run_id,
                            observed_at AS decided_at,
                            ticker,
                            action AS decision,
                            final_score,
                            regime,
                            outcome_20d,
                            outcome_basis,
                            outcome_filled_at,
                            status,
                            metric_scope,
                            source
                        FROM position_hold_observations
                        WHERE owner_chat_id = $1
                          AND observed_at >= $2
                          AND observed_at < $3
                        ORDER BY observed_at, id
                        """,
                        owner,
                        cutoff,
                        run_as_of,
                    )
                    if allow_legacy_null:
                        legacy_hold_markers = await conn.fetch(
                            """
                            SELECT
                                id,
                                run_id::text AS run_id,
                                observed_at AS decided_at,
                                ticker,
                                action AS decision,
                                final_score,
                                regime,
                                outcome_20d,
                                outcome_basis,
                                outcome_filled_at,
                                status,
                                metric_scope,
                                source
                            FROM position_hold_observations
                            WHERE owner_chat_id IS NULL
                              AND observed_at >= $1
                              AND observed_at < $2
                            ORDER BY observed_at, id
                            """,
                            cutoff,
                            run_as_of,
                        )
                        hold_markers = [*hold_markers, *legacy_hold_markers]
                except asyncpg.UndefinedTableError:
                    hold_markers = []
                owner_history.extend(dict(row) for row in hold_markers)
                owner_history.sort(
                    key=lambda row: (
                        _aware(row.get("decided_at")),
                        str(row.get("id") or ""),
                    )
                )
                history[owner] = owner_history
            except Exception:
                # Historical Edge is an experimental shadow challenger. A schema
                # or data-quality failure must never break A/B/C or production.
                logger.exception(
                    "[META][HIST] no pude cargar histórico owner=%s; META-D fail-closed",
                    owner,
                )
                history[owner] = []
        return history

    async def run_once(self) -> WatcherRunSummary:
        existing = self._existing_keys()
        runs_seen = 0
        candidates_seen = 0
        records_written = 0

        conn = await asyncpg.connect(self.database_url)
        try:
            run_ids = await self._stable_runs(conn)
            for run_id in run_ids:
                rows = await self._candidate_rows(conn, run_id)
                if not rows:
                    continue
                runs_seen += 1
                candidates_seen += len(rows)
                turnover = _portfolio_turnover(rows)
                cost_bps = await self._estimated_cost_bps(conn, run_id)
                historical_rows = await self._history_by_owner(conn, rows)
                historical_cost_bps = max(DEFAULT_RESEARCH_COST_BPS, cost_bps)

                for row in rows:
                    ticker = str(row.get("ticker") or "").upper().strip()
                    owner = row.get("owner_chat_id")
                    source_kind = str(row.get("source_kind") or "analysis")
                    opportunity_id = (
                        f"analysis:{DB_INPUT_VERSION}:{run_id}:"
                        f"{owner if owner is not None else 0}:{ticker}:{source_kind}"
                    )
                    candidate_action = _action(row.get("action"))
                    candidate_as_of = _aware(row.get("as_of"))
                    historical_edge: dict[str, Any] = {}
                    normalized_action = normalize_action(candidate_action)
                    if owner is not None and normalized_action in {"BUY", "SELL"}:
                        try:
                            historical_edge = match_historical_edge(
                                historical_rows.get(int(owner), []),
                                candidate_action=normalized_action,
                                candidate_score=_float(row.get("final_score"), 0.0),
                                candidate_regime=str(row.get("regime") or "UNKNOWN"),
                                as_of=candidate_as_of,
                                lookback_days=DEFAULT_LOOKBACK_DAYS,
                                cost_bps=historical_cost_bps,
                            ).to_dict()
                        except Exception:
                            logger.exception(
                                "[META][HIST] match falló %s %s; META-D fail-closed",
                                ticker,
                                normalized_action,
                            )

                    candidate = {
                        "ticker": ticker,
                        "candidate_action": candidate_action,
                        "candidate_score": _float(row.get("final_score"), 0.0),
                        "as_of": candidate_as_of,
                        "estimated_cost_bps": cost_bps,
                        "portfolio_turnover": turnover,
                        "market_regime": str(row.get("regime") or "UNKNOWN"),
                        "expected_edge_vs_hold_bps": None,
                        "edge_uncertainty_bps": None,
                        "historical_edge": historical_edge,
                        "opportunity_id": opportunity_id,
                    }
                    records = evaluate_all_preregistered(candidate, run_id=run_id)
                    for record in records:
                        key = (opportunity_id, record.policy_name)
                        if key in existing:
                            continue
                        self.store.append(record)
                        existing.add(key)
                        records_written += 1
        finally:
            await conn.close()

        return WatcherRunSummary(
            runs_seen=runs_seen,
            candidates_seen=candidates_seen,
            records_written=records_written,
        )


async def run_economic_meta_watcher_loop(
    database_url: str,
    *,
    poll_interval_seconds: int = 15,
    settle_seconds: int = 45,
    store_path: str | Path = DEFAULT_SHADOW_PATH,
    state_path: str | Path = DEFAULT_STATE_PATH,
) -> None:
    watcher = EconomicMetaAnalysisWatcher(
        database_url,
        store_path=store_path,
        state_path=state_path,
        settle_seconds=settle_seconds,
    )
    interval = max(5, int(poll_interval_seconds))
    logger.info(
        "[META][WATCHER] activo desde %s; poll=%ss settle=%ss SHADOW_ONLY",
        watcher.activated_at.isoformat(),
        interval,
        watcher.settle_seconds,
    )
    while True:
        try:
            summary = await watcher.run_once()
            if summary.records_written:
                logger.info("[META][WATCHER] %s", summary.to_dict())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[META][WATCHER] fallo de polling; producción no afectada")
        await asyncio.sleep(interval)


__all__ = [
    "DB_INPUT_VERSION",
    "DEFAULT_STATE_PATH",
    "EconomicMetaAnalysisWatcher",
    "WatcherRunSummary",
    "run_economic_meta_watcher_loop",
]
