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

logger = logging.getLogger(__name__)

DEFAULT_STATE_PATH = Path("outputs/economic_meta_policy/watcher_state.json")


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


def _action(value: Any) -> str:
    raw = str(value or "").upper().strip()
    if raw in {"SELL_FULL", "SELL_PARTIAL", "EXIT", "CLOSE"}:
        return "SELL"
    if raw in {"BUY_FULL", "BUY_PARTIAL", "ADD"}:
        return "BUY"
    return raw


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
                turnover = sum(abs(_float(row.get("delta_weight"))) for row in rows)
                cost_bps = await self._estimated_cost_bps(conn, run_id)

                for row in rows:
                    ticker = str(row.get("ticker") or "").upper().strip()
                    owner = row.get("owner_chat_id")
                    source_kind = str(row.get("source_kind") or "analysis")
                    opportunity_id = (
                        f"analysis:{run_id}:{owner if owner is not None else 0}:"
                        f"{ticker}:{source_kind}"
                    )
                    candidate = {
                        "ticker": ticker,
                        "candidate_action": _action(row.get("action")),
                        "candidate_score": _float(row.get("final_score"), 0.0),
                        "as_of": _aware(row.get("as_of")),
                        "estimated_cost_bps": cost_bps,
                        "portfolio_turnover": turnover,
                        "market_regime": str(row.get("regime") or "UNKNOWN"),
                        "expected_edge_vs_hold_bps": None,
                        "edge_uncertainty_bps": None,
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
    "DEFAULT_STATE_PATH",
    "EconomicMetaAnalysisWatcher",
    "WatcherRunSummary",
    "run_economic_meta_watcher_loop",
]
