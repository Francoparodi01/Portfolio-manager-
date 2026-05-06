"""
src/analysis/feature_builder.py

Feature store / dataset builder para la capa ML de Cocos Copilot.

Responsabilidades:
  - definir el esquema lógico de features ML
  - capturar features de una decisión en el momento de entrada
  - persistir esas features en ml_decision_features
  - reconstruir datasets para entrenamiento
  - ayudar a update_outcomes.py a completar labels reales

Principio:
  Las features se capturan en tiempo de decisión, no después.
  Así evitamos look-ahead bias y dejamos trazabilidad completa.

Tabla esperada:
  ml_decision_features

Columnas mínimas esperadas:
  - decision_log_id
  - ticker
  - captured_at
  - ...features...
  - label_target_hit
  - label_stop_hit
  - label_timeout
  - outcome_return_pct
  - outcome_days
  - target_pct
  - stop_loss_pct

Integración:
  - decision_engine debe llamar FeatureBuilder.capture(...)
  - update_outcomes.py debe llamar fill_labels_for_closed(...)
  - train_model.py lee esta tabla para entrenamiento
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

from src.core.logger import get_logger

logger = get_logger(__name__)

UTC = timezone.utc

# ── Configuración de labels ───────────────────────────────────────

LABEL_COLUMN = "label_target_hit"

# Features base del modelo.
# Mantener orden estable para consistencia entre training e inferencia.
FEATURE_COLUMNS = [
    # Señal / decisión
    "final_score",
    "confidence_score",
    "prob_target_hit_prior",
    "expected_value_prior",

    # Riesgo / trade setup
    "stop_loss_pct",
    "target_pct",
    "rr_ratio",
    "horizon_days",
    "size_pct",

    # Técnico
    "technical_score",
    "rsi_14",
    "macd_hist",
    "bb_pos",
    "atr_pct",
    "distance_sma20_pct",
    "distance_sma50_pct",
    "distance_sma200_pct",
    "momentum_20d",
    "momentum_60d",
    "volatility_20d",
    "drawdown_60d",

    # Macro / benchmark
    "macro_score",
    "vix_level",
    "spy_return_5d",
    "spy_return_20d",
    "dxy_return_20d",
    "tnx_level",
    "wti_return_20d",

    # Régimen / portfolio
    "regime_code",
    "cash_pct",
    "portfolio_concentration_pct",
    "weight_in_portfolio_pct",
    "relative_strength_vs_spy_20d",

    # Sector / cross-sectional
    "sector_score",
    "sector_momentum_20d",
    "sector_relative_strength_20d",
]

# Columnas que además queremos persistir como metadata
META_COLUMNS = [
    "decision_log_id",
    "ticker",
    "captured_at",
    "decision",
    "regime",
    "source",
]

OUTCOME_COLUMNS = [
    "label_target_hit",
    "label_stop_hit",
    "label_timeout",
    "outcome_return_pct",
    "outcome_days",
    "closed_at",
]

ALL_COLUMNS = META_COLUMNS + FEATURE_COLUMNS + OUTCOME_COLUMNS


@dataclass
class FeatureCaptureResult:
    decision_log_id: int
    ticker: str
    inserted: bool

    def to_dict(self) -> dict:
        return {
            "decision_log_id": self.decision_log_id,
            "ticker": self.ticker,
            "inserted": self.inserted,
        }


class FeatureBuilder:
    """
    Captura y persiste features ML.

    Uso típico desde decision_engine:
        builder = FeatureBuilder(pool)
        await builder.capture(
            decision_log_id=123,
            ticker="NVDA",
            decision="BUY",
            regime="NORMAL",
            feature_payload={...}
        )

    Uso desde training:
        df = await FeatureBuilder(pool).load_training_frame()
    """

    def __init__(self, pool):
        self.pool = pool

    async def capture(
        self,
        decision_log_id: int,
        ticker: str,
        decision: str,
        regime: str,
        feature_payload: dict[str, Any],
        source: str = "decision_engine",
        captured_at: Optional[datetime] = None,
    ) -> FeatureCaptureResult:
        """
        Inserta o actualiza features para una decisión.

        feature_payload:
          dict con cualquier subset de FEATURE_COLUMNS.
          Las faltantes se guardan como NULL.

        Reglas:
          - una fila por decision_log_id
          - si ya existe, hace UPDATE
          - captured_at por default = now UTC
        """
        captured_at = captured_at or datetime.now(tz=UTC)

        row = {
            "decision_log_id": decision_log_id,
            "ticker": ticker.upper(),
            "captured_at": captured_at,
            "decision": decision,
            "regime": regime,
            "source": source,
        }

        for col in FEATURE_COLUMNS:
            row[col] = _safe_numeric(feature_payload.get(col))

        # outcomes empiezan nulos
        row["label_target_hit"] = None
        row["label_stop_hit"] = None
        row["label_timeout"] = None
        row["outcome_return_pct"] = None
        row["outcome_days"] = None
        row["closed_at"] = None

        insert_cols = ALL_COLUMNS
        placeholders = ", ".join(f"${i}" for i in range(1, len(insert_cols) + 1))
        update_set = ", ".join(
            f"{col} = EXCLUDED.{col}"
            for col in (["ticker", "captured_at", "decision", "regime", "source"] + FEATURE_COLUMNS)
        )

        values = [row[c] for c in insert_cols]

        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO ml_decision_features ({", ".join(insert_cols)})
                VALUES ({placeholders})
                ON CONFLICT (decision_log_id) DO UPDATE SET
                    {update_set}
                """,
                *values,
            )

        logger.info(
            "FeatureBuilder.capture: decision_log_id=%s ticker=%s",
            decision_log_id,
            ticker.upper(),
        )
        return FeatureCaptureResult(
            decision_log_id=decision_log_id,
            ticker=ticker.upper(),
            inserted=True,
        )

    async def fill_labels_for_closed(
        self,
        decision_log_id: int,
        exit_reason: Optional[str],
        final_pnl_pct: Optional[float],
        decided_at: Optional[datetime],
        closed_at: Optional[datetime],
    ) -> bool:
        """
        Completa labels para una decisión ya cerrada.

        Reglas:
          exit_reason:
            TARGET -> label_target_hit = 1
            STOP   -> label_stop_hit = 1
            TIME   -> label_timeout = 1
          El resto va a 0 si no corresponde.
        """
        reason = (exit_reason or "").upper()
        label_target_hit = 1 if reason == "TARGET" else 0
        label_stop_hit = 1 if reason == "STOP" else 0
        label_timeout = 1 if reason == "TIME" else 0

        outcome_days = None
        if decided_at and closed_at:
            try:
                decided_dt = _ensure_utc(decided_at)
                closed_dt = _ensure_utc(closed_at)
                outcome_days = max(0, (closed_dt.date() - decided_dt.date()).days)
            except Exception:
                outcome_days = None

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE ml_decision_features
                SET
                    label_target_hit = $2,
                    label_stop_hit = $3,
                    label_timeout = $4,
                    outcome_return_pct = $5,
                    outcome_days = $6,
                    closed_at = $7
                WHERE decision_log_id = $1
                """,
                decision_log_id,
                label_target_hit,
                label_stop_hit,
                label_timeout,
                _safe_numeric(final_pnl_pct),
                outcome_days,
                _ensure_utc(closed_at) if closed_at else None,
            )

        updated = not result.endswith("0")
        if updated:
            logger.info(
                "FeatureBuilder.fill_labels_for_closed: decision_log_id=%s reason=%s pnl=%s",
                decision_log_id,
                reason,
                final_pnl_pct,
            )
        return updated

    async def load_training_frame(
        self,
        only_labeled: bool = True,
        min_captured_at: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Devuelve DataFrame listo para train_model.py.
        """
        where = []
        params: list[Any] = []

        if only_labeled:
            where.append(f"{LABEL_COLUMN} IS NOT NULL")

        if min_captured_at is not None:
            params.append(_ensure_utc(min_captured_at))
            where.append(f"captured_at >= ${len(params)}")

        where_sql = ""
        if where:
            where_sql = "WHERE " + " AND ".join(where)

        query = f"""
            SELECT
                {", ".join(ALL_COLUMNS)}
            FROM ml_decision_features
            {where_sql}
            ORDER BY captured_at ASC
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        if not rows:
            return pd.DataFrame(columns=ALL_COLUMNS)

        df = pd.DataFrame([dict(r) for r in rows])
        if "captured_at" in df.columns:
            df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
        if "closed_at" in df.columns:
            df["closed_at"] = pd.to_datetime(df["closed_at"], utc=True, errors="coerce")
        return df

    async def get_features_by_decision(self, decision_log_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM ml_decision_features
                WHERE decision_log_id = $1
                """,
                decision_log_id,
            )
        return dict(row) if row else None

    async def count_rows(self) -> int:
        async with self.pool.acquire() as conn:
            value = await conn.fetchval("SELECT COUNT(*) FROM ml_decision_features")
        return int(value or 0)

    async def count_labeled_rows(self) -> int:
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                f"SELECT COUNT(*) FROM ml_decision_features WHERE {LABEL_COLUMN} IS NOT NULL"
            )
        return int(value or 0)


# ── Helpers públicos para integración simple ─────────────────────

def build_feature_payload(
    *,
    final_score: Optional[float] = None,
    confidence_score: Optional[float] = None,
    prob_target_hit_prior: Optional[float] = None,
    expected_value_prior: Optional[float] = None,
    stop_loss_pct: Optional[float] = None,
    target_pct: Optional[float] = None,
    rr_ratio: Optional[float] = None,
    horizon_days: Optional[float] = None,
    size_pct: Optional[float] = None,
    technical_score: Optional[float] = None,
    rsi_14: Optional[float] = None,
    macd_hist: Optional[float] = None,
    bb_pos: Optional[float] = None,
    atr_pct: Optional[float] = None,
    distance_sma20_pct: Optional[float] = None,
    distance_sma50_pct: Optional[float] = None,
    distance_sma200_pct: Optional[float] = None,
    momentum_20d: Optional[float] = None,
    momentum_60d: Optional[float] = None,
    volatility_20d: Optional[float] = None,
    drawdown_60d: Optional[float] = None,
    macro_score: Optional[float] = None,
    vix_level: Optional[float] = None,
    spy_return_5d: Optional[float] = None,
    spy_return_20d: Optional[float] = None,
    dxy_return_20d: Optional[float] = None,
    tnx_level: Optional[float] = None,
    wti_return_20d: Optional[float] = None,
    regime_code: Optional[float] = None,
    cash_pct: Optional[float] = None,
    portfolio_concentration_pct: Optional[float] = None,
    weight_in_portfolio_pct: Optional[float] = None,
    relative_strength_vs_spy_20d: Optional[float] = None,
    sector_score: Optional[float] = None,
    sector_momentum_20d: Optional[float] = None,
    sector_relative_strength_20d: Optional[float] = None,
) -> dict:
    """
    Helper explícito para construir el payload sin depender de kwargs libres.
    """
    return {
        "final_score": _safe_numeric(final_score),
        "confidence_score": _safe_numeric(confidence_score),
        "prob_target_hit_prior": _safe_numeric(prob_target_hit_prior),
        "expected_value_prior": _safe_numeric(expected_value_prior),
        "stop_loss_pct": _safe_numeric(stop_loss_pct),
        "target_pct": _safe_numeric(target_pct),
        "rr_ratio": _safe_numeric(rr_ratio),
        "horizon_days": _safe_numeric(horizon_days),
        "size_pct": _safe_numeric(size_pct),
        "technical_score": _safe_numeric(technical_score),
        "rsi_14": _safe_numeric(rsi_14),
        "macd_hist": _safe_numeric(macd_hist),
        "bb_pos": _safe_numeric(bb_pos),
        "atr_pct": _safe_numeric(atr_pct),
        "distance_sma20_pct": _safe_numeric(distance_sma20_pct),
        "distance_sma50_pct": _safe_numeric(distance_sma50_pct),
        "distance_sma200_pct": _safe_numeric(distance_sma200_pct),
        "momentum_20d": _safe_numeric(momentum_20d),
        "momentum_60d": _safe_numeric(momentum_60d),
        "volatility_20d": _safe_numeric(volatility_20d),
        "drawdown_60d": _safe_numeric(drawdown_60d),
        "macro_score": _safe_numeric(macro_score),
        "vix_level": _safe_numeric(vix_level),
        "spy_return_5d": _safe_numeric(spy_return_5d),
        "spy_return_20d": _safe_numeric(spy_return_20d),
        "dxy_return_20d": _safe_numeric(dxy_return_20d),
        "tnx_level": _safe_numeric(tnx_level),
        "wti_return_20d": _safe_numeric(wti_return_20d),
        "regime_code": _safe_numeric(regime_code),
        "cash_pct": _safe_numeric(cash_pct),
        "portfolio_concentration_pct": _safe_numeric(portfolio_concentration_pct),
        "weight_in_portfolio_pct": _safe_numeric(weight_in_portfolio_pct),
        "relative_strength_vs_spy_20d": _safe_numeric(relative_strength_vs_spy_20d),
        "sector_score": _safe_numeric(sector_score),
        "sector_momentum_20d": _safe_numeric(sector_momentum_20d),
        "sector_relative_strength_20d": _safe_numeric(sector_relative_strength_20d),
    }


# ── Helpers internos ──────────────────────────────────────────────

def _safe_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)