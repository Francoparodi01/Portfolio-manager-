"""
src/analysis/decision_memory.py — Memoria de decisiones y convicción ajustada

Tres funciones principales:
  1. save_decision()     — guarda cada decisión del pipeline en DB
  2. load_conviction()   — devuelve convicción ajustada por consistencia histórica
  3. audit_decisions()   — calcula hit rate real de las señales pasadas

Schema nuevo en DB (se crea automáticamente):
  decision_log: historial de decisiones por ticker + outcome real

Integración en run_analysis.py:
  1. Después de blend_scores_local(), llamar load_conviction() para ajustar conf
  2. Al final del run, llamar save_decision() para cada resultado
  3. En el reporte, agregar bloque de auditoría si hay suficiente historia

Filosofía:
  - La convicción no es solo abs(score) — es score × consistencia histórica
  - Una señal que históricamente acierta merece más tamaño
  - Una señal que históricamente falla merece menos, aunque sea fuerte hoy
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── Schema SQL (agregar a db.py init_schema) ──────────────────────────────────
DECISION_MEMORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_log (
    id              BIGSERIAL    PRIMARY KEY,
    decided_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ticker          TEXT         NOT NULL,
    decision        TEXT         NOT NULL,   -- BUY/ACCUMULATE/HOLD/REDUCE/SELL
    final_score     FLOAT        NOT NULL,
    confidence      FLOAT        NOT NULL,
    layers          JSONB,                   -- snapshot de capas en ese momento
    price_at_decision FLOAT,                 -- precio al momento de decidir
    vix_at_decision   FLOAT,
    regime          TEXT,                    -- risk_on/neutral/risk_off
    -- Outcome (se llena después, con job diario/semanal)
    outcome_5d      FLOAT,                   -- retorno 5 días después
    outcome_10d     FLOAT,                   -- retorno 10 días después
    outcome_20d     FLOAT,                   -- retorno 20 días después
    outcome_filled_at TIMESTAMPTZ,           -- cuándo se llenó el outcome
    was_correct     BOOLEAN                  -- True si dirección = retorno > 0
);

CREATE INDEX IF NOT EXISTS idx_decision_log_ticker ON decision_log(ticker);
CREATE INDEX IF NOT EXISTS idx_decision_log_decided_at ON decision_log(decided_at DESC);
"""


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class DecisionRecord:
    ticker: str
    decision: str
    final_score: float
    confidence: float
    layers: dict = field(default_factory=dict)
    price_at_decision: float = 0.0
    vix_at_decision: float = 0.0
    regime: str = "neutral"
    decided_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ConvictionAdjustment:
    ticker: str
    base_confidence: float       # confianza original del pipeline
    consistency_score: float     # hit rate histórico en condiciones similares
    adjusted_confidence: float   # base × consistency
    n_history: int               # cuántas decisiones históricas usamos
    note: str = ""               # razón del ajuste


@dataclass
class DecisionAudit:
    ticker: str
    n_decisions: int
    hit_rate_5d: float           # % de veces que la dirección fue correcta a 5d
    hit_rate_20d: float          # ídem a 20d
    avg_return_when_buy: float   # retorno promedio cuando decimos BUY
    avg_return_when_sell: float  # retorno promedio cuando decimos SELL
    signal_quality: str          # EXCELLENT / GOOD / FAIR / POOR
    best_regime: str             # régimen donde mejor funciona
    worst_regime: str            # régimen donde peor funciona


# ── Funciones principales ─────────────────────────────────────────────────────

async def save_decision(db_pool, record: DecisionRecord) -> bool:
    """
    Guarda una decisión del pipeline en decision_log.
    Llamar al final de cada run, para cada ticker analizado.
    """
    try:
        layers_json = json.dumps({
            k: round(v, 4) for k, v in record.layers.items()
        }) if record.layers else "{}"

        await db_pool.execute("""
            INSERT INTO decision_log
                (decided_at, ticker, decision, final_score, confidence,
                 layers, price_at_decision, vix_at_decision, regime)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
        """,
            record.decided_at, record.ticker, record.decision,
            record.final_score, record.confidence,
            layers_json, record.price_at_decision,
            record.vix_at_decision, record.regime,
        )
        logger.debug(f"Memory: guardada decisión {record.ticker} {record.decision} score={record.final_score:+.3f}")
        return True
    except Exception as e:
        logger.warning(f"Memory: no se pudo guardar decisión {record.ticker}: {e}")
        return False


async def fill_outcomes(db_pool, prices_fn) -> int:
    """
    Llena outcome_5d, outcome_10d, outcome_20d para decisiones sin outcome.
    prices_fn(ticker, date) → precio en esa fecha (o None).

    Llamar desde un job diario (ej: al inicio del run, antes del análisis).
    Retorna cuántos registros actualizó.
    """
    updated = 0
    try:
        # Buscar decisiones sin outcome que tengan al menos 5 días de historia
        cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        rows = await db_pool.fetch("""
            SELECT id, ticker, decision, final_score, decided_at, price_at_decision
            FROM decision_log
            WHERE outcome_filled_at IS NULL
              AND decided_at < $1
            ORDER BY decided_at DESC
            LIMIT 200
        """, cutoff)

        for row in rows:
            tid     = row["id"]
            ticker  = row["ticker"]
            dec_at  = row["decided_at"]
            p0      = float(row["price_at_decision"] or 0)
            decision = row["decision"]

            if p0 <= 0:
                continue

            p5  = await prices_fn(ticker, dec_at + timedelta(days=7))    # ~5 bdays
            p10 = await prices_fn(ticker, dec_at + timedelta(days=14))
            p20 = await prices_fn(ticker, dec_at + timedelta(days=28))

            r5  = (p5 / p0 - 1)  if p5  else None
            r10 = (p10 / p0 - 1) if p10 else None
            r20 = (p20 / p0 - 1) if p20 else None

            # correct = dirección del score coincide con retorno
            bullish = decision in ("BUY", "ACCUMULATE")
            bearish = decision in ("SELL", "REDUCE")
            correct = None
            if r5 is not None:
                if bullish: correct = r5 > 0
                elif bearish: correct = r5 < 0

            await db_pool.execute("""
                UPDATE decision_log
                SET outcome_5d=$1, outcome_10d=$2, outcome_20d=$3,
                    outcome_filled_at=NOW(), was_correct=$4
                WHERE id=$5
            """, r5, r10, r20, correct, tid)
            updated += 1

    except Exception as e:
        logger.warning(f"Memory fill_outcomes: {e}")

    if updated:
        logger.info(f"Memory: actualizados {updated} outcomes")
    return updated


async def load_conviction(db_pool, ticker: str, base_confidence: float,
                           regime: str = "neutral",
                           lookback_days: int = 90) -> ConvictionAdjustment:
    """
    Ajusta la confianza base del pipeline según hit rate histórico.

    Lógica:
      - Si históricamente el sistema acierta >65% en este ticker → boost +15%
      - Si acierta 50-65% → sin cambio
      - Si acierta <50% → penalización -20%
      - Si acierta <35% → penalización -35%
      - Si no hay historia suficiente (< 5 decisiones) → sin ajuste

    Retorna ConvictionAdjustment con la convicción ajustada.
    """
    adj = ConvictionAdjustment(
        ticker=ticker,
        base_confidence=base_confidence,
        consistency_score=0.5,
        adjusted_confidence=base_confidence,
        n_history=0,
        note="sin historia suficiente",
    )

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        rows = await db_pool.fetch("""
            SELECT was_correct, regime, final_score
            FROM decision_log
            WHERE ticker = $1
              AND decided_at > $2
              AND outcome_filled_at IS NOT NULL
              AND decision != 'HOLD'
            ORDER BY decided_at DESC
            LIMIT 30
        """, ticker, cutoff)

        if len(rows) < 5:
            return adj   # sin historia suficiente, no ajustar

        adj.n_history = len(rows)
        correct_count = sum(1 for r in rows if r["was_correct"] is True)
        hit_rate = correct_count / len(rows)
        adj.consistency_score = round(hit_rate, 3)

        # Mismo régimen tiene más peso
        same_regime = [r for r in rows if r["regime"] == regime]
        if len(same_regime) >= 3:
            regime_hr = sum(1 for r in same_regime if r["was_correct"]) / len(same_regime)
            # Blend: 60% hit rate general, 40% hit rate en mismo régimen
            blended_hr = hit_rate * 0.60 + regime_hr * 0.40
        else:
            blended_hr = hit_rate

        # Ajuste de convicción según hit rate
        if blended_hr >= 0.65:
            multiplier = 1.15
            note = f"señal histórica fuerte ({blended_hr:.0%} hit rate, n={len(rows)})"
        elif blended_hr >= 0.50:
            multiplier = 1.00
            note = f"señal histórica normal ({blended_hr:.0%} hit rate, n={len(rows)})"
        elif blended_hr >= 0.35:
            multiplier = 0.80
            note = f"señal histórica débil ({blended_hr:.0%} hit rate) — convicción reducida"
        else:
            multiplier = 0.65
            note = f"señal históricamente mala ({blended_hr:.0%} hit rate) — convicción baja"

        adj.adjusted_confidence = round(min(base_confidence * multiplier, 0.95), 4)
        adj.note = note

    except Exception as e:
        logger.warning(f"Memory load_conviction {ticker}: {e}")

    return adj


async def get_decision_audit(db_pool, ticker: str,
                              lookback_days: int = 180) -> Optional[DecisionAudit]:
    """
    Calcula métricas de calidad de señal para un ticker.
    Retorna None si no hay suficiente historia.
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        rows = await db_pool.fetch("""
            SELECT decision, final_score, outcome_5d, outcome_20d,
                   was_correct, regime
            FROM decision_log
            WHERE ticker = $1
              AND decided_at > $2
              AND outcome_filled_at IS NOT NULL
            ORDER BY decided_at DESC
        """, ticker, cutoff)

        if len(rows) < 5:
            return None

        with_outcome_5d  = [r for r in rows if r["outcome_5d"]  is not None]
        with_outcome_20d = [r for r in rows if r["outcome_20d"] is not None]

        if not with_outcome_5d:
            return None

        hit_5d  = sum(1 for r in with_outcome_5d  if r["was_correct"]) / len(with_outcome_5d)
        hit_20d = sum(1 for r in with_outcome_20d if r["was_correct"]) / len(with_outcome_20d) if with_outcome_20d else 0.0

        buy_rows  = [r for r in rows if r["decision"] in ("BUY", "ACCUMULATE") and r["outcome_5d"] is not None]
        sell_rows = [r for r in rows if r["decision"] in ("SELL", "REDUCE")    and r["outcome_5d"] is not None]

        avg_buy  = sum(r["outcome_5d"] for r in buy_rows)  / len(buy_rows)  if buy_rows  else 0.0
        avg_sell = sum(r["outcome_5d"] for r in sell_rows) / len(sell_rows) if sell_rows else 0.0

        # Calidad de señal
        if hit_5d >= 0.65 and abs(avg_buy) > 0.02:
            quality = "EXCELLENT"
        elif hit_5d >= 0.55:
            quality = "GOOD"
        elif hit_5d >= 0.45:
            quality = "FAIR"
        else:
            quality = "POOR"

        # Régimen con mejor/peor hit rate
        regimes = {}
        for r in with_outcome_5d:
            reg = r["regime"] or "neutral"
            if reg not in regimes:
                regimes[reg] = {"correct": 0, "total": 0}
            regimes[reg]["total"] += 1
            if r["was_correct"]:
                regimes[reg]["correct"] += 1

        regime_rates = {
            reg: v["correct"] / v["total"]
            for reg, v in regimes.items() if v["total"] >= 2
        }
        best_regime  = max(regime_rates, key=regime_rates.get) if regime_rates else "neutral"
        worst_regime = min(regime_rates, key=regime_rates.get) if regime_rates else "neutral"

        return DecisionAudit(
            ticker=ticker,
            n_decisions=len(rows),
            hit_rate_5d=round(hit_5d, 3),
            hit_rate_20d=round(hit_20d, 3),
            avg_return_when_buy=round(avg_buy, 4),
            avg_return_when_sell=round(avg_sell, 4),
            signal_quality=quality,
            best_regime=best_regime,
            worst_regime=worst_regime,
        )

    except Exception as e:
        logger.warning(f"Memory get_audit {ticker}: {e}")
        return None


async def get_portfolio_audit_summary(db_pool, tickers: list[str]) -> dict:
    """
    Resumen de auditoría para todos los tickers del portfolio.
    Para incluir en el reporte de Telegram.
    """
    results = {}
    for ticker in tickers:
        audit = await get_decision_audit(db_pool, ticker)
        if audit:
            results[ticker] = audit
    return results


def format_audit_telegram(audits: dict) -> str:
    """Formatea el bloque de auditoría para Telegram."""
    if not audits:
        return ""

    quality_icons = {
        "EXCELLENT": "🟢",
        "GOOD":      "🟡",
        "FAIR":      "🟠",
        "POOR":      "🔴",
    }

    lines = [
        "━" * 35,
        "🧠 <b>CALIDAD DE SEÑALES (histórico)</b>",
        "",
    ]

    for ticker, a in sorted(audits.items()):
        icon = quality_icons.get(a.signal_quality, "⚪")
        lines.append(
            f"  {icon} <b>{ticker}</b>  "
            f"hit={a.hit_rate_5d:.0%}  "
            f"buy_ret={a.avg_return_when_buy:+.1%}  "
            f"n={a.n_decisions}  [{a.signal_quality}]"
        )
        if a.signal_quality == "POOR":
            lines.append(f"    ⚠️ Señal históricamente débil — reducir tamaño")
        elif a.signal_quality == "EXCELLENT":
            lines.append(f"    ✅ Señal confiable — tamaño normal/ampliado")

    return "\n".join(lines)
