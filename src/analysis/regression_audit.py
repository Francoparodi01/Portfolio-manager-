"""
src/analysis/regression_audit.py

Auditoría estadística para calibrar el sistema cuantitativo.

Objetivo:
    No reemplaza al Execution Planner.
    No genera órdenes.
    No decide BUY/SELL.

Sirve para responder:
    - ¿El final_score predice retornos futuros?
    - ¿El score funciona en retorno bruto del activo?
    - ¿El score funciona sobre el resultado direccional de la decisión?
    - ¿Qué capas aportan más?
    - ¿Qué umbral de score empieza a cubrir costos?

Datos usados:
    Usa decision_log, no precios históricos crudos.

    final_score     = score generado por tu análisis
    outcome_5d/10d  = retorno posterior guardado en DB
    decision        = BUY / SELL / HOLD / WATCH / BLOCKED

Targets:
    raw:
        outcome_Xd tal como está guardado.

    directional:
        BUY  -> outcome_Xd
        SELL -> -outcome_Xd
        HOLD/WATCH/BLOCKED -> se excluyen por default
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import asyncpg
import numpy as np
import pandas as pd

try:
    import statsmodels.api as sm
    HAS_STATSMODELS = True
except Exception:
    sm = None
    HAS_STATSMODELS = False


DEFAULT_HORIZONS = ("5d", "10d", "20d")
ACTIVE_ACTIONS = ("BUY", "SELL", "SELL_PARTIAL", "SELL_FULL")


@dataclass
class RegressionAuditConfig:
    database_url: str
    days: int = 180
    min_n: int = 12
    cost_bps: float = 75.0
    horizons: tuple[str, ...] = DEFAULT_HORIZONS

    # raw = outcome bruto del activo
    # directional = outcome ajustado por dirección de la decisión
    target_mode: str = "directional"

    # acciones a incluir. None = default según target_mode
    actions: Optional[tuple[str, ...]] = None

    # fecha mínima opcional ISO YYYY-MM-DD
    since: Optional[str] = None

    # incluir HOLD/WATCH/BLOCKED en raw audit si se desea
    include_non_active: bool = False


@dataclass
class RegressionModelResult:
    horizon: str
    model_name: str
    target_col: str
    features: list[str]
    n: int
    r2: Optional[float]
    adj_r2: Optional[float]
    rmse: Optional[float]
    intercept: Optional[float]
    coefficients: dict[str, float]
    pvalues: dict[str, float]
    score_coef: Optional[float]
    score_pvalue: Optional[float]
    ic: Optional[float]
    suggested_buy_threshold: Optional[float]
    expected_return_at_buy_min_008: Optional[float]
    notes: list[str]


@dataclass
class RegressionAuditReport:
    generated_at: datetime
    rows_loaded: int
    rows_usable: int
    cost_threshold: float
    target_mode: str
    actions_used: list[str]
    models: list[RegressionModelResult]
    bucket_tables: dict[str, pd.DataFrame]
    warnings: list[str]


# ══════════════════════════════════════════════════════════════════════════════
# DB LOADING
# ══════════════════════════════════════════════════════════════════════════════

async def _get_existing_columns(conn, table: str = "decision_log") -> set[str]:
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1
        """,
        table,
    )
    return {str(r["column_name"]) for r in rows}


async def load_decision_log(
    database_url: str,
    days: int = 180,
    since: Optional[str] = None,
) -> pd.DataFrame:
    """
    Carga decision_log de manera tolerante a columnas faltantes.

    No usa precios históricos crudos.
    Usa los resultados ya calculados por update_outcomes / decision_log.
    """
    conn = await asyncpg.connect(database_url)

    try:
        cols = await _get_existing_columns(conn, "decision_log")

        wanted = [
            "id",
            "decided_at",
            "ticker",
            "decision",
            "final_score",
            "confidence",
            "conviction",
            "layers",
            "price_at_decision",
            "vix_at_decision",
            "regime",
            "size_pct",
            "stop_loss_pct",
            "target_pct",
            "horizon_days",
            "outcome_5d",
            "outcome_10d",
            "outcome_20d",
            "was_correct",
            "guard_triggered",
            "block_reason",
        ]

        selected = [c for c in wanted if c in cols]

        if not selected:
            return pd.DataFrame()

        if since:
            try:
                cutoff = datetime.fromisoformat(since).replace(tzinfo=timezone.utc)
            except Exception:
                cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
        else:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)

        query = f"""
            SELECT {", ".join(selected)}
            FROM decision_log
            WHERE decided_at >= $1
            ORDER BY decided_at ASC
        """

        rows = await conn.fetch(query, cutoff)

        if not rows:
            return pd.DataFrame(columns=selected)

        df = pd.DataFrame([dict(r) for r in rows])

    finally:
        await conn.close()

    return normalize_decision_frame(df)


# ══════════════════════════════════════════════════════════════════════════════
# NORMALIZATION
# ══════════════════════════════════════════════════════════════════════════════

def normalize_decision_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    if "decided_at" in out.columns:
        out["decided_at"] = pd.to_datetime(out["decided_at"], utc=True, errors="coerce")

    if "decision" in out.columns:
        out["decision"] = out["decision"].astype(str).str.upper().str.strip()

    for col in [
        "final_score",
        "confidence",
        "conviction",
        "price_at_decision",
        "vix_at_decision",
        "size_pct",
        "stop_loss_pct",
        "target_pct",
        "horizon_days",
        "outcome_5d",
        "outcome_10d",
        "outcome_20d",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["confidence", "conviction"]:
        if col in out.columns:
            mask = out[col].abs() > 1.0
            out.loc[mask, col] = out.loc[mask, col] / 100.0

    # Extraer capas desde JSONB layers.
    if "layers" in out.columns:
        layer_rows = out["layers"].apply(_extract_layers)
        layer_df = pd.DataFrame(list(layer_rows))

        for c in layer_df.columns:
            out[c] = pd.to_numeric(layer_df[c], errors="coerce")

    for col in ["technical_score", "macro_score", "sentiment_score", "risk_score"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = out[col].fillna(0.0)

    # Outcome sanity:
    # outcome normal: 0.078 = +7.8%.
    # si viene como 7.8, lo convertimos.
    for col in ["outcome_5d", "outcome_10d", "outcome_20d"]:
        if col in out.columns:
            med = out[col].dropna().abs().median()
            if pd.notna(med) and med > 2:
                out[col] = out[col] / 100.0

    return out


def _extract_layers(raw: Any) -> dict[str, float]:
    """
    Soporta formatos:
    - dict JSONB:
      {"technical": {"weighted": 0.03}, "macro": {"weighted": 0.02}}

    - list:
      [{"name": "technical", "weighted": 0.03}, ...]

    - string JSON
    """
    result = {
        "technical_score": 0.0,
        "macro_score": 0.0,
        "sentiment_score": 0.0,
        "risk_score": 0.0,
    }

    if raw is None:
        return result

    try:
        if isinstance(raw, str):
            raw = json.loads(raw)
    except Exception:
        return result

    def pick_value(obj: dict) -> float:
        for key in (
            "weighted",
            "score",
            "value",
            "raw",
            "final",
            "weighted_score",
            "layer_score",
        ):
            if key in obj and obj[key] is not None:
                try:
                    return float(obj[key])
                except Exception:
                    continue
        return 0.0

    if isinstance(raw, dict):
        # Caso posible: {"layers": [...]}
        if "layers" in raw and isinstance(raw["layers"], list):
            return _extract_layers(raw["layers"])

        for name, value in raw.items():
            n = str(name).lower()

            if isinstance(value, dict):
                v = pick_value(value)
            else:
                try:
                    v = float(value)
                except Exception:
                    v = 0.0

            if "tech" in n:
                result["technical_score"] = v
            elif "macro" in n:
                result["macro_score"] = v
            elif "sent" in n or "news" in n:
                result["sentiment_score"] = v
            elif "risk" in n:
                result["risk_score"] = v

        return result

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue

            n = str(
                item.get("name")
                or item.get("layer")
                or item.get("type")
                or ""
            ).lower()

            v = pick_value(item)

            if "tech" in n:
                result["technical_score"] = v
            elif "macro" in n:
                result["macro_score"] = v
            elif "sent" in n or "news" in n:
                result["sentiment_score"] = v
            elif "risk" in n:
                result["risk_score"] = v

    return result


# ══════════════════════════════════════════════════════════════════════════════
# TARGET BUILDING
# ══════════════════════════════════════════════════════════════════════════════

def prepare_model_frame(
    df: pd.DataFrame,
    horizon: str,
    config: RegressionAuditConfig,
) -> tuple[pd.DataFrame, str, list[str], list[str]]:
    """
    Prepara target según target_mode.

    raw:
        target = outcome_Xd

    directional:
        BUY  => outcome_Xd
        SELL => -outcome_Xd
    """
    warnings: list[str] = []

    if df.empty:
        return df.copy(), f"outcome_{horizon}", [], warnings

    out = df.copy()
    raw_col = f"outcome_{horizon}"

    if raw_col not in out.columns:
        return pd.DataFrame(), raw_col, [], [f"No existe columna {raw_col}"]

    target_col = raw_col

    # Elegir acciones
    if config.actions:
        actions = [a.upper().strip() for a in config.actions]
    elif config.target_mode == "directional":
        actions = list(ACTIVE_ACTIONS)
    else:
        actions = [] if config.include_non_active else list(ACTIVE_ACTIONS)

    if actions and "decision" in out.columns:
        out = out[out["decision"].isin(actions)].copy()

    if config.target_mode == "directional":
        target_col = f"directional_{horizon}"

        def directional(row) -> float:
            val = row.get(raw_col)
            dec = str(row.get("decision", "")).upper()

            if pd.isna(val):
                return np.nan

            if dec == "BUY":
                return float(val)

            if dec in ("SELL", "SELL_PARTIAL", "SELL_FULL"):
                return -float(val)

            return np.nan

        out[target_col] = out.apply(directional, axis=1)

        if out[target_col].notna().sum() == 0:
            warnings.append(
                f"Sin datos direccionales para {horizon}. "
                "Necesitás BUY/SELL con outcome poblado."
            )

    else:
        out[target_col] = out[raw_col]

    actions_used = sorted(out["decision"].dropna().unique().tolist()) if "decision" in out.columns else []

    return out, target_col, actions_used, warnings


# ══════════════════════════════════════════════════════════════════════════════
# MODELING
# ══════════════════════════════════════════════════════════════════════════════
def render_regression_audit_compact(report: RegressionAuditReport) -> str:
    """
    Render compacto para Telegram.

    Objetivo:
    - Evitar que Telegram parta el mensaje.
    - Mostrar solo lo accionable para MVP.
    - Dejar el reporte completo para CLI o /regression_full.
    """
    lines: list[str] = []

    title_target = (
        "RETORNO DIRECCIONAL"
        if report.target_mode == "directional"
        else "RETORNO BRUTO"
    )

    lines.append("📈 <b>REGRESSION AUDIT — RESUMEN</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🎯 Target: <b>{title_target}</b>")
    lines.append(
        f"📦 Filas: <b>{report.rows_loaded}</b> | "
        f"Obs usadas: <b>{report.rows_usable}</b>"
    )
    lines.append(f"💸 Costo mínimo: <b>{report.cost_threshold:.2%}</b>")

    if report.actions_used:
        lines.append(f"🔎 Acciones: <code>{', '.join(report.actions_used)}</code>")

    # Warning compacto por layers
    has_layer_warning = any(
        "está todo en 0" in w
        for w in report.warnings or []
    )

    if has_layer_warning:
        lines.append("")
        lines.append(
            "⚠️ Capas técnicas/macro/riesgo aún no disponibles en decisiones viejas."
        )

    # Buscar baseline 5d
    baseline_5d = next(
        (
            m for m in report.models
            if m.horizon == "5d" and m.model_name == "baseline_score"
        ),
        None,
    )

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("<b>HORIZONTE 5D</b>")

    if baseline_5d is None:
        lines.append("⚠️ Sin modelo baseline 5D disponible.")
    elif baseline_5d.r2 is None:
        lines.append(
            f"⚠️ Sin modelo: {' | '.join(baseline_5d.notes)}"
        )
    else:
        lines.append(
            f"n={baseline_5d.n} | "
            f"R² <b>{baseline_5d.r2:.3f}</b> | "
            f"RMSE {baseline_5d.rmse:.2%}"
        )

        if baseline_5d.ic is not None:
            lines.append(
                f"IC score/target: <code>{baseline_5d.ic:+.3f}</code>"
            )

        if baseline_5d.score_coef is not None:
            ptxt = (
                f" | p={baseline_5d.score_pvalue:.3f}"
                if baseline_5d.score_pvalue is not None
                else ""
            )
            lines.append(
                f"Coef score: <code>{baseline_5d.score_coef:+.4f}</code>{ptxt}"
            )

        if baseline_5d.expected_return_at_buy_min_008 is not None:
            lines.append(
                f"Score +0.08 → ret esperado "
                f"<b>{baseline_5d.expected_return_at_buy_min_008:+.2%}</b>"
            )

        if baseline_5d.suggested_buy_threshold is not None:
            th = baseline_5d.suggested_buy_threshold
            if 0.00 <= th <= 0.30:
                lines.append(
                    f"Umbral estimado para cubrir costo: <b>{th:+.3f}</b>"
                )
            else:
                lines.append("Umbral estimado: <b>fuera de rango razonable</b>")

    # Buckets 5D compactos
    bucket = report.bucket_tables.get("5d")

    if bucket is not None and not bucket.empty:
        lines.append("")
        lines.append("<b>Buckets 5D</b>")

        wanted = {"POS_DEBIL", "POS_OPERABLE", "POS_FUERTE", "NEG_OPERABLE"}

        for _, row in bucket.iterrows():
            bucket_name = str(row["bucket"])

            if bucket_name not in wanted:
                continue

            if int(row["n"]) == 0:
                continue

            lines.append(
                f"  {bucket_name}: n={int(row['n'])} | "
                f"target {row['avg_return']:+.2%} | "
                f"hit {row['hit_rate']:.0%}"
            )

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("<b>LECTURA</b>")

    for line in _build_human_reading(report):
        lines.append(line)

    lines.append("")
    lines.append(
        "<i>Auditoría auxiliar — no genera órdenes ni reemplaza al Execution Planner.</i>"
    )

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# BUCKETS / RENDER
# ══════════════════════════════════════════════════════════════════════════════

def build_score_bucket_table(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    cols = ["final_score", target_col]
    data = df[cols].replace([np.inf, -np.inf], np.nan).dropna().copy()

    if len(data) < 5:
        return pd.DataFrame()

    bins = [-np.inf, -0.08, -0.05, 0.05, 0.08, 0.12, np.inf]
    labels = [
        "NEG_OPERABLE",
        "NEG_DEBIL",
        "NEUTRAL",
        "POS_DEBIL",
        "POS_OPERABLE",
        "POS_FUERTE",
    ]

    data["bucket"] = pd.cut(
        data["final_score"],
        bins=bins,
        labels=labels,
        include_lowest=True,
    )

    out = (
        data.groupby("bucket", observed=False)
        .agg(
            n=(target_col, "count"),
            avg_score=("final_score", "mean"),
            avg_return=(target_col, "mean"),
            hit_rate=(target_col, lambda s: float((s > 0).mean())),
        )
        .reset_index()
    )

    return out


def render_regression_audit(report: RegressionAuditReport) -> str:
    lines: list[str] = []

    title_target = "RETORNO DIRECCIONAL" if report.target_mode == "directional" else "RETORNO BRUTO"

    lines.append("📈 <b>REGRESSION AUDIT — CALIBRACIÓN DEL SCORE</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🕐 {report.generated_at.astimezone().strftime('%d/%m/%Y %H:%M')}")
    lines.append(f"🎯 Target: <b>{title_target}</b>")
    lines.append(f"📦 Filas cargadas: <b>{report.rows_loaded}</b>")
    lines.append(f"🧪 Observaciones usadas acumuladas: <b>{report.rows_usable}</b>")
    lines.append(f"💸 Costo mínimo a cubrir: <b>{report.cost_threshold:.2%}</b>")

    if report.actions_used:
        lines.append(f"🔎 Acciones incluidas: <code>{', '.join(report.actions_used)}</code>")

    if report.target_mode == "directional":
        lines.append("Direccional: BUY usa outcome; SELL invierte el signo del outcome.")

    if report.warnings:
        lines.append("")
        lines.append("⚠️ <b>Warnings</b>")
        for w in report.warnings[:5]:
            lines.append(f"• {w}")

    lines.append("")

    if not report.models:
        lines.append("⚠️ No hay suficientes datos cerrados para correr regresión.")
        lines.append("")
        lines.append("<i>Necesitás más decisiones con outcome_5d / outcome_10d / outcome_20d poblados.</i>")
        return "\n".join(lines)

    by_horizon: dict[str, list[RegressionModelResult]] = {}
    for m in report.models:
        by_horizon.setdefault(m.horizon, []).append(m)

    for horizon, models in by_horizon.items():
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"<b>HORIZONTE {horizon.upper()}</b>")

        for m in models:
            lines.append("")
            lines.append(f"Modelo: <b>{m.model_name}</b>")
            lines.append(f"Target col: <code>{m.target_col}</code>")
            lines.append(f"Features: <code>{', '.join(m.features) if m.features else '—'}</code>")
            lines.append(f"n={m.n}")

            if m.r2 is None:
                lines.append("⚠️ Sin modelo: " + " | ".join(m.notes))
                continue

            lines.append(
                f"R²: <b>{m.r2:.3f}</b>"
                + (f" | Adj R²: {m.adj_r2:.3f}" if m.adj_r2 is not None else "")
                + (f" | RMSE: {m.rmse:.2%}" if m.rmse is not None else "")
            )

            if m.ic is not None:
                lines.append(f"IC simple score/target: <code>{m.ic:+.3f}</code>")

            if m.intercept is not None:
                lines.append(f"Intercept: <code>{m.intercept:+.4f}</code>")

            if m.score_coef is not None:
                pv = ""
                if m.score_pvalue is not None:
                    pv = f" | p={m.score_pvalue:.3f}"

                lines.append(
                    f"Coef final_score: <code>{m.score_coef:+.4f}</code>{pv}"
                )

            layer_coefs = {
                k: v
                for k, v in m.coefficients.items()
                if k != "final_score"
            }

            if layer_coefs:
                lines.append("Coef features:")
                for k, v in layer_coefs.items():
                    ptxt = ""
                    if k in m.pvalues:
                        ptxt = f" p={m.pvalues[k]:.3f}"
                    lines.append(f"  {k}: <code>{v:+.4f}</code>{ptxt}")

            if m.expected_return_at_buy_min_008 is not None:
                lines.append(
                    f"Ret esperado con score +0.08: "
                    f"<b>{m.expected_return_at_buy_min_008:+.2%}</b>"
                )

            if m.suggested_buy_threshold is not None:
                lines.append(
                    f"Umbral score estimado para cubrir costo: "
                    f"<b>{m.suggested_buy_threshold:+.3f}</b>"
                )
            else:
                lines.append(
                    "Umbral score estimado: <b>N/A</b> "
                    "(coeficiente no positivo o datos insuficientes)"
                )

            for note in m.notes:
                lines.append(f"• {note}")

        bucket = report.bucket_tables.get(horizon)
        if bucket is not None and not bucket.empty:
            lines.append("")
            lines.append("<b>Buckets por score</b>")
            for _, row in bucket.iterrows():
                if int(row["n"]) == 0:
                    continue
                lines.append(
                    f"  {row['bucket']}: n={int(row['n'])} | "
                    f"avg score {row['avg_score']:+.3f} | "
                    f"target {row['avg_return']:+.2%} | "
                    f"hit {row['hit_rate']:.0%}"
                )

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("<b>LECTURA</b>")
    lines.extend(_build_human_reading(report))

    lines.append("")
    lines.append("<i>Auditoría estadística auxiliar — no genera órdenes ni reemplaza al Execution Planner.</i>")

    return "\n".join(lines)


def _build_human_reading(report: RegressionAuditReport) -> list[str]:
    lines: list[str] = []

    baseline_5d = next(
        (
            m for m in report.models
            if m.horizon == "5d" and m.model_name == "baseline_score"
        ),
        None,
    )

    if baseline_5d is None or baseline_5d.r2 is None:
        return [
            "⚠️ Todavía no hay suficiente muestra para calibrar thresholds con regresión.",
            "Seguí acumulando decisiones cerradas y outcomes.",
        ]

    coef = baseline_5d.score_coef
    pval = baseline_5d.score_pvalue
    r2 = baseline_5d.r2
    ic = baseline_5d.ic

    if coef is None:
        return ["⚠️ No se pudo estimar coeficiente de final_score."]

    target_name = "direccional" if report.target_mode == "directional" else "bruto"

    if coef <= 0:
        lines.append(
            f"🔴 El coeficiente de final_score es negativo o nulo sobre retorno {target_name}: "
            "en esta muestra, mayor score no se tradujo en mejor resultado."
        )
        lines.append(
            "No conviene bajar thresholds todavía. Mantener TRADE_QUALITY_GUARD conservador."
        )
        return lines

    if pval is not None and pval > 0.10:
        lines.append(
            f"🟡 El coeficiente de final_score es positivo sobre retorno {target_name}, "
            "pero no significativo. Hay señal posible, pero todavía débil."
        )
    else:
        lines.append(
            f"🟢 El coeficiente de final_score es positivo sobre retorno {target_name} "
            "y estadísticamente útil para calibración."
        )

    if r2 < 0.03:
        lines.append(
            "R² bajo: el score explica poco del resultado. Usarlo como filtro, no como predictor exacto."
        )
    elif r2 < 0.10:
        lines.append(
            "R² moderado/bajo: el score aporta, pero el ruido sigue dominando."
        )
    else:
        lines.append(
            "R² razonable para una señal financiera simple. Buena base para calibrar thresholds."
        )

    if ic is not None:
        if ic < 0:
            lines.append(
                "IC negativo: mantener modo conservador hasta mejorar consistencia."
            )
        elif ic < 0.05:
            lines.append(
                "IC positivo débil: hay algo de relación, pero todavía no suficiente para agresividad."
            )
        else:
            lines.append(
                "IC positivo relevante: el ranking de scores empieza a tener valor predictivo."
            )

    if baseline_5d.suggested_buy_threshold is not None:
        th = baseline_5d.suggested_buy_threshold
        if 0.00 <= th <= 0.30:
            lines.append(
                f"Umbral sugerido para cubrir costos: score >= {th:+.3f}. "
                "Comparar contra el BUY_MIN actual antes de tocar el planner."
            )
        else:
            lines.append(
                f"Umbral estimado fuera de rango razonable ({th:+.3f}); no ajustar automáticamente."
            )

    return lines


# ══════════════════════════════════════════════════════════════════════════════
# MODELING
# ══════════════════════════════════════════════════════════════════════════════

def run_regression_audit_sync(
    df: pd.DataFrame,
    config: RegressionAuditConfig,
) -> RegressionAuditReport:
    """
    Ejecuta auditoría estadística sobre un DataFrame ya cargado.
    """
    generated_at = datetime.now(tz=timezone.utc)
    cost_threshold = float(config.cost_bps) / 10_000.0

    if df.empty:
        return RegressionAuditReport(
            generated_at=generated_at,
            rows_loaded=0,
            rows_usable=0,
            cost_threshold=cost_threshold,
            target_mode=config.target_mode,
            actions_used=[],
            models=[],
            bucket_tables={},
            warnings=["No se cargaron filas desde decision_log."],
        )

    models: list[RegressionModelResult] = []
    buckets: dict[str, pd.DataFrame] = {}
    warnings: list[str] = []

    rows_loaded = len(df)
    rows_usable_total = 0
    all_actions_used: set[str] = set()

    # Detectar si las capas están vacías.
    for col in ["technical_score", "macro_score", "sentiment_score", "risk_score"]:
        if col in df.columns and df[col].abs().sum() == 0:
            warnings.append(
                f"{col} está todo en 0. Revisar guardado/extracción de layers si querés regresión por capas."
            )

    for horizon in config.horizons:
        hdf, target_col, actions_used, prep_warnings = prepare_model_frame(
            df=df,
            horizon=horizon,
            config=config,
        )

        warnings.extend(prep_warnings)
        all_actions_used.update(actions_used)

        if hdf.empty or target_col not in hdf.columns:
            continue

        hdf = hdf.dropna(subset=["final_score", target_col]).copy()
        hdf = hdf[np.isfinite(hdf["final_score"])]
        hdf = hdf[np.isfinite(hdf[target_col])]

        if hdf.empty:
            continue

        rows_usable_total += len(hdf)

        buckets[horizon] = build_score_bucket_table(hdf, target_col)

        # Modelo simple: score → outcome
        models.append(
            fit_ols_model(
                df=hdf,
                horizon=horizon,
                model_name="baseline_score",
                target_col=target_col,
                features=["final_score"],
                min_n=config.min_n,
                cost_threshold=cost_threshold,
            )
        )

        # Modelo por capas
        layer_features = [
            "final_score",
            "technical_score",
            "macro_score",
            "sentiment_score",
            "risk_score",
        ]
        layer_features = [f for f in layer_features if f in hdf.columns]

        models.append(
            fit_ols_model(
                df=hdf,
                horizon=horizon,
                model_name="score_layers",
                target_col=target_col,
                features=layer_features,
                min_n=config.min_n,
                cost_threshold=cost_threshold,
            )
        )

        # Modelo contexto
        context_features = [
            "final_score",
            "vix_at_decision",
            "size_pct",
            "technical_score",
            "macro_score",
        ]
        context_features = [f for f in context_features if f in hdf.columns]

        if (
            "vix_at_decision" in context_features
            and hdf["vix_at_decision"].notna().sum() >= config.min_n
        ):
            models.append(
                fit_ols_model(
                    df=hdf,
                    horizon=horizon,
                    model_name="score_context",
                    target_col=target_col,
                    features=context_features,
                    min_n=config.min_n,
                    cost_threshold=cost_threshold,
                )
            )

    warnings = list(dict.fromkeys(warnings))

    return RegressionAuditReport(
        generated_at=generated_at,
        rows_loaded=rows_loaded,
        rows_usable=rows_usable_total,
        cost_threshold=cost_threshold,
        target_mode=config.target_mode,
        actions_used=sorted(all_actions_used),
        models=models,
        bucket_tables=buckets,
        warnings=warnings,
    )


def fit_ols_model(
    df: pd.DataFrame,
    horizon: str,
    model_name: str,
    target_col: str,
    features: list[str],
    min_n: int,
    cost_threshold: float,
) -> RegressionModelResult:
    notes: list[str] = []

    # Quitar features constantes/vacías para evitar p=nan.
    clean_features = []

    for f in features:
        if f not in df.columns:
            continue

        s = pd.to_numeric(df[f], errors="coerce")

        if s.dropna().nunique() <= 1:
            if f != "final_score":
                notes.append(f"Feature {f} omitida por ser constante o vacía.")
            else:
                clean_features.append(f)
            continue

        clean_features.append(f)

    features = clean_features

    usable_cols = [target_col] + features
    mdf = df[usable_cols].replace([np.inf, -np.inf], np.nan).dropna().copy()

    n = len(mdf)

    if n < min_n:
        return RegressionModelResult(
            horizon=horizon,
            model_name=model_name,
            target_col=target_col,
            features=features,
            n=n,
            r2=None,
            adj_r2=None,
            rmse=None,
            intercept=None,
            coefficients={},
            pvalues={},
            score_coef=None,
            score_pvalue=None,
            ic=_corr(df.get("final_score"), df.get(target_col)),
            suggested_buy_threshold=None,
            expected_return_at_buy_min_008=None,
            notes=[f"Muestra insuficiente: n={n}, mínimo={min_n}"] + notes,
        )

    if not features:
        return RegressionModelResult(
            horizon=horizon,
            model_name=model_name,
            target_col=target_col,
            features=[],
            n=n,
            r2=None,
            adj_r2=None,
            rmse=None,
            intercept=None,
            coefficients={},
            pvalues={},
            score_coef=None,
            score_pvalue=None,
            ic=None,
            suggested_buy_threshold=None,
            expected_return_at_buy_min_008=None,
            notes=["Sin features útiles para regresión"] + notes,
        )

    X = mdf[features].astype(float)
    y = mdf[target_col].astype(float)

    ic = _corr(mdf["final_score"], y) if "final_score" in mdf.columns else None

    if not HAS_STATSMODELS:
        notes.append("statsmodels no instalado; usando fallback numpy sin p-values")
        return _fit_numpy_fallback(
            horizon=horizon,
            model_name=model_name,
            target_col=target_col,
            features=features,
            X=X,
            y=y,
            cost_threshold=cost_threshold,
            ic=ic,
            notes=notes,
        )

    X_sm = sm.add_constant(X, has_constant="add")
    model = sm.OLS(y, X_sm).fit()

    preds = model.predict(X_sm)
    rmse = float(np.sqrt(np.mean((y - preds) ** 2)))

    params = model.params.to_dict()
    pvals = model.pvalues.to_dict()

    intercept = float(params.get("const", 0.0))

    coefficients = {
        k: float(v)
        for k, v in params.items()
        if k != "const"
    }

    pvalues = {
        k: float(v)
        for k, v in pvals.items()
        if k != "const"
    }

    score_coef = coefficients.get("final_score")
    score_pvalue = pvalues.get("final_score")

    # Importante:
    # Threshold y retorno esperado con score +0.08 solo tienen sentido
    # en el modelo simple final_score -> target.
    is_score_only_model = features == ["final_score"]

    suggested = (
        _suggest_score_threshold(
            intercept=intercept,
            score_coef=score_coef,
            cost_threshold=cost_threshold,
        )
        if is_score_only_model
        else None
    )

    expected_008 = (
        intercept + score_coef * 0.08
        if is_score_only_model and score_coef is not None
        else None
    )

    if score_coef is not None:
        if score_coef <= 0:
            notes.append(
                "Coeficiente de final_score no positivo: el score no está calibrando retornos positivos en esta muestra."
            )
        elif score_pvalue is not None and score_pvalue > 0.10:
            notes.append(
                "Coeficiente de final_score no significativo con p>0.10; interpretar con cautela."
            )
        else:
            notes.append(
                "Coeficiente de final_score positivo; hay relación útil para calibración."
            )

    return RegressionModelResult(
        horizon=horizon,
        model_name=model_name,
        target_col=target_col,
        features=features,
        n=n,
        r2=float(model.rsquared),
        adj_r2=float(model.rsquared_adj),
        rmse=rmse,
        intercept=intercept,
        coefficients=coefficients,
        pvalues=pvalues,
        score_coef=score_coef,
        score_pvalue=score_pvalue,
        ic=ic,
        suggested_buy_threshold=suggested,
        expected_return_at_buy_min_008=expected_008,
        notes=notes,
    )


def _fit_numpy_fallback(
    horizon: str,
    model_name: str,
    target_col: str,
    features: list[str],
    X: pd.DataFrame,
    y: pd.Series,
    cost_threshold: float,
    ic: Optional[float],
    notes: list[str],
) -> RegressionModelResult:
    X_mat = np.column_stack([np.ones(len(X)), X.values])
    beta, *_ = np.linalg.lstsq(X_mat, y.values, rcond=None)

    preds = X_mat @ beta
    resid = y.values - preds
    ss_res = float(np.sum(resid ** 2))
    ss_tot = float(np.sum((y.values - np.mean(y.values)) ** 2))

    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else None
    rmse = float(np.sqrt(np.mean(resid ** 2)))

    intercept = float(beta[0])

    coefficients = {
        f: float(beta[i + 1])
        for i, f in enumerate(features)
    }

    score_coef = coefficients.get("final_score")

    is_score_only_model = features == ["final_score"]

    suggested = (
        _suggest_score_threshold(
            intercept=intercept,
            score_coef=score_coef,
            cost_threshold=cost_threshold,
        )
        if is_score_only_model
        else None
    )

    expected_008 = (
        intercept + score_coef * 0.08
        if is_score_only_model and score_coef is not None
        else None
    )

    return RegressionModelResult(
        horizon=horizon,
        model_name=model_name,
        target_col=target_col,
        features=features,
        n=len(X),
        r2=r2,
        adj_r2=None,
        rmse=rmse,
        intercept=intercept,
        coefficients=coefficients,
        pvalues={},
        score_coef=score_coef,
        score_pvalue=None,
        ic=ic,
        suggested_buy_threshold=suggested,
        expected_return_at_buy_min_008=expected_008,
        notes=notes,
    )


def _suggest_score_threshold(
    intercept: Optional[float],
    score_coef: Optional[float],
    cost_threshold: float,
) -> Optional[float]:
    """
    Resuelve:
        cost_threshold = intercept + score_coef * score

    Solo usar en modelo simple.
    """
    if intercept is None or score_coef is None:
        return None

    if score_coef <= 0:
        return None

    threshold = (cost_threshold - intercept) / score_coef

    if not math.isfinite(threshold):
        return None

    return float(threshold)


def _corr(x, y) -> Optional[float]:
    try:
        xs = pd.to_numeric(pd.Series(x), errors="coerce")
        ys = pd.to_numeric(pd.Series(y), errors="coerce")
        data = pd.concat([xs, ys], axis=1).dropna()

        if len(data) < 5:
            return None

        if data.iloc[:, 0].std() < 1e-12 or data.iloc[:, 1].std() < 1e-12:
            return None

        c = float(data.iloc[:, 0].corr(data.iloc[:, 1]))

        if not math.isfinite(c):
            return None

        return c
    except Exception:
        return None
    

async def run_regression_audit(
    config: RegressionAuditConfig,
) -> RegressionAuditReport:
    """
    Entry point async usado por scripts/run_regression_audit.py.

    Carga decision_log desde DB y ejecuta la auditoría estadística.
    """
    df = await load_decision_log(
        database_url=config.database_url,
        days=config.days,
        since=getattr(config, "since", None),
    )

    return run_regression_audit_sync(df, config)