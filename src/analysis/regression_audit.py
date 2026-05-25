"""
src/analysis/regression_audit.py

Auditoría estadística para calibrar el sistema cuantitativo.

Objetivo:
    No reemplaza al Execution Planner.
    No genera órdenes.
    No decide BUY/SELL.

Sirve para responder:
    1. Signal Audit:
        ¿El final_score predice retornos futuros?

    2. Optimizer Audit:
        ¿Los targets / rotaciones teóricas del optimizer tenían buen outcome?

    3. Execution Audit:
        ¿Los fills reales confirmados funcionaron?

    4. Blocked Audit:
        ¿Los guards bloquearon bien o fueron demasiado conservadores?

Datos usados:
    Usa decision_log, no precios históricos crudos.

    final_score     = score generado por el análisis
    outcome_5d/10d  = retorno posterior guardado en DB
    decision        = BUY / SELL / SELL_PARTIAL / SELL_FULL
    source          = optimizer / execution_plan / radar / manual
    decision_type   = theoretical / executable / blocked / manual / pilot
    status          = THEORETICAL / APPROVED / BLOCKED / EXECUTED / EXECUTED_MANUAL / SKIPPED

Targets:
    raw:
        outcome_Xd tal como está guardado.

    directional:
        outcome_Xd canonico ya viene ajustado por la direccion.
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
NULL_TEXT_VALUES = {"", "none", "nan", "nat", "<na>", "null"}

VALID_MODES = ("signal", "optimizer", "execution", "blocked", "all")

MODE_TITLES = {
    "signal": "SIGNAL AUDIT — ¿el score predice retornos?",
    "optimizer": "OPTIMIZER AUDIT — ¿los targets teóricos funcionaron?",
    "execution": "EXECUTION AUDIT — ¿los fills reales funcionaron?",
    "blocked": "BLOCKED AUDIT — ¿los guards bloquearon bien?",
    "all": "AUDIT GLOBAL — mezcla exploratoria",
}

MODE_READINGS = {
    "signal": (
        "Este modo mide si el score ordena correctamente retornos futuros. "
        "No implica que todas las señales debían ejecutarse."
    ),
    "optimizer": (
        "Este modo mide ideas teóricas del optimizer. Sirve para calibrar targets "
        "y detectar si el planner bloquea ideas buenas, pero no mide performance operativa real."
    ),
    "execution": (
        "Este modo mide fills reales confirmados/reconciliados. "
        "Si el fill fue manual, valida la ejecución real pero no necesariamente calibra el score del planner."
    ),
    "blocked": (
        "Este modo mide operaciones rechazadas por guards. Si los outcomes son positivos "
        "de forma consistente, los guards podrían ser demasiado conservadores. "
        "Si son negativos, el planner protegió bien."
    ),
    "all": (
        "Este modo mezcla señales de distintos orígenes. Es exploratorio y no debería usarse "
        "para calibración final sin separar fuentes."
    ),
}


@dataclass
class RegressionAuditConfig:
    database_url: str
    days: int = 180
    min_n: int = 12
    cost_bps: float = 75.0
    horizons: tuple[str, ...] = DEFAULT_HORIZONS

    # signal / optimizer / execution / blocked / all
    mode: str = "optimizer"

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
    threshold_reason: Optional[str]
    expected_return_at_buy_min_008: Optional[float]
    notes: list[str]


@dataclass
class RegressionAuditReport:
    generated_at: datetime
    rows_loaded: int
    rows_usable: int
    cost_threshold: float
    target_mode: str
    mode: str
    actions_used: list[str]
    source_counts: dict[str, int]
    status_counts: dict[str, int]
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
    dsn = database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

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
            "outcome_basis",
            "was_correct",
            "guard_triggered",
            "block_reason",

            # Nuevas columnas de clasificación de evento
            "source",
            "decision_type",
            "status",
            "theoretical_amount_ars",
            "executed_amount_ars",
            "current_weight",
            "target_weight",
            "delta_weight",
            "is_executable",
            "was_blocked",
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
        "source",
        "decision_type",
        "status",
        "block_reason",
        "ticker",
        "regime",
        "outcome_basis",
    ]:
        if col in out.columns:
            out[col] = out[col].astype("string").str.strip()
            out[col] = out[col].mask(out[col].str.lower().isin(NULL_TEXT_VALUES))

    if "source" not in out.columns:
        out["source"] = None

    if "status" not in out.columns:
        out["status"] = None

    if "decision_type" not in out.columns:
        out["decision_type"] = None

    # Completar source desde layers si existe.
    if "layers" in out.columns:
        out["_source_from_layers"] = out["layers"].apply(_extract_source_from_layers)
        out["source"] = out["source"].fillna(out["_source_from_layers"])
        out.drop(columns=["_source_from_layers"], inplace=True, errors="ignore")

    out["source"] = out["source"].fillna("sin_source").astype(str).str.lower().str.strip()
    out["status"] = out["status"].fillna("UNKNOWN").astype(str).str.upper().str.strip()
    out["decision_type"] = out["decision_type"].fillna("unknown").astype(str).str.lower().str.strip()

    numeric_cols = [
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
        "theoretical_amount_ars",
        "executed_amount_ars",
        "current_weight",
        "target_weight",
        "delta_weight",
    ]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["confidence", "conviction"]:
        if col in out.columns:
            mask = out[col].abs() > 1.0
            out.loc[mask, col] = out.loc[mask, col] / 100.0

    for col in ["is_executable", "was_blocked", "was_correct", "guard_triggered"]:
        if col in out.columns:
            out[col] = out[col].map(_to_bool)

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


def _to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None or pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in {"true", "t", "1", "yes", "y", "si", "sí"}


def _json_load_maybe(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        if isinstance(raw, str):
            return json.loads(raw)
    except Exception:
        return None
    return raw


def _extract_source_from_layers(raw: Any) -> Optional[str]:
    raw = _json_load_maybe(raw)
    if not isinstance(raw, dict):
        return None

    for key in ("source", "decision_source", "origin"):
        if key in raw and raw[key]:
            return str(raw[key]).lower().strip()

    extra = raw.get("extra")
    if isinstance(extra, dict):
        for key in ("source", "decision_source", "origin"):
            if key in extra and extra[key]:
                return str(extra[key]).lower().strip()

    return None


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

    raw = _json_load_maybe(raw)

    if raw is None:
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
# MODE FILTERING
# ══════════════════════════════════════════════════════════════════════════════
def apply_audit_mode_filter(
    df: pd.DataFrame,
    config: RegressionAuditConfig,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Filtra el DataFrame según el modo de auditoría.

    signal:
        Todas las señales activas BUY/SELL, sin filtrar source.

    optimizer:
        Ideas teóricas del optimizer.

    execution:
        Solo fills reales confirmados por broker.
        NO incluye BLOCKED aunque source sea execution_plan.

    blocked:
        Solo eventos bloqueados por guards / funding / planner.

    all:
        Todo BUY/SELL activo, exploratorio.
    """
    warnings: list[str] = []

    if df.empty:
        return df, warnings

    mode = (config.mode or "optimizer").lower().strip()

    if mode not in VALID_MODES:
        warnings.append(f"Modo inválido {mode}; usando optimizer.")
        mode = "optimizer"

    out = df.copy()

    if "decision" in out.columns:
        out = out[out["decision"].isin(ACTIVE_ACTIONS)].copy()

    if mode == "signal":
        return out, warnings

    if mode == "optimizer":
        legacy_theoretical = out["source"].isin(["sin_source", "unknown"]) & (
            out["decision_type"].eq("theoretical")
            | out["status"].eq("THEORETICAL")
        )
        mask = (
            out["source"].eq("optimizer")
            | legacy_theoretical
        )
        return out[mask].copy(), warnings

    if mode == "execution":
        mask = out["status"].isin(["EXECUTED", "EXECUTED_MANUAL"])

        # Protección extra: excluir explícitamente bloqueados.
        mask = mask & ~out["status"].eq("BLOCKED")
        mask = mask & ~out["decision_type"].eq("blocked")

        approved_executable = (
            out["source"].eq("execution_plan")
            & out["status"].eq("APPROVED")
            & out["decision_type"].eq("executable")
        )
        if "is_executable" in out.columns:
            approved_executable = approved_executable & out["is_executable"].fillna(False).astype(bool)

        approved_count = int(approved_executable.sum())
        executed_count = int(mask.sum())
        if executed_count == 0 and approved_count:
            warnings.append(
                f"Hay {approved_count} execution_plan APPROVED/executable, pero 0 EXECUTED/EXECUTED_MANUAL. "
                "Execution audit mide fills reconciliados; correr/importar broker_fills para validarlo."
            )

        return out[mask].copy(), warnings

    if mode == "blocked":
        mask = (
            out["status"].eq("BLOCKED")
            | out["decision_type"].eq("blocked")
        )

        if "was_blocked" in out.columns:
            mask = mask | out["was_blocked"].fillna(False).astype(bool)

        return out[mask].copy(), warnings

    if mode == "all":
        return out, warnings

    return out, warnings

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
        outcome_Xd canonico ya viene ajustado por direccion.
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
            if pd.isna(val):
                return np.nan
            return float(val)

        out[target_col] = out.apply(directional, axis=1)

        if out[target_col].notna().sum() == 0:
            warnings.append(
                f"Sin datos direccionales para {horizon}. "
                "Necesitás BUY/SELL con outcome poblado."
            )

    else:
        out[target_col] = out[raw_col]

    actions_used = (
        sorted(out["decision"].dropna().unique().tolist())
        if "decision" in out.columns
        else []
    )

    return out, target_col, actions_used, warnings


# ══════════════════════════════════════════════════════════════════════════════
# BUCKETS / MATURITY / THRESHOLD
# ══════════════════════════════════════════════════════════════════════════════

def build_score_bucket_table(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    cols = ["final_score", target_col]
    data = df[cols].replace([np.inf, -np.inf], np.nan).dropna().copy()

    if len(data) < 5:
        return pd.DataFrame()

    bins = [-np.inf, -0.15, -0.08, 0.03, 0.08, 0.15, np.inf]
    labels = [
        "NEG_FUERTE",
        "NEG_OPERABLE",
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

    out["reliability"] = out["n"].apply(bucket_reliability)

    return out


def bucket_reliability(n: int) -> str:
    n = int(n or 0)
    if n < 5:
        return "muy baja"
    if n < 15:
        return "baja"
    if n < 30:
        return "media"
    return "alta"


def sample_maturity(n: int) -> tuple[str, str]:
    n = int(n or 0)
    if n < 30:
        return "EXPLORATORIO", "Usar como monitoreo, no como calibrador."
    if n < 60:
        return "PRELIMINAR", "Hay lectura inicial, pero no ajustar thresholds."
    if n < 100:
        return "ÚTIL", "Puede empezar a usarse para calibración conservadora."
    return "ROBUSTO", "Muestra razonable para comparar modelos simples."


def safe_threshold(
    intercept: Optional[float],
    coef: Optional[float],
    cost: float,
    n: int,
    p_value: Optional[float] = None,
) -> tuple[Optional[float], str]:
    if intercept is None:
        return None, "intercept no disponible"

    if coef is None:
        return None, "coeficiente no disponible"

    if abs(coef) < 1e-6:
        return None, "coeficiente demasiado cercano a cero"

    if n < 60:
        return None, f"muestra insuficiente para calibrar threshold (n={n}, mínimo sugerido=60)"

    if p_value is not None and p_value > 0.10:
        return None, f"coeficiente no significativo (p={p_value:.3f})"

    th = (cost - intercept) / coef

    if not math.isfinite(th):
        return None, "threshold no finito"

    if not (0.00 <= th <= 0.30):
        return None, f"threshold fuera de rango razonable ({th:+.3f})"

    return float(th), "ok"


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
    mode = (config.mode or "optimizer").lower().strip()

    if df.empty:
        return RegressionAuditReport(
            generated_at=generated_at,
            rows_loaded=0,
            rows_usable=0,
            cost_threshold=cost_threshold,
            target_mode=config.target_mode,
            mode=mode,
            actions_used=[],
            source_counts={},
            status_counts={},
            models=[],
            bucket_tables={},
            warnings=["No se cargaron filas desde decision_log."],
        )

    df, mode_warnings = apply_audit_mode_filter(df, config)

    rows_loaded = len(df)

    source_counts = (
        df["source"].value_counts(dropna=False).to_dict()
        if "source" in df.columns and not df.empty
        else {}
    )

    status_counts = (
        df["status"].value_counts(dropna=False).to_dict()
        if "status" in df.columns and not df.empty
        else {}
    )

    models: list[RegressionModelResult] = []
    buckets: dict[str, pd.DataFrame] = {}
    warnings: list[str] = list(mode_warnings)

    rows_usable_total = 0
    all_actions_used: set[str] = set()

    if df.empty:
        warnings.append(
            f"Sin filas para mode={mode}. "
            "Puede ser normal si todavía no se registraron eventos de ese tipo."
        )

    # Detectar si el score/capas no tienen variacion util para regresion.
    for col in ["final_score", "technical_score", "macro_score", "sentiment_score", "risk_score"]:
        if col in df.columns and df[col].abs().sum() == 0:
            warnings.append(
                f"{col} está todo en 0. Revisar guardado/extracción si querés regresión por score/capas."
            )

    for horizon in config.horizons:
        hdf, target_col, actions_used, prep_warnings = prepare_model_frame(
            df=df,
            horizon=horizon,
            config=config,
        )

        warnings.extend(prep_warnings)

        if hdf.empty or target_col not in hdf.columns:
            continue

        hdf = hdf.dropna(subset=["final_score", target_col]).copy()
        hdf = hdf[np.isfinite(hdf["final_score"])]
        hdf = hdf[np.isfinite(hdf[target_col])]

        if hdf.empty:
            continue

        # Acciones realmente usadas por el modelo, luego de exigir outcome disponible.
        if "decision" in hdf.columns:
            all_actions_used.update(
        sorted(hdf["decision"].dropna().unique().tolist())
        )

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
            "current_weight",
            "target_weight",
            "delta_weight",
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
        mode=mode,
        actions_used=sorted(all_actions_used),
        source_counts={str(k): int(v) for k, v in source_counts.items()},
        status_counts={str(k): int(v) for k, v in status_counts.items()},
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
            threshold_reason=f"Muestra insuficiente: n={n}, mínimo={min_n}",
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
            threshold_reason="Sin features útiles",
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

    # Threshold y retorno esperado con score +0.08 solo tienen sentido
    # en el modelo simple final_score -> target.
    is_score_only_model = features == ["final_score"]

    suggested = None
    threshold_reason = None

    if is_score_only_model:
        suggested, threshold_reason = safe_threshold(
            intercept=intercept,
            coef=score_coef,
            cost=cost_threshold,
            n=n,
            p_value=score_pvalue,
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

    if is_score_only_model and threshold_reason and threshold_reason != "ok":
        notes.append(f"Threshold no usable: {threshold_reason}.")

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
        threshold_reason=threshold_reason,
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

    suggested = None
    threshold_reason = None

    if is_score_only_model:
        suggested, threshold_reason = safe_threshold(
            intercept=intercept,
            coef=score_coef,
            cost=cost_threshold,
            n=len(X),
            p_value=None,
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
        threshold_reason=threshold_reason,
        expected_return_at_buy_min_008=expected_008,
        notes=notes,
    )


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


# ══════════════════════════════════════════════════════════════════════════════
# RENDER
# ══════════════════════════════════════════════════════════════════════════════

def _mode_title(mode: str) -> str:
    return MODE_TITLES.get((mode or "optimizer").lower(), str(mode).upper())


def _mode_reading(mode: str) -> str:
    return MODE_READINGS.get((mode or "optimizer").lower(), "")


_MATURITY_SHORT = {
    "EXPLORATORIO": "EXPL",
    "PRELIMINAR": "PREL",
    "ÚTIL": "UTIL",
    "ROBUSTO": "ROB",
}


def _fmt_float(value: Optional[float], digits: int, *, signed: bool = False) -> str:
    if value is None:
        return "-"
    prefix = "+" if signed else ""
    return format(value, f"{prefix}.{digits}f")


def _fmt_pct(value: Optional[float], digits: int = 2, *, signed: bool = False) -> str:
    if value is None:
        return "-"
    prefix = "+" if signed else ""
    return format(value, f"{prefix}.{digits}%")


def _kv_counts(counts: dict[str, int]) -> str:
    return " | ".join(f"{k}:{v}" for k, v in sorted(counts.items())) or "-"


def _render_overview_table(report: RegressionAuditReport) -> list[str]:
    target = "DIRECCIONAL" if report.target_mode == "directional" else "BRUTO"
    actions = ", ".join(report.actions_used) if report.actions_used else "-"

    rows = [
        f"{'Target':<12}{target:<18}{'Costo':<12}{report.cost_threshold:.2%}",
        f"{'Filas':<12}{report.rows_loaded:<18}{'Obs usadas':<12}{report.rows_usable}",
        f"{'Acciones':<12}{actions}",
        f"{'Fuentes':<12}{_kv_counts(report.source_counts)}",
        f"{'Status':<12}{_kv_counts(report.status_counts)}",
    ]
    return ["<pre>", *rows, "</pre>"]


def _render_model_scorecard(report: RegressionAuditReport) -> list[str]:
    if not report.models:
        return ["⚠️ No hay suficientes datos cerrados para correr regresión."]

    rows = [
        f"{'Hz':<4}{'Modelo':<16}{'n':>3} {'R2':>6} {'RMSE':>7} {'IC':>7} "
        f"{'Coef':>8} {'p':>6} {'Ret@.08':>8} {'Mad':>5}"
    ]

    for model in report.models:
        maturity, _ = sample_maturity(model.n)
        rows.append(
            f"{model.horizon:<4}"
            f"{model.model_name:<16}"
            f"{model.n:>3} "
            f"{_fmt_float(model.r2, 3):>6} "
            f"{_fmt_pct(model.rmse):>7} "
            f"{_fmt_float(model.ic, 3, signed=True):>7} "
            f"{_fmt_float(model.score_coef, 4, signed=True):>8} "
            f"{_fmt_float(model.score_pvalue, 3):>6} "
            f"{_fmt_pct(model.expected_return_at_buy_min_008, signed=True):>8} "
            f"{_MATURITY_SHORT.get(maturity, maturity[:4]):>5}"
        )

    return ["<pre>", *rows, "</pre>"]


def _render_bucket_scorecard(
    report: RegressionAuditReport,
    *,
    compact: bool,
) -> list[str]:
    wanted_compact = {
        "NEG_FUERTE",
        "NEG_OPERABLE",
        "POS_DEBIL",
        "POS_OPERABLE",
        "POS_FUERTE",
    }
    rows = [
        f"{'Hz':<4}{'Bucket':<15}{'n':>3} {'AvgScore':>9} "
        f"{'Target':>8} {'Hit':>6} {'Conf':<10}"
    ]

    for horizon, bucket in report.bucket_tables.items():
        if compact and horizon != "5d":
            continue
        if bucket is None or bucket.empty:
            continue

        for _, row in bucket.iterrows():
            bucket_name = str(row["bucket"])
            if compact and bucket_name not in wanted_compact:
                continue
            if int(row["n"]) == 0:
                continue
            rows.append(
                f"{horizon:<4}"
                f"{bucket_name:<15}"
                f"{int(row['n']):>3} "
                f"{_fmt_float(float(row['avg_score']), 3, signed=True):>9} "
                f"{_fmt_pct(float(row['avg_return']), signed=True):>8} "
                f"{_fmt_pct(float(row['hit_rate']), digits=0):>6} "
                f"{str(row.get('reliability', '-')):<10}"
            )

    if len(rows) == 1:
        return []

    return ["<pre>", *rows, "</pre>"]


def _dedup_model_notes(report: RegressionAuditReport) -> list[str]:
    notes: list[str] = []
    for model in report.models:
        for note in model.notes:
            if note not in notes:
                notes.append(note)
    return notes


def _render_regression_audit_aligned(
    report: RegressionAuditReport,
    *,
    compact: bool,
) -> str:
    lines: list[str] = []

    title_target = (
        "RETORNO DIRECCIONAL"
        if report.target_mode == "directional"
        else "RETORNO BRUTO"
    )

    mode = (report.mode or "optimizer").lower()
    mode_title = _mode_title(mode)

    lines.append(f"📈 <b>REGRESSION AUDIT — {mode_title}</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if not compact:
        lines.append(f"🕐 {report.generated_at.astimezone().strftime('%d/%m/%Y %H:%M')}")
    lines.append(f"🎯 <b>{title_target}</b>")
    lines.extend(_render_overview_table(report))

    reading = _mode_reading(mode)
    if reading:
        lines.append(f"ℹ️ {reading}")

    if report.warnings:
        lines.append("")
        lines.append("⚠️ <b>Warnings</b>")
        max_warnings = 3 if compact else 8
        for w in report.warnings[:max_warnings]:
            lines.append(f"• {w}")

    lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("<b>RESUMEN DE MODELOS</b>")
    lines.extend(_render_model_scorecard(report))

    bucket_lines = _render_bucket_scorecard(report, compact=compact)
    if bucket_lines:
        lines.append("")
        lines.append("<b>BUCKETS POR SCORE</b>")
        lines.extend(bucket_lines)

    if not compact:
        notes = _dedup_model_notes(report)
        if notes:
            lines.append("")
            lines.append("<b>Notas diagnósticas</b>")
            for note in notes[:10]:
                lines.append(f"• {note}")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("<b>LECTURA</b>")
    lines.extend(_build_human_reading(report))

    lines.append("")
    lines.append("<i>Auditoría estadística auxiliar — no genera órdenes ni reemplaza al Execution Planner.</i>")

    return "\n".join(lines)


def render_regression_audit_compact(report: RegressionAuditReport) -> str:
    """Render compacto, alineado y apto para Telegram."""
    return _render_regression_audit_aligned(report, compact=True)


def render_regression_audit(report: RegressionAuditReport) -> str:
    """Render completo en una sola salida alineada."""
    return _render_regression_audit_aligned(report, compact=False)


def _build_human_reading(report: RegressionAuditReport) -> list[str]:
    lines: list[str] = []

    mode = (report.mode or "optimizer").lower()

    baseline_5d = next(
        (
            m for m in report.models
            if m.horizon == "5d" and m.model_name == "baseline_score"
        ),
        None,
    )

    if baseline_5d is None or baseline_5d.r2 is None:
        prefix = {
            "execution": "Todavia no hay suficientes fills EXECUTED/reconciliados con outcome.",
            "blocked": "Todavía no hay suficientes operaciones bloqueadas con outcome.",
            "optimizer": "Todavía no hay suficiente muestra del optimizer.",
            "signal": "Todavía no hay suficiente muestra de señales.",
            "all": "Todavía no hay suficiente muestra global.",
        }.get(mode, "Todavía no hay suficiente muestra.")

        return [
            f"⚠️ {prefix}",
            "Seguí acumulando eventos cerrados y outcomes.",
        ]

    coef = baseline_5d.score_coef
    pval = baseline_5d.score_pvalue
    r2 = baseline_5d.r2
    ic = baseline_5d.ic
    n = baseline_5d.n

    maturity, maturity_msg = sample_maturity(n)
    lines.append(f"Madurez estadística: {maturity} — {maturity_msg}")

    if mode == "optimizer":
        lines.append(
            "Este resultado mide ideas teóricas del optimizer, no órdenes finales ejecutadas."
        )
    elif mode == "execution":
        lines.append(
            "Este resultado mide fills reales confirmados/reconciliados. Si fueron manuales, valida execution real pero no calibra el planner."
        )
    elif mode == "blocked":
        lines.append(
            "Este resultado mide operaciones bloqueadas. Sirve para saber si los guards protegen o bloquean demasiado."
        )
    elif mode == "signal":
        lines.append(
            "Este resultado mide si el score tiene valor predictivo general."
        )

    if coef is None:
        return lines + ["⚠️ No se pudo estimar coeficiente de final_score."]

    target_name = "direccional" if report.target_mode == "directional" else "bruto"

    if coef <= 0:
        lines.append(
            f"🔴 El coeficiente de final_score es negativo o nulo sobre retorno {target_name}: "
            "en esta muestra, mayor score no se tradujo en mejor resultado."
        )
        lines.append(
            "No conviene bajar thresholds. Mantener guards conservadores."
        )
        return lines

    if pval is not None and pval > 0.10:
        lines.append(
            f"🟡 El coeficiente de final_score es positivo sobre retorno {target_name}, "
            "pero no significativo. Hay señal posible, todavía débil."
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
        lines.append(
            f"Umbral sugerido para cubrir costos: score >= {th:+.3f}. "
            "Comparar contra el BUY_MIN actual antes de tocar el planner."
        )
    elif baseline_5d.threshold_reason:
        lines.append(
            f"Umbral estimado no usable: {baseline_5d.threshold_reason}. "
            "No ajustar automáticamente."
        )

    return lines


# ══════════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

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
