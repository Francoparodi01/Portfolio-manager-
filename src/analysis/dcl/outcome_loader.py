from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import asyncpg
import pandas as pd

# ---------------------------------------------------------------------------
# Intento importar normalize_decision_frame; si no existe, usamos identidad.
# ---------------------------------------------------------------------------
try:
    from src.analysis.regression_audit import normalize_decision_frame
except Exception:  # pragma: no cover
    def normalize_decision_frame(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[misc]
        return df


# ---------------------------------------------------------------------------
# Modos de calidad para filtrar decisiones auditables
# ---------------------------------------------------------------------------
QUALITY_MODES: dict[str, set[str]] = {
    "strict":  {"clean"},
    "relaxed": {"clean", "mixed"},
    "all":     {"clean", "mixed", "unknown", "reconstructed"},
}


# ---------------------------------------------------------------------------
# Dataclass central
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class EnrichedDecision:
    decision_id: str
    ticker: str
    decision_type: str
    final_score: float
    layer_scores: dict[str, Optional[float]] = field(default_factory=dict)
    was_blocked: bool = False
    block_reason: Optional[str] = None
    data_quality: str = "unknown"
    market_regime: str = "unknown"
    outcome_5d: Optional[float] = None
    outcome_10d: Optional[float] = None
    outcome_20d: Optional[float] = None
    is_auditable: bool = False


# ---------------------------------------------------------------------------
# Helpers de extracción
# ---------------------------------------------------------------------------

def _clean_text(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default
    try:
        if isinstance(value, float) and pd.isna(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    return text if text else default


def _optional_float(value: Any) -> Optional[float]:
    """Retorna None si el valor no es numérico. Distingue 0.0 de 'no disponible'."""
    if value is None:
        return None
    try:
        if isinstance(value, float) and pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    try:
        if isinstance(value, float) and pd.isna(value):
            return False
    except Exception:
        pass
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "si", "s"}


def _layers_dict(row: pd.Series) -> dict[str, Any]:
    """Extrae el JSON de layers desde la fila. Retorna {} si no hay nada parseable."""
    value = row.get("layers")
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Extracción de calidad de dato
# Orden de prioridad:
#   1. Columnas directas de la fila (si existen en el schema)
#   2. Campos embebidos en layers por execution_plan
#   3. Inferencia desde technical_has_reconstructed_candles
# ---------------------------------------------------------------------------

def _quality_from_layers(layers: dict[str, Any]) -> str:
    """
    Lee calidad desde el JSON de layers que execution_plan embebe.

    execution_plan guarda:
        "technical_data_source_mode": "official"
        "technical_has_reconstructed_candles": false
        "technical_candle_sources": ["COCOS"]

    optimizer sin síntesis guarda solo:
        {"source": "optimizer", "delta_pct": 0.04}
    → retorna "unknown"
    """
    mode = layers.get("technical_data_source_mode")
    has_reconstructed = layers.get("technical_has_reconstructed_candles")
    candle_sources: list = layers.get("technical_candle_sources") or []

    # Optimizer sin paso por síntesis — layers mínimo
    if mode is None and not candle_sources:
        return "unknown"

    mode_text = _clean_text(mode, "").lower()

    if mode_text in {"reconstructed", "internal_snapshot"}:
        return "reconstructed"

    if mode_text == "mixed":
        return "mixed"

    # Fuente oficial declarada. Si explícitamente trae reconstruidas es mixed;
    # si no, la tratamos como clean para no degradar registros antiguos que
    # guardaban mode pero todavía no guardaban el boolean auxiliar.
    if mode_text in {"official", "clean", "cocos"}:
        return "mixed" if has_reconstructed is True else "clean"

    # Solo velas COCOS sin indicador de reconstruidas → clean
    if candle_sources == ["COCOS"]:
        return "clean"

    # Cualquier otra combinación con datos disponibles → mixed
    if mode_text or candle_sources:
        return "mixed"

    return "unknown"


def _quality_from_row(row: pd.Series) -> str:
    """
    Determina calidad de dato con cascada de fuentes.
    Prioriza columnas directas, luego layers embebidos.
    """
    layers = _layers_dict(row)

    # 1. Columnas directas del schema (si existen)
    direct_candidates = [
        row.get("data_quality"),
        row.get("candle_source_mode"),
        row.get("technical_source_mode"),
        row.get("source_quality"),
    ]
    for value in direct_candidates:
        text = _clean_text(value, "").lower()
        if text:
            if text in {"official", "clean", "cocos"}:
                return "clean"
            if text == "mixed":
                return "mixed"
            if text in {"reconstructed", "internal_snapshot"}:
                return "reconstructed"

    # 2. Campos embebidos por execution_plan en layers
    quality_from_layers = _quality_from_layers(layers)
    if quality_from_layers != "unknown":
        return quality_from_layers

    # 3. Campo genérico en layers (fuentes antiguas)
    for key in ("technical_data_source_mode", "technical_candle_source_mode"):
        text = _clean_text(layers.get(key), "").lower()
        if text:
            if text in {"official", "clean", "cocos"}:
                return "clean"
            if text == "mixed":
                return "mixed"
            if text in {"reconstructed", "internal_snapshot"}:
                return "reconstructed"

    return "unknown"


# ---------------------------------------------------------------------------
# Extracción de scores por capa
# IMPORTANTE: None ≠ 0.0
#   None  → capa no disponible para esta decisión (no auditar en atribución)
#   0.0   → capa evaluó neutral
# ---------------------------------------------------------------------------

def _layer_score(
    row: pd.Series,
    layer: str,
    *,
    fallback: pd.Series | None = None,
) -> Optional[float]:
    """
    Extrae score de una capa con cascada:
    1. Columna directa {layer}_score
    2. JSON layers → {layer} → raw / score / weighted
    3. Fila de fallback (normalizada)
    4. None si no hay dato (no 0.0)
    """
    # 1. Columna directa
    direct = _optional_float(row.get(f"{layer}_score"))
    if direct is not None:
        return direct

    # 2. Payload en layers
    layer_payload = _layers_dict(row).get(layer)
    if isinstance(layer_payload, dict):
        for key in ("raw", "score", "weighted"):
            value = _optional_float(layer_payload.get(key))
            if value is not None:
                return value

    # 3. Fallback explícito. Se usa solo si trae la columna directa original;
    # normalize_decision_frame agrega 0.0 sintético cuando no hay capa, y eso
    # contaminaría atribución al confundir "no disponible" con neutral real.
    if fallback is not None and f"{layer}_score" in fallback.index:
        direct = _optional_float(fallback.get(f"{layer}_score"))
        if direct is not None:
            return direct

    # 4. No disponible
    return None


def _decision_kind(row: pd.Series) -> str:
    decision = _clean_text(row.get("decision"), "UNKNOWN").upper()
    dtype = _clean_text(row.get("decision_type"), "").upper()
    if dtype and dtype not in {"UNKNOWN", "NAN", "NONE", ""}:
        return dtype
    return decision


def _is_auditable_for_mode(
    has_any_outcome: bool,
    quality: str,
    quality_mode: str,
) -> bool:
    """
    Determina si una decisión es auditable según el modo de calidad.

    strict  → solo clean con outcome (para propuestas de calibración)
    relaxed → clean + mixed con outcome (para auditoría descriptiva)
    all     → cualquier calidad con outcome (solo diagnóstico)
    """
    if not has_any_outcome:
        return False
    allowed = QUALITY_MODES.get(quality_mode, QUALITY_MODES["relaxed"])
    return quality in allowed


# ---------------------------------------------------------------------------
# Constructor principal de EnrichedDecision desde DataFrame
# ---------------------------------------------------------------------------

def decisions_from_frame(
    df: pd.DataFrame,
    *,
    quality_mode: str = "relaxed",
) -> list[EnrichedDecision]:
    """
    Convierte un DataFrame de decision_log en EnrichedDecisions.

    quality_mode:
        "strict"  → is_auditable solo para clean
        "relaxed" → is_auditable para clean + mixed  (default)
        "all"     → is_auditable para cualquier calidad con outcome
    """
    if df.empty:
        return []

    raw_rows = [pd.Series(record) for record in df.to_dict("records")]

    try:
        norm = normalize_decision_frame(df)
    except Exception:
        norm = df.copy()

    decisions: list[EnrichedDecision] = []

    for idx, (_, row) in enumerate(norm.iterrows()):
        raw_row = raw_rows[idx] if idx < len(raw_rows) else row

        # Calidad: primero raw_row (tiene layers completo), luego fila normalizada
        quality = _quality_from_row(raw_row)
        if quality == "unknown":
            quality = _quality_from_row(row)

        outcomes = {
            "outcome_5d":  _optional_float(row.get("outcome_5d")),
            "outcome_10d": _optional_float(row.get("outcome_10d")),
            "outcome_20d": _optional_float(row.get("outcome_20d")),
        }
        has_any_outcome = any(v is not None for v in outcomes.values())

        # layer_scores: None cuando el dato no está disponible
        layer_scores: dict[str, Optional[float]] = {
            "technical": _layer_score(raw_row, "technical"),
            "macro":     _layer_score(raw_row, "macro"),
            "sentiment": _layer_score(raw_row, "sentiment"),
            "risk":      _layer_score(raw_row, "risk"),
        }

        was_blocked = (
            _bool_value(row.get("was_blocked"))
            or _clean_text(row.get("status"), "").upper() == "BLOCKED"
        )

        decisions.append(
            EnrichedDecision(
                decision_id=_clean_text(row.get("id"), ""),
                ticker=_clean_text(row.get("ticker"), "").upper(),
                decision_type=_decision_kind(row),
                final_score=float(_optional_float(row.get("final_score")) or 0.0),
                layer_scores=layer_scores,
                was_blocked=was_blocked,
                block_reason=_clean_text(row.get("block_reason"), "") or None,
                data_quality=quality,
                market_regime=_clean_text(row.get("regime"), "unknown").lower(),
                outcome_5d=outcomes["outcome_5d"],
                outcome_10d=outcomes["outcome_10d"],
                outcome_20d=outcomes["outcome_20d"],
                is_auditable=_is_auditable_for_mode(
                    has_any_outcome, quality, quality_mode
                ),
            )
        )

    return decisions


# ---------------------------------------------------------------------------
# OutcomeLoader — interfaz async con la base de datos
# ---------------------------------------------------------------------------

class OutcomeLoader:
    """Carga decision_log y enriquece decisiones para calibración offline."""

    # Columnas que queremos leer (se filtra contra las que existen en el schema)
    _WANTED_COLS = [
        "id",
        "owner_chat_id",
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
        "outcome_5d",
        "outcome_10d",
        "outcome_20d",
        "outcome_basis",
        "was_correct",
        "guard_triggered",
        "block_reason",
        "source",
        "decision_type",
        "status",
        "run_intent",
        "decision_stage",
        "metric_scope",
        "is_primary_metric",
        "is_executable",
        "was_blocked",
        # Columnas de calidad directa (pueden no existir aún)
        "data_quality",
        "candle_source_mode",
        "technical_source_mode",
        "source_quality",
    ]

    def __init__(self, database_url: str):
        self.database_url = database_url.replace(
            "postgresql+asyncpg://", "postgresql://"
        )

    async def load(
        self,
        *,
        days: int = 180,
        since: str | None = None,
        owner_chat_id: int | None = None,
        quality_mode: str = "relaxed",
    ) -> list[EnrichedDecision]:
        df = await self.load_frame(
            days=days, since=since, owner_chat_id=owner_chat_id
        )
        return decisions_from_frame(df, quality_mode=quality_mode)

    async def load_frame(
        self,
        *,
        days: int = 180,
        since: str | None = None,
        owner_chat_id: int | None = None,
    ) -> pd.DataFrame:
        conn = await asyncpg.connect(self.database_url)
        try:
            cols = await self._existing_columns(conn)
            selected = [c for c in self._WANTED_COLS if c in cols]
            if not selected:
                return pd.DataFrame()

            cutoff = self._cutoff(days=days, since=since)
            owner_filter = (
                "AND owner_chat_id = $2"
                if owner_chat_id is not None and "owner_chat_id" in cols
                else ""
            )
            radar_filter = (
                "AND COALESCE(source, layers->>'source', '') <> 'radar'"
                if {"source", "layers"}.issubset(cols)
                else ""
            )
            scope_filter = (
                "AND COALESCE(metric_scope, 'planner_audit') <> 'debug'"
                if "metric_scope" in cols
                else ""
            )
            args: list[Any] = [cutoff]
            if owner_filter:
                args.append(owner_chat_id)

            rows = await conn.fetch(
                f"""
                SELECT {", ".join(selected)}
                FROM decision_log
                WHERE decided_at >= $1
                {owner_filter}
                {radar_filter}
                {scope_filter}
                ORDER BY decided_at ASC
                """,
                *args,
            )
        finally:
            await conn.close()

        if not rows:
            return pd.DataFrame(columns=selected)
        return pd.DataFrame([dict(row) for row in rows])

    @staticmethod
    async def _existing_columns(conn: asyncpg.Connection) -> set[str]:
        rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'decision_log'
            """
        )
        return {str(row["column_name"]) for row in rows}

    @staticmethod
    def _cutoff(*, days: int, since: str | None) -> datetime:
        if since:
            try:
                parsed = datetime.fromisoformat(since)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except Exception:
                pass
        return datetime.now(timezone.utc) - timedelta(days=days)
