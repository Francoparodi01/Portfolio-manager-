"""
src/analysis/synthesis.py

Motor de sintesis probabilistica.

Combina todas las capas con pesos configurables y llama a Claude API
para razonamiento final cuando hay señales contradictorias.

Pesos por defecto (ajustables en LAYER_WEIGHTS):
  Tecnico:   30%  — momentum, tendencias, patrones de precio
  Macro:     30%  — contexto global (WTI, VIX, tasas, dolar)
  Riesgo:    25%  — penalizacion por vol, drawdown, sizing
  Sentiment: 15%  — noticias, flujo de informacion

Output final:
  decision:       BUY | ACCUMULATE | HOLD | REDUCE | SELL
  confidence:     0.0 – 1.0
  position_size:  % sugerido del portfolio
  reasoning:      texto explicativo (generado por Claude si disponible)
  score_breakdown: desglose por capa
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ── Pesos de cada capa ───────────────────────────────────────────────────────
LAYER_WEIGHTS = {
    "technical":  0.30,
    "macro":      0.30,
    "risk":       0.25,
    "sentiment":  0.15,
}

# ── Thresholds para la decision final ───────────────────────────────────────
THRESHOLDS = {
    "BUY":        0.35,
    "ACCUMULATE": 0.15,
    "HOLD":      -0.15,
    "REDUCE":    -0.30,
    "SELL":      -0.45,
}

ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL  = "claude-sonnet-4-20250514"


@dataclass
class LayerScore:
    name: str
    raw_score: float     # -1.0 a +1.0
    weight: float
    weighted: float
    reasons: list[str] = field(default_factory=list)


@dataclass
class SynthesisResult:
    ticker: str
    decision: str                  # BUY | ACCUMULATE | HOLD | REDUCE | SELL
    confidence: float              # 0.0 – 1.0
    final_score: float             # -1.0 a +1.0 (score ponderado)
    position_size: float           # % sugerido del portfolio
    layers: list[LayerScore]       = field(default_factory=list)
    reasoning: str                 = ""   # razonamiento del LLM
    llm_used: bool                 = False
    generated_at: datetime         = field(default_factory=lambda: datetime.now(timezone.utc))
    warnings: list[str]            = field(default_factory=list)

    def to_telegram(self, include_reasoning: bool = True) -> str:
        icons = {
            "BUY":        "🟢🟢",
            "ACCUMULATE": "🟢",
            "HOLD":       "🟡",
            "REDUCE":     "🔴",
            "SELL":       "🔴🔴",
        }
        icon = icons.get(self.decision, "⚪")
        bar  = "█" * int(self.confidence * 5) + "░" * (5 - int(self.confidence * 5))

        lines = [
            f"{icon} <b>{self.ticker}</b> → <b>{self.decision}</b>",
            f"Score: {self.final_score:+.2f}  |  Confianza: [{bar}] {self.confidence:.0%}",
            f"Sizing sugerido: <b>{self.position_size:.1%}</b> del portfolio",
        ]

        # Breakdown por capa
        for layer in self.layers:
            if abs(layer.raw_score) > 0.05:
                direction = "+" if layer.weighted > 0 else ""
                lines.append(
                    f"  {layer.name.capitalize():10s} {layer.raw_score:+.2f} "
                    f"× {layer.weight:.0%} = {direction}{layer.weighted:.3f}"
                )

        # Razonamiento del LLM
        if include_reasoning and self.reasoning:
            lines.append("")
            lines.append(f"🧠 <i>{self.reasoning[:600]}</i>")

        if self.warnings:
            for w in self.warnings[:2]:
                lines.append(f"⚠️ {w}")

        return "\n".join(lines)


def _map_technical_signal(signal: str, strength: float) -> float:
    """Convierte señal tecnica (BUY/SELL/HOLD) a score -1/+1."""
    base = {"BUY": 1.0, "SELL": -1.0, "HOLD": 0.0}.get(signal, 0.0)
    return base * strength


def blend_scores(
    ticker: str,
    technical_signal: str,
    technical_strength: float,
    macro_score: float,
    risk_position: dict,
    sentiment_score: float,
) -> SynthesisResult:
    """
    Combina todos los scores con pesos y genera la decision final.
    No requiere LLM — es el motor base deterministico.
    """
    layers = []

    # ── Capa tecnica ────────────────────────────────────────────
    tech_raw = _map_technical_signal(technical_signal, technical_strength)
    tech_w   = LAYER_WEIGHTS["technical"]
    layers.append(LayerScore(
        name="technical",
        raw_score=tech_raw,
        weight=tech_w,
        weighted=tech_raw * tech_w,
    ))

    # ── Capa macro ──────────────────────────────────────────────
    macro_w = LAYER_WEIGHTS["macro"]
    layers.append(LayerScore(
        name="macro",
        raw_score=macro_score,
        weight=macro_w,
        weighted=macro_score * macro_w,
    ))

    # ── Capa riesgo (penalizacion) ──────────────────────────────
    # La capa de riesgo actua como multiplicador/penalizacion
    # Un activo de alta volatilidad o en drawdown reduce el score final
    current_pct  = risk_position.get("current_pct", 0.10)
    suggested    = risk_position.get("suggested_pct_adj", 0.10)
    risk_level   = risk_position.get("risk_level", "NORMAL")

    # Si el sizing sugerido es menor al actual = riesgo penaliza
    size_ratio = suggested / current_pct if current_pct > 0 else 1.0
    risk_penalty = {
        "LOW":      0.05,
        "NORMAL":   0.00,
        "ELEVATED": -0.10,
        "HIGH":     -0.20,
        "EXTREME":  -0.50,
    }.get(risk_level, 0.0)

    size_ratio = suggested / current_pct if current_pct > 0 else 1.0

    # penalización por exceso de tamaño
    overweight_penalty = 0.0
    if size_ratio < 0.5:
        overweight_penalty = -0.25
    elif size_ratio < 0.8:
        overweight_penalty = -0.10

    risk_penalty_base = {
        "LOW":      0.05,
        "NORMAL":   0.00,
        "ELEVATED": -0.10,
        "HIGH":     -0.20,
        "EXTREME":  -0.50,
    }.get(risk_level, 0.0)

    risk_raw = risk_penalty_base + overweight_penalty

    # ── Capa sentiment ──────────────────────────────────────────
    sent_w = LAYER_WEIGHTS["sentiment"]
    layers.append(LayerScore(
        name="sentiment",
        raw_score=sentiment_score,
        weight=sent_w,
        weighted=sentiment_score * sent_w,
    ))

    # ── Score final ponderado ───────────────────────────────────
    final_score = sum(l.weighted for l in layers)
    final_score = round(float(max(-1.0, min(1.0, final_score))), 4)

    # ── Decision ────────────────────────────────────────────────
    if final_score >= THRESHOLDS["BUY"]:
        decision = "BUY"
    elif final_score >= THRESHOLDS["ACCUMULATE"]:
        decision = "ACCUMULATE"
    elif final_score <= -THRESHOLDS["BUY"]:
        decision = "SELL"
    elif final_score <= THRESHOLDS["REDUCE"]:
        decision = "REDUCE"
    else:
        decision = "HOLD"

    # ── Confianza: consenso entre capas ────────────────────────
    signs = [1 if l.weighted > 0.01 else (-1 if l.weighted < -0.01 else 0) for l in layers]
    sign_consensus = abs(sum(signs)) / len(signs) if signs else 0.0
    confidence = round(abs(final_score) * 0.6 + sign_consensus * 0.4, 4)
    confidence = min(confidence, 0.95)  # nunca 100% seguro

    # ── Position size ───────────────────────────────────────────
    # Ajustar el sizing sugerido por el riesgo por la confianza de la señal
    base_size = suggested if suggested > 0 else 0.05

    # factor de convicción (suavizado)
    conviction = max(0.15, confidence)  # piso para evitar cero total

    if decision in ("BUY", "ACCUMULATE"):
        pos_size = base_size * conviction

    elif decision in ("SELL", "REDUCE"):
        pos_size = base_size * (1 - confidence)

    else:  # HOLD
        # HOLD debe ser defensivo
        pos_size = base_size * (0.25 + confidence * 0.25)

    # hard cap final
    pos_size = round(float(max(0.0, min(0.25, pos_size))), 4)

    return SynthesisResult(
        ticker=ticker,
        decision=decision,
        confidence=confidence,
        final_score=final_score,
        position_size=pos_size,
        layers=layers,
    )


def synthesize_with_llm(
    result: SynthesisResult,
    macro_snap: "MacroSnapshot",
    macro_reasons: list[str],
    technical_reasons: list[str],
    sentiment_headlines: list[dict],
    risk_position: dict,
    portfolio_context: dict,
) -> SynthesisResult:
    """
    Enriquece el resultado con razonamiento de Claude API.
    Si falla (sin API key, rate limit, etc.) retorna el resultado base intacto.

    Args:
        result:              SynthesisResult del blend deterministico
        macro_snap:          snapshot macro completo
        macro_reasons:       razones de la capa macro
        technical_reasons:   razones de la capa tecnica
        sentiment_headlines: titulares relevantes
        risk_position:       metricas de riesgo del activo
        portfolio_context:   contexto general del portfolio
    """
    try:
        # Construir prompt estructurado para Claude
        macro_dict = macro_snap.to_dict() if hasattr(macro_snap, "to_dict") else {}
        headlines_str = "\n".join(
            f"  - [{h.get('source','')}] {h.get('title','')} (score={h.get('score',0):.1f})"
            for h in sentiment_headlines[:5]
        ) or "  Sin noticias relevantes"

        prompt = f"""Eres un analista cuantitativo senior. Analizas el activo {result.ticker} 
y debes razonar sobre la decision de trading sugerida por el sistema.

DECISION DEL SISTEMA: {result.decision} (score={result.final_score:+.2f}, confianza={result.confidence:.0%})

BREAKDOWN POR CAPA:
- Tecnico ({LAYER_WEIGHTS['technical']:.0%}): {result.layers[0].raw_score:+.2f}
  {chr(10).join(technical_reasons[:3]) or 'Sin datos'}
- Macro ({LAYER_WEIGHTS['macro']:.0%}): {result.layers[1].raw_score:+.2f}
  {chr(10).join(macro_reasons[:3]) or 'Sin datos'}
- Riesgo ({LAYER_WEIGHTS['risk']:.0%}): volatilidad anual {risk_position.get('volatility_annual', 0):.0%}, sharpe {risk_position.get('sharpe', 0):.2f}
- Sentiment ({LAYER_WEIGHTS['sentiment']:.0%}): {result.layers[3].raw_score:+.2f}

CONTEXTO MACRO ACTUAL:
- WTI: ${macro_dict.get('wti', 'N/A')} ({macro_dict.get('wti_chg', 0):+.1f}% hoy, tendencia: {macro_dict.get('wti_trend', 0):+.2f})
- VIX: {macro_dict.get('vix', 'N/A')} ({macro_dict.get('vix_chg', 0):+.1f}%)
- DXY: {macro_dict.get('dxy', 'N/A')} ({macro_dict.get('dxy_chg', 0):+.1f}%)
- SP500: {macro_dict.get('sp500', 'N/A'):} ({macro_dict.get('sp500_chg', 0):+.1f}%)
- Tasa 10Y: {macro_dict.get('tnx', 'N/A')}%

NOTICIAS RECIENTES:
{headlines_str}

PORTFOLIO:
- Posicion actual: {risk_position.get('current_pct', 0):.1%} del portfolio
- Sizing sugerido: {risk_position.get('suggested_pct_adj', 0):.1%}
- Portfolio total: ${portfolio_context.get('total_ars', 0):,.0f} ARS

TAREA:
1. Valida o cuestiona la decision del sistema en 2-3 oraciones.
2. Identifica el factor mas relevante que el modelo cuantitativo puede estar subestimando.
3. Da una recomendacion de accion concreta (ej: "mantener 54 unidades CVX hasta confirmar reversión MACD").
4. Se especifico y breve (max 4 oraciones en total).
Responde en español."""

        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": 400,
            "messages": [{"role": "user", "content": prompt}],
        }

        resp = requests.post(
            ANTHROPIC_API,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if resp.status_code == 200:
            data = resp.json()
            reasoning = data["content"][0]["text"].strip() if data.get("content") else ""
            if reasoning:
                result.reasoning = reasoning
                result.llm_used  = True
                logger.info(f"LLM synthesis para {result.ticker}: {len(reasoning)} chars")
        else:
            logger.warning(f"Claude API {resp.status_code} para {result.ticker}: {resp.text[:200]}")

    except Exception as e:
        logger.warning(f"LLM synthesis fallo para {result.ticker} (no critico): {e}")

    return result


def build_full_report(results: list[SynthesisResult], macro_snap, portfolio_total: float) -> str:
    """Construye el reporte completo para Telegram."""
    if not results:
        return "Sin resultados de sintesis disponibles"

    macro_summary = macro_snap.summary() if hasattr(macro_snap, "summary") else ""

    lines = [
        "🧠 <b>ANALISIS CUANTITATIVO COMPLETO</b>",
        f"Portfolio: <b>${portfolio_total:,.0f} ARS</b>",
        f"📊 Macro: {macro_summary}",
        f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')} ART",
        "═" * 35,
    ]

    for r in results:
        lines.append("")
        lines.append(r.to_telegram(include_reasoning=True))
        lines.append("─" * 35)

    # Resumen ejecutivo
    buys    = [r for r in results if r.decision in ("BUY", "ACCUMULATE")]
    sells   = [r for r in results if r.decision in ("SELL", "REDUCE")]
    holds   = [r for r in results if r.decision == "HOLD"]

    lines.append("")
    lines.append(
        f"📋 Resumen: {len(buys)} comprar | {len(holds)} mantener | {len(sells)} vender"
    )

    if buys:
        lines.append(f"🟢 Acumular: <b>{', '.join(r.ticker for r in buys)}</b>")
    if sells:
        lines.append(f"🔴 Reducir: <b>{', '.join(r.ticker for r in sells)}</b>")

    lines.append("")
    lines.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")

    return "\n".join(lines)