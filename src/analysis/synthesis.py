"""
src/analysis/synthesis.py — Motor de síntesis probabilística.

Arquitectura de capas:
  técnico  30%  : señal de precio, tendencia y momentum
  macro    30%  : entorno global + régimen de mercado
  riesgo   25%  : solo penaliza condiciones extremas (vol>80%, drawdown)
  sentiment 15% : flujo de noticias RSS

Conviction (acuerdo entre capas):
  NO es abs(score). Es el porcentaje de capas activas que apuntan
  en la misma dirección que el score final.
  CVX con macro positivo + sentiment positivo + técnico negativo = 2/3 = 67%.
  Una señal con 3/3 capas alineadas tiene convicción 100%.
  Una señal con 1/3 capas tiene convicción 33% aunque el score sea alto.

Position sizing:
  Basado en el sizing del risk engine, ajustado por convicción.
  No tiene sentido poner tamaño máximo en una posición de convicción 33%.
"""
from __future__ import annotations

import html
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests
import numpy as np

logger = logging.getLogger(__name__)

LAYER_WEIGHTS = {"technical": 0.30, "macro": 0.30, "risk": 0.25, "sentiment": 0.15}
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL  = "claude-sonnet-4-20250514"


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class LayerScore:
    name: str
    raw_score: float
    weight: float
    weighted: float
    reasons: list = field(default_factory=list)


@dataclass
class SynthesisResult:
    ticker: str
    decision: str                # BUY | ACCUMULATE | HOLD | REDUCE | SELL
    confidence: float            # 0-1 — conviction entre capas
    final_score: float           # -1 a +1
    position_size: float         # tamaño sugerido (0-0.25)
    layers: list = field(default_factory=list)
    reasoning: str = ""
    llm_used: bool = False
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    warnings: list = field(default_factory=list)
    conviction: float = 0.0       # = confidence (alias explícito para render)
    sentiment_active: bool = True

    def to_telegram(self) -> str:
        icons = {"BUY": "🟢🟢", "ACCUMULATE": "🟢", "HOLD": "🟡", "REDUCE": "🔴", "SELL": "🔴🔴"}
        icon  = icons.get(self.decision, "⚪")
        bar   = "█" * int(self.conviction * 5) + "░" * (5 - int(self.conviction * 5))
        lines = [
            f"{icon} <b>{self.ticker}</b> → <b>{self.decision}</b>   [{bar}] {self.conviction:.0%}",
            f"   Score: <code>{self.final_score:+.3f}</code>   Sizing: <b>{self.position_size:.1%}</b>",
        ]
        for layer in self.layers:
            if abs(layer.raw_score) > 0.02:
                bar_len  = min(int(abs(layer.raw_score) * 5), 5)
                bar_char = "█" if layer.weighted > 0 else "▓"
                mini_bar = bar_char * bar_len + "░" * (5 - bar_len)
                sign     = "+" if layer.weighted >= 0 else ""
                lines.append(f"   <code>{layer.name:10s} {mini_bar} {sign}{layer.weighted:.3f}</code>")
        if self.reasoning:
            safe = html.escape(self.reasoning.strip())
            if len(safe) > 400:
                cut = safe[:400].rfind(".")
                safe = safe[:cut + 1] if cut > 100 else safe[:400]
            lines.append(f"   🧠 <i>{safe}</i>")
        for w in self.warnings[:2]:
            lines.append(f"   ⚠️ {w}")
        return "\n".join(lines)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _map_tech(signal: str, strength: float, score_raw: float = 0.0) -> float:
    """
    Convierte señal técnica a score [-1, +1].
    HOLD NO es 0 — preserva dirección via score_raw normalizado.
    Esto evita que activos con HOLD neutral contribuyan 0 al blend,
    ignorando que pueden tener sesgo positivo o negativo débil.
    """
    if signal == "BUY":
        return min(strength, 1.0)
    elif signal == "SELL":
        return -min(strength, 1.0)
    else:   # HOLD — usar score normalizado para preservar dirección
        return float(np.clip(score_raw / 12.0, -1.0, 1.0))


def _compute_conviction(layers: list[LayerScore], final: float) -> float:
    """
    Conviction = % de capas activas que acuerdan con la dirección del score final.

    "Activa" = weight > 0 y aporte absoluto significativo (>0.5% del rango máximo).
    "Acuerda" = el aporte ponderado tiene el mismo signo que el score final.

    Ejemplos con layers [tech=-0.10, macro=+0.16, risk=0, sent=+0.05]:
      final = +0.11 (positivo)
      active = [macro, sent] (tech es negativo pero activo también)
                ↑ tech también cuenta aunque sea contrario
      active_w = [-0.10, +0.16, +0.05] (risk=0 → inactivo)
      agreeing = 2 (macro y sent)
      conviction = 2/3 = 67%
    """
    direction  = 1 if final >= 0 else -1
    # Capas activas: weight>0 y aporte no trivial
    active_w   = [l.weighted for l in layers if l.weight > 0 and abs(l.weighted) > 0.005]
    if not active_w:
        return 0.0
    agreeing   = sum(1 for v in active_w if v * direction > 0)
    return round(agreeing / len(active_w), 4)


# ── Motor de blend ─────────────────────────────────────────────────────────────

def blend_scores(ticker: str, technical_signal: str, technical_strength: float,
                 macro_score: float, risk_position: dict, sentiment_score: float,
                 technical_score_raw: float = 0.0) -> SynthesisResult:
    """
    Combina las 4 capas en un score final y toma la decisión.

    Risk layer: NO penaliza volatilidad alta en tech stocks (NVDA vol=36% es normal).
    Solo penaliza condiciones extremas: vol>80%, drawdown activo, riesgo sistémico.
    """
    layers = []

    # ── Técnico ───────────────────────────────────────────────────────────────
    tech_raw = _map_tech(technical_signal, technical_strength, technical_score_raw)
    layers.append(LayerScore(
        "technical", tech_raw, LAYER_WEIGHTS["technical"],
        tech_raw * LAYER_WEIGHTS["technical"]
    ))

    # ── Macro ──────────────────────────────────────────────────────────────────
    layers.append(LayerScore(
        "macro", macro_score, LAYER_WEIGHTS["macro"],
        macro_score * LAYER_WEIGHTS["macro"]
    ))

    # ── Riesgo — penaliza solo condiciones extremas ────────────────────────────
    rl = risk_position.get("risk_level", "NORMAL")
    # ELEVATED y HIGH son normales para tech: NVDA, MU, AMD tienen vol>35%.
    # Solo EXTREME (vol>80%) merece penalización en el score.
    rp = {"LOW": 0.05, "NORMAL": 0.00, "ELEVATED": 0.00,
          "HIGH": -0.05, "EXTREME": -0.40}.get(rl, 0.0)
    warnings = risk_position.get("warnings", [])
    if any("drawdown" in w.lower() for w in warnings):
        rp -= 0.10   # drawdown activo del portfolio → señal de cautela
    layers.append(LayerScore(
        "risk", rp, LAYER_WEIGHTS["risk"],
        rp * LAYER_WEIGHTS["risk"], reasons=warnings
    ))

    # ── Sentiment ──────────────────────────────────────────────────────────────
    layers.append(LayerScore(
        "sentiment", sentiment_score, LAYER_WEIGHTS["sentiment"],
        sentiment_score * LAYER_WEIGHTS["sentiment"]
    ))

    # ── Score final ────────────────────────────────────────────────────────────
    final = float(np.clip(sum(l.weighted for l in layers), -1.0, 1.0))

    if   final >=  0.40: decision = "BUY"
    elif final >=  0.15: decision = "ACCUMULATE"
    elif final <= -0.40: decision = "SELL"
    elif final <= -0.15: decision = "REDUCE"
    else:                decision = "HOLD"

    # ── Conviction (nueva fórmula — % de acuerdo entre capas) ─────────────────
    conviction = _compute_conviction(layers, final)

    # ── Position sizing — ajustado por convicción ──────────────────────────────
    sug = risk_position.get("suggested_pct_adj", 0.05)
    if decision in ("BUY", "ACCUMULATE"):
        # A máxima convicción → sizing completo. A mínima → 40% del sizing.
        ps = sug * (0.40 + conviction * 0.60)
    elif decision in ("SELL", "REDUCE"):
        ps = sug * max(0.10, 0.50 - conviction * 0.40)
    else:
        ps = sug * (0.70 + conviction * 0.30)   # HOLD: mantener aprox peso actual

    return SynthesisResult(
        ticker=ticker, decision=decision,
        confidence=conviction, final_score=round(final, 4),
        position_size=round(float(np.clip(ps, 0, 0.25)), 4),
        layers=layers, conviction=conviction,
    )


# Alias para compatibilidad
blend_scores_local = blend_scores


# ── LLM: Ollama local (deepseek-r1:14b) ───────────────────────────────────────

def synthesize_with_llm_local(result: SynthesisResult, macro_snap,
                               macro_reasons: list[str], technical_reasons: list[str],
                               sentiment_headlines: list[dict], risk_position: dict,
                               portfolio_context: dict, ollama_url: str = "") -> SynthesisResult:
    """
    Enriquece el resultado con análisis LLM local via Ollama.
    No es crítico — si falla, el resultado cuantitativo queda intacto.
    El LLM es solo display: NO modifica score ni decisión.
    """
    import re, os
    try:
        macro_dict     = macro_snap.to_dict() if hasattr(macro_snap, "to_dict") else {}
        headlines_str  = "\n".join(
            f"  [{h.get('source','')}] {h.get('title','')} ({h.get('score',0):+.1f})"
            for h in sentiment_headlines[:5]
        ) or "  Sin noticias"

        lbn = {l.name: l for l in result.layers}
        t   = lbn.get("technical");  m = lbn.get("macro");  s = lbn.get("sentiment")

        system_msg = (
            "Eres un analista financiero senior de Argentina. "
            "SIEMPRE en español. EXACTAMENTE 3 oraciones cortas numeradas (1. 2. 3.). "
            "Máximo 25 palabras por oración. Sin preámbulos."
        )
        user_msg = (
            f"Activo: {result.ticker}. Decisión: {result.decision} (score={result.final_score:+.2f}, "
            f"convicción={result.conviction:.0%}).\n"
            f"Técnico: {t.raw_score:+.2f} | Macro: {m.raw_score:+.2f} | "
            f"Sentiment: {f'{s.raw_score:+.2f}' if s else 'N/A'}\n"
            f"VIX {macro_dict.get('vix','?')} | WTI {macro_dict.get('wti','?')} "
            f"({macro_dict.get('wti_chg',0):+.1f}%) | SP500 {macro_dict.get('sp500_chg',0):+.1f}%\n"
            f"Posición: {risk_position.get('current_pct',0):.1%} | "
            f"Vol: {risk_position.get('volatility_annual',0):.0%} | "
            f"Sharpe: {risk_position.get('sharpe',0):.2f}\n"
            f"Noticias: {headlines_str}\n\n"
            "1. Valida o cuestiona la decisión considerando macro actual.\n"
            "2. Factor más importante que el modelo puede subestimar.\n"
            "3. Acción concreta recomendada."
        )

        url = ollama_url or os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
        logger.info(f"LLM llamando Ollama para {result.ticker} ({url})...")
        resp = requests.post(
            f"{url}/api/chat",
            json={
                "model": "deepseek-r1:14b",
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 512},
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user",   "content": user_msg},
                ],
            },
            timeout=300,
        )
        if resp.status_code == 200:
            text = resp.json().get("message", {}).get("content", "").strip()
            # Limpiar tags <think> residuales del deepseek
            text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
            if text:
                result.reasoning = text
                result.llm_used  = True
                logger.info(f"Ollama {result.ticker}: ok ({len(text)} chars)")
            else:
                logger.warning(f"Ollama {result.ticker}: respuesta vacía")
        else:
            logger.warning(f"Ollama {resp.status_code} para {result.ticker}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"LLM falló {result.ticker} (no crítico): {type(e).__name__}: {e}")
    return result


# ── LLM: Claude API (opcional) ────────────────────────────────────────────────

def synthesize_with_llm(result: SynthesisResult, macro_snap,
                         macro_reasons: list[str], technical_reasons: list[str],
                         sentiment_headlines: list[dict], risk_position: dict,
                         portfolio_context: dict) -> SynthesisResult:
    """Versión Claude API (requiere ANTHROPIC_API_KEY en env)."""
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.debug("ANTHROPIC_API_KEY no configurada — usando Ollama")
        return result
    try:
        macro_dict = macro_snap.to_dict() if hasattr(macro_snap, "to_dict") else {}
        lbn = {l.name: l for l in result.layers}
        t = lbn.get("technical"); m = lbn.get("macro"); s = lbn.get("sentiment")
        headlines_str = "\n".join(
            f"  [{h.get('source','')}] {h.get('title','')} ({h.get('score',0):+.1f})"
            for h in sentiment_headlines[:5]
        ) or "  Sin noticias"

        prompt = (
            f"Activo: {result.ticker}. Decisión: {result.decision} "
            f"(score={result.final_score:+.2f}, convicción={result.conviction:.0%}).\n"
            f"Técnico {t.raw_score:+.2f} | Macro {m.raw_score:+.2f} | "
            f"Sentiment {f'{s.raw_score:+.2f}' if s else 'N/A'}\n"
            f"Macro: VIX={macro_dict.get('vix','?')}, WTI={macro_dict.get('wti','?')}, "
            f"SP500={macro_dict.get('sp500_chg',0):+.1f}%, 10Y={macro_dict.get('tnx','?')}%\n"
            f"Noticias: {headlines_str}\n"
            "Responde en 3 oraciones en español: 1) valida o cuestiona la decisión "
            "2) factor que el modelo puede subestimar 3) acción concreta."
        )
        resp = requests.post(
            ANTHROPIC_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 350,
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"Content-Type": "application/json",
                     "x-api-key": api_key, "anthropic-version": "2023-06-01"},
            timeout=30,
        )
        if resp.status_code == 200:
            for block in resp.json().get("content", []):
                if block.get("type") == "text":
                    result.reasoning = block["text"].strip()
                    result.llm_used  = True
                    logger.info(f"Claude API {result.ticker}: ok")
                    break
    except Exception as e:
        logger.warning(f"Claude API falló {result.ticker}: {e}")
    return result
