"""src/analysis/synthesis.py — Motor de sintesis probabilistica + Claude API"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import requests

logger = logging.getLogger(__name__)

LAYER_WEIGHTS = {"technical":0.30,"macro":0.30,"risk":0.25,"sentiment":0.15}
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL  = "claude-sonnet-4-20250514"

@dataclass
class LayerScore:
    name: str; raw_score: float; weight: float; weighted: float
    reasons: list=field(default_factory=list)

@dataclass
class SynthesisResult:
    ticker: str; decision: str; confidence: float; final_score: float; position_size: float
    layers: list=field(default_factory=list); reasoning: str=""; llm_used: bool=False
    generated_at: datetime=field(default_factory=lambda: datetime.now(timezone.utc))
    warnings: list=field(default_factory=list)

    def to_telegram(self) -> str:
        icons={"BUY":"🟢🟢","ACCUMULATE":"🟢","HOLD":"🟡","REDUCE":"🔴","SELL":"🔴🔴"}
        icon=icons.get(self.decision,"⚪")
        bar="█"*int(self.confidence*5)+"░"*(5-int(self.confidence*5))
        conf_pct=f"{self.confidence:.0%}"
        lines=[
            f"{icon} <b>{self.ticker}</b> → <b>{self.decision}</b>   [{bar}] {conf_pct}",
            f"   Score: <code>{self.final_score:+.3f}</code>   Sizing: <b>{self.position_size:.1%}</b>",
        ]
        for layer in self.layers:
            if abs(layer.raw_score)>0.02:
                bar_len=min(int(abs(layer.raw_score)*5),5)
                bar_char="█" if layer.weighted>0 else "▓"
                mini_bar=bar_char*bar_len+"░"*(5-bar_len)
                sign="+" if layer.weighted>=0 else ""
                lines.append(f"   <code>{layer.name:10s} {mini_bar} {sign}{layer.weighted:.3f}</code>")
        if self.reasoning:
            lines.append(f"   🧠 <i>{self.reasoning[:500]}</i>")
        for w in self.warnings[:2]:
            lines.append(f"   ⚠️ {w}")
        return "\n".join(lines)


def _map_tech(signal: str, strength: float, score_raw: float = 0.0) -> float:
    """
    Convierte señal tecnica a score -1/+1.
    
    FIX: HOLD no es siempre 0.
    El score_raw del tecnico (normalizado /9) preserva la direccion:
      HOLD con score +2.0 = bullish debil = +0.22
      HOLD con score -2.0 = bearish debil = -0.22
    BUY/SELL usan strength directamente con signo.
    """
    if signal == "BUY":
        return min(strength, 1.0)
    elif signal == "SELL":
        return -min(strength, 1.0)
    else:  # HOLD — usar score raw normalizado para preservar direccion
        return float(max(min(score_raw / 9.0, 1.0), -1.0))


def blend_scores(ticker, technical_signal, technical_strength, macro_score,
                 risk_position, sentiment_score, technical_score_raw=0.0):
    """
    Blend deterministico de las 4 capas.
    
    FIX: acepta technical_score_raw para que HOLD no sea siempre 0.
    FIX: risk sizing es informativo, no penaliza el score directamente —
         un activo de alta vol con buen sharpe no debe penalizarse.
    """
    import numpy as np
    layers=[]

    # ── Tecnico ────────────────────────────────────────────
    tech_raw = _map_tech(technical_signal, technical_strength, technical_score_raw)
    layers.append(LayerScore(
        "technical", tech_raw, LAYER_WEIGHTS["technical"],
        tech_raw * LAYER_WEIGHTS["technical"]
    ))

    # ── Macro ──────────────────────────────────────────────
    layers.append(LayerScore(
        "macro", macro_score, LAYER_WEIGHTS["macro"],
        macro_score * LAYER_WEIGHTS["macro"]
    ))

    # ── Riesgo — penaliza solo condiciones extremas ────────
    rl = risk_position.get("risk_level", "NORMAL")
    # FIX: ELEVATED/HIGH son comunes en tech stocks (NVDA vol=36% es normal)
    # Solo penalizar EXTREME o drawdown activo
    rp = {"LOW": 0.05, "NORMAL": 0.00, "ELEVATED": 0.00,
          "HIGH": -0.10, "EXTREME": -0.40}.get(rl, 0.0)
    # Penalizacion adicional si hay drawdown activo
    warnings = risk_position.get("warnings", [])
    if any("drawdown" in w.lower() for w in warnings):
        rp -= 0.10
    layers.append(LayerScore(
        "risk", rp, LAYER_WEIGHTS["risk"],
        rp * LAYER_WEIGHTS["risk"], reasons=warnings
    ))

    # ── Sentiment ──────────────────────────────────────────
    layers.append(LayerScore(
        "sentiment", sentiment_score, LAYER_WEIGHTS["sentiment"],
        sentiment_score * LAYER_WEIGHTS["sentiment"]
    ))

    # ── Score final ────────────────────────────────────────
    final = float(np.clip(sum(l.weighted for l in layers), -1.0, 1.0))

    if   final >=  0.40: decision = "BUY"
    elif final >=  0.15: decision = "ACCUMULATE"
    elif final <= -0.40: decision = "SELL"
    elif final <= -0.15: decision = "REDUCE"
    else:                decision = "HOLD"

    # Confianza = consensus entre capas + magnitud del score
    signs = [1 if l.weighted > 0.01 else (-1 if l.weighted < -0.01 else 0) for l in layers]
    consensus = abs(sum(signs)) / len(signs) if signs else 0.0
    conf = min(abs(final) * 0.6 + consensus * 0.4, 0.95)

    # Position size basado en el sizing del risk engine
    sug = risk_position.get("suggested_pct_adj", 0.05)
    if decision in ("BUY", "ACCUMULATE"):
        ps = sug * (0.5 + conf * 0.5)
    elif decision in ("SELL", "REDUCE"):
        ps = sug * max(0.1, 0.5 - conf * 0.4)
    else:
        ps = sug

    return SynthesisResult(
        ticker=ticker, decision=decision,
        confidence=round(conf, 4), final_score=round(final, 4),
        position_size=round(float(np.clip(ps, 0, 0.25)), 4),
        layers=layers
    )


def synthesize_with_llm(result, macro_snap, macro_reasons, technical_reasons,
                         sentiment_headlines, risk_position, portfolio_context):
    """Enriquece con razonamiento de Claude API. No critico si falla."""
    try:
        macro_dict = macro_snap.to_dict() if hasattr(macro_snap, "to_dict") else {}
        headlines_str = "\n".join(
            f"  [{h.get('source','')}] {h.get('title','')} ({h.get('score',0):+.1f})"
            for h in sentiment_headlines[:5]
        ) or "  Sin noticias"

        # FIX: acceder a layers por nombre, no por indice fijo
        layers_by_name = {l.name: l for l in result.layers}
        tech_layer  = layers_by_name.get("technical")
        macro_layer = layers_by_name.get("macro")
        sent_layer  = layers_by_name.get("sentiment")

        tech_score_str  = f"{tech_layer.raw_score:+.2f}"  if tech_layer  else "N/A"
        macro_score_str = f"{macro_layer.raw_score:+.2f}" if macro_layer else "N/A"
        sent_score_str  = f"{sent_layer.raw_score:+.2f}"  if sent_layer  else "N/A"

        prompt = f"""Eres un analista cuantitativo senior. Activo: {result.ticker}.
Decision del sistema: {result.decision} (score={result.final_score:+.2f}, confianza={result.confidence:.0%}).

Breakdown de capas:
- Tecnico (30%): {tech_score_str} — {'; '.join(technical_reasons[:3]) or 'N/A'}
- Macro (30%): {macro_score_str} — {'; '.join(macro_reasons[:3]) or 'N/A'}
- Riesgo (25%): vol={risk_position.get('volatility_annual',0):.0%}, sharpe={risk_position.get('sharpe',0):.2f}, accion_sugerida={risk_position.get('action','')}
- Sentiment (15%): {sent_score_str}

Contexto macro actual:
WTI ${macro_dict.get('wti','N/A')} ({macro_dict.get('wti_chg',0):+.1f}%), VIX {macro_dict.get('vix','N/A')} ({macro_dict.get('vix_chg',0):+.1f}%), DXY {macro_dict.get('dxy','N/A')} ({macro_dict.get('dxy_chg',0):+.1f}%), 10Y {macro_dict.get('tnx','N/A')}%, SP500 {macro_dict.get('sp500_chg',0):+.1f}%

Noticias recientes:
{headlines_str}

Portfolio: posicion actual {risk_position.get('current_pct',0):.1%}, sizing sugerido {risk_position.get('suggested_pct_adj',0):.1%}, total ${portfolio_context.get('total_ars',0):,.0f} ARS.

Responde en 3-4 oraciones CONCISAS en español:
1. Valida o cuestiona la decision del sistema considerando el contexto macro real.
2. El factor mas importante que el modelo cuantitativo puede estar subestimando.
3. Accion concreta recomendada (ej: mantener, reducir X%, no tocar hasta que pase Y)."""

        resp = requests.post(
            ANTHROPIC_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 350,
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            text = ""
            if data.get("content"):
                for block in data["content"]:
                    if isinstance(block, dict) and block.get("type") == "text":
                        text = block.get("text", "").strip()
                        break
            if text:
                result.reasoning = text
                result.llm_used  = True
                logger.info(f"LLM {result.ticker}: ok ({len(text)} chars)")
        else:
            logger.warning(f"Claude API {resp.status_code} para {result.ticker}: {resp.text[:150]}")
    except Exception as e:
        logger.warning(f"LLM fallo {result.ticker} (no critico): {e}")
    return result


def build_full_report(results, macro_snap, portfolio_total, portfolio_risk=None, rebalance_report=None) -> str:
    macro_summary = macro_snap.summary() if hasattr(macro_snap, "summary") else ""
    regime_map = {}
    try:
        from src.analysis.macro import get_macro_regime
        regime_map = get_macro_regime(macro_snap)
    except Exception:
        pass

    regime_icons = {"risk_on": "🟢", "risk_off": "🔴", "neutral": "🟡"}
    oil_icons    = {"bull": "🛢️↑", "bear": "🛢️↓", "neutral": "🛢️"}
    rates_icons  = {"hawkish": "📈", "dovish": "📉", "neutral": "➡️"}
    dollar_icons = {"fortaleciendo": "💪", "debilitando": "📉", "neutral": "➡️"}

    buys  = [r for r in results if r.decision in ("BUY", "ACCUMULATE")]
    sells = [r for r in results if r.decision in ("SELL", "REDUCE")]
    holds = [r for r in results if r.decision == "HOLD"]

    lines = [
        "╔══════════════════════════════════════╗",
        "║  🧠  ANALISIS CUANTITATIVO COMPLETO  ║",
        "╚══════════════════════════════════════╝",
        f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} ART",
        f"💼 Portfolio: <b>${portfolio_total:,.0f} ARS</b>",
        "",
        "📊 <b>CONTEXTO MACRO</b>",
        f"   {macro_summary}",
    ]
    if regime_map:
        lines.append(
            f"   {regime_icons.get(regime_map.get('market',''),'⚪')} Mercado: {regime_map.get('market','')}  "
            f"{oil_icons.get(regime_map.get('oil',''),'🛢️')} Petroleo: {regime_map.get('oil','')}  "
            f"{rates_icons.get(regime_map.get('rates',''),'➡️')} Tasas: {regime_map.get('rates','')}  "
            f"{dollar_icons.get(regime_map.get('dollar',''),'➡️')} Dolar: {regime_map.get('dollar','')}"
        )
    lines += ["", "━" * 38, "📋 <b>SEÑALES POR ACTIVO</b>", ""]

    for r in results:
        lines.append(r.to_telegram())
        lines.append("")

    lines += [
        "━" * 38,
        "📌 <b>RESUMEN EJECUTIVO</b>",
        f"   🟢 Comprar/Acumular:  <b>{', '.join(r.ticker for r in buys) or 'ninguno'}</b>",
        f"   🟡 Mantener:          <b>{', '.join(r.ticker for r in holds) or 'ninguno'}</b>",
        f"   🔴 Reducir/Vender:    <b>{', '.join(r.ticker for r in sells) or 'ninguno'}</b>",
    ]
    if buys:
        top = max(buys, key=lambda r: r.confidence)
        lines.append(f"   ⚡ Mayor conviction: <b>{top.ticker}</b> ({top.confidence:.0%})")
    if sells:
        top_s = min(sells, key=lambda r: r.final_score)
        lines.append(f"   🚨 Mayor urgencia: <b>{top_s.ticker}</b> (score {top_s.final_score:+.2f})")

    if portfolio_risk:
        lines += ["", portfolio_risk.to_telegram()]

    if rebalance_report:
        lines += ["", rebalance_report.to_telegram()]

    lines += [
        "",
        "━" * 38,
        "<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>",
    ]
    return "\n".join(lines)
