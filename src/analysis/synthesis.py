"""src/analysis/synthesis.py — Motor de sintesis probabilistica + Claude API"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import requests

logger = logging.getLogger(__name__)

LAYER_WEIGHTS = {"technical": 0.30, "macro": 0.30, "risk": 0.25, "sentiment": 0.15}
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL  = "claude-sonnet-4-6"


@dataclass
class LayerScore:
    name: str; raw_score: float; weight: float; weighted: float
    reasons: list = field(default_factory=list)


@dataclass
class SynthesisResult:
    ticker: str; decision: str; confidence: float; final_score: float; position_size: float
    layers: list = field(default_factory=list); reasoning: str = ""; llm_used: bool = False
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    warnings: list = field(default_factory=list)

    def get_layer(self, name: str) -> Optional[LayerScore]:
        """Acceso por nombre — nunca falla por indice."""
        return next((l for l in self.layers if l.name == name), None)

    def to_telegram(self) -> str:
        icons = {"BUY":"🟢🟢","ACCUMULATE":"🟢","HOLD":"🟡","REDUCE":"🔴","SELL":"🔴🔴"}
        icon = icons.get(self.decision, "⚪")
        bar = "█"*int(self.confidence*5) + "░"*(5-int(self.confidence*5))
        lines = [
            f"{icon} <b>{self.ticker}</b> → <b>{self.decision}</b>   [{bar}] {self.confidence:.0%}",
            f"   Score: <code>{self.final_score:+.3f}</code>   Sizing sugerido: <b>{self.position_size:.1%}</b>",
        ]
        for layer in self.layers:
            if abs(layer.weighted) > 0.001:
                bar_len = min(int(abs(layer.raw_score)*5), 5)
                bar_char = "█" if layer.weighted >= 0 else "▓"
                mini_bar = bar_char*bar_len + "░"*(5-bar_len)
                sign = "+" if layer.weighted >= 0 else ""
                lines.append(f"   <code>{layer.name:10s} {mini_bar} {sign}{layer.weighted:.3f}</code>")
        if self.reasoning:
            lines.append(f"   🧠 <i>{self.reasoning[:500]}</i>")
        for w in self.warnings[:2]:
            lines.append(f"   ⚠️ {w}")
        return "\n".join(lines)


def _tech_to_score(signal: str, strength: float) -> float:
    """
    BUY/SELL usan la fuerza directa.
    HOLD: la fuerza alta (ej 0.94) significa "muy convencido de no hacer nada",
    lo que equivale a un score casi 0 pero no exactamente 0.
    residual = 1-strength nos da un score debil que no distorsiona el blend.
    """
    if signal == "BUY":   return strength
    elif signal == "SELL": return -strength
    else:
        residual = 1.0 - strength  # fuerza 0.94 => residual 0.06
        return residual * 0.3


def blend_scores(ticker, technical_signal, technical_strength, macro_score, risk_position, sentiment_score):
    import numpy as np
    layers = []
    tech_raw = _tech_to_score(technical_signal, technical_strength)
    layers.append(LayerScore("technical", tech_raw, LAYER_WEIGHTS["technical"], tech_raw*LAYER_WEIGHTS["technical"]))
    layers.append(LayerScore("macro", macro_score, LAYER_WEIGHTS["macro"], macro_score*LAYER_WEIGHTS["macro"]))
    rl = risk_position.get("risk_level","NORMAL")
    rp = {"LOW":0.05,"NORMAL":0.00,"ELEVATED":-0.10,"HIGH":-0.20,"EXTREME":-0.50}.get(rl, 0.0)
    layers.append(LayerScore("risk", rp, LAYER_WEIGHTS["risk"], rp*LAYER_WEIGHTS["risk"],
                             reasons=risk_position.get("warnings",[])))
    layers.append(LayerScore("sentiment", sentiment_score, LAYER_WEIGHTS["sentiment"],
                             sentiment_score*LAYER_WEIGHTS["sentiment"]))
    final = float(np.clip(sum(l.weighted for l in layers), -1.0, 1.0))
    if final >= 0.40:    decision = "BUY"
    elif final >= 0.20:  decision = "ACCUMULATE"
    elif final <= -0.40: decision = "SELL"
    elif final <= -0.20: decision = "REDUCE"
    else:                decision = "HOLD"
    signs = [1 if l.weighted>0.01 else(-1 if l.weighted<-0.01 else 0) for l in layers]
    consensus = abs(sum(signs))/len(signs) if signs else 0.0
    conf = min(abs(final)*0.6 + consensus*0.4, 0.95)
    sug = risk_position.get("suggested_pct_adj", 0.05)
    if decision in ("BUY","ACCUMULATE"):   ps = sug*(0.5+conf*0.5)
    elif decision in ("SELL","REDUCE"):    ps = sug*(0.5-conf*0.4)
    else:                                  ps = sug
    return SynthesisResult(
        ticker=ticker, decision=decision, confidence=round(conf,4),
        final_score=round(final,4), position_size=round(float(np.clip(ps,0,0.25)),4),
        layers=layers,
    )


def synthesize_with_llm(result, macro_snap, macro_reasons, technical_reasons,
                        sentiment_headlines, risk_position, portfolio_context):
    """FIX: accede a layers por nombre (get_layer), no por indice fijo."""
    try:
        macro_dict = macro_snap.to_dict() if hasattr(macro_snap,"to_dict") else {}
        headlines_str = "\n".join(
            f"  [{h.get('source','')}] {h.get('title','')} ({h.get('score',0):+.1f})"
            for h in sentiment_headlines[:5]
        ) or "  Sin noticias"

        tech_layer  = result.get_layer("technical")
        macro_layer = result.get_layer("macro")
        sent_layer  = result.get_layer("sentiment")

        tech_score  = tech_layer.raw_score  if tech_layer  else 0.0
        macro_score = macro_layer.raw_score if macro_layer else 0.0
        sent_score  = sent_layer.raw_score  if sent_layer  else 0.0

        prompt = f"""Eres un analista cuantitativo senior. Activo: {result.ticker}. Decision: {result.decision} (score={result.final_score:+.2f}, conf={result.confidence:.0%}).

Capas del modelo:
  Tecnico  (30%): {tech_score:+.2f}  {technical_reasons[0] if technical_reasons else 'N/A'}
  Macro    (30%): {macro_score:+.2f}  {'; '.join(macro_reasons[:2]) if macro_reasons else 'N/A'}
  Riesgo   (25%): vol={risk_position.get('volatility_annual',0):.0%}, sharpe={risk_position.get('sharpe',0):.2f}, accion={risk_position.get('action','')}
  Sentiment(15%): {sent_score:+.2f}

Macro ahora: WTI ${macro_dict.get('wti','?')} ({macro_dict.get('wti_chg',0):+.1f}%), VIX {macro_dict.get('vix','?')}, DXY {macro_dict.get('dxy','?')}, 10Y {macro_dict.get('tnx','?')}%, SP500 {macro_dict.get('sp500_chg',0):+.1f}%

Noticias:
{headlines_str}

Portfolio: actual {risk_position.get('current_pct',0):.1%} -> sugerido {risk_position.get('suggested_pct_adj',0):.1%}. Total: ${portfolio_context.get('total_ars',0):,.0f} ARS.

En 3 oraciones en español:
1. La decision {result.decision} — tiene sentido dado el contexto macro real hoy?
2. Que factor importante subestima el modelo?
3. Accion concreta recomendada."""

        resp = requests.post(
            ANTHROPIC_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 300,
                  "messages": [{"role":"user","content": prompt}]},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if resp.status_code == 200:
            data = resp.json()
            # Parseo defensivo — recorre content buscando el primer bloque de texto
            content = data.get("content") or []
            text = ""
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text = block.get("text","").strip()
                    break
            if text:
                result.reasoning = text
                result.llm_used = True
                logger.info(f"LLM {result.ticker}: ok ({len(text)} chars)")
            else:
                logger.warning(f"LLM {result.ticker}: respuesta vacia — {data}")
        elif resp.status_code == 401:
            logger.warning("LLM: ANTHROPIC_API_KEY no configurada o invalida")
        else:
            logger.warning(f"LLM {result.ticker}: HTTP {resp.status_code} — {resp.text[:200]}")

    except Exception as e:
        logger.warning(f"LLM fallo {result.ticker} (no critico): {e}")

    return result


def build_full_report(results, macro_snap, portfolio_total, portfolio_risk=None) -> str:
    macro_summary = macro_snap.summary() if hasattr(macro_snap,"summary") else ""
    regime_map = {}
    try:
        from src.analysis.macro import get_macro_regime
        regime_map = get_macro_regime(macro_snap)
    except Exception:
        pass

    regime_icons = {"risk_on":"🟢","risk_off":"🔴","neutral":"🟡"}
    oil_icons    = {"bull":"🛢️↑","bear":"🛢️↓","neutral":"🛢️"}
    rates_icons  = {"hawkish":"📈","dovish":"📉","neutral":"➡️"}
    dollar_icons = {"fortaleciendo":"💪","debilitando":"📉","neutral":"➡️"}

    buys  = [r for r in results if r.decision in ("BUY","ACCUMULATE")]
    sells = [r for r in results if r.decision in ("SELL","REDUCE")]
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
            f"{oil_icons.get(regime_map.get('oil',''),'🛢️')} Petróleo: {regime_map.get('oil','')}  "
            f"{rates_icons.get(regime_map.get('rates',''),'➡️')} Tasas: {regime_map.get('rates','')}  "
            f"{dollar_icons.get(regime_map.get('dollar',''),'➡️')} Dólar: {regime_map.get('dollar','')}"
        )
    lines += ["","━"*38,"📋 <b>SEÑALES POR ACTIVO</b>",""]

    for r in results:
        lines.append(r.to_telegram())
        lines.append("")

    lines += [
        "━"*38,
        "📌 <b>RESUMEN EJECUTIVO</b>",
        f"   🟢 Comprar/Acumular:  <b>{', '.join(r.ticker for r in buys) or 'ninguno'}</b>",
        f"   🟡 Mantener:          <b>{', '.join(r.ticker for r in holds) or 'ninguno'}</b>",
        f"   🔴 Reducir/Vender:    <b>{', '.join(r.ticker for r in sells) or 'ninguno'}</b>",
    ]
    if buys:
        top = max(buys, key=lambda r: r.confidence)
        lines.append(f"   ⚡ Mayor convicción: <b>{top.ticker}</b> ({top.confidence:.0%})")
    if sells:
        top_s = min(sells, key=lambda r: r.final_score)
        lines.append(f"   🚨 Mayor urgencia salida: <b>{top_s.ticker}</b> (score {top_s.final_score:+.2f})")

    if portfolio_risk:
        lines += ["", portfolio_risk.to_telegram()]

    lines += [
        "",
        "━"*38,
        "<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>",
    ]
    return "\n".join(lines)