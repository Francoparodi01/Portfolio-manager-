"""Render the shared Top 10 payload; no ranking or market calculation here."""
from src.core.telegram_format import html_text


def _num(value):
    return "no informado" if value is None else f"{value:,.2f}"


def render_top10(payload: dict) -> list[str]:
    messages = []
    status = payload.get("status")
    if status != "READY":
        reasons = {
            "NO_CAPTURE": "No hay una captura completa para esta cuenta.",
            "SCHEMA_MISSING": "La fuente de oportunidades no está disponible.",
            "OWNER_NOT_CONFIGURED": "Falta configurar la cuenta del monitor.",
            "INVALID_CAPTURE": "La captura tiene datos inconsistentes.",
        }
        return ["<b>Top 10 de oportunidades</b>\n" + reasons.get(status, "Datos no disponibles.")
                + " No se generaron recomendaciones."]
    messages.append(
        "<b>Top 10 de oportunidades</b>\n"
        f"Captura: {html_text(payload.get('captured_at'))}\n"
        f"{payload['count']} candidatos · mismo orden que el dashboard.\n"
        f"Versión: {html_text(payload.get('scoring_version'), limit=100)}\n"
        + ("<b>Captura vencida.</b>\n" if payload.get("stale") else "")
        + "Precios registrados en la captura; no son cotizaciones en vivo. "
        "El estado refleja las reglas del Radar y la calidad disponible. No ejecuta órdenes."
    )
    if not payload["items"]:
        messages.append("La última captura no tiene candidatos elegibles fuera de cartera.")
    # One bounded card per item keeps HTML balanced at Telegram's length limit.
    for item in payload["items"]:
        age = item.get("price_age_seconds")
        age_text = "no verificable" if age is None or age < 0 else f"{age / 3600:.1f} h"
        sources = ", ".join(item.get("data_source_counts", {})) or item.get("data_source_mode") or "no informada"
        card = (
            f"<b>#{item['rank_position']} · {html_text(item['ticker'], limit=30)} — {html_text(item['display_state'])}</b>\n"
            f"Precio registrado: {_num(item.get('reference_price'))} {html_text(item.get('currency') or 'moneda no verificada')}\n"
            f"Actualización: {html_text(item.get('reference_ts') or 'no informada')} · antigüedad {age_text}\n"
            f"Score técnico: {_num(item.get('technical_score'))} · ranking: {_num(item.get('ranking_score'))}\n"
            f"Tendencia: {html_text(item.get('technical_regime') or 'no informada', limit=60)}\n"
            f"Volumen relativo: {_num(item.get('volume_ratio'))} · {html_text(item.get('volume_definition') or 'sin definición', limit=70)}\n"
            f"Motivo: {html_text(item.get('selection_reason'), limit=220)}\n"
            f"Riesgo: {html_text(item.get('principal_risk'), limit=220)}\n"
            f"Fuente: {html_text(sources, limit=120)} · calidad {html_text(item.get('quality'))}\n"
            f"Precio: {html_text(item.get('price_quality_flag') or 'UNKNOWN', limit=60)} · "
            f"estado guardado: {html_text(item.get('radar_status') or 'UNKNOWN', limit=40)}"
        )
        if item.get("missing_data"):
            card += "\nFaltantes: " + html_text("; ".join(item["missing_data"]), limit=350)
        messages.append(card)
    return messages
