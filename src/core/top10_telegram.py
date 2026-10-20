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
        f"ID: {html_text(payload.get('run_id'))}\n"
        f"Entradas: {html_text(payload.get('input_sha256') or 'hash no conservado', limit=64)}\n"
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
            f"Fecha de referencia/vela: {html_text(item.get('reference_ts') or 'no informada')} · antigüedad de vela {age_text}\n"
            f"Score técnico: {_num(item.get('technical_score'))} · ranking: {_num(item.get('ranking_score'))}\n"
            f"Tendencia: {html_text(item.get('technical_regime') or 'no informada', limit=60)}\n"
            f"Volumen relativo: {_num(item.get('volume_ratio'))} · {html_text(item.get('volume_definition') or 'sin definición', limit=70)}\n"
            f"Motivo (regla guardada): {html_text(item.get('selection_reason'), limit=220)}\n"
            f"Riesgo: {html_text(item.get('principal_risk'), limit=220)}\n"
            f"Fuente: {html_text(sources, limit=120)} · calidad {html_text(item.get('quality'))}\n"
            f"Precio: {html_text(item.get('price_quality_flag') or 'UNKNOWN', limit=60)} · "
            f"estado guardado: {html_text(item.get('radar_status') or 'UNKNOWN', limit=40)}"
        )
        if item.get("missing_data"):
            card += "\nFaltantes: " + html_text("; ".join(item["missing_data"]), limit=350)
        if item.get('retrieved_at'):
            ratio, ccl = item.get('ratio') or {}, item.get('ccl') or {}
            confirmed = item.get('confirmed_close') or {}
            card += (
                f"\nConsulta: {html_text(item['retrieved_at'])} · demora fuente: {html_text(item.get('source_delay_seconds'))} s"
                f"\nBase: {html_text(item.get('price_basis_label') or item.get('price_basis'), limit=80)}"
                f"\nÚltimo negocio: {html_text(item.get('price_updated_at') or 'hora no provista; la fecha de vela es su inicio')}"
                f"\nCierre corroborado: {_num(confirmed.get('price'))} · {html_text(confirmed.get('session') or 'sin confirmar')}"
                f"\nVolumen: {_num(item.get('volume'))} · unidad: {html_text(item.get('volume_unit') or 'no verificada')}"
                f"\nRatio CEDEAR/acción o ADR: {html_text(ratio.get('ratio') or 'no verificado')} · Comafi {html_text(ratio.get('as_of') or 'sin fecha')}"
                f"\nCCL referencia venta: {_num(ccl.get('value'))} ARS/USD · {html_text(ccl.get('as_of') or 'no verificado')}"
                f"\nFuente CCL: {html_text(ccl.get('source') or 'no disponible', limit=100)}. No es CCL implícito del instrumento."
            )
        independent = item.get('independent_close') or {}
        if independent:
            card += (
                f"\nCierre independiente BYMADATA T+1: {_num(independent.get('price'))} ARS"
                f" · rueda {html_text(independent.get('session'))}"
                f"\nVolumen BYMADATA: {_num(independent.get('volume'))} nominales"
                f" · comparación: {html_text(item.get('volume_comparison'))}"
                f"\nComparación de cierre: {html_text(item.get('close_comparison'))}"
                f" · plazo de la serie: {'verificado' if item.get('settlement_verified') else 'no verificado'}"
                f"\nConsulta independiente: {html_text(independent.get('retrieved_at'))}"
            )
        messages.append(card)
    return messages
