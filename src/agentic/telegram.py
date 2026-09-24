"""On-demand Telegram adapter. Account authorization stays in the bot handler."""
import json
import sys
import tempfile
from html import escape
from pathlib import Path


PROMPT = (
    "<b>Agente de Quantia</b>\n"
    "Escribí un objetivo después del comando:\n"
    "<code>/agente Revisá mi cartera y explicá qué evidencia falta para decidir.</code>\n\n"
    "Consulta evidencia de tu cuenta y adjunta su traza. No ejecuta órdenes."
)
_active_chats = set()


async def run_report(context, chat_id, goal, *, run_command, send_text):
    goal = " ".join(goal.split())
    if not goal:
        await send_text(context, chat_id, PROMPT)
        return
    if len(goal) > 4000:
        await send_text(context, chat_id, "El objetivo admite hasta 4.000 caracteres.")
        return
    if chat_id in _active_chats:
        await send_text(context, chat_id, "Ya hay una consulta del agente en curso para tu cuenta.")
        return
    _active_chats.add(chat_id)
    try:
        await send_text(context, chat_id, "Consultando evidencia con el agente… Puede demorar hasta cuatro minutos.")
        with tempfile.TemporaryDirectory(prefix="quantia_agent_") as folder:
            artifact = Path(folder) / "quantia_agent_trace.json"
            rc, _out, _err, _elapsed = await run_command(
                [sys.executable, "scripts/run_agent.py", "--goal", goal,
                 "--owner-chat-id", str(chat_id), "--max-steps", "4", "--timeout-seconds", "240",
                 "--output-json", str(artifact)], timeout=300)
            if not artifact.is_file():
                await send_text(context, chat_id,
                                "No pude completar la consulta del agente. El servicio puede estar deshabilitado "
                                "o haber agotado su tiempo. Reintentá con /agente y un objetivo más puntual.")
                return
            result = json.loads(artifact.read_text(encoding="utf-8"))
            status = str(result.get("status", "FAILED"))
            audit = "completa" if result.get("audit_persisted") else "incompleta"
            answer = str(result.get("answer") or "La consulta terminó sin respuesta.")
            steps = result.get("steps", [])
            consultations = sum(step.get("decision", {}).get("kind") == "tool" for step in steps)
            completions = sum(step.get("decision", {}).get("kind") == "final" for step in steps)
            limit_note = ("\nLímite de consultas alcanzado: la síntesis puede dejar verificaciones pendientes."
                          if status == "LIMIT_REACHED" else "")
            await send_text(context, chat_id, "<b>Agente de Quantia</b>\n" + escape(answer)
                            + limit_note
                            + f"\n\nEstado: <code>{escape(status)}</code> · Traza: {audit}"
                            + f"\nConsultas: {consultations} · Cierres: {completions}"
                            + "\nLa traza registra las consultas; no valida la conclusión."
                            + f"\nRun: <code>{escape(str(result.get('run_id', 'N/D')))}</code>")
            with artifact.open("rb") as document:
                await context.bot.send_document(chat_id=chat_id, document=document,
                                                filename=artifact.name,
                                                caption="Traza del agente: herramientas, observaciones, límites y estado de auditoría.")
    finally:
        _active_chats.discard(chat_id)
