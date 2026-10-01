"""On-demand Telegram adapter. Account authorization stays in the bot handler."""
import asyncio
import json
import sys
import tempfile
from html import escape
from pathlib import Path

from .progress import read_progress_events, render_progress


PROMPT = (
    "<b>Agente de Quantia</b>\n"
    "Escribí un objetivo después del comando:\n"
    "<code>/agente Revisá mi cartera y explicá qué evidencia falta para decidir.</code>\n\n"
    "Consulta evidencia de tu cuenta y adjunta su traza. No ejecuta órdenes.\n"
    "Decision Lab: <code>/agente PLAN vs HOLD 20D</code> o <code>/agente DVA de MSFT 20D</code>.\n"
    "Retoma hasta tres consultas tuyas de las últimas 24 horas. Para empezar de cero: "
    "<code>/agente nuevo &lt;consulta&gt;</code>."
)
_active_chats = set()


async def _start_progress_message(context, chat_id: int) -> tuple[int | None, str]:
    bot = getattr(context, "bot", None)
    sender = getattr(bot, "send_message", None)
    initial = render_progress([])
    if not callable(sender):
        return None, initial
    try:
        message = await sender(
            chat_id=chat_id,
            text=initial,
            disable_web_page_preview=True,
        )
        return getattr(message, "message_id", None), initial
    except Exception:
        # Progress is UX telemetry: never block the actual agent response.
        return None, initial


async def _edit_progress_message(
    context,
    chat_id: int,
    message_id: int | None,
    text: str,
) -> None:
    if message_id is None:
        return
    editor = getattr(getattr(context, "bot", None), "edit_message_text", None)
    if not callable(editor):
        return
    try:
        await editor(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            disable_web_page_preview=True,
        )
    except Exception:
        return


async def _delete_progress_message(context, chat_id: int, message_id: int | None) -> None:
    if message_id is None:
        return
    deleter = getattr(getattr(context, "bot", None), "delete_message", None)
    if not callable(deleter):
        return
    try:
        await deleter(chat_id=chat_id, message_id=message_id)
    except Exception:
        return


async def _follow_progress(
    context,
    chat_id: int,
    message_id: int | None,
    progress_path: Path,
    command_task: asyncio.Task,
    initial_text: str,
) -> None:
    """Poll the sanitized process boundary while the existing subprocess runs."""
    last_text = initial_text
    while not command_task.done():
        await asyncio.sleep(0.6)
        text = render_progress(read_progress_events(progress_path))
        if text != last_text:
            await _edit_progress_message(context, chat_id, message_id, text)
            last_text = text

    # Consume a final event written immediately before subprocess exit.
    text = render_progress(read_progress_events(progress_path))
    if text != last_text:
        await _edit_progress_message(context, chat_id, message_id, text)


async def run_report(context, chat_id, goal, *, run_command, send_text):
    goal = " ".join(goal.split())
    new_conversation = goal.lower() == "nuevo" or goal.lower().startswith("nuevo ")
    if new_conversation:
        goal = goal[5:].strip()
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
    progress_message_id = None
    try:
        with tempfile.TemporaryDirectory(prefix="quantia_agent_") as folder:
            artifact = Path(folder) / "quantia_agent_trace.json"
            progress_path = Path(folder) / "quantia_agent_progress.jsonl"
            progress_message_id, initial_progress = await _start_progress_message(context, chat_id)
            command_task = asyncio.create_task(
                run_command(
                    [sys.executable, "scripts/run_agent.py", "--goal", goal,
                     "--owner-chat-id", str(chat_id), "--max-steps", "4", "--timeout-seconds", "240",
                     "--new-conversation" if new_conversation else "--continue-conversation",
                     "--output-json", str(artifact),
                     "--progress-jsonl", str(progress_path)], timeout=300
                )
            )
            await _follow_progress(
                context,
                chat_id,
                progress_message_id,
                progress_path,
                command_task,
                initial_progress,
            )
            rc, _out, _err, _elapsed = await command_task
            await _delete_progress_message(context, chat_id, progress_message_id)
            progress_message_id = None

            if not artifact.is_file():
                await send_text(context, chat_id,
                                "No pude completar la consulta del agente. El servicio puede estar deshabilitado "
                                "o haber agotado su tiempo. Reintentá con /agente y un objetivo más puntual.")
                return
            result = json.loads(artifact.read_text(encoding="utf-8"))
            status = str(result.get("status", "FAILED"))
            objective = {"EXPLAINED": "Mecánica explicada", "PARTIAL": "Respuesta parcial",
                         "INSUFFICIENT": "Evidencia insuficiente", "NOT_ASSESSED": "Fuentes consultadas"}.get(
                             result.get("objective_status"), "Fuentes consultadas")
            audit = "completa" if result.get("audit_persisted") else "incompleta"
            answer = str(result.get("answer") or "La consulta terminó sin respuesta.")
            steps = result.get("steps", [])
            consultations = sum(step.get("decision", {}).get("kind") == "tool" for step in steps)
            completions = sum(step.get("decision", {}).get("kind") == "final" for step in steps)
            limit_note = ("\nLímite de consultas alcanzado: la síntesis puede dejar verificaciones pendientes."
                          if status == "LIMIT_REACHED" else "")
            await send_text(context, chat_id, "<b>Agente de Quantia</b>\n" + escape(answer)
                            + limit_note
                            + f"\n\nResultado: {objective}"
                            + f"\n\nEstado: <code>{escape(status)}</code> · Traza: {audit}"
                            + f"\nConsultas: {consultations} · Cierres: {completions}"
                            + "\nLa traza registra las consultas; no valida la conclusión."
                            + f"\nRun: <code>{escape(str(result.get('run_id', 'N/D')))}</code>")
            with artifact.open("rb") as document:
                await context.bot.send_document(chat_id=chat_id, document=document,
                                                filename=artifact.name,
                                                caption="Traza del agente: herramientas, observaciones, límites y estado de auditoría.")
    finally:
        await _delete_progress_message(context, chat_id, progress_message_id)
        _active_chats.discard(chat_id)
