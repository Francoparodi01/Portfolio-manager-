"""Small HTML-safe formatting helpers for Telegram reports.

The helpers keep report rendering consistent without changing any trading,
audit, scoring or persistence logic.
"""
from __future__ import annotations

from datetime import datetime
from html import escape
from html.parser import HTMLParser
from zoneinfo import ZoneInfo


ART = ZoneInfo("America/Argentina/Buenos_Aires")
DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


def html_text(value, *, limit: int | None = None) -> str:
    """Escape user/data text for Telegram HTML."""
    text = "" if value is None else str(value)
    text = clean_text(text)
    if limit is not None and len(text) > limit:
        text = text[: max(0, limit - 1)].rstrip() + "…"
    return escape(text)


def clean_text(value) -> str:
    """Normalize common mojibake leftovers from broker/saved reasons."""
    text = "" if value is None else str(value)
    replacements = {
        "posici?n": "posición",
        "exposici?n": "exposición",
        "se?al": "señal",
        "te?rico": "teórico",
        "ejecuci?n": "ejecución",
        "decisi?n": "decisión",
        "auditor?a": "auditoría",
        "m?trica": "métrica",
        " ? ": " → ",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text.replace("\ufffd", "")


def money_ars(value, *, signed: bool = False) -> str:
    if value is None:
        return "N/A"
    number = float(value)
    sign = "+" if signed and number > 0 else ""
    return f"{sign}${number:,.0f} ARS".replace(",", ".")


def pct(value, *, signed: bool = True, decimals: int = 1) -> str:
    if value is None:
        return "N/A"
    sign = "+" if signed else ""
    return f"{float(value):{sign}.{decimals}%}"


def score(value) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):+.3f}"


def count(value) -> str:
    return f"{int(value or 0):,}".replace(",", ".")


def fmt_dt(value, fmt: str = "%d/%m %H:%M") -> str:
    if not value:
        return "N/A"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(ART)
        return value.strftime(fmt)
    return str(value)


def header(title: str, *, subtitle: str | None = None) -> list[str]:
    lines = [f"<b>{html_text(title)}</b>", DIVIDER]
    if subtitle:
        lines.append(html_text(subtitle))
    lines.append("")
    return lines


def section(title: str) -> str:
    return f"<b>{html_text(title)}</b>"


def note(text: str) -> str:
    return f"<i>{html_text(text)}</i>"


def kv(label: str, value) -> str:
    return f"   {html_text(label)}: <b>{html_text(value)}</b>"


def short_list(items: list[str], *, limit: int = 6, more_label: str = "más") -> list[str]:
    shown = items[:limit]
    if len(items) > limit:
        shown.append(f"+{len(items) - limit} {more_label}")
    return shown


class _TelegramHTMLValidator(HTMLParser):
    _VOID = {"br"}

    def __init__(self):
        super().__init__()
        self.stack: list[str] = []
        self.errors: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in self._VOID:
            return
        self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack:
            self.errors.append(f"closing tag without opening: {tag}")
            return
        opening = self.stack.pop()
        if opening != tag:
            self.errors.append(f"tag mismatch: {opening} closed by {tag}")


def validate_telegram_html(text: str) -> tuple[bool, list[str]]:
    """Best-effort validation for unbalanced HTML before Telegram sends it."""
    parser = _TelegramHTMLValidator()
    try:
        parser.feed(text or "")
        parser.close()
    except Exception as exc:
        return False, [str(exc)]
    errors = parser.errors[:]
    if parser.stack:
        errors.append("unclosed tags: " + ", ".join(parser.stack[-5:]))
    return not errors, errors
