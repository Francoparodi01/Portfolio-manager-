from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from typing import Iterable

from .schemas import EvidenceMode, EvidenceObject, TaskSpec, VerificationReport


# Do not capture the horizon label in `5D`/`10D`, but do capture localized
# values such as `25 días`, `16,8%`, `-$115.960` and `2.895.125,00` as one token.
_NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9_])[-+]?\$?(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)%?(?![A-Za-z])"
)
_NUMERIC_INTENTS = {
    "portfolio_review",
    "position_analysis",
    "position_comparison",
    "opportunities",
    "performance",
    "bot_follow_pnl",
    "net_performance",
    "analytics_v2",
    "viability",
    "regression_audit",
    "calibration_audit",
    "decision_history",
    "decision_lab",
    "meta_policy",
    "market_context",
}


class HarnessVerifier:
    """Deterministic final gate; it never creates missing financial facts."""

    def verify(
        self,
        *,
        task: TaskSpec,
        answer: str,
        evidence: Iterable[EvidenceObject],
        required_tools: list[str] | None = None,
    ) -> VerificationReport:
        items = list(evidence)
        failures: list[str] = []
        warnings: list[str] = []
        required = set(required_tools or [])
        observed = {item.tool_name for item in items if item.ok}

        if not answer.strip():
            failures.append("empty_answer")
        if not any(item.ok for item in items):
            failures.append("no_successful_evidence")
        missing = sorted(required - observed)
        if missing:
            failures.append("missing_required_tools:" + ",".join(missing))

        shadow = any(item.mode == EvidenceMode.SHADOW for item in items)
        lowered = answer.lower()
        if shadow and "shadow" not in lowered and any(term in lowered for term in ("producción", "produccion", "vigente")):
            failures.append("shadow_presented_as_production")

        evidence_text = "\n".join(self._evidence_text(item) for item in items if item.ok)
        unmatched = self._unmatched_numbers(answer, evidence_text)
        # One unmatched discourse/derived count is tolerated (for example the
        # number of positions derived from a list). More than that means the
        # conversational model introduced unsupported quantitative content.
        numeric_consistency = len(unmatched) <= 1
        if not numeric_consistency:
            warnings.append("numbers_not_directly_traceable:" + ",".join(unmatched[:8]))
            if task.intent in _NUMERIC_INTENTS:
                failures.append("numeric_claims_not_grounded")

        stale = []
        now = datetime.now(timezone.utc)
        for item in items:
            if item.timestamp.tzinfo is None:
                warnings.append(f"naive_timestamp:{item.tool_name}")
                continue
            age = (now - item.timestamp).total_seconds()
            if item.source in {"market", "macro", "portfolio"} and age > 24 * 3600:
                stale.append(item.tool_name)
            warnings.extend(item.warnings)

        passed = not failures
        return VerificationReport(
            passed=passed,
            grounded=bool(items) and not any(code == "no_successful_evidence" for code in failures),
            numeric_consistency=numeric_consistency,
            stale_or_missing_sources=sorted(set(stale)),
            failures=failures,
            warnings=list(dict.fromkeys(warnings)),
        )

    @staticmethod
    def _evidence_text(item: EvidenceObject) -> str:
        if isinstance(item.payload, str):
            return item.payload
        return json.dumps(item.payload, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _token_values(token: str) -> set[float]:
        raw = str(token or "").strip().replace("$", "")
        is_percent = raw.endswith("%")
        raw = raw.rstrip("%")
        if not raw:
            return set()

        candidates: set[str] = set()
        dot_count, comma_count = raw.count("."), raw.count(",")

        if dot_count and comma_count:
            # Interpret the last separator as decimal and the other as thousands,
            # covering both 2.895.125,00 and 2,895,125.00.
            if raw.rfind(",") > raw.rfind("."):
                candidates.add(raw.replace(".", "").replace(",", "."))
            else:
                candidates.add(raw.replace(",", ""))
        elif dot_count > 1:
            candidates.add(raw.replace(".", ""))
        elif comma_count > 1:
            candidates.add(raw.replace(",", ""))
        else:
            candidates.add(raw.replace(",", "."))
            # A single separator followed by exactly three digits is ambiguous:
            # 115.960 can be 115960 while 0.168 can be a decimal. Keep both.
            for sep in (".", ","):
                if raw.count(sep) == 1:
                    head, tail = raw.split(sep)
                    if len(tail) == 3 and head.lstrip("+-").isdigit() and tail.isdigit():
                        candidates.add(head + tail)

        values: set[float] = set()
        for candidate in candidates:
            try:
                value = float(candidate)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            values.add(value)
            if is_percent:
                values.add(value / 100.0)
        return values

    @staticmethod
    def _close(left: float, right: float) -> bool:
        tolerance = max(1e-9, 1e-6 * max(abs(left), abs(right), 1.0))
        return abs(left - right) <= tolerance

    def _unmatched_numbers(self, answer: str, evidence_text: str) -> list[str]:
        source_values: list[float] = []
        for token in _NUMBER_RE.findall(evidence_text):
            source_values.extend(self._token_values(token))

        result: list[str] = []
        for token in _NUMBER_RE.findall(answer):
            answer_values = self._token_values(token)
            if not answer_values:
                continue
            if answer_values <= {0.0, 1.0, 2.0, 3.0}:
                continue
            if not any(
                self._close(answer_value, source_value)
                for answer_value in answer_values
                for source_value in source_values
            ):
                result.append(token)
        return list(dict.fromkeys(result))
