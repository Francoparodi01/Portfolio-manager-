from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Iterable

from .schemas import EvidenceMode, EvidenceObject, TaskSpec, VerificationReport


_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])[-+]?\d+(?:[.,]\d+)?%?")


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
        # A few formatting/session numbers are expected. A large mismatch is a
        # warning because deterministic renderers can derive percentages from
        # observed ratios. It is never silently promoted to a hard financial fact.
        numeric_consistency = len(unmatched) <= 3
        if not numeric_consistency:
            warnings.append("numbers_not_directly_traceable:" + ",".join(unmatched[:8]))

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
    def _normalize_number(value: str) -> str:
        return value.replace("%", "").replace(".", "").replace(",", ".").lstrip("+")

    def _unmatched_numbers(self, answer: str, evidence_text: str) -> list[str]:
        source = {self._normalize_number(value) for value in _NUMBER_RE.findall(evidence_text)}
        result = []
        for value in _NUMBER_RE.findall(answer):
            normalized = self._normalize_number(value)
            # Ignore tiny discourse ordinals and calendar years.
            if normalized in {"0", "1", "2", "3"}:
                continue
            if normalized not in source:
                result.append(value)
        return list(dict.fromkeys(result))
