from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from typing import Iterable

from .evidence_planner import EvidencePlanner
from .schemas import (
    ClaimStatus,
    ClaimVerification,
    EvidenceClaim,
    EvidenceMode,
    EvidenceObject,
    TaskSpec,
    VerificationReport,
)


# Do not capture the horizon label in `5D`/`10D`, but do capture localized
# values such as `25 días`, `16,8%`, `-$115.960` and `2.895.125,00` as one token.
_NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9_])[-+]?\$?(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)%?(?![A-Za-z])"
)
# Human-facing deterministic renderers localize/reshape ISO timestamps. Date/time
# components are provenance labels, not financial numeric claims, so they are
# removed before scalar traceability checks. Their source freshness is verified
# independently below from EvidenceObject.timestamp/source.
_DATETIME_RE = re.compile(
    r"\b(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?"
    r"|\d{1,2}/\d{1,2}(?:/\d{2,4})?"
    r"|\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:ART|UTC))?)\b",
    re.IGNORECASE,
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
_STALE_SOURCES = {"market", "macro", "portfolio"}
_STALE_AFTER_SECONDS = 24 * 3600


class HarnessVerifier:
    """Deterministic final gate; it never creates missing financial facts.

    Besides answer-level grounding, the verifier evaluates the evidence plan
    claim by claim. A claim is SUPPORTED only when at least one canonical tool
    produced a successful, non-stale observation. Required claims fail closed;
    optional gaps remain visible in observability instead of being treated as
    proven facts.
    """

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

        stale: list[str] = []
        now = datetime.now(timezone.utc)
        for item in items:
            if item.timestamp.tzinfo is None:
                warnings.append(f"naive_timestamp:{item.tool_name}")
                continue
            if self._is_stale(item, now):
                stale.append(item.tool_name)
            warnings.extend(item.warnings)

        # Rebuild the deterministic claim contract from the semantic task. The
        # planner emits canonical claims even when an optional tool was never
        # called, so telemetry can distinguish "missing" from "supported".
        claim_plan = EvidencePlanner().plan(
            task=task,
            available_tools={item.tool_name for item in items} | required,
            required_tools=list(required),
        )
        claim_results = self._verify_claims(
            claims=claim_plan,
            items=items,
            stale_tools=set(stale),
        )
        required_claims = [result for result in claim_results if result.required]
        supported_required = sum(
            1 for result in required_claims if result.status == ClaimStatus.SUPPORTED
        )
        required_claim_coverage = (
            supported_required / len(required_claims) if required_claims else 1.0
        )
        claim_status_counts = {
            status.value: sum(1 for result in claim_results if result.status == status)
            for status in ClaimStatus
        }

        for result in claim_results:
            if result.required and result.status != ClaimStatus.SUPPORTED:
                failures.append(
                    f"required_claim_not_supported:{result.claim_id}:{result.status.value.lower()}"
                )
            elif not result.required and result.status != ClaimStatus.SUPPORTED:
                warnings.append(
                    f"optional_claim_{result.status.value.lower()}:{result.claim_id}"
                )

        passed = not failures
        return VerificationReport(
            passed=passed,
            grounded=bool(items) and not any(code == "no_successful_evidence" for code in failures),
            numeric_consistency=numeric_consistency,
            stale_or_missing_sources=sorted(set(stale)),
            claim_results=claim_results,
            claim_status_counts=claim_status_counts,
            required_claim_coverage=required_claim_coverage,
            failures=list(dict.fromkeys(failures)),
            warnings=list(dict.fromkeys(warnings)),
        )

    @staticmethod
    def _is_stale(item: EvidenceObject, now: datetime) -> bool:
        if item.timestamp.tzinfo is None:
            return False
        age = (now - item.timestamp).total_seconds()
        return item.source in _STALE_SOURCES and age > _STALE_AFTER_SECONDS

    @staticmethod
    def _verify_claims(
        *,
        claims: list[EvidenceClaim],
        items: list[EvidenceObject],
        stale_tools: set[str],
    ) -> list[ClaimVerification]:
        results: list[ClaimVerification] = []
        for claim in claims:
            relevant = [item for item in items if item.tool_name in claim.tools]
            successful = [item for item in relevant if item.ok]
            fresh = [item for item in successful if item.tool_name not in stale_tools]

            if fresh:
                status = ClaimStatus.SUPPORTED
                used = fresh
                claim_warnings: list[str] = []
            elif successful:
                status = ClaimStatus.STALE
                used = successful
                claim_warnings = ["supporting_evidence_stale"]
            elif relevant:
                status = ClaimStatus.FAILED
                used = relevant
                claim_warnings = ["supporting_tool_failed"]
            else:
                status = ClaimStatus.MISSING
                used = []
                claim_warnings = ["supporting_evidence_not_observed"]

            supporting_tools = list(dict.fromkeys(item.tool_name for item in used))
            evidence_ids = [item.evidence_id for item in used]
            for item in used:
                claim_warnings.extend(item.warnings)

            results.append(ClaimVerification(
                claim_id=claim.claim_id,
                description=claim.description,
                required=claim.required,
                status=status,
                supporting_tools=supporting_tools,
                evidence_ids=evidence_ids,
                warnings=list(dict.fromkeys(claim_warnings)),
            ))
        return results

    @staticmethod
    def _evidence_text(item: EvidenceObject) -> str:
        if isinstance(item.payload, str):
            return item.payload
        return json.dumps(item.payload, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _token_values(token: str) -> set[float]:
        return {value for value, _tolerance in HarnessVerifier._token_candidates(token)}

    @staticmethod
    def _decimal_places(token: str) -> int:
        raw = str(token or "").strip().replace("$", "").rstrip("%")
        unsigned = raw.lstrip("+-")
        dot_count, comma_count = unsigned.count("."), unsigned.count(",")
        if dot_count and comma_count:
            decimal_sep = "," if unsigned.rfind(",") > unsigned.rfind(".") else "."
            return len(unsigned.rsplit(decimal_sep, 1)[1])
        if dot_count == 1:
            head, tail = unsigned.split(".")
            # 2.895 is ambiguous; keep display precision conservative while the
            # alternate thousands candidate remains exact.
            return len(tail) if head == "0" or len(tail) != 3 else 3
        if comma_count == 1:
            head, tail = unsigned.split(",")
            return len(tail) if head == "0" or len(tail) != 3 else 3
        return 0

    @staticmethod
    def _token_candidates(token: str) -> list[tuple[float, float]]:
        raw_token = str(token or "").strip().replace("$", "")
        is_percent = raw_token.endswith("%")
        raw = raw_token.rstrip("%")
        if not raw:
            return []

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

        decimals = HarnessVerifier._decimal_places(token)
        display_tolerance = 0.5 * (10 ** (-decimals)) if decimals else 0.5
        values: list[tuple[float, float]] = []
        for candidate in candidates:
            try:
                value = float(candidate)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            values.append((value, display_tolerance))
            if is_percent:
                values.append((value / 100.0, display_tolerance / 100.0))
        return values

    @staticmethod
    def _close(left: float, right: float, display_tolerance: float = 0.0) -> bool:
        tolerance = max(display_tolerance, 1e-9, 1e-6 * max(abs(left), abs(right), 1.0))
        return abs(left - right) <= tolerance

    def _unmatched_numbers(self, answer: str, evidence_text: str) -> list[str]:
        source_values: list[float] = []
        evidence_without_times = _DATETIME_RE.sub(" ", evidence_text)
        answer_without_times = _DATETIME_RE.sub(" ", answer)
        for token in _NUMBER_RE.findall(evidence_without_times):
            source_values.extend(self._token_values(token))

        result: list[str] = []
        for token in _NUMBER_RE.findall(answer_without_times):
            answer_candidates = self._token_candidates(token)
            if not answer_candidates:
                continue
            answer_values = {value for value, _tolerance in answer_candidates}
            if answer_values <= {0.0, 1.0, 2.0, 3.0}:
                continue
            if not any(
                self._close(answer_value, source_value, display_tolerance)
                for answer_value, display_tolerance in answer_candidates
                for source_value in source_values
            ):
                result.append(token)
        return list(dict.fromkeys(result))
