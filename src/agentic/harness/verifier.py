from __future__ import annotations

import math
import re

from .contracts import Evidence, EvidenceMode, EvidenceQuality, RunState, VerificationResult


_NUMBER = re.compile(r"(?<![A-Za-z0-9])[-+]?\d+(?:[.,]\d+)?%?")
_EDGE_CLAIMS = ("demuestra", "prueba que", "tiene edge", "edge positivo", "supera a hold", "conviene", "es mejor que hold")


def _number_candidates(raw: str) -> set[float]:
    text = raw.strip().lstrip("+")
    percent = text.endswith("%")
    text = text.rstrip("%")
    candidates: set[float] = set()
    variants = {text}
    if "," in text and "." in text:
        # Spanish rendered number: 1.234,56
        variants.add(text.replace(".", "").replace(",", "."))
        # English rendered number: 1,234.56
        variants.add(text.replace(",", ""))
    elif "," in text:
        variants.add(text.replace(",", "."))
        variants.add(text.replace(",", ""))
    elif "." in text:
        variants.add(text.replace(".", ""))
    for variant in variants:
        try:
            value = float(variant)
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        candidates.add(value)
        if percent:
            candidates.add(value / 100.0)
    return candidates


def _matches_observed(raw: str, observed: set[float]) -> bool:
    for candidate in _number_candidates(raw):
        for value in observed:
            tolerance = max(1e-9, abs(value) * 1e-6)
            if abs(candidate - value) <= tolerance:
                return True
    return False


def _numeric_grounding(answer: str, evidence: list[Evidence], user_message: str) -> tuple[bool, list[str]]:
    evidence_text = "\n".join(item.excerpt for item in evidence if item.ok) + "\n" + user_message
    observed: set[float] = set()
    for raw in _NUMBER.findall(evidence_text):
        observed.update(_number_candidates(raw))

    missing: list[str] = []
    exempt = {"0", "1", "2", "3", "4", "5", "10", "20", "40", "90", "180"}
    for raw in _NUMBER.findall(answer):
        clean = raw.strip().lstrip("+").rstrip("%")
        if clean in exempt:
            continue
        if not _matches_observed(raw, observed):
            missing.append(raw)
    return not missing, list(dict.fromkeys(missing))[:8]


def verify_response(*, answer: str, state: RunState) -> VerificationResult:
    successful = [item for item in state.evidence if item.ok]
    checks: dict[str, bool] = {}
    warnings: list[str] = []
    blocking: list[str] = []

    checks["has_successful_evidence"] = bool(successful)
    if not successful:
        blocking.append("No successful Quantia evidence source supports the answer.")

    attempted = {item.tool for item in state.evidence}
    missing_required = [name for name in state.task.required_tools if name not in attempted]
    checks["required_tools_attempted"] = not missing_required
    if missing_required:
        warnings.append("Required evidence not attempted: " + ", ".join(missing_required))

    failed = [item.tool for item in state.evidence if not item.ok]
    checks["no_tool_failures"] = not failed
    if failed:
        warnings.append("Unavailable/failed evidence: " + ", ".join(dict.fromkeys(failed)))

    numeric_ok, missing_numbers = _numeric_grounding(answer, successful, state.task.raw_message)
    checks["numeric_grounding"] = numeric_ok
    if not numeric_ok:
        blocking.append("Ungrounded numeric value(s) in final answer: " + ", ".join(missing_numbers))

    research = [item for item in successful if item.mode in {EvidenceMode.RESEARCH, EvidenceMode.SHADOW}]
    weak_research = [item for item in research if item.quality in {EvidenceQuality.LOW, EvidenceQuality.INSUFFICIENT}]
    answer_lower = answer.lower()
    unsafe_edge_claim = bool(weak_research) and any(term in answer_lower for term in _EDGE_CLAIMS)
    checks["weak_research_not_promoted"] = not unsafe_edge_claim
    if unsafe_edge_claim:
        blocking.append("Low/insufficient research evidence was promoted into an economic superiority claim.")

    checks["modes_explicit"] = all(item.mode in EvidenceMode for item in successful)

    if blocking:
        return VerificationResult(
            passed=False,
            status="FAIL",
            checks=checks,
            warnings=warnings,
            blocking_errors=blocking,
        )
    if warnings or not all(checks.values()):
        return VerificationResult(
            passed=True,
            status="DEGRADED",
            checks=checks,
            warnings=warnings,
            blocking_errors=[],
        )
    return VerificationResult(passed=True, status="PASS", checks=checks)


def evidence_disclosure(evidence: list[Evidence]) -> str:
    modes = {item.mode for item in evidence if item.ok}
    notes: list[str] = []
    if EvidenceMode.SHADOW in modes:
        notes.append("Hay evidencia SHADOW: es experimental y no modifica capital ni política de producción.")
    if EvidenceMode.RESEARCH in modes:
        notes.append("Hay evidencia RESEARCH/Decision Lab: sirve para comparar escenarios; no es una orden ni política vigente.")
    weak = [item for item in evidence if item.ok and item.quality in {EvidenceQuality.LOW, EvidenceQuality.INSUFFICIENT}]
    if weak:
        notes.append("Parte de la evidencia histórica tiene calidad baja/insuficiente; no la trato como prueba de edge.")
    return "\n".join(notes)
