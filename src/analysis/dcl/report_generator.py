from __future__ import annotations

from src.analysis.dcl.run_calibration import CalibrationReport


def _pct(value) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):+.1%}"


def _num(value) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):+.3f}"


def _ci(ci: tuple) -> str:
    low, high = ci
    if low is None or high is None:
        return "N/A"
    return f"{_pct(low)} a {_pct(high)}"


def _quality_line(counts: dict[str, int]) -> str:
    if not counts:
        return "   Sin filas para clasificar"
    ordered = ["clean", "mixed", "reconstructed", "unknown"]
    parts = [f"{key} {counts[key]}" for key in ordered if counts.get(key)]
    parts.extend(
        f"{key} {value}"
        for key, value in sorted(counts.items())
        if key not in ordered and value
    )
    return "   " + " | ".join(parts)


def render_calibration_report(report: CalibrationReport) -> str:
    audit = report.audit
    safety = report.safety
    lines = [
        "<b>DECISION CALIBRATION LAYER</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Status: <b>{report.status}</b>",
        f"Periodo: ultimos {report.lookback_days} dias",
        f"Modo calidad: <b>{report.quality_mode}</b>",
        "",
        "<b>MUESTRA</b>",
        f"   Total decision_log: <b>{safety.n_total}</b>",
        f"   Auditables clean/canonicas: <b>{safety.n_auditable}</b>",
        f"   Tickers auditables: <b>{len(safety.n_by_ticker)}</b>",
        f"   Regimenes auditables: <b>{len(safety.n_by_regime)}</b>",
        "",
        "<b>CALIDAD DE DATOS</b>",
        _quality_line(report.quality_counts),
        "   strict: propuestas solo con clean/canonica.",
        "   relaxed: auditoria con clean + mixed.",
        "",
        "<b>AUDITORIA ESTADISTICA</b>",
        f"   IC 5D:  <b>{_num(audit.ic_5d)}</b>",
        f"   IC 10D: <b>{_num(audit.ic_10d)}</b>",
        f"   IC 20D: <b>{_num(audit.ic_20d)}</b>",
        f"   IC t-stat: <b>{_num(audit.ic_tstat)}</b>",
        f"   Win rate: <b>{_pct(audit.win_rate)}</b>  CI95 { _ci(audit.win_rate_ci_95) }",
        f"   EV medio 5D: <b>{_pct(audit.ev_mean)}</b>  CI95 { _ci(audit.ev_bootstrap_ci) }",
        f"   Confianza: <b>{audit.confidence_level}</b>",
        "",
    ]

    warnings = [*safety.warnings, *audit.warning_flags]
    if warnings:
        lines.append("<b>WARNINGS</b>")
        for warning in warnings[:8]:
            lines.append(f"   - {warning}")
        lines.append("")

    lines += [
        "<b>POLITICA DE SEGURIDAD</b>",
        "   No se aplican cambios automaticos.",
        "   DCL v0 solo audita y prepara futuras propuestas PENDING.",
    ]
    return "\n".join(lines)
