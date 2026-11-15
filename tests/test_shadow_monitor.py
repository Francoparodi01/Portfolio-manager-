from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_shadow_monitor_api_is_read_only_and_registered():
    source = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")
    start = source.index("async def shadow_view")
    end = source.index("SECRET_PATTERNS", start)
    block = source[start:end]

    assert 'app.router.add_get("/api/shadow", shadow_view)' in source
    assert "shadow_thesis_runs" in block
    assert "shadow_thesis_forecasts" in block
    assert "shadow_thesis_outcomes" in block
    assert "MAX_ABS_REALIZED_RETURN_FOR_METRICS" in block
    assert "excluded_samples" in block
    assert "feature_snapshot" in block
    assert "INSERT INTO" not in block
    assert "UPDATE " not in block
    assert "DELETE FROM" not in block


def test_shadow_calibration_api_is_read_only_and_exposes_promotion_gates():
    source = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")
    start = source.index("async def shadow_calibration_view")
    end = source.index("SECRET_PATTERNS", start)
    block = source[start:end]

    assert 'app.router.add_get("/api/shadow-calibration", shadow_calibration_view)' in source
    for table in (
        "shadow_calibration_runs",
        "shadow_calibration_models",
        "shadow_calibrated_forecasts",
        "shadow_calibration_gate_state",
        "shadow_calibration_gate_events",
    ):
        assert table in block
    assert '"writes": []' in block
    assert "INSERT INTO" not in block
    assert "UPDATE " not in block
    assert "DELETE FROM" not in block


def test_react_audit_page_renders_shadow_v3_gate_metrics():
    page = (ROOT / "frontend" / "src" / "pages" / "AuditPage.tsx").read_text(
        encoding="utf-8"
    )
    service = (ROOT / "frontend" / "src" / "services" / "monitorApi.ts").read_text(
        encoding="utf-8"
    )

    for marker in (
        "useShadowCalibrationQuery",
        "Shadow calibrado v3",
        "Compuertas por horizonte",
        "Brier v2 -> v3",
        "MAE v2 -> v3",
        "Prospectivos",
    ):
        assert marker in page
    assert 'path: "/api/shadow-calibration"' in service


def test_shadow_monitor_has_navigation_controls_chart_and_ranking():
    html = (ROOT / "src" / "monitor" / "static" / "index.html").read_text(
        encoding="utf-8"
    )

    for marker in (
        'data-section="shadow"',
        'id="section-shadow"',
        'id="shadowTicker"',
        'id="shadowChart"',
        'id="shadowRankingTable"',
        'api("/api/shadow")',
        "function renderShadowChart",
        "function renderShadow",
    ):
        assert marker in html


def test_shadow_chart_labels_axes_and_experimental_scope():
    html = (ROOT / "src" / "monitor" / "static" / "index.html").read_text(
        encoding="utf-8"
    )

    assert "X · HORIZONTE (RUEDAS)" in html
    assert "Y · RETORNO" in html
    assert "Rango de incertidumbre" in html
    assert "EXPERIMENTAL · NO EJECUTABLE" in html
    assert "No modifica Análisis, Radar, optimizer, planner ni órdenes" in html
    assert ".shadow-grid," in html
    assert "grid-template-columns: 1fr" in html


def test_score_scatter_has_human_decision_filters():
    html = (ROOT / "src" / "monitor" / "static" / "index.html").read_text(
        encoding="utf-8"
    )

    for marker in (
        'id="scoreScatterMeta"',
        'data-scatter-filter="all"',
        'data-scatter-filter="followed"',
        'data-scatter-filter="ignored"',
        'data-scatter-filter="partial"',
        'data-scatter-filter="opposite"',
        'data-scatter-filter="unmatched"',
        'data-score-filter="signal"',
        'data-score-filter="zero"',
        'data-score-filter="all"',
        "pointMatchesScatterFilter",
        "pointMatchesScoreFilter",
        "overrideStatusForPoint",
        "Con score",
        "Score 0",
    ):
        assert marker in html

    assert 'scatterScoreFilter: "signal"' in html


def test_override_audit_exposes_full_matches_for_scatter_lookup():
    source = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")
    start = source.index("async def override_audit")
    end = source.index("async def decision_ledger", start)
    block = source[start:end]

    assert '"matches": [' in block
    assert '"override_status": item.get("override_status")' in block
    assert '"recent": items[:30]' in block
