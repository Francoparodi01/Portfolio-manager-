"""QEEF orchestration and deterministic JSON/Markdown reporting."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
from statistics import mean
from . import METHODOLOGY_VERSION, MODE
from .counterfactuals import freeze_alternatives, evaluate_counterfactuals
from .historical import adapt_history, compare_history, legacy_lab_comparison, obj
from .metrics import allocation_quality, attribution, realized_value
from src.decision_lab.models import canonical, digest


def metadata(*, code_version, evidence_digest, data_as_of, cohort, benchmarks, costs, references):
    if not code_version or not evidence_digest or datetime.fromisoformat(data_as_of).tzinfo is None:
        raise ValueError('version, digest and timezone-aware cutoff required')
    return {'methodology_version': METHODOLOGY_VERSION, 'code_version': code_version,
            'evidence_digest': evidence_digest, 'data_as_of': data_as_of,
            # Logical evaluation time is explicitly fixed, not the wall clock of a rerun.
            'evaluated_at': data_as_of, 'cohort_definition': cohort,
            'benchmark_definition': benchmarks, 'cost_assumptions': costs,
            'source_references': references, 'mode': MODE,
            'affects_analysis': False, 'affects_execution': False, 'promotion_allowed': False}


def evaluate(*, episode, dataset, evaluated_as_of, code_version, benchmarks=(), horizons=(5, 10, 20), economic_dataset=None):
    frozen = freeze_alternatives(episode, tuple(benchmarks))
    outcomes = evaluate_counterfactuals(frozen, dataset, evaluated_as_of=evaluated_as_of, horizons=horizons)
    weights = {p.ticker: float(p.quantity*p.mark_ars/episode.state.capital_base_ars) for p in episode.state.positions}
    weights['CASH'] = float(episode.state.cash_ars/episode.state.capital_base_ars)
    pairs = []
    for h in horizons:
        group = {r['alternative']: r for r in outcomes if r['horizon'] == h}
        plan = group['PLAN']
        for name, alt in group.items():
            if name == 'PLAN':
                continue
            paired = plan.get('net_return') is not None and alt.get('net_return') is not None
            pairs.append({'horizon_sessions': h, 'benchmark': name,
                'incremental_return': plan['net_return']-alt['net_return'] if paired else None,
                'status': 'VERIFIED' if paired and plan['status'] == alt['status'] == 'VERIFIED' else 'PARTIAL' if paired else 'NOT_IDENTIFIABLE',
                'result_kind': 'COUNTERFACTUAL', 'causal_effect': None})
    result = metadata(code_version=code_version, evidence_digest=digest([episode, dataset, benchmarks]),
        data_as_of=evaluated_as_of.isoformat(), cohort={'owner': episode.state.owner, 'episode_id': episode.episode_id,
        'quality': episode.quality.level, 'result_kind': 'COUNTERFACTUAL', 'split': episode.split,
        'experiment_id': episode.experiment_id, 'strategy_version': episode.plan.strategy.strategy_version,
        'replay_mode': episode.plan.mode},
        benchmarks=[b.model_dump(mode='json') for b in benchmarks], costs=episode.cost_model.model_dump(mode='json'),
        references=[e.record_id for e in episode.state.records])
    result.update({'owner': episode.state.owner, 'schema_version': 'qeef-evaluation-v1',
        'dimensions': {
            'A_OPPORTUNITY_DETECTION': {'status': 'NOT_IDENTIFIABLE', 'reason': 'FROZEN_UNIVERSE_AND_LABELS_NOT_SUPPLIED'},
            'B_RESEARCH_THESIS': {'status': 'NOT_IDENTIFIABLE', 'reason': 'THESIS_CONTRACT_NOT_SUPPLIED'},
            'C_DECISION_QUALITY': {'ex_ante': {'status': 'PARTIAL', 'feasible_plan': episode.plan.feasible,
                 'reason': 'FEASIBILITY_DOES_NOT_ESTABLISH_OPTIMALITY'}, 'ex_post': pairs},
            'D_ALLOCATION_QUALITY': allocation_quality(weights),
            'E_REALIZED_ECONOMIC_VALUE': realized_value(economic_dataset, owner=episode.state.owner, evaluated_as_of=evaluated_as_of) if economic_dataset else
                 {'status': 'NOT_IDENTIFIABLE', 'economic_pnl_net': None, 'reason': 'RECONCILED_NAV_FLOWS_AND_FEES_REQUIRED'},
            'F_AUDITABILITY_CONFIDENCE': {'DATA_QUALITY': episode.quality.model_dump(mode='json'),
                 'STATISTICAL_CONFIDENCE': {'status': 'INSUFFICIENT_DATA', 'n_portfolio_episodes': 1, 'ci95': None},
                 'ECONOMIC_PERFORMANCE': {'result_kind': 'COUNTERFACTUAL', 'pairs': pairs}}},
        'outcomes': outcomes, 'attribution': attribution([])})
    result['evaluation_id'] = digest(result)
    return result


def audit_capture(capture, *, code_version, capture_digest):
    cases = adapt_history(capture)
    comparisons = compare_history(cases)
    lab = legacy_lab_comparison(capture)
    tables = capture['tables']
    fills = tables.get('broker_fills', [])
    holds = tables.get('position_hold_observations', [])
    plan_captures = [obj(r['payload']) for r in tables.get('decision_lab_plan_captures', [])]
    candles = tables.get('market_candle_observations', [])
    candle_quality = {'n': len(candles), 'available_at_known': sum(bool(r.get('available_at')) for r in candles),
                      'closed': sum(r.get('is_closed') is True for r in candles),
                      'quality_counts': dict(Counter(str(r.get('quality')) for r in candles))}
    lab_summary = []
    for h in (5, 10, 20):
        rows = [r for r in lab if r['horizon_sessions'] == h]
        vals = [r['legacy_paired_delta'] for r in rows if r['legacy_paired_delta'] is not None]
        lab_summary.append({'horizon_sessions': h, 'n_candidates': len(rows), 'n_recorded_pairs': len(vals),
            'legacy_mean_paired_delta': mean(vals) if vals else None, 'n_qeef_verified': 0, 'qeef_verified_mean': None})
    summary = {'n_decision_log': len(tables.get('decision_log', [])), 'n_hold_observations': len(holds),
        'n_cases': len(cases), 'n_recurrence_proxies': len({c['recurrence_proxy_id'] for c in cases}),
        'n_canonical_opportunities': 0, 'n_verified_economic_pairs': 0,
        'n_legacy_portfolio_episodes': len({r['episode_id'] for r in lab}),
        'n_fills': len(fills), 'n_fills_known_fees': sum(f.get('fees_ars') is not None for f in fills),
        'n_exact_plan_execution_attributions': len(tables.get('plan_execution_attributions', [])),
        'n_plans': len(tables.get('execution_plans', [])),
        'n_order_intents': len(tables.get('order_intents', [])),
        'plan_link_counts': dict(Counter(c['plan_link_kind'] for c in cases)),
        'plan_capture_types': dict(sorted(Counter(c.get('capture_kind', c.get('schema', 'UNKNOWN')) for c in plan_captures).items())),
        'status_counts': dict(Counter(c['quality_gate'] for c in cases)),
        'action_counts': dict(sorted(Counter(c['action'] for c in cases).items())),
        'n_tickers': len({c['ticker'] for c in cases}),
        'regime_counts': dict(sorted(Counter(c['regime'] for c in cases).items())),
        'first_at': min((c['decision_at'] for c in cases), default=None),
        'last_at': max((c['decision_at'] for c in cases), default=None),
        'new_candle_evidence': candle_quality,
        'legacy_lab_same_observations': lab_summary,
        'economic_pnl_net': None, 'estimated_alpha': None,
        'verdict': 'EXPERIMENTAL', 'economic_validation': 'BLOCKED_BY_EVIDENCE',
        'selection': 'CENSUS_OF_ALL_OWNER_EXACT_DECISIONS_AND_HOLDS_PLUS_SEPARATE_LEGACY_LAB',
        'minimum_30_defensible_opportunities_met': False}
    result = metadata(code_version=code_version, evidence_digest=capture_digest, data_as_of=capture['captured_at'],
        cohort={'owner_token': capture['owner_token'], 'method': summary['selection'],
                'partitions': ['source', 'source_status', 'metric_scope', 'action', 'regime', 'strategy_version', 'evidence_quality', 'horizon']},
        benchmarks={'legacy': 'PRESERVED_AS_RECORDED', 'qeef': 'NO_TRADE/HOLD_EXISTING/CASH; OTHERS_REQUIRE_EX_ANTE_RULE'},
        costs={'observed_fees': 'UNKNOWN', 'legacy_lab': 'RECORDED_MODEL_PRESERVED_NOT_OBSERVED_FEES'},
        references=['capture:'+capture_digest])
    result.update({'schema_version': 'qeef-history-v1', 'owner_token': capture['owner_token'],
        'summary': summary, 'profiles': capture['profiles'], 'lineage': capture['lineage'],
        'market_quality': capture.get('market_quality', []), 'cases': cases,
        'comparisons': comparisons, 'legacy_lab_cases': lab,
        'attribution': attribution([]), 'limitations': capture.get('limitations', []) + [
            'NO_CANONICAL_THESIS_IDS_IN_CAPTURE', 'NO_OBSERVED_FILL_FEES',
            'NO_RECONCILED_FLOW_ADJUSTED_NAV', 'LEGACY_LAB_LOW_QUALITY_NOT_PROMOTED',
            'CONTEXTUAL_CLOCKS_DO_NOT_CERTIFY_EXECUTION_OR_CORPORATE_ACTION_COVERAGE']})
    result['evaluation_id'] = digest(result)
    return result


def report_markdown(report):
    lines = ['# Quantia Economic Evaluation Framework', '', f"Modo: {MODE}. Método: {METHODOLOGY_VERSION}.",
             f"Evaluación: `{report['evaluation_id']}`. Corte: {report['data_as_of']}.", '',
             'La evidencia técnica no demuestra rentabilidad. Los contrafactuales no son PnL realizado.', '']
    if 'summary' in report:
        s = report['summary']
        lines += [f"Casos reales inspeccionados: {s['n_cases']}; proxies de recurrencia: {s['n_recurrence_proxies']}.",
                  f"Oportunidades canónicas identificadas: {s['n_canonical_opportunities']}; pares económicos verificados: {s['n_verified_economic_pairs']}.",
                  f"Fills con comisiones conocidas: {s['n_fills_known_fees']}/{s['n_fills']}.",
                  f"Episodios legacy separados: {s['n_legacy_portfolio_episodes']}. Veredicto: {s['verdict']}.", '',
                  '| Fuente / estado | Acción | Régimen | Horizonte | n observado / total | Media legacy | Neto QEEF |',
                  '|---|---|---|---|---:|---:|---|']
        for r in report['comparisons']:
            v = r['legacy_mean_recorded_outcome']
            lines.append(f"| {r['source']} / {r['source_status']} | {r['action']} | {r['regime']} | {r['horizon_label']} ({r['horizon_unit']}) | {r['n_recorded_outcomes']}/{r['n_decisions']} | {v:.6f} | NOT_IDENTIFIABLE |" if v is not None else
                         f"| {r['source']} / {r['source_status']} | {r['action']} | {r['regime']} | {r['horizon_label']} | 0/{r['n_decisions']} | N/D | NOT_IDENTIFIABLE |")
        lines += ['', 'Todas las medias legacy son retornos direccionales registrados; no equivalen a valor incremental ni a rendimiento de cuenta.', '', '## Limitaciones']
        lines += ['- '+r for r in report['limitations']]
    else:
        lines += ['| Alternativa | Sesiones | Estado | Retorno contrafactual |', '|---|---:|---|---:|']
        for r in report['outcomes']:
            lines.append(f"| {r['alternative']} | {r['horizon']} | {r['status']} | {r.get('net_return')} |")
    return '\n'.join(lines)+'\n'


def write_report(report, directory):
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    contents = {'report.json': canonical(report)+'\n', 'report.md': report_markdown(report)}
    for name, text in contents.items():
        file = path/name
        if file.exists() and file.read_text(encoding='utf-8') != text:
            raise FileExistsError('immutable report conflict; choose a new directory')
    for name, text in contents.items():
        file = path/name
        if not file.exists():
            with file.open('x', encoding='utf-8', newline='\n') as stream:
                stream.write(text)
    return path
