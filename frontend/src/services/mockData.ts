import type {
  CandlesPayload,
  DecisionsPayload,
  FillsPayload,
  HealthPayload,
  HumanActivityPayload,
  IngestionPayload,
  LogsPayload,
  LearningShadowPayload,
  OverrideAuditPayload,
  PerformancePayload,
  PortfolioPayload,
  RadarPayload,
  RowRecord,
  ShadowPayload,
} from "../types/api";

const now = () => new Date().toISOString();
const hoursAgo = (hours: number) => new Date(Date.now() - hours * 3_600_000).toISOString();
const daysAgo = (days: number) => new Date(Date.now() - days * 86_400_000).toISOString();

const positions: RowRecord[] = [
  { asset_type: "CEDEAR", avg_cost: 18250, current_price: 21480, market_value: 1224360, quantity: 57, ticker: "MSFT", unrealized_pnl: 184110, unrealized_pnl_pct: 0.177, weight_in_portfolio: 0.253 },
  { asset_type: "CEDEAR", avg_cost: 18300, current_price: 17620, market_value: 898620, quantity: 51, ticker: "AMZN", unrealized_pnl: -34680, unrealized_pnl_pct: -0.037, weight_in_portfolio: 0.186 },
  { asset_type: "ETF", avg_cost: 48200, current_price: 50640, market_value: 759600, quantity: 15, ticker: "SPY", unrealized_pnl: 36600, unrealized_pnl_pct: 0.051, weight_in_portfolio: 0.157 },
  { asset_type: "CEDEAR", avg_cost: 12600, current_price: 11940, market_value: 585060, quantity: 49, ticker: "KO", unrealized_pnl: -32340, unrealized_pnl_pct: -0.052, weight_in_portfolio: 0.121 },
  { asset_type: "CEDEAR", avg_cost: 12880, current_price: 13910, market_value: 500760, quantity: 36, ticker: "V", unrealized_pnl: 37080, unrealized_pnl_pct: 0.08, weight_in_portfolio: 0.104 },
];

export const demoHealth: HealthPayload = {
  ok: true,
  database: { ok: true },
  redis: { ok: true },
  market: { business_day: true, closed_reason: "Demo", now_art: now(), open: false, settlement_day: true, session_note: "Datos demostrativos" },
  services: {
    market_heartbeat_age_seconds: 420,
    risk_heartbeat_age_seconds: 680,
    scheduler: { alive: true, heartbeat_age_seconds: 54 },
    telegram_bot: { alive: true, busy: false, heartbeat_age_seconds: 45 },
  },
};

export const demoIngestion: IngestionPayload = {
  ok: true,
  market_prices: { latest: { latest_ts: hoursAgo(1), rows_24h: 184, rows_7d: 1028, tickers_24h: 38, tickers_7d: 41 } },
  portfolio: { counts: { last_24h: 2, last_7d: 11, total: 164 }, latest: { cash_ars: 156200, confidence_score: 0.94, scraped_at: hoursAgo(1), total_value_ars: 4832500 } },
};

export const demoPortfolio: PortfolioPayload = {
  allocation: [{ asset_type: "CEDEAR", market_value: 3496000, positions: 6 }, { asset_type: "ETF", market_value: 1180300, positions: 2 }],
  days: 90,
  history: [
    { cash_ars: 134000, confidence_score: 0.91, scraped_at: daysAgo(8), total_value_ars: 4610200 },
    { cash_ars: 142000, confidence_score: 0.92, scraped_at: daysAgo(6), total_value_ars: 4684900 },
    { cash_ars: 158200, confidence_score: 0.93, scraped_at: daysAgo(4), total_value_ars: 4741800 },
    { cash_ars: 156200, confidence_score: 0.94, scraped_at: daysAgo(2), total_value_ars: 4832500 },
  ],
  ok: true,
  positions,
  snapshot: { cash_ars: 156200, confidence_score: 0.94, scraped_at: hoursAgo(1), snapshot_id: "demo-snapshot", total_value_ars: 4832500 },
};

export const demoDecisions: DecisionsPayload = {
  days: 90,
  groups: [
    { con_10d: 12, con_20d: 7, con_5d: 18, decision: "BUY", decision_type: "executable", metric_scope: "primary", n: 22, run_intent: "formal_plan", source: "execution_plan", status: "EXECUTED" },
    { con_10d: 9, con_20d: 5, con_5d: 14, decision: "SELL", decision_type: "executable", metric_scope: "planner_audit", n: 19, run_intent: "formal_plan", source: "execution_plan", status: "APPROVED" },
    { con_10d: 21, con_20d: 10, con_5d: 27, decision: "BUY", decision_type: "candidate", metric_scope: "radar_audit", n: 41, run_intent: "scheduled_context", source: "radar", status: "THEORETICAL" },
  ],
  ok: true,
  recent: [
    { decided_at: hoursAgo(2), decision: "SELL", decision_stage: "approved_decision", final_score: -0.112, metric_scope: "planner_audit", outcome_5d: null, run_intent: "formal_plan", source: "execution_plan", status: "APPROVED", ticker: "AMZN" },
    { decided_at: daysAgo(2), decision: "BUY", decision_stage: "executed_decision", final_score: 0.089, metric_scope: "primary", outcome_5d: 0.021, run_intent: "formal_plan", source: "execution_plan", status: "EXECUTED", ticker: "V" },
  ],
  summary: { approved: 19, blocked: 6, closed_5d: 59, debug_events: 5, executed: 28, execution_plan: 64, pending_5d: 26, primary_metric: 31, radar_audit: 41, total: 136 },
};

export const demoPerformance: PerformancePayload = {
  bot_direction_breakdown: [{ avg_5d: 0.018, closed_5d: 28, decision: "BUY", payoff_ratio: 1.28, pending_5d: 8, total: 36, win_rate_5d: 0.57 }, { avg_5d: 0.007, closed_5d: 21, decision: "SELL", payoff_ratio: 1.1, pending_5d: 5, total: 26, win_rate_5d: 0.52 }],
  bot_prediction_recent: [
    { bot_was_right: null, buy_confirmation: "SHADOW_ONLY", causal_conclusion: "MIXTO", decided_at: hoursAgo(2), decision: "SELL", final_score: -0.112, metric_scope: "planner_audit", outcome_5d: null, signal_family: "SIGNAL_GENUINE", status: "APPROVED", ticker: "AMZN", trend_shadow_regime: "WEAKENING", trend_shadow_score: -0.18 },
    { bot_was_right: true, buy_confirmation: "CAUSAL_ONLY", causal_conclusion: "FUNDADO", decided_at: daysAgo(2), decision: "BUY", final_score: 0.089, metric_scope: "primary", outcome_5d: 0.021, signal_family: "REBALANCE", status: "EXECUTED", ticker: "V", trend_shadow_regime: "NEUTRAL", trend_shadow_score: 0.04 },
  ],
  bot_predictions: { avg_directional_5d: 0.013, best_directional_5d: 0.061, closed_5d: 49, pending_5d: 13, total: 62, win_rate_5d: 0.55, worst_directional_5d: -0.037 },
  bot_signal_breakdown: [{ avg_5d: 0.022, closed_5d: 16, pending_5d: 4, signal_family: "SIGNAL_GENUINE", total: 20, win_rate_5d: 0.63 }, { avg_5d: 0.009, closed_5d: 24, pending_5d: 6, signal_family: "REBALANCE", total: 30, win_rate_5d: 0.54 }, { avg_5d: -0.004, closed_5d: 9, pending_5d: 3, signal_family: "WEAK_MECHANICAL", total: 12, win_rate_5d: 0.44 }],
  by_ticker: [{ avg_5d: 0.026, n: 7, ticker: "V", win_rate_5d: 0.71 }, { avg_5d: 0.013, n: 9, ticker: "MSFT", win_rate_5d: 0.56 }, { avg_5d: -0.012, n: 5, ticker: "AMZN", win_rate_5d: 0.4 }],
  days: 180,
  ok: true,
  score_points: [{ decided_at: daysAgo(15), decision: "BUY", final_score: 0.12, metric_scope: "primary", outcome_5d: 0.041, ticker: "V" }, { decided_at: daysAgo(12), decision: "SELL", final_score: -0.15, metric_scope: "planner_audit", outcome_5d: 0.018, ticker: "KO" }, { decided_at: daysAgo(10), decision: "BUY", final_score: 0.06, metric_scope: "primary", outcome_5d: -0.019, ticker: "AMZN" }, { decided_at: daysAgo(7), decision: "BUY", final_score: 0.09, metric_scope: "primary", outcome_5d: 0.022, ticker: "MSFT" }, { decided_at: daysAgo(6), decision: "BUY", final_score: 0, metric_scope: "radar_audit", outcome_5d: -0.008, source: "radar", status: "THEORETICAL", ticker: "ZERO" }],
  source_breakdown: [{ avg_5d: 0.012, closed_5d: 31, metric_scope: "primary", source: "execution_plan", total: 39, win_rate_5d: 0.55 }, { avg_5d: 0.006, closed_5d: 28, metric_scope: "planner_audit", source: "execution_plan", total: 45, win_rate_5d: 0.51 }, { avg_5d: -0.001, closed_5d: 27, metric_scope: "radar_audit", source: "radar", total: 41, win_rate_5d: 0.48 }],
  status_counts: [{ closed_5d: 31, metric_scope: "primary", n: 39, source: "execution_plan", status: "EXECUTED" }, { closed_5d: 28, metric_scope: "planner_audit", n: 45, source: "execution_plan", status: "APPROVED" }, { closed_5d: 27, metric_scope: "radar_audit", n: 41, source: "radar", status: "THEORETICAL" }],
  summary: { avg_10d: 0.018, avg_20d: 0.024, avg_5d: 0.012, avg_loss_5d: -0.024, avg_win_5d: 0.031, best_5d: 0.076, closed_5d: 31, ev_5d: 0.006, win_rate_5d: 0.55, worst_5d: -0.047 },
  window_counts: { closed_any_5d: 86, closed_audit_5d: 55, closed_primary_5d: 31, pending_primary_5d: 9, total: 125 },
};

export const demoOverride: OverrideAuditPayload = {
  days: 90,
  match_window_days: 2,
  ok: true,
  recent: [
    { decided_at: hoursAgo(2), decision: "SELL", final_score: -0.112, match_basis: "intraday", metric_scope: "planner_audit", opposite_ratio: 0, outcome_5d: null, override_status: "IGNORED", price_at_decision: 17620, reason: "El activo perdió fuerza relativa y aumentó la concentración de riesgo.", run_intent: "formal_plan", same_amount_ars: null, same_ratio: 0, target_amount_ars: 260000, ticker: "AMZN" },
    { decided_at: daysAgo(4), decision: "BUY", final_score: 0.074, match_basis: "next_executable", metric_scope: "primary", opposite_ratio: 0, outcome_5d: 0.018, override_status: "FOLLOWED", price_at_decision: 13910, reason: "Rebalanceo moderado con soporte de tendencia y menor concentración.", run_intent: "formal_plan", same_amount_ars: 185000, same_ratio: 0.91, target_amount_ars: 203000, ticker: "V" },
  ],
  summary: { avg_bot_5d: 0.011, avg_override_delta_5d: 0.002, bot_wins_ignored: 5, by_status: { FOLLOWED: 18, IGNORED: 11, OPPOSITE: 2, PARTIAL: 4 }, closed_5d: 28, human_wins_ignored: 4, plans: 35, repeated_plans: 5, unique_intents: 30 },
};

export const demoRadar: RadarPayload = {
  chart_items: [
    { audit_entry_price: 50640, candidate_status: "COMPRABLE_AHORA", decided_at: hoursAgo(3), decision: "BUY", edge_label: "score_fuerte_rr_ok", final_score: 0.176, mae_10d: -0.021, mfe_10d: 0.064, outcome_10d: 0.043, outcome_20d: null, outcome_2d: 0.011, outcome_5d: 0.026, path_risk: "LOW", price_at_decision: 50640, rr_ratio: 1.8, ticker: "SPY" },
    { audit_entry_price: 11940, candidate_status: "BLOCKED", decided_at: hoursAgo(5), decision: "BUY", edge_label: "falta_historial", final_score: 0.052, mae_10d: -0.074, mfe_10d: 0.018, outcome_10d: -0.041, outcome_20d: null, outcome_2d: -0.012, outcome_5d: -0.028, path_risk: "HIGH", price_at_decision: 11940, rr_ratio: 0.7, ticker: "KO" },
    { audit_entry_price: 17620, candidate_status: "VIGILANCIA_A", decided_at: daysAgo(3), decision: "SELL", edge_label: "riesgo_path", final_score: -0.091, mae_10d: -0.018, mfe_10d: 0.036, outcome_10d: -0.012, outcome_20d: 0.009, outcome_2d: 0.004, outcome_5d: 0.015, path_risk: "LOW", price_at_decision: 17620, rr_ratio: 1.2, ticker: "AMZN" },
    { audit_entry_price: 21480, candidate_status: "SWAP_CANDIDATO", decided_at: daysAgo(8), decision: "BUY", edge_label: "mejora_vs_holding", final_score: 0.128, mae_10d: -0.039, mfe_10d: 0.052, outcome_10d: 0.018, outcome_20d: 0.034, outcome_2d: -0.006, outcome_5d: 0.009, path_risk: "MEDIUM", price_at_decision: 21480, rr_ratio: 1.5, ticker: "MSFT" },
    { audit_entry_price: 13910, candidate_status: "VIGILANCIA_B", decided_at: daysAgo(12), decision: "BUY", edge_label: "edge_parcial_sin_setup", final_score: 0.084, mae_10d: -0.046, mfe_10d: 0.021, outcome_10d: -0.019, outcome_20d: -0.006, outcome_2d: 0.008, outcome_5d: -0.011, path_risk: "MEDIUM", price_at_decision: 13910, rr_ratio: 1.1, ticker: "V" },
    { audit_entry_price: 48200, candidate_status: "THEORETICAL", decided_at: daysAgo(18), decision: "SELL", edge_label: "solo_audit", final_score: -0.068, mae_10d: -0.027, mfe_10d: 0.058, outcome_10d: 0.027, outcome_20d: 0.012, outcome_2d: 0.015, outcome_5d: 0.019, path_risk: "LOW", price_at_decision: 48200, rr_ratio: 1.3, ticker: "QQQ" },
    { audit_entry_price: 10200, candidate_status: "THEORETICAL", decided_at: daysAgo(22), decision: "BUY", edge_label: "score_cero_auditable", final_score: 0, mae_10d: -0.052, mfe_10d: 0.014, outcome_10d: -0.017, outcome_20d: null, outcome_2d: -0.002, outcome_5d: -0.011, path_risk: "MEDIUM", price_at_decision: 10200, rr_ratio: 0.9, ticker: "ZERO" },
  ],
  days: 90,
  ok: true,
  recent: [
    { candidate_status: "COMPRABLE_AHORA", decided_at: hoursAgo(3), decision: "BUY", edge_label: "score_fuerte_rr_ok", final_score: 0.176, path_risk: "LOW", price_at_decision: 50640, rr_ratio: 1.8, technical_source: "internal_snapshot", ticker: "SPY" },
    { candidate_status: "BLOCKED", decided_at: hoursAgo(5), decision: "BUY", edge_label: "falta_historial", final_score: 0.052, path_risk: "HIGH", price_at_decision: 11940, rr_ratio: 0.7, technical_source: "internal_snapshot", ticker: "KO" },
    { candidate_status: "VIGILANCIA_A", decided_at: daysAgo(3), decision: "SELL", edge_label: "riesgo_path", final_score: -0.091, path_risk: "LOW", price_at_decision: 17620, rr_ratio: 1.2, technical_source: "internal_snapshot", ticker: "AMZN" },
  ],
  summary: { avg_10d: 0.0, avg_2d: 0.0026, avg_5d: 0.0027, avg_mae_10d: -0.0396, avg_mfe_10d: 0.0376, blocked: 1, closed_10d: 7, closed_5d: 7, high_path_risk: 1, theoretical: 2, total: 7, win_rate_10d: 0.43, win_rate_5d: 0.57 },
};

export const demoShadow: ShadowPayload = {
  available: true,
  forecasts: [{ expected_return: 0.011, horizon_sessions: 5, lower_return: -0.018, probability_up: 0.57, realized_return: null, reference_price: 21480, thesis_action: "WATCH", thesis_confidence: 0.56, ticker: "MSFT", universe_role: "POSITION", upper_return: 0.035 }, { expected_return: -0.009, horizon_sessions: 5, lower_return: -0.041, probability_up: 0.43, realized_return: -0.006, reference_price: 17620, thesis_action: "REDUCE", thesis_confidence: 0.62, ticker: "AMZN", universe_role: "POSITION", upper_return: 0.018 }],
  metrics: [{ directional_accuracy: 0.54, horizon_sessions: 5, mean_absolute_error: 0.031, mean_realized_return: 0.008, samples: 24 }, { directional_accuracy: 0.5, horizon_sessions: 20, mean_absolute_error: 0.058, mean_realized_return: 0.014, samples: 12 }],
  note: "Shadow es experimental y no modifica decision_log ni genera órdenes.",
  ok: true,
  run: { as_of_ts: hoursAgo(2), captured_at: hoursAgo(2), model_version: "demo-shadow", run_id: "demo-shadow-run", status: "demo", universe_count: 38 },
};

export const demoLearningShadow: LearningShadowPayload = {
  available: true,
  data_quality: { benchmark_linked_cases: 36, control_linked_cases: 41, matured_cases: 41, total_cases: 47, unique_control_cases: 12, usable_path_cases: 39 },
  by_block_reason: [
    { block_reason: "BUY_SCORE_GUARD", matured_cases: 18, potential_false_negative_rate: 0.39, potential_false_negatives: 7 },
    { block_reason: "Compra pendiente por funding/señal", matured_cases: 12, potential_false_negative_rate: 0.25, potential_false_negatives: 3 },
  ],
  cohorts: [
    { cohort_date: daysAgo(35).slice(0, 10), horizon_days: 5, matured_cases: 8, potential_false_negative_rate: 0.25, potential_false_negatives: 2, shadow_coverage_rate: 0.5 },
    { cohort_date: daysAgo(28).slice(0, 10), horizon_days: 5, matured_cases: 11, potential_false_negative_rate: 0.36, potential_false_negatives: 4, shadow_coverage_rate: 0.64 },
    { cohort_date: daysAgo(21).slice(0, 10), horizon_days: 5, matured_cases: 9, potential_false_negative_rate: 0.33, potential_false_negatives: 3, shadow_coverage_rate: 0.78 },
    { cohort_date: daysAgo(14).slice(0, 10), horizon_days: 5, matured_cases: 13, potential_false_negative_rate: 0.31, potential_false_negatives: 4, shadow_coverage_rate: 0.85 },
  ],
  days: 365,
  metrics: [
    { benchmark_coverage_rate: 0.88, clean_miss_rate: 0.22, clean_missed_opportunities: 9, horizon_days: 5, matured_cases: 41, pending_cases: 6, potential_false_negative_rate: 0.317, potential_false_negatives: 13, risky_counterfactual_wins: 2, shadow_coverage_rate: 0.73, total_cases: 47 },
    { benchmark_coverage_rate: 0.79, clean_miss_rate: 0.13, clean_missed_opportunities: 3, horizon_days: 20, matured_cases: 24, pending_cases: 23, potential_false_negative_rate: 0.25, potential_false_negatives: 6, risky_counterfactual_wins: 2, shadow_coverage_rate: 0.7, total_cases: 47 },
  ],
  note: "Experimental y solo auditoría. Un falso negativo potencial no prueba que el bloqueo haya sido incorrecto.",
  ok: true,
  recent_cases: [
    { alpha_vs_benchmark: 0.022, block_category: "SCORE_GUARD", decided_at: daysAgo(8), decision: "BUY", directional_outcome: 0.041, horizon_days: 5, mae: -0.018, review_label: "CLEAN_MISSED_OPPORTUNITY", ticker: "MSFT" },
    { alpha_vs_benchmark: null, block_category: "FUNDING", decided_at: daysAgo(10), decision: "BUY", directional_outcome: 0.028, horizon_days: 5, mae: -0.071, review_label: "RISKY_COUNTERFACTUAL_WIN", ticker: "AXP" },
  ],
  review_summary: [
    { cases: 28, mean_alpha_vs_benchmark: -0.003, mean_directional_outcome: -0.012, mean_mae: -0.051, review_label: "NO_MATERIAL_UPSIDE" },
    { cases: 9, mean_alpha_vs_benchmark: 0.019, mean_directional_outcome: 0.037, mean_mae: -0.024, review_label: "CLEAN_MISSED_OPPORTUNITY" },
    { cases: 2, mean_alpha_vs_benchmark: 0.012, mean_directional_outcome: 0.048, mean_mae: -0.083, review_label: "RISKY_COUNTERFACTUAL_WIN" },
  ],
  rule_candidates: [
    { block_category: "SCORE_GUARD", candidate_type: "SHADOW_THRESHOLD_REVIEW", clean_miss_count: 7, clean_miss_rate: 0.28, id: 1, sample_size: 25, status: "PROPOSED" },
  ],
  run: { captured_at: hoursAgo(1), decisions_seen: 47, material_return_bps: 75, policy_version: "learning-shadow-v2", status: "COMPLETE" },
  trend: [],
};

export const demoHuman: HumanActivityPayload = {
  days: 7,
  ok: true,
  recent: [{ confirmed_amount_ars: 185000, confirmed_at: daysAgo(4), confirmed_price: 13910, inferred_amount_ars: 203000, quantity: 13.3, scraped_at: daysAgo(4), side: "BUY", ticker: "V" }, { confirmed_amount_ars: null, confirmed_at: null, inferred_amount_ars: 80000, quantity: 4.5, scraped_at: daysAgo(1), side: "SELL", ticker: "KO" }],
  summary: { confirmed: 1, note: "Provisional: no entra al EV principal hasta que Cocos movements confirme el movimiento.", pending: 1, scope: "inferred_from_portfolio_snapshots", total: 2 },
};

export const demoFills: FillsPayload = {
  by_source: [{ latest_executed_at: daysAgo(4), n: 1, source: "cocos" }],
  days: 90,
  movements: { recent: [{ amount: 185000, currency: "ARS", executed_at: daysAgo(4), instrument_type: "CEDEAR", movement_type: "BUY", price: 13910, quantity: 13.3, settlement_date: daysAgo(2), ticker: "V" }], summary: { last_24h: 0, latest_executed_at: daysAgo(4), total: 1, trades: 1 } },
  ok: true,
  recent: [{ avg_fill_price: 13910, decision_log_id: "demo-decision", executed_at: daysAgo(4), gross_amount_ars: 185000, quantity: 13.3, reconciled_at: daysAgo(4), side: "BUY", source: "cocos", ticker: "V" }],
  summary: { last_24h: 0, last_7d: 1, latest_executed_at: daysAgo(4), reconciled: 1, total: 1, unreconciled: 0 },
};

export const demoCandles: CandlesPayload = {
  coverage: { business_day: daysAgo(1), internal_candles: 38, missing_internal: 3, price_assets: 41 },
  market: { business_day: true, closed_reason: "Demo", expects_daily_candle: false, open: false, session_note: "Datos demostrativos", settlement_day: true },
  ok: true,
  recent: [{ business_day: daysAgo(1), max_ts: daysAgo(1), min_ts: daysAgo(1), rows: 38, tickers: 38 }, { business_day: daysAgo(2), max_ts: daysAgo(2), min_ts: daysAgo(2), rows: 41, tickers: 41 }],
};

export const demoLogs: LogsPayload = { items: [{ file: "scheduler.log", line: "WARNING demo: una señal quedó pendiente por falta de precio de cierre." }], log_dir: "demo", note: null, ok: true };
export const demoLedger: RowRecord = { ok: true, timeline: [{ at: hoursAgo(2), label: "Plan formal", stage: "planner_audit", ticker: "AMZN" }] };

export function demoPayloadFor(key: string): unknown {
  return {
    candles: demoCandles,
    decisions: demoDecisions,
    fills: demoFills,
    health: demoHealth,
    human: demoHuman,
    ingestion: demoIngestion,
    ledger: demoLedger,
    learning: demoLearningShadow,
    logs: demoLogs,
    override: demoOverride,
    performance: demoPerformance,
    portfolio: demoPortfolio,
    radar: demoRadar,
    shadow: demoShadow,
  }[key];
}
