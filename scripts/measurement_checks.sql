-- Named, read-only aggregate checks. $1 is the exact configured owner.
-- Sections are executed independently in one repeatable-read read-only snapshot.
-- name: decision_cohorts
SELECT source, metric_scope, run_intent, decision_stage, outcome_basis,
 count(*) n, min(decided_at) first_at, max(decided_at) last_at,
 count(*) FILTER (WHERE run_id IS NULL) missing_run,
 count(*) FILTER (WHERE outcome_20d IS NOT NULL) outcome_20d_known,
 count(*) FILTER (WHERE outcome_20d IS NOT NULL AND outcome_filled_at IS NULL) outcome_without_availability,
 count(*) FILTER (WHERE outcome_filled_at < decided_at) outcome_available_before_decision,
 count(*) FILTER (WHERE decided_at > CURRENT_TIMESTAMP) future_decision
FROM decision_log WHERE owner_chat_id=$1 GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5;
-- name: fills
SELECT source, executed_at_precision, executed_at_source, side, count(*) n,
 min(executed_at) first_at,max(executed_at) last_at,
 count(*) FILTER (WHERE fees_ars IS NULL) unknown_fees,
 count(*) FILTER (WHERE quantity IS NULL OR avg_fill_price IS NULL OR gross_amount_ars IS NULL) missing_arithmetic_input,
 count(*) FILTER (WHERE abs(quantity*avg_fill_price-gross_amount_ars)>.01) gross_arithmetic_gap,
 count(*) FILTER (WHERE executed_at>created_at) executed_after_recorded,
 count(*) FILTER (WHERE decision_log_id IS NOT NULL) recorded_decision_links
FROM broker_fills WHERE owner_chat_id=$1 GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;
-- name: fill_decision_owner_integrity
SELECT count(*) n,
 count(*) FILTER (WHERE d.id IS NULL) dangling,
 count(*) FILTER (WHERE d.id IS NOT NULL AND d.owner_chat_id IS DISTINCT FROM f.owner_chat_id) different_or_unknown_owner
FROM broker_fills f LEFT JOIN decision_log d ON d.id=f.decision_log_id
WHERE f.owner_chat_id=$1 AND f.decision_log_id IS NOT NULL;
-- name: order_plan_decision_integrity
SELECT count(*) n,
 count(*) FILTER (WHERE i.decision_log_id IS NULL) no_decision,
 count(*) FILTER (WHERE i.decision_log_id IS NOT NULL AND d.id IS NULL) dangling,
 count(*) FILTER (WHERE d.id IS NOT NULL AND d.owner_chat_id IS DISTINCT FROM p.owner_chat_id) different_or_unknown_owner,
 count(*) FILTER (WHERE d.id IS NOT NULL AND d.run_id IS DISTINCT FROM p.run_id) different_run
FROM order_intents i JOIN execution_plans p ON p.id=i.execution_plan_id
LEFT JOIN decision_log d ON d.id=i.decision_log_id WHERE p.owner_chat_id=$1;
-- name: hold_parent_integrity
SELECT count(*) n,
 count(*) FILTER (WHERE h.execution_plan_id IS NOT NULL AND p.id IS NULL) dangling_plan,
 count(*) FILTER (WHERE p.id IS NOT NULL AND p.owner_chat_id IS DISTINCT FROM h.owner_chat_id) different_plan_owner,
 count(*) FILTER (WHERE h.portfolio_snapshot_id IS NOT NULL AND s.snapshot_id IS NULL) dangling_snapshot,
 count(*) FILTER (WHERE s.snapshot_id IS NOT NULL AND s.owner_chat_id IS DISTINCT FROM h.owner_chat_id) different_snapshot_owner
FROM position_hold_observations h LEFT JOIN execution_plans p ON p.id=h.execution_plan_id
LEFT JOIN portfolio_snapshots s ON s.snapshot_id::text=h.portfolio_snapshot_id WHERE h.owner_chat_id=$1;
-- name: position_consistency
SELECT p.currency,count(*) n,
 count(*) FILTER (WHERE p.scraped_at<>s.scraped_at) parent_timestamp_mismatch,
 count(*) FILTER (WHERE p.quantity IS NULL OR p.current_price IS NULL OR p.market_value IS NULL) missing_arithmetic_input,
 count(*) FILTER (WHERE abs(p.quantity*p.current_price-p.market_value)>.01) market_value_arithmetic_gap,
 count(*) FILTER (WHERE p.quantity<0) negative_quantity,
 count(*) FILTER (WHERE p.current_price<=0) nonpositive_price
FROM positions p JOIN portfolio_snapshots s USING(snapshot_id)
WHERE s.owner_chat_id=$1 GROUP BY 1;
-- name: duplicate_snapshot_clocks
SELECT count(*) groups,coalesce(sum(n-1),0) extra_rows FROM
(SELECT scraped_at,count(*) n FROM portfolio_snapshots WHERE owner_chat_id=$1 GROUP BY scraped_at HAVING count(*)>1) q;
-- name: duplicate_position_grain_normalized
SELECT count(*) groups,coalesce(sum(n-1),0) extra_rows FROM
(SELECT p.snapshot_id,upper(trim(p.ticker)),p.currency,count(*) n FROM positions p
JOIN portfolio_snapshots s USING(snapshot_id) WHERE s.owner_chat_id=$1 GROUP BY 1,2,3 HAVING count(*)>1) q;
-- name: repeated_decision_grain_not_necessarily_duplicates
SELECT source, count(*) groups,sum(n-1) extra_rows FROM
(SELECT source,run_id,ticker,decision,decision_stage,count(*) n FROM decision_log
WHERE owner_chat_id=$1 AND run_id IS NOT NULL GROUP BY 1,2,3,4,5 HAVING count(*)>1) q GROUP BY source;
-- name: movement_population_unassigned
SELECT source,currency,movement_type,executed_at_precision,count(*) n,
min(executed_at) first_at,max(executed_at) last_at,
count(*) FILTER (WHERE amount IS NULL) missing_amount,
count(*) FILTER (WHERE executed_at>created_at) executed_after_recorded
FROM broker_movements GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;
-- name: movement_link_coverage_unassigned
SELECT count(*) movement_rows,
count(*) FILTER (WHERE (SELECT count(*) FROM broker_fills f WHERE f.external_fill_id=m.external_movement_id)=0) no_external_id_fill_match,
count(*) FILTER (WHERE (SELECT count(*) FROM broker_fills f WHERE f.external_fill_id=m.external_movement_id)>1) ambiguous_external_id_match,
count(*) FILTER (WHERE EXISTS (SELECT 1 FROM broker_fills f WHERE f.external_fill_id=m.external_movement_id AND f.owner_chat_id=$1)) configured_owner_id_match_only
FROM broker_movements m;
-- name: market_candle_contract_global
SELECT source,currency,asset_type,venue,interval,count(*) n,min(ts) first_at,max(ts) last_at,
count(*) FILTER (WHERE available_at IS NULL) missing_available_at,
count(*) FILTER (WHERE bar_end IS NULL) missing_bar_end,
count(*) FILTER (WHERE calendar IS NULL) missing_calendar,
count(*) FILTER (WHERE adjustment_policy IS NULL) missing_adjustment_policy,
count(*) FILTER (WHERE depositary_ratio IS NULL) missing_ratio,
count(*) FILTER (WHERE available_at<bar_end) available_before_bar_end,
count(*) FILTER (WHERE high_price<low_price OR open_price>high_price OR open_price<low_price OR close_price>high_price OR close_price<low_price) invalid_ohlc,
count(*) FILTER (WHERE close_price<=0) nonpositive_close
FROM market_candles GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5;
-- name: observed_candle_chronology
SELECT source,quality,count(*) n,
count(*) FILTER (WHERE is_closed AND available_at<bar_end) closed_available_before_bar_end,
count(*) FILTER (WHERE available_at>created_at) available_after_recorded,
count(*) FILTER (WHERE created_at>CURRENT_TIMESTAMP) future_recorded,
count(*) FILTER (WHERE high_price<low_price OR open_price>high_price OR open_price<low_price OR close_price>high_price OR close_price<low_price) invalid_ohlc
FROM market_candle_observations WHERE owner_chat_id=$1 GROUP BY 1,2 ORDER BY 1,2;
-- name: market_prices_global
SELECT currency,asset_type,count(*) n,min(ts) first_at,max(ts) last_at,
count(*) FILTER (WHERE last_price IS NULL) missing_price,
count(*) FILTER (WHERE last_price<=0) nonpositive_price,
count(*) FILTER (WHERE ts>CURRENT_TIMESTAMP) future_timestamp
FROM market_prices GROUP BY 1,2 ORDER BY 1,2;
-- name: native_snapshot_coverage
SELECT count(*) n,min(s.scraped_at) first_at,max(s.scraped_at) last_at,
count(*) FILTER (WHERE s.total_value_ars IS NULL) missing_invested_value,
count(*) FILTER (WHERE s.cash_ars IS NULL) missing_cash,
count(*) FILTER (WHERE s.created_at<s.scraped_at) scraped_after_recorded,
count(*) FILTER (WHERE s.scraped_at>CURRENT_TIMESTAMP) future_scraped,
count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM positions p WHERE p.snapshot_id=s.snapshot_id)) no_positions,
count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM raw_snapshots r WHERE r.snapshot_id=s.snapshot_id AND r.scraped_at=s.scraped_at)) no_raw_snapshot
FROM portfolio_snapshots s WHERE s.owner_chat_id=$1;
-- name: snapshot_clock_gap_buckets
SELECT count(*) FILTER (WHERE scraped_at>created_at) positive_gap,
count(*) FILTER (WHERE scraped_at>created_at AND scraped_at-created_at<=interval '1 second') up_to_one_second,
count(*) FILTER (WHERE scraped_at-created_at>interval '1 second' AND scraped_at-created_at<=interval '1 minute') one_second_to_minute,
count(*) FILTER (WHERE scraped_at-created_at>interval '1 minute') over_one_minute,
min(extract(epoch FROM scraped_at-created_at)) min_seconds,
max(extract(epoch FROM scraped_at-created_at)) max_seconds
FROM portfolio_snapshots WHERE owner_chat_id=$1;
-- name: fill_decision_owner_classes
SELECT CASE WHEN d.owner_chat_id IS NULL THEN 'OWNER_NULL' WHEN d.owner_chat_id=0 THEN 'OWNER_ZERO'
WHEN d.owner_chat_id=$1 THEN 'EXACT_OWNER' ELSE 'OTHER_OWNER' END linked_decision_scope,count(*) n
FROM broker_fills f JOIN decision_log d ON d.id=f.decision_log_id WHERE f.owner_chat_id=$1 GROUP BY 1;
-- name: missing_hold_snapshot_id_shape
SELECT count(*) missing,
count(*) FILTER (WHERE h.portfolio_snapshot_id ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') valid_uuid_shape,
min(h.observed_at) first_at,max(h.observed_at) last_at
FROM position_hold_observations h WHERE h.owner_chat_id=$1 AND h.portfolio_snapshot_id IS NOT NULL
AND NOT EXISTS(SELECT 1 FROM portfolio_snapshots s WHERE s.snapshot_id::text=h.portfolio_snapshot_id);
-- name: fills_payload_flags
SELECT count(*) n,
count(*) FILTER (WHERE raw_payload ? 'superseded_by_real') superseded_marked,
count(*) FILTER (WHERE raw_payload->>'_price_derived_from_amount'='true') derived_price_marked,
count(*) FILTER (WHERE external_fill_id LIKE 'synthetic:%') synthetic_id_prefix
FROM broker_fills WHERE owner_chat_id=$1;
-- name: movement_fill_currency_identity
SELECT count(*) owner_id_matches,
count(*) FILTER (WHERE f.source IS DISTINCT FROM m.source) source_disagreement,
count(*) FILTER (WHERE m.currency IS DISTINCT FROM 'ARS') non_ars_movement,
count(*) FILTER (WHERE m.quantity IS DISTINCT FROM f.quantity) quantity_disagreement,
count(*) FILTER (WHERE abs(m.quantity) IS DISTINCT FROM abs(f.quantity)) absolute_quantity_disagreement,
count(*) FILTER (WHERE m.quantity IS NULL OR f.quantity IS NULL) missing_quantity,
count(*) FILTER (WHERE m.ticker IS DISTINCT FROM f.ticker) ticker_disagreement
FROM broker_fills f JOIN broker_movements m ON m.external_movement_id=f.external_fill_id WHERE f.owner_chat_id=$1;
