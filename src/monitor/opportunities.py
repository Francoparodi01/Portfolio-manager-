"""Shared read-only Top 10: recorded Radar order, no second score calculation."""
from __future__ import annotations
from datetime import datetime, timezone
import json
import math
from typing import Any

STALE_AFTER_SECONDS = 7 * 24 * 60 * 60  # Existing PR25 policy.


def _empty(status):
    return dict(status=status, source='radar_discovery_snapshots', research_only=True,
                ranking_basis='frozen_production_rank', captured_at=None,
                scoring_version=None, stale=None, count=0, items=[])


def _object(value):
    if isinstance(value, str):
        try: value = json.loads(value)
        except (ValueError, TypeError): return {}
    return value if isinstance(value, dict) else {}


def _number(value):
    if value is None or isinstance(value, bool): return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError): return None


def _timestamp(value):
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return stamp.astimezone(timezone.utc) if stamp.tzinfo else None
    except (ValueError, TypeError): return None


def present_top_opportunities(run: dict, candidates: list[dict], *, as_of: datetime) -> dict:
    """One presentation for API, Telegram and frozen evidence.

    Quality may withhold an actionable label, never change the saved order,
    score or status. Do not fill legacy fields using a newer or shadow score.
    """
    now, captured = _timestamp(as_of), _timestamp(run.get('captured_at'))
    if now is None or captured is None or captured > now: return _empty('INVALID_CAPTURE')
    age = (now-captured).total_seconds()
    stale = age > STALE_AFTER_SECONDS
    ranks = [r.get('rank_position') for r in candidates]
    tickers = [r.get('ticker') for r in candidates]
    if (any(not isinstance(n,int) or isinstance(n,bool) or n<1 for n in ranks)
            or len(set(ranks))!=len(ranks) or len(set(tickers))!=len(tickers)):
        return _empty('INVALID_CAPTURE')
    items = []
    for row in sorted(candidates,key=lambda r:(r['rank_position'],r['ticker']))[:10]:
        item = dict(row)
        metadata = _object(item.pop('metadata',{}))
        evidence = _object(metadata.get('top10_evidence'))
        setup = _object(item.pop('setup_features',{}))
        quality = _object(evidence.get('data_quality'))
        reference = _timestamp(item.get('reference_ts'))
        price = _number(item.get('reference_price'))
        currency = evidence.get('currency') or None
        tech_score = _number(evidence.get('technical_score'))
        volume = _number(evidence.get('volume_ratio'))
        definition = evidence.get('volume_definition')
        if volume is None:
            volume = _number(setup.get('volume_ratio_latest_20'))
            definition = 'setup_features.volume_ratio_latest_20' if volume is not None else None
        price_age = (now-reference).total_seconds() if reference else None
        missing = []
        if price is None or price <= 0:
            price = None; missing.append('precio no verificable')
        if not currency or currency == 'UNKNOWN':
            currency = None; missing.append('moneda no verificada')
        if reference is None or reference > captured: missing.append('fecha del precio no verificable')
        if stale or (price_age is not None and price_age > STALE_AFTER_SECONDS): missing.append('datos vencidos')
        if tech_score is None: missing.append('score técnico no conservado')
        if volume is None or volume < 0:
            volume = None; missing.append('volumen no verificable')
        if item.get('price_quality_flag') != 'CANONICAL_COCOS': missing.append('precio provisional o fuente no canónica')
        if any(quality.get(k)!='VALID' for k in ('price_status','volume_status','provenance_status')):
            missing.append('evidencia incompleta')
        raw_status = item.get('radar_status')
        state = ('COMPRAR' if raw_status=='COMPRABLE_AHORA' else
                 'VIGILAR' if raw_status in {'COMPRA_HABILITADA','SWAP_CANDIDATO',
                                             'VIGILANCIA_A','VIGILANCIA_B','VIGILANCIA_C'} else 'ESPERAR')
        if missing: state = 'ESPERAR'
        risk = (metadata.get('manual_event_risk') or metadata.get('why_not_now')
                or evidence.get('principal_risk') or '; '.join(missing) or 'Riesgo no informado')
        item.update(reference_price=price, reference_ts=reference.isoformat() if reference else None,
                    currency=currency, technical_score=tech_score, volume_ratio=volume,
                    volume_definition=definition, display_state=state,
                    selection_reason=evidence.get('selection_reason') or item.get('acceptance_reason') or 'Motivo no conservado',
                    principal_risk=str(risk), missing_data=missing, quality='PARTIAL' if missing else 'VERIFIED',
                    price_age_seconds=price_age, price_basis='LAST_RECORDED_NOT_LIVE_QUOTE',
                    data_source_counts=_object(item.get('data_source_counts')),
                    why_not_now=metadata.get('why_not_now'), manual_event_risk=metadata.get('manual_event_risk'))
        items.append(item)
    return dict(status='READY', source='radar_discovery_snapshots', research_only=True,
                ranking_basis='frozen_production_rank', captured_at=captured.isoformat(),
                evaluated_at=now.isoformat(), run_id=str(run['run_id']), scoring_version=str(run['scoring_version']),
                captured_session=str(run['captured_session']), universe_count=int(run['universe_count']),
                eligible_count=int(run['eligible_count']), capture_age_seconds=age, stale=stale,
                count=len(items), items=items)


async def fetch_top_opportunities(conn: Any, *, owner_chat_id: int) -> dict:
    """SELECT only. Call inside a read-only repeatable-read transaction."""
    if not owner_chat_id or isinstance(owner_chat_id,bool): return _empty('OWNER_NOT_CONFIGURED')
    schema_ready = await conn.fetchval("""SELECT to_regclass('public.radar_discovery_runs') IS NOT NULL
               AND to_regclass('public.radar_discovery_snapshots') IS NOT NULL""")
    if not schema_ready: return _empty('SCHEMA_MISSING')
    run = await conn.fetchrow("""SELECT run_id, captured_at, captured_session, scoring_version,
                  universe_count, eligible_count, NOW() AS evaluated_at
           FROM radar_discovery_runs
           WHERE owner_chat_id = $1 AND status = 'COMPLETE' AND captured_at <= NOW()
           ORDER BY captured_at DESC, run_id DESC LIMIT 1""", owner_chat_id)
    if run is None: return _empty('NO_CAPTURE')
    candidates = await conn.fetch("""SELECT ticker, asset_type, reference_ts, reference_price,
                  price_quality_flag, feature_quality_flag, data_source_mode, data_source_counts,
                  radar_status, readiness_state, technical_regime, v3_classification,
                  trigger_price, invalidation_price, target_price, setup_risk_reward,
                  ranking_score, rank_position, trend_component_score,
                  relative_strength_component_score, compression_component_score,
                  setup_component_score, acceptance_reason, metadata, setup_features
           FROM radar_discovery_snapshots
           WHERE run_id = $1 AND radar_eligible = TRUE AND screener_passed = TRUE
             AND in_portfolio = FALSE AND rank_position IS NOT NULL
             AND ticker NOT IN ('SPY', 'QQQ')
           ORDER BY rank_position ASC, ticker ASC LIMIT 10""", run['run_id'])
    return present_top_opportunities(dict(run),[dict(r) for r in candidates],
                                     as_of=run.get('evaluated_at') or datetime.now(timezone.utc))
