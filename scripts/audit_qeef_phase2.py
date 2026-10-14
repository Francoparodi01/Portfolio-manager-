"""Read-only readiness audit of the frozen v1 capture; publishes aggregates only."""
import argparse
from collections import Counter,defaultdict
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.analysis.qeef.historical import adapt_history,obj
from src.analysis.qeef.market import from_observation
from datetime import datetime
from src.decision_lab.models import digest


def audit(capture,*,capture_sha256):
    cases=adapt_history(capture);tables=capture['tables'];fills=tables['broker_fills']
    snapshots=sorted(tables['portfolio_snapshots'],key=lambda r:(r['scraped_at'],r['snapshot_id']))
    positions=defaultdict(list)
    for p in tables['positions']: positions[p['snapshot_id']].append(p)
    valuation=Counter();quantity=Counter();market=Counter();gaps=Counter()
    for s in snapshots:
        pp=positions[s['snapshot_id']]
        if any(p['currency']!='ARS' for p in pp): valuation['FX_REQUIRED']+=1;continue
        nav=sum((D(str(p['quantity']))*D(str(p['current_price'])) for p in pp),D(0))
        valuation['COMPONENTS_MATCH' if abs(nav-D(str(s['total_value_ars'])))<=D('.01') else 'COMPONENT_GAP']+=1
    for a,b in zip(snapshots,snapshots[1:]):
        if a['scraped_at']==b['scraped_at']: quantity['SAME_TIMESTAMP_NOT_ORDERED']+=1;continue
        q=defaultdict(D)
        for p in positions[a['snapshot_id']]: q[(p['ticker'],p['currency'])]+=D(str(p['quantity']))
        ff=[f for f in fills if a['scraped_at']<f['executed_at']<=b['scraped_at']]
        if any(f.get('executed_at_precision')!='exact' for f in ff): quantity['FILL_TIME_NOT_EXACT']+=1;continue
        for f in ff: q[(f['ticker'],'ARS')]+=D(str(f['quantity']))*(1 if f['side']=='BUY' else -1)
        for p in positions[b['snapshot_id']]: q[(p['ticker'],p['currency'])]-=D(str(p['quantity']))
        quantity['VISIBLE_QUANTITIES_MATCH' if all(v==0 for v in q.values()) else 'VISIBLE_QUANTITY_GAP']+=1
    for r in tables['market_candle_observations']:
        try: status=from_observation(r).assess(datetime.fromisoformat(capture['captured_at']))
        except (ValueError,KeyError): status={'status':'INVALID','reasons':['MARKET_CONTRACT_INVALID']}
        market[status['status']]+=1;gaps.update(status['reasons'])
    captures=[obj(c['payload']) for c in tables['decision_lab_plan_captures']]
    complete=[c for c in captures if c.get('capture_kind')=='FULL_CONTEXT']
    n=len(cases)
    missing=Counter(reason for c in cases for reason in c['limitations'])
    result={'schema_version':'qeef-phase2-readiness-v1','capture_sha256':capture_sha256,
        'source_as_of':capture['captured_at'],'source_transaction_read_only':capture['transaction_read_only'],
        'historical_classification_changed':False,'mode':'RESEARCH_ONLY','affects_analysis':False,'affects_execution':False,
        'promotion_allowed':False,'denominator':n,'tickers':len({c['ticker'] for c in cases}),
        'source_cases':{'decision_log':len(tables['decision_log']),'position_hold_observations':len(tables['position_hold_observations'])},
        'case_readiness':{'identifiable_frozen_thesis':sum(c['thesis'] is not None for c in cases),
            'exact_item_or_hold_plan_link':sum(c['plan_link_kind']=='EXACT_ITEM_OR_HOLD_LINK' for c in cases),
            'full_context_case_link':sum(c['evidence_quality']=='PARTIAL_CAPTURE' for c in cases),
            'complete_prospective_decision_contract':sum(bool(c.get('qeef_bundle')) for c in complete),
            'valid_frozen_economic_alternatives':0,
            'recorded_decision_fill_links':sum(bool(c['execution_ids']) for c in cases),
            'exact_formal_plan_execution_attributions':len(tables['plan_execution_attributions']),
            'cases_with_all_linked_fees_known':sum(bool(c['execution_ids']) and all(f['fees_ars'] is not None for f in fills if str(f['id']) in c['execution_ids']) for c in cases),
            'certified_reconciled_account_path':0,'verified_economic_cases':0},
        'plans':{'denominator':len(tables['execution_plans']),'captures':len(captures),'full_context':len(complete)},
        'fills':{'denominator':len(fills),'fees_known':sum(f['fees_ars'] is not None for f in fills),
            'fees_unknown':sum(f['fees_ars'] is None for f in fills),
            'duplicate_source_external_ids':len(fills)-len({(f['source'],f['external_fill_id']) for f in fills})},
        'snapshots':{'denominator':len(snapshots),'positions':len(tables['positions']),'valuation_probe':dict(valuation),
            'adjacent_pairs_denominator':max(len(snapshots)-1,0),'quantity_probe':dict(quantity),
            'certified_net_periods':0,'coverage_status':'UNKNOWN_COSTS_FLOWS_ACTIONS_FINANCING',
            'interpretation':'Arithmetic matches do not prove complete ledger or NAV reconciliation.'},
        'market':{'denominator':len(tables['market_candle_observations']),'contract_statuses':dict(market),'gaps':dict(gaps)},
        'case_missingness':dict(missing),'broker_movements_scope':'OWNER_LINKED_FILL_ROWS_ONLY_NOT_COMPLETE_OWNER_LEDGER',
        'legacy_episodes':sum(o['kind'].upper()=='EPISODE' for o in tables['decision_lab_objects']),
        'economic_pnl_net':None,'alpha_claim':None,
        'remaining_contracts':['EXPLICIT_PROSPECTIVE_THESIS_AND_BUNDLE','PREDECLARED_COSTS_AND_ALTERNATIVES',
            'BROKER_ORDER_EXECUTION_IDENTITY','OBSERVED_FEES_TAXES_OTHER_COSTS','OWNER_SCOPED_COMPLETE_CASH_LEDGER',
            'CORPORATE_ACTION_AND_FINANCING_COVERAGE','PIT_CALENDAR_RATIO_VOLUME_UNITS_AND_EXECUTABLE_PRICES']}
    result['audit_id']=digest(result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();path=Path(args.capture);raw=path.read_bytes();before=hashlib.sha256(raw).hexdigest()
    result=audit(json.loads(raw),capture_sha256=before)
    assert before==hashlib.sha256(path.read_bytes()).hexdigest()
    output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True)
    text=json.dumps(result,indent=2,sort_keys=True)+'\n'
    if output.exists() and output.read_text(encoding='utf-8')!=text: raise FileExistsError('immutable audit conflict')
    output.write_text(text,encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__': main()
