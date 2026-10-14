"""Versioned market facts; missing ratio/calendar/volume never acquire defaults."""
from datetime import datetime
from decimal import Decimal
from pydantic import AwareDatetime, Field, model_validator
from src.decision_lab.models import Frozen, digest


class MarketFact(Frozen):
    instrument_id: str
    ticker: str
    exchange: str = 'UNKNOWN'
    currency: str = 'UNKNOWN'
    price_unit: str = 'UNKNOWN'
    volume_unit: str = 'UNKNOWN'
    calendar_version: str = 'UNKNOWN'
    session_id: str = 'UNKNOWN'
    observed_at: AwareDatetime
    available_at: AwareDatetime
    source_timestamp: AwareDatetime
    revision_at: AwareDatetime | None = None
    corporate_action_version: str = 'UNKNOWN'
    ratio_version: str = 'UNKNOWN'
    depositary_ratio: Decimal | None = Field(default=None,gt=0)
    asset_type: str = 'UNKNOWN'
    source_id: str
    source_reference: str
    price_basis: str = 'UNKNOWN'
    is_closed: bool | None = None
    close: Decimal = Field(gt=0)

    @model_validator(mode='after')
    def valid(self):
        if not self.instrument_id or not self.source_reference:
            raise ValueError('instrument and source identity required')
        if self.available_at < max(self.observed_at,self.source_timestamp) or (self.revision_at and self.revision_at < self.available_at):
            raise ValueError('invalid market availability chronology')
        return self

    def assess(self, decision_at):
        if max(self.available_at,self.revision_at or self.available_at)>decision_at:
            return {'status':'INVALID','reasons':['FUTURE_MARKET_VERSION']}
        fields=('exchange','currency','price_unit','volume_unit','calendar_version','session_id','corporate_action_version','price_basis','asset_type')
        missing=[k.upper()+'_UNKNOWN' for k in fields if getattr(self,k)=='UNKNOWN']
        if self.asset_type=='CEDEAR' and (self.ratio_version=='UNKNOWN' or self.depositary_ratio is None):
            missing.append('CEDEAR_RATIO_UNKNOWN')
        if self.is_closed is not True:
            missing.append('BAR_NOT_CONFIRMED_CLOSED')
        return {'status':'PARTIAL' if missing else 'VERIFIED','reasons':missing,'evidence_digest':digest(self)}


def from_observation(row):
    def obj(value):
        import json
        return json.loads(value) if isinstance(value,str) else value or {}
    provenance=obj(row.get('provenance'))
    return MarketFact(instrument_id=row['instrument_id'],ticker=row['ticker'],exchange=row.get('market') or 'UNKNOWN',
        currency=row.get('currency') or 'UNKNOWN',price_unit=row.get('price_unit') or 'UNKNOWN',
        volume_unit=row.get('volume_unit') or 'UNKNOWN',calendar_version=provenance.get('calendar_version') or 'UNKNOWN',
        session_id=provenance.get('session_id') or 'UNKNOWN',observed_at=row['scraped_at'],
        available_at=row['available_at'],source_timestamp=row['candle_timestamp'],
        revision_at=provenance.get('revision_at'),corporate_action_version=provenance.get('corporate_action_version') or 'UNKNOWN',
        ratio_version=provenance.get('ratio_version') or 'UNKNOWN',depositary_ratio=provenance.get('depositary_ratio'),
        source_id=row['source'],source_reference=str(row['observation_id']),price_basis=provenance.get('price_mode') or 'UNKNOWN',
        is_closed=row.get('is_closed'),close=row['close_price'],asset_type=row.get('asset_type') or 'UNKNOWN')


def history_window(facts, *, instrument_id, decision_at, required_sessions):
    if required_sessions<1:
        raise ValueError('positive history length required')
    rows=[f for f in facts if f.instrument_id==instrument_id and max(f.available_at,f.revision_at or f.available_at)<=decision_at]
    versions={}
    for f in sorted(rows,key=lambda r:(r.revision_at or r.available_at,r.source_reference)):
        versions[(f.source_id,f.calendar_version,f.session_id)]=f
    known=[f for f in versions.values() if f.session_id!='UNKNOWN']
    sources={(f.source_id,f.calendar_version) for f in known}
    enough=len(known)>=required_sessions and len(sources)==1
    return {'status':'VERIFIED' if enough and all(f.assess(decision_at)['status']=='VERIFIED' for f in known) else 'INSUFFICIENT_DATA',
            'n_available_sessions':len(known),'n_required':required_sessions,
            'reasons':sorted({r for f in known for r in f.assess(decision_at)['reasons']}) if enough else ['SHORT_OR_INCOMPATIBLE_HISTORY'],
            'records':[f.model_dump(mode='json') for f in known]}
