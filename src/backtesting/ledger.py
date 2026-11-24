"""Sequential fictional ledger, causal recorded quotes and explicit modeled costs."""
from decimal import Decimal, ROUND_FLOOR
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bisect import bisect_left,bisect_right
D=lambda v: Decimal(str(v))
ART=ZoneInfo('America/Argentina/Buenos_Aires')
FEE=D('0.006');SLIP=D('0.0015')
def dt(v):return v if isinstance(v,datetime) else datetime.fromisoformat(str(v).replace('Z','+00:00'))
class Ledger:
 def __init__(self,rows,tickers,initial_cash=3000000,fee_rate="0.006",slippage_rate="0.0015",latency_seconds=60):
  self.initial_cash=D(initial_cash);self.fee=D(fee_rate);self.slip=D(slippage_rate);self.latency_seconds=int(latency_seconds)
  self.cash=self.initial_cash;self.qty={t:0 for t in tickers};self.basis={t:D(0) for t in tickers};self.realized={t:D(0) for t in tickers};self.buy_cost={t:D(0) for t in tickers}
  self.rows=rows;self.fills=[];self.orders=[];self.marks=[];self.adjustments=[];self.applied=set()
  self.points={t:[] for t in tickers};self.bars={t:[] for t in tickers};self.identity={};self.ambiguous=set()
  for r in rows['intraday_points']:
   if r['ticker'] not in self.points:continue
   assert r['currency']=='ARS' and D(r['last_price'])>0
   t=r['ticker'];ident=(r['asset_type'],r['currency'])
   if t in self.identity and self.identity[t]!=ident:self.ambiguous.add(t)
   self.identity[t]=ident;self.points[t].append(dict(r,_ts=dt(r['ts'])))
  for r in rows['daily_candles']:
   if r['ticker'] in self.bars and r['currency']=='ARS' and r['close_price'] and D(r['close_price'])>0:
    self.bars[r['ticker']].append(dict(r,_ts=dt(r['ts']),_scraped=dt(r['scraped_at']) if r.get('scraped_at') else None))
  self.times={}
  for t in tickers:
   self.points[t].sort(key=lambda r:r['_ts']);self.times[t]=[r['_ts'] for r in self.points[t]]
 def apply_corporate(self,asof):
  for r in self.rows.get('corporate_effects',[]):
   key=r['id'];t=r['ticker']
   if key in self.applied or dt(r['effective_at'])>asof:continue
   if t not in self.qty or self.identity.get(t)!=(r['asset_type'],r['currency']):continue
   if not r['is_active']:continue
   known=max(dt(r[k]) for k in ['event_created_at','effect_created_at','event_updated_at','effect_updated_at'])
   if known>asof:continue
   self.applied.add(key)
   if not self.qty[t]:continue
   new=D(self.qty[t])*D(r['quantity_factor']);assert new==int(new)
   self.adjustments.append(dict(asof=asof.isoformat(),ticker=t,quantity_before=self.qty[t],quantity_after=int(new),effect_id=key,cost_basis_total_unchanged=float(self.basis[t])))
   self.qty[t]=int(new)
 def mark(self,t,asof,eod=False):
  day=asof.astimezone(ART).date();idx=bisect_right(self.times[t],asof)-1
  if idx>=0:
   r=self.points[t][idx]
   if r['_ts'].astimezone(ART).date()==day:return D(r['last_price']),dict(basis='RECORDED_INTRADAY_LAST',ts=r['ts'],age_minutes=(asof-r['_ts']).total_seconds()/60)
  eligible=[r for r in self.bars[t] if r['_ts'].date()<day and r['_scraped'] and r['_scraped']<=asof and self.identity.get(t)==(r['asset_type'],r['currency'])]
  if eligible:
   priority={'COCOS':0,'TRADINGVIEW_BYMA':1,'internal_snapshot':2}
   r=min(eligible,key=lambda r:(-r['_ts'].date().toordinal(),priority.get(r['source'],3),-r['_scraped'].timestamp(),-r['_ts'].timestamp()))
   return D(r['close_price']),dict(basis='PREVIOUS_DAILY_CLOSE',ts=r['ts'],source=r['source'])
  if idx>=0:
   r=self.points[t][idx];return D(r['last_price']),dict(basis='STALE_RECORDED_QUOTE',ts=r['ts'],age_minutes=(asof-r['_ts']).total_seconds()/60)
  raise ValueError('Missing mark '+t)
 def snapshot(self,asof):
  self.apply_corporate(asof);positions=[];nav=self.cash
  for t,q in self.qty.items():
   if not q:continue
   price,meta=self.mark(t,asof);fresh=meta['basis']=='RECORDED_INTRADAY_LAST';value=q*price;nav+=value
   typ,currency=self.identity[t]
   positions.append(dict(ticker=t,quantity=q,current_price=float(price),market_value=float(value),average_price=float(self.basis[t]/q),asset_type=typ,currency=currency,venue='BYMA',market_data_status='FRESH' if fresh else 'STALE',is_operable=fresh,market_price_ts=dt(meta['ts']),price_lineage=meta))
  return positions,float(nav),float(self.cash)
 def execute(self,plan,cutoff,end):
  start=len(self.fills)
  if not plan or not plan.get('feasible'):return []
  events=[]
  for side in ['sell','buy']:
   for index,o in enumerate(plan.get(side+'_orders') or []):
    t=o['ticker'];assert t in self.qty;q=int(D(o['quantity_est']).to_integral_value(rounding=ROUND_FLOOR));assert abs(float(o['quantity_est'])-q)<1e-6
    at=bisect_left(self.times[t],cutoff+timedelta(seconds=self.latency_seconds));quote=self.points[t][at] if at<len(self.points[t]) else None
    if quote and not (quote['_ts']<end and quote['_ts'].astimezone(ART).date()==cutoff.astimezone(ART).date()):quote=None
    order=dict(cutoff=cutoff.isoformat(),expires_at=end.isoformat(),ticker=t,side=side.upper(),planned_qty=q,reference_price=o.get('reference_price'),reason=o.get('reason'),status='NO_QUOTE_BEFORE_NEXT_ANALYSIS',filled_qty=0)
    self.orders.append(order)
    if t in self.ambiguous:order['status']='AMBIGUOUS_INSTRUMENT_IDENTITY';quote=None
    if quote:events.append((quote['_ts'],0 if side=='sell' else 1,index,order,quote))
  for ts,_,index,o,r in sorted(events,key=lambda e:e[:3]):
   t=o['ticker'];buy=o['side']=='BUY';price=D(r['last_price'])*(1+self.slip if buy else 1-self.slip);q=o['planned_qty']
   if buy:q=min(q,int((self.cash/(price*(1+self.fee))).to_integral_value(rounding=ROUND_FLOOR)))
   else:q=min(q,self.qty[t])
   if q<=0:o['status']='NO_CASH_OR_POSITION_AT_FILL';continue
   gross=q*price;fee=gross*self.fee;net=gross+fee if buy else gross-fee;pnl=None
   if buy:self.cash-=net;self.qty[t]+=q;self.basis[t]+=net;self.buy_cost[t]+=net
   else:
    cost=self.basis[t]*q/self.qty[t];pnl=net-cost;self.cash+=net;self.qty[t]-=q;self.basis[t]-=cost;self.realized[t]+=pnl
   assert self.cash>=0 and all(x>=0 for x in self.qty.values())
   o.update(status='FILLED' if q==o['planned_qty'] else 'PARTIAL',filled_qty=q,filled_at=ts.isoformat())
   self.fills.append(dict(cutoff=o['cutoff'],filled_at=ts.isoformat(),ticker=t,side=o['side'],quantity=q,quote_price=float(r['last_price']),fill_price=float(price),gross_ars=float(gross),fee_ars=float(fee),slippage_ars=float(q*abs(price-D(r['last_price']))),cash_after=float(self.cash),position_after=self.qty[t],realized_pnl=None if pnl is None else float(pnl),reason=o['reason'],price_source='CAPTURED_MARKET_PRICES_LAST',asset_type=self.identity[t][0],currency='ARS'))
  return self.fills[start:]
 def valuation(self,asof,eod=False):
  self.apply_corporate(asof)
  asset=[]
  for t,q in self.qty.items():
   if q==0 and self.basis[t]==0 and self.realized[t]==0 and self.buy_cost[t]==0:continue
   price,meta=self.mark(t,asof,eod) if q else (D(0),dict(basis='CLOSED_POSITION_NO_MARK_REQUIRED',ts=None))
   asset.append(dict(ticker=t,quantity=q,price=float(price) if q else None,value_ars=float(q*price),cost_basis=float(self.basis[t]),buy_cost_total=float(self.buy_cost[t]),realized_pnl=float(self.realized[t]),unrealized_pnl=float(q*price-self.basis[t]),total_pnl=float(self.realized[t]+q*price-self.basis[t]),mark=meta))
  nav=float(self.cash)+sum(a['value_ars'] for a in asset);assert abs(nav-float(self.initial_cash)-sum(a['total_pnl'] for a in asset))<0.01
  return dict(asof=asof.isoformat(),cash_ars=float(self.cash),nav_ars=nav,return_pct=(nav/float(self.initial_cash)-1)*100,assets=asset)
