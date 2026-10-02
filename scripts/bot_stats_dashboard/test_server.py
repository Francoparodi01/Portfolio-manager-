import asyncio
import pytest
import server


class Transaction:
    async def __aenter__(self): return self
    async def __aexit__(self,*args): pass


class Connection:
    def __init__(self): self.calls=[]; self.closed=False
    def transaction(self,**kw):
        assert kw==dict(readonly=True,isolation='repeatable_read')
        return Transaction()
    async def fetchval(self,sql,*args):
        from datetime import datetime,timezone
        self.calls.append((sql,args))
        if 'NOW()' in sql: return datetime(2026,10,2,18,tzinfo=timezone.utc)
        if 'current_setting' in sql: return 'on'
        if 'to_regclass' in sql: return True
        raise AssertionError(sql)
    async def fetchrow(self,sql,*args):
        self.calls.append((sql,args))
        assert args[0]==123
        assert 'owner_chat_id=$1' in sql
        if 'all_owner_plans' in sql:
            return dict(all_owner_plans=0,all_bot_plans=0,window_bot_plans=0,first_plan=None,last_plan=None)
        if 'eligible_intents' in sql: return dict(raw_intents=0,eligible_intents=0,unique_signals=0)
        if 'snapshots' in sql: return dict(snapshots=0,raw_fills=0)
        raise AssertionError(sql)
    async def fetch(self,sql,*args):
        self.calls.append((sql,args))
        assert args[0]==123
        assert 'owner_chat_id=$1' in sql
        return []
    async def close(self): self.closed=True


def test_readonly_owner_scope_and_empty_reconciliation(monkeypatch):
    db=Connection()
    async def connect(dsn,**kw):
        assert kw['server_settings']['default_transaction_read_only']=='on'
        assert kw['server_settings']['statement_timeout']=='15000'
        return db
    monkeypatch.setattr(server.asyncpg,'connect',connect)
    monkeypatch.setenv('DATABASE_URL','postgresql://test.invalid/test')
    r=asyncio.run(server.stats(123))
    assert r['counts_reconciled'] is True
    assert r['metrics']['5']['mean_pct'] is None
    assert db.closed
    assert all(sql.lstrip().startswith('SELECT') for sql,args in db.calls)
    assert not any('outcome_' in sql for sql,args in db.calls)
    assert not any('market_candles' in sql for sql,args in db.calls)  # no owner bot tickers


def test_owner_is_required_before_connection():
    with pytest.raises(ValueError): asyncio.run(server.load(None,180))


def test_connection_closed_when_query_fails(monkeypatch):
    db=Connection()
    async def fail(*args): raise RuntimeError('test query failure')
    db.fetchrow=fail
    async def connect(*args,**kw): return db
    monkeypatch.setattr(server.asyncpg,'connect',connect)
    monkeypatch.setenv('DATABASE_URL','postgresql://test.invalid/test')
    with pytest.raises(RuntimeError): asyncio.run(server.load(123,180))
    assert db.closed
