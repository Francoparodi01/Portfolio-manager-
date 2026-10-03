import asyncio
import pytest
import server


class Transaction:
    async def __aenter__(self): return self
    async def __aexit__(self,*args): pass


class Connection:
    def __init__(self, *, requested_owner=True, other_owner=False):
        self.calls=[]; self.closed=False
        self.requested_owner=requested_owner; self.other_owner=other_owner
    def transaction(self,**kw):
        assert kw==dict(readonly=True,isolation='repeatable_read')
        return Transaction()
    async def fetchval(self,sql,*args):
        from datetime import datetime,timezone
        self.calls.append((sql,args))
        if 'NOW()' in sql: return datetime(2026,10,2,18,tzinfo=timezone.utc)
        if 'current_setting' in sql: return 'on'
        if 'to_regclass' in sql: return True
        if 'SELECT EXISTS' in sql and 'owner_chat_id<>$1' in sql: return self.other_owner
        if 'SELECT EXISTS' in sql and 'owner_chat_id=$1' in sql: return self.requested_owner
        raise AssertionError(sql)
    async def fetchrow(self,sql,*args):
        self.calls.append((sql,args))
        assert args[0]==123
        assert 'owner_chat_id=$1' in sql
        if 'all_owner_plans' in sql:
            return dict(all_owner_plans=0,all_bot_plans=0,window_bot_plans=0,
                        explicit_owner_plans=0,legacy_null_plans=0,
                        first_plan=None,last_plan=None)
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
    assert r['legacy_owner_inferred'] is True
    assert r['owner_scope']=='EXPLICIT_PLUS_VERIFIED_LEGACY_NULL'
    assert r['metrics']['5']['mean_pct'] is None
    assert db.closed
    assert all(sql.lstrip().startswith('SELECT') for sql,args in db.calls)
    assert not any('outcome_' in sql for sql,args in db.calls)
    assert not any('market_candles' in sql for sql,args in db.calls)  # no owner bot tickers


def test_legacy_null_scope_fails_closed_when_another_owner_exists(monkeypatch):
    db=Connection(other_owner=True)
    async def connect(*args,**kw): return db
    monkeypatch.setattr(server.asyncpg,'connect',connect)
    monkeypatch.setenv('DATABASE_URL','postgresql://test.invalid/test')
    r=asyncio.run(server.stats(123))
    assert r['legacy_owner_inferred'] is False
    assert r['owner_scope']=='EXPLICIT_ONLY'
    scoped=[args for sql,args in db.calls if '$4::boolean' in sql]
    assert scoped and all(args[3] is False for args in scoped)


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
