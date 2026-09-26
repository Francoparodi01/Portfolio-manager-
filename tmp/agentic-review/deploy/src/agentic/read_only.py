"""Per-connection protection, including servers overriding startup parameters."""
import asyncpg


async def protect_connection(conn):
    await conn.execute("SET default_transaction_read_only = on")
    await conn.execute("SET statement_timeout = '60s'")
    if await conn.fetchval("SHOW transaction_read_only") != "on":
        raise RuntimeError("database read-only guard could not be established")


async def connect_read_only(dsn, **kwargs):
    conn = await asyncpg.connect(dsn, **kwargs)
    try:
        await protect_connection(conn)
    except BaseException:
        await conn.close()
        raise
    return conn


def guarded_pool(dsn, **kwargs):
    return asyncpg.create_pool(dsn, init=protect_connection, setup=protect_connection, **kwargs)


def install_process_guards():
    """Install only inside the isolated evidence subprocess, never the auditor."""
    original_connect, original_pool = asyncpg.connect, asyncpg.create_pool

    async def connect(*args, **kwargs):
        conn = await original_connect(*args, **kwargs)
        try:
            await protect_connection(conn)
        except BaseException:
            await conn.close()
            raise
        return conn

    def pool(*args, **kwargs):
        original_init, original_setup = kwargs.pop("init", None), kwargs.pop("setup", None)

        async def initialize(conn):
            await protect_connection(conn)
            if original_init:
                await original_init(conn)

        async def setup(conn):
            await protect_connection(conn)
            if original_setup:
                await original_setup(conn)
            await protect_connection(conn)

        return original_pool(*args, init=initialize, setup=setup, **kwargs)

    asyncpg.connect, asyncpg.create_pool = connect, pool
