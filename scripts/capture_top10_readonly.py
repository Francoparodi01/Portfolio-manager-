"""Read the same Top 10 used by the API and bot. Never refresh or persist a run."""
from __future__ import annotations
import argparse
import asyncio
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import dotenv_values
import asyncpg
from src.monitor.opportunities import fetch_top_opportunities
from src.core.top10_telegram import render_top10


async def capture(args):
    config = dotenv_values(args.env_file)
    owner = int(config.get("TELEGRAM_CHAT_ID") or 0)
    if not owner:
        raise ValueError("Owner missing")
    dsn = config["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://", 1)
    if args.host:
        parts = urlsplit(dsn)
        credentials = parts.netloc.rsplit("@", 1)[0]
        dsn = urlunsplit((parts.scheme, credentials + "@" + args.host + ":" + str(args.port),
                          parts.path, parts.query, parts.fragment))
    args.output.mkdir(parents=True, exist_ok=False)
    conn = await asyncpg.connect(dsn, timeout=5, command_timeout=15,
                                 server_settings={"default_transaction_read_only": "on"})
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            readonly = await conn.fetchval("SELECT current_setting('transaction_read_only')")
            if readonly != "on":
                raise ValueError("Read-only session required")
            payload = await fetch_top_opportunities(conn, owner_chat_id=owner)
    finally:
        await conn.close()
    with (args.output / "top10-private.json").open("x", encoding="utf8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)
    with (args.output / "telegram-preview.html").open("x", encoding="utf8") as f:
        f.write("\n\n".join(render_top10(payload)))
    print(json.dumps({"status": payload["status"], "count": payload["count"],
                      "captured_at": payload["captured_at"], "readonly": readonly}))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--env-file", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--host", help="Explicit verified local port-forward host, if needed")
    p.add_argument("--port", type=int, default=5432)
    asyncio.run(capture(p.parse_args()))
