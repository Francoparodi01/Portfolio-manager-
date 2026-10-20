"""Isolated DB/API/Telegram contract check. Never polls or changes a webhook.

--send-telegram sends the real /top10 handler output to the configured owner.
The incoming command is injected locally and explicitly reported as such.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import asyncpg
from src.analysis.radar_discovery import RadarDiscoveryStore, RadarDiscoveryObservation, RADAR_DISCOVERY_SCHEMA_SQL
from src.core.top10_telegram import render_top10


def require_lab():
    dsn = os.environ["DATABASE_URL"]
    parsed = urlsplit(dsn)
    if parsed.hostname != "quantia-radar-daily-db" or parsed.path != "/radar_validation":
        raise ValueError("This script can only use the isolated validation database")
    return dsn, int(os.environ["TELEGRAM_CHAT_ID"])


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--seed", action="store_true")
    parser.add_argument("--send-telegram", action="store_true")
    args = parser.parse_args()
    dsn, owner = require_lab()
    if owner <= 0:
        raise ValueError("Positive configured owner required")
    run = json.loads((args.replay/"run.json").read_text(encoding="utf8"))
    if args.seed:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.execute(RADAR_DISCOVERY_SCHEMA_SQL)  # lab DB only
            data = json.loads((args.replay/"observations-private.json").read_text(encoding="utf8"))
            for row in data:
                if row.get("reference_ts"):
                    row["reference_ts"] = datetime.fromisoformat(row["reference_ts"])
            observations = [RadarDiscoveryObservation(**row) for row in data]
            kwargs = dict(run_id=run["run_id"], owner_chat_id=owner,
                captured_at=datetime.fromisoformat(run["captured_at"]), scoring_version=run["scoring_version"],
                observations=observations, parameters=run["parameters"], initialize_schema=False,
                evidence_session=datetime.fromisoformat(run["captured_session"]).date())
            store = RadarDiscoveryStore(pool)
            first = await store.save_snapshot(**kwargs)
            second = await store.save_snapshot(**kwargs)
            if not second["duplicate"]:
                raise AssertionError("Replay publication was not idempotent")
        finally:
            await pool.close()
        result = dict(status="SEEDED_LAB_ONLY", run_id=run["run_id"], first=first, second=second)
    else:
        import aiohttp
        async with aiohttp.ClientSession() as client:
            async with client.get("http://quantia-radar-daily-api:8010/api/opportunities/top10",
                headers={"X-API-Token": os.environ["MONITOR_API_TOKEN"]}) as response:
                if response.status != 200:
                    raise AssertionError("Lab API did not return HTTP 200")
                payload = await response.json()
        assert payload["run_id"] == run["run_id"]
        expected = render_top10(payload)
        result = dict(status="API_VERIFIED", run_id=payload["run_id"],
            input_sha256=payload["input_sha256"], tickers=[r["ticker"] for r in payload["items"]],
            incoming_command="LOCALLY_INJECTED_NOT_RECEIVED_FROM_TELEGRAM", messages=[])
        if args.send_telegram:
            from telegram import Update
            from telegram.ext import Application, ExtBot
            from telegram.request import HTTPXRequest
            from scripts.telegram_bot import build_app, top10_handler
            logging.getLogger("httpx").setLevel(logging.WARNING)
            sent = []

            class CheckedTransport(HTTPXRequest):
                async def do_request(self, url, method, request_data=None, **kwargs):
                    action = url.rsplit("/", 1)[-1]
                    if action not in {"getMe", "sendMessage"}:
                        raise AssertionError("Unexpected Telegram API action")
                    params = request_data.parameters if request_data else {}
                    if action == "sendMessage" and int(params["chat_id"]) != owner:
                        raise AssertionError("Wrong Telegram recipient")
                    status, raw = await super().do_request(url, method, request_data=request_data, **kwargs)
                    if action == "sendMessage":
                        data = json.loads(raw)
                        if status != 200 or not data.get("ok"):
                            raise AssertionError("Telegram delivery failed")
                        sent.append(dict(text=params["text"], message_id=data["result"]["message_id"]))
                    return status, raw

            registered = build_app()
            handler = next(h for group in registered.handlers.values() for h in group
                           if getattr(h, "callback", None) is top10_handler)
            bot = ExtBot(os.environ["TELEGRAM_BOT_TOKEN"], request=CheckedTransport())
            app = Application.builder().bot(bot).updater(None).build()
            app.add_handler(handler)
            await app.initialize()  # no post_init, menus, heartbeat or polling
            try:
                await bot.send_message(owner, "Prueba aislada del PR #31: /top10 con datos reales. "
                    f"Captura {run['captured_at']}, rueda {run['captured_session']}. "
                    "Sin despliegue ni operaciones.")
                sent.clear()
                update = Update.de_json(dict(update_id=1, message=dict(message_id=1,
                    date=int(datetime.now(timezone.utc).timestamp()),
                    chat=dict(id=owner, type="private"), **{"from": dict(id=owner, is_bot=False, first_name="Owner")},
                    text="/top10", entities=[dict(type="bot_command", offset=0, length=6)])), bot)
                await app.process_update(update)
                # Price ages may cross a display minute; compare after stripping that display-only text.
                import re
                normalize = lambda text: re.sub(r"antigüedad de vela \d+\.\d+ h", "antigüedad de vela <clock>", text)
                if [normalize(s["text"]) for s in sent] != [normalize(s) for s in expected]:
                    raise AssertionError("Telegram and API cards differ")
                result.update(status="REAL_TELEGRAM_TRANSPORT_VERIFIED", messages=[
                    dict(message_id=s["message_id"], sha256=hashlib.sha256(s["text"].encode()).hexdigest()) for s in sent])
            finally:
                await app.shutdown()
    with args.receipt.open("x", encoding="utf8") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps({k:v for k,v in result.items() if k != "messages"}))


if __name__ == "__main__":
    asyncio.run(main())
