"""List or review learning-shadow rule candidates.

Approval means approval for a future shadow experiment only. This command can
write learning_shadow_rule_candidates and cannot alter live analysis or orders.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.db import PortfolioDatabase
from src.core.config import get_config


REVIEW_STATUSES = {"APPROVED_FOR_SHADOW", "REJECTED", "ARCHIVED"}


async def main(args: argparse.Namespace) -> int:
    db = PortfolioDatabase(get_config().database.url)
    await db.connect()
    try:
        pool = await db.get_pool()
        if pool is None:
            raise RuntimeError("database pool unavailable")
        async with pool.acquire() as conn:
            if args.id is None:
                rows = await conn.fetch(
                    """
                    SELECT id, policy_version, block_category, horizon_days,
                           candidate_type, sample_size, clean_miss_count,
                           clean_miss_rate, status, evidence_start, evidence_end,
                           reviewed_at, reviewed_by, review_note
                    FROM learning_shadow_rule_candidates
                    WHERE owner_chat_id = $1
                    ORDER BY status, block_category, id
                    """,
                    int(args.owner_chat_id),
                )
                payload = [dict(row) for row in rows]
            else:
                status = str(args.status or "").upper()
                if status not in REVIEW_STATUSES:
                    raise ValueError(
                        "--status must be APPROVED_FOR_SHADOW, REJECTED or ARCHIVED"
                    )
                row = await conn.fetchrow(
                    """
                    UPDATE learning_shadow_rule_candidates
                    SET status = $3,
                        reviewed_at = NOW(),
                        reviewed_by = $4,
                        review_note = $5,
                        updated_at = NOW()
                    WHERE id = $1 AND owner_chat_id = $2
                    RETURNING id, policy_version, block_category, candidate_type,
                              status, reviewed_at, reviewed_by, review_note,
                              proposed_rule
                    """,
                    int(args.id),
                    int(args.owner_chat_id),
                    status,
                    args.reviewed_by,
                    args.note,
                )
                if row is None:
                    raise ValueError("candidate not found for owner")
                payload = dict(row)

        print(json.dumps({
            "ok": True,
            "result": payload,
            "boundary": {
                "writes": ["learning_shadow_rule_candidates"],
                "affects_analysis": False,
                "affects_execution": False,
            },
        }, ensure_ascii=False, default=str))
        return 0
    finally:
        await db.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-chat-id", type=int, default=0)
    parser.add_argument("--id", type=int)
    parser.add_argument("--status")
    parser.add_argument("--note")
    parser.add_argument("--reviewed-by", default="manual_cli")
    args = parser.parse_args()
    if args.id is None and (args.status or args.note):
        parser.error("--status/--note require --id")
    if args.id is not None and not args.status:
        parser.error("--id requires --status")
    return args


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(parse_args())))
