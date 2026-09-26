"""Stable episode anchors; repeated recommendations remain inspectable."""
from .models import digest


def unique_rows(rows, key):
    result = {}
    for row in rows:
        identity = key(row)
        if identity in result and result[identity] != row:
            raise ValueError(f"conflicting evidence identity: {identity}")
        result[identity] = row
    return list(result.values())


def build_episodes(recommendations, version):
    rows = unique_rows(recommendations, lambda r: r.recommendation_id)
    rows.sort(key=lambda r: (r.decision_as_of, r.recommendation_id))
    active, episodes, links = {}, [], []
    for rec in rows:
        partition = (rec.account_id, rec.source_module, rec.instrument_id, rec.original_instrument_id)
        prior = active.get(partition)
        reason = "REPEATED_DIRECTION"
        if prior is None:
            reason = "FIRST_RECOMMENDATION"
        elif rec.direction != prior["direction"]:
            reason = "DIRECTION_CHANGE"
        elif rec.explicit_close_or_reopen:
            reason = "EXPLICIT_CLOSE_REOPEN"
        elif prior["expiry_at"] and rec.decision_as_of >= prior["expiry_at"]:
            reason = "EXPIRED"
        elif rec.direction in {"NEUTRAL", "ABSTAIN"}:
            reason = "NON_DIRECTIONAL"
        if reason != "REPEATED_DIRECTION":
            if prior:
                prior["episode_end"] = min(rec.decision_as_of, prior["expiry_at"] or rec.decision_as_of)
            prior = {"decision_episode_id": digest([version, partition, rec.recommendation_id]),
                     "anchor_id": rec.recommendation_id, "account_id": rec.account_id,
                     "source_module": rec.source_module, "cohort": rec.cohort,
                     "instrument_id": rec.instrument_id, "ticker": rec.ticker,
                     "direction": rec.direction, "episode_start": rec.decision_as_of,
                     "episode_end": None, "expiry_at": rec.expiry_at,
                     "recommendation_count": 0, "dedup_policy_version": version,
                     "source_hash": digest(rec)}
            episodes.append(prior)
            active[partition] = prior
        prior["recommendation_count"] += 1
        links.append({"episode_id": prior["decision_episode_id"], "recommendation_id": rec.recommendation_id,
                      "link_reason": reason, "source_row_hash": digest(rec)})
    return episodes, links
