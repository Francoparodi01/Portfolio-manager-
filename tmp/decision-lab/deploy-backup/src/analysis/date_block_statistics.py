"""Date-block inference, preserving date cross-sections and paired rows."""

import numpy as np


def ranks(values):
    _, inverse, counts = np.unique(values, return_inverse=True, return_counts=True)
    return (np.cumsum(counts) - (counts - 1) / 2)[inverse]


def spearman(x, y):
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 3:
        return None
    x, y = ranks(x[mask]), ranks(y[mask])
    x, y = x - x.mean(), y - y.mean()
    denom = np.linalg.norm(x) * np.linalg.norm(y)
    return float(np.dot(x, y) / denom) if denom > 0 else None


def date_draws(date_count, block_length, resamples, seed):
    rng = np.random.default_rng(seed)
    for _ in range(resamples):
        starts = rng.integers(
            0, date_count, size=(date_count + block_length - 1) // block_length
        )
        yield ((starts[:, None] + np.arange(block_length)) % date_count).ravel()[
            :date_count
        ]


def interval(dates, values, policy, block_length, scores=None):
    values = np.asarray(values, dtype=float)
    labels, codes = np.unique(np.asarray(dates), return_inverse=True)
    estimate = (
        spearman(scores, values)
        if scores is not None
        else (float(values.mean()) if len(values) else None)
    )
    result = {
        "estimate": estimate,
        "lower": None,
        "upper": None,
        "p_value": None,
        "resamples": policy.bootstrap_resamples,
        "seed": policy.bootstrap_seed,
        "block_length": block_length,
        "confidence_level": policy.confidence_level,
        "n_dates": len(labels),
        "valid_resamples": 0,
        "reason_code": None,
    }
    if estimate is None or len(labels) < 2 * block_length:
        result["reason_code"] = "INSUFFICIENT_DATE_BLOCKS_OR_VARIATION"
        return result
    if scores is not None:
        scores = np.asarray(scores, dtype=float)
        rows = [np.flatnonzero(codes == i) for i in range(len(labels))]
    else:
        sums = np.bincount(codes, weights=values)
        counts = np.bincount(codes)
    draws = []
    for sampled in date_draws(
        len(labels), block_length, policy.bootstrap_resamples, policy.bootstrap_seed
    ):
        if scores is None:
            value = float(sums[sampled].sum() / counts[sampled].sum())
        else:
            indices = np.concatenate([rows[i] for i in sampled])
            value = spearman(scores[indices], values[indices])
        if value is not None:
            draws.append(value)
    result["valid_resamples"] = len(draws)
    if len(draws) < 0.9 * policy.bootstrap_resamples:
        result["reason_code"] = "INSUFFICIENT_VALID_RESAMPLES"
        return result
    draws = np.asarray(draws)
    alpha = (1 - policy.confidence_level) / 2
    result["lower"], result["upper"] = map(
        float, np.quantile(draws, [alpha, 1 - alpha])
    )
    result["p_value"] = float(
        (1 + np.sum(draws - estimate >= estimate)) / (len(draws) + 1)
    )
    return result


def benjamini_hochberg(p_values):
    """Missing preregistered hypotheses count as p=1, with null reported q."""
    if any(
        p is not None and (not np.isfinite(p) or not 0 <= p <= 1)
        for p in p_values.values()
    ):
        raise ValueError("invalid p-value")
    order = sorted(
        p_values, key=lambda k: (p_values[k] if p_values[k] is not None else 1, k)
    )
    result, running = {}, 1.0
    for i in range(len(order) - 1, -1, -1):
        key = order[i]
        running = min(
            running,
            (p_values[key] if p_values[key] is not None else 1) * len(order) / (i + 1),
        )
        result[key] = float(running) if p_values[key] is not None else None
    return result
