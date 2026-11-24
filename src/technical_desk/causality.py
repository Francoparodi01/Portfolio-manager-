"""Prefix invariance checks for deterministic frozen-bar features."""
from .evidence_capture import canonical


def assert_prefix_causal(rows, calculator, cuts):
    """Compare the same snapshot at k with and without all bars after k."""
    full = calculator(rows)["snapshots"]
    checked = []
    for k in cuts:
        if type(k) is not int or not 0 <= k < len(rows):
            raise ValueError("INVALID_PREFIX_CUT")
        prefix = calculator(rows[:k + 1])["snapshots"]
        if canonical(full[k]) != canonical(prefix[-1]):
            changed = sorted(key for key in set(full[k]) | set(prefix[-1])
                             if canonical(full[k].get(key)) != canonical(prefix[-1].get(key)))
            raise AssertionError(f"PREFIX_CAUSALITY_VIOLATION k={k} fields={','.join(changed)}")
        checked.append(k)
    return checked
