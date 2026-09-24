"""Compatibility exports for the existing Analytics v2 numerical contract."""

from src.analysis.date_block_statistics import (
    ranks,
    spearman,
    date_draws,
    interval,
    benjamini_hochberg,
)

__all__ = ["ranks", "spearman", "date_draws", "interval", "benjamini_hochberg"]
