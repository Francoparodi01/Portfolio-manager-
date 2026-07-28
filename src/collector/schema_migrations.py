from __future__ import annotations


EXECUTION_TIMESTAMP_META_SQL = """
ALTER TABLE broker_fills
    ADD COLUMN IF NOT EXISTS executed_at_precision TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS executed_at_source    TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE broker_movements
    ADD COLUMN IF NOT EXISTS executed_at_precision TEXT NOT NULL DEFAULT 'date_only',
    ADD COLUMN IF NOT EXISTS executed_at_source    TEXT NOT NULL DEFAULT 'cocos_movements.execution_date';

UPDATE broker_movements
SET executed_at_precision = 'date_only'
WHERE executed_at_precision IS NULL
   OR executed_at_precision = ''
   OR executed_at_precision = 'unknown';

UPDATE broker_fills
SET executed_at_precision = 'date_only',
    executed_at_source = 'cocos_movements.execution_date'
WHERE source = 'cocos_movements'
  AND (
        executed_at_precision IS NULL
     OR executed_at_precision = ''
     OR executed_at_precision = 'unknown'
  );
"""


OUTCOME_HORIZON_SQL = """
ALTER TABLE decision_log
    ADD COLUMN IF NOT EXISTS outcome_40d FLOAT,
    ADD COLUMN IF NOT EXISTS executable_outcome_40d FLOAT;
"""


__all__ = [
    "EXECUTION_TIMESTAMP_META_SQL",
    "OUTCOME_HORIZON_SQL",
]
