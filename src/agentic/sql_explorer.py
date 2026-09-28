from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from .contracts import ToolObservation, ToolSpec, ToolValidationError
from .read_only import connect_read_only
from .tools import ToolContext, ToolRegistry, read_only_dsn


_ALLOWED_TABLES = {
    "decision_log",
    "portfolio_snapshots",
    "broker_fills",
    "position_hold_observations",
}
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|copy|alter|drop|create|truncate|grant|revoke|call|do|execute|prepare|deallocate|vacuum|analyze|refresh|reindex|cluster|listen|notify|lock|begin|commit|rollback|savepoint|release|set|reset|show|union|intersect|except)\b",
    re.IGNORECASE,
)
_DANGEROUS_FUNCTIONS = re.compile(
    r"\b(pg_sleep|set_config|current_setting|nextval|setval|dblink|lo_import|lo_export|pg_read_file|pg_ls_dir|pg_stat_file)\s*\(",
    re.IGNORECASE,
)
_RELATION = re.compile(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)", re.IGNORECASE)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return str(value)


def validate_exploratory_sql(sql: str) -> tuple[str, tuple[str, ...]]:
    query = " ".join(str(sql or "").strip().split())
    if not query:
        raise ToolValidationError("sql cannot be empty")
    if len(query) > 8000:
        raise ToolValidationError("sql exceeds 8000 characters")
    if ";" in query or "--" in query or "/*" in query or "*/" in query:
        raise ToolValidationError("comments and statement separators are not allowed")
    if not re.match(r"^select\b", query, re.IGNORECASE):
        raise ToolValidationError("only a single SELECT statement is allowed")
    if len(re.findall(r"\bselect\b", query, re.IGNORECASE)) != 1:
        raise ToolValidationError("nested SELECTs are not allowed in the generic explorer")
    if _FORBIDDEN.search(query):
        raise ToolValidationError("sql contains a forbidden statement/operator")
    if _DANGEROUS_FUNCTIONS.search(query):
        raise ToolValidationError("sql contains a forbidden database function")

    relations = tuple(dict.fromkeys(match.group(1).lower() for match in _RELATION.finditer(query)))
    if not relations:
        raise ToolValidationError("sql must read at least one allowlisted relation")
    for relation in relations:
        if "." in relation:
            raise ToolValidationError("schema-qualified relations are not allowed")
        if relation not in _ALLOWED_TABLES:
            raise ToolValidationError(f"relation not allowed: {relation}")

    # Disallow comma-style cross joins. Explicit JOIN makes relation extraction and owner scoping auditable.
    from_tail = re.split(r"\b(?:where|group\s+by|order\s+by|limit|offset|having)\b", query, maxsplit=1, flags=re.IGNORECASE)[0]
    from_pos = re.search(r"\bfrom\b", from_tail, re.IGNORECASE)
    if from_pos and "," in from_tail[from_pos.end():]:
        raise ToolValidationError("comma-style FROM lists are not allowed; use explicit JOIN")

    return query, relations


def _scoped_query(query: str, relations: tuple[str, ...], *, allow_legacy_null: bool, max_rows: int) -> str:
    owner_predicate = "owner_chat_id = $1"
    if allow_legacy_null:
        owner_predicate = "(owner_chat_id = $1 OR owner_chat_id IS NULL)"
    ctes = [
        f"{table} AS (SELECT * FROM public.{table} WHERE {owner_predicate})"
        for table in relations
    ]
    return f"WITH {', '.join(ctes)} SELECT * FROM ({query}) AS agent_query LIMIT {int(max_rows)}"


async def _inspect_catalog(context: ToolContext) -> dict[str, Any]:
    conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=30)
    try:
        rows = await conn.fetch(
            """
            SELECT table_name, column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name = ANY($1::text[])
            ORDER BY table_name, ordinal_position
            """,
            sorted(_ALLOWED_TABLES),
        )
    finally:
        await conn.close()
    tables: dict[str, list[dict[str, Any]]] = {name: [] for name in sorted(_ALLOWED_TABLES)}
    for row in rows:
        tables[str(row["table_name"])].append(
            {"name": str(row["column_name"]), "type": str(row["data_type"])}
        )
    return {
        "schema_version": "quantia-agent-data-catalog-v1",
        "owner_scoped": True,
        "relations": {
            name: {"available": bool(columns), "columns": columns}
            for name, columns in tables.items()
        },
        "notes": [
            "final_score is not return or PnL",
            "outcome_* fields are directional outcomes under outcome_basis",
            "Decision Lab owns canonical PLAN vs HOLD / DVA semantics",
            "Historical Edge owns canonical independent episode methodology",
        ],
    }


def register_sql_explorer_tools(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    async def inspect_handler(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        try:
            payload = await _inspect_catalog(context)
            content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
            return ToolObservation(
                tool_name="inspect_quantia_schema",
                arguments={},
                ok=True,
                content=content[: context.output_limit_chars],
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode()).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="inspect_quantia_schema", arguments={}, ok=False, content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )

    async def query_handler(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        query = str(arguments.get("sql") or "")
        max_rows = max(1, min(200, int(arguments.get("max_rows", 100))))
        purpose = str(arguments.get("purpose") or "exploratory analysis")[:500]
        normalized = ""
        relations: tuple[str, ...] = ()
        try:
            normalized, relations = validate_exploratory_sql(query)
            wrapped = _scoped_query(
                normalized,
                relations,
                allow_legacy_null=bool(context.legacy_single_owner),
                max_rows=max_rows,
            )
            conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=30)
            try:
                rows = await conn.fetch(wrapped, int(context.owner_chat_id or 0))
            finally:
                await conn.close()
            records = [_json_safe(dict(row)) for row in rows]
            payload = {
                "schema_version": "quantia-sql-explorer-v1",
                "status": "observed",
                "purpose": purpose,
                "owner_scoped": True,
                "legacy_null_included": bool(context.legacy_single_owner),
                "relations": list(relations),
                "row_count": len(records),
                "max_rows": max_rows,
                "query_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
                "rows": records,
                "limitations": [
                    "exploratory SQL does not redefine canonical PnL, DVA or Historical Edge episode semantics",
                    "result set is bounded by the runtime",
                ],
            }
            content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
            if len(content) > context.output_limit_chars:
                payload["rows"] = records[: max(1, len(records) // 2)]
                payload["truncated"] = True
                content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
                content = content[: context.output_limit_chars]
            return ToolObservation(
                tool_name="query_quantia_sql",
                arguments={"sql": normalized, "purpose": purpose, "max_rows": max_rows},
                ok=True,
                content=content,
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode()).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="query_quantia_sql",
                arguments={"sql": normalized or query[:8000], "purpose": purpose, "max_rows": max_rows},
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )

    registry.register(
        ToolSpec(
            name="inspect_quantia_schema",
            description="Inspect the live columns/types of the small allowlisted Quantia analytical data surface. Use when exact column availability is uncertain. Read-only and does not expose other relations.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            read_only=True,
            timeout_seconds=30,
            capability="READ",
        ),
        inspect_handler,
    )
    registry.register(
        ToolSpec(
            name="query_quantia_sql",
            description=(
                "Run one exploratory owner-scoped SELECT over allowlisted Quantia relations. Use for open-ended grouping, filtering, joins and pattern exploration not already defined by a canonical metric. "
                "Do not use it to reinvent canonical account PnL, Decision Lab DVA or Historical Edge independent-episode metrics. No nested SELECT/CTE/UNION; use explicit JOINs."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "maxLength": 8000},
                    "purpose": {"type": "string", "maxLength": 500, "default": "exploratory analysis"},
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 200, "default": 100},
                },
                "required": ["sql"],
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=30,
            capability="READ",
        ),
        query_handler,
    )
    return registry


__all__ = ["register_sql_explorer_tools", "validate_exploratory_sql"]
