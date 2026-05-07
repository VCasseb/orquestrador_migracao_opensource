from __future__ import annotations

from dataclasses import dataclass

import sqlglot


@dataclass
class TranspileResult:
    converted_sql: str
    confidence: str
    method: str
    notes: list[str]
    error: str | None = None


def transpile_bq_to_databricks(sql: str) -> TranspileResult:
    """Try sqlglot first. Returns confidence label and notes about edge cases."""
    if not sql or not sql.strip():
        return TranspileResult("", "0%", "empty", ["Input SQL is empty"], "empty input")

    notes: list[str] = []
    upper = sql.upper()
    if "GEOGRAPHY" in upper:
        notes.append("Contains GEOGRAPHY type — Databricks has no equivalent (manual review).")
    if "ST_GEOGFROMTEXT" in upper or "ST_DWITHIN" in upper:
        notes.append("Geospatial functions — likely need rewrite using H3 or external lib.")
    if "QUALIFY" in upper:
        notes.append("QUALIFY → handled by sqlglot via window function rewrite.")
    if "ARRAY_AGG" in upper:
        notes.append("ARRAY_AGG → check ordering semantics in Spark.")
    if "TIMESTAMP" in upper and "DATETIME" in upper:
        notes.append("TIMESTAMP/DATETIME mix — confirm timezone semantics (TIMESTAMP_NTZ in Spark).")

    try:
        out = sqlglot.transpile(sql, read="bigquery", write="databricks", pretty=True)
        if not out:
            return TranspileResult(sql, "0%", "failed", notes + ["sqlglot returned empty"], "empty output")
        return TranspileResult(out[0], "high", "sqlglot", notes, None)
    except Exception as e:
        return TranspileResult(sql, "0%", "failed", notes, str(e))
