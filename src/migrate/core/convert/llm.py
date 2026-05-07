from __future__ import annotations


def llm_repair(bq_sql: str, sqlglot_error: str | None = None) -> tuple[str, str]:
    """Use the active LLM provider to repair SQL that sqlglot couldn't transpile.

    Returns (converted_sql, model_used).
    """
    from migrate.core.llm import complete

    sys = (
        "You convert BigQuery SQL to Databricks SQL (Spark SQL dialect, Unity Catalog ready). "
        "Reply with ONLY the converted SQL, no prose, no markdown fences, no commentary. "
        "Preserve semantics exactly. If a BigQuery construct has no Databricks equivalent, "
        "use the closest available and add a -- comment on that line explaining the substitution."
    )
    user_parts = [f"BigQuery SQL:\n{bq_sql}"]
    if sqlglot_error:
        user_parts.append(f"\nSqlglot transpile error context: {sqlglot_error}")

    text, model = complete(sys, "\n".join(user_parts), max_tokens=4000)
    if text.startswith("```"):
        lines = [ln for ln in text.splitlines() if not ln.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text, model
