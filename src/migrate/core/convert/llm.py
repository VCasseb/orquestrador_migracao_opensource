from __future__ import annotations

from migrate.core.credentials import get_env, load_env


def llm_repair(bq_sql: str, sqlglot_error: str | None = None) -> tuple[str, str]:
    """Use Claude to repair SQL that sqlglot couldn't transpile.

    Returns (converted_sql, model_used). Raises RuntimeError if no API key.
    """
    load_env()
    api_key = get_env("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not configured")

    from anthropic import Anthropic

    model = get_env("ANTHROPIC_MODEL", "claude-sonnet-4-6")
    client = Anthropic(api_key=api_key)

    sys = (
        "You convert BigQuery SQL to Databricks SQL (Spark SQL dialect, Unity Catalog ready). "
        "Reply with ONLY the converted SQL, no prose, no markdown fences, no commentary. "
        "Preserve semantics exactly. If a BigQuery construct has no Databricks equivalent, "
        "use the closest available and add a -- comment on that line explaining the substitution."
    )
    user_parts = [f"BigQuery SQL:\n{bq_sql}"]
    if sqlglot_error:
        user_parts.append(f"\nSqlglot transpile error context: {sqlglot_error}")

    msg = client.messages.create(
        model=model,
        max_tokens=4000,
        system=sys,
        messages=[{"role": "user", "content": "\n".join(user_parts)}],
    )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    if text.startswith("```"):
        lines = [ln for ln in text.splitlines() if not ln.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text, model
