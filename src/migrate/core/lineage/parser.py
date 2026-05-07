from __future__ import annotations

import sqlglot
from sqlglot import exp


def extract_refs(sql: str, default_project: str | None = None, default_dataset: str | None = None) -> set[str]:
    """Extract fully-qualified table refs from a SQL string (BigQuery dialect).

    Returns set of `project.dataset.table` strings. Refs missing project/dataset are
    completed using defaults when provided; otherwise dropped. CTE aliases (WITH foo AS ...)
    are excluded — they are not real tables.
    """
    if not sql:
        return set()
    try:
        parsed = sqlglot.parse_one(sql, read="bigquery")
    except Exception:
        return set()

    cte_names: set[str] = set()
    for cte in parsed.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            cte_names.add(alias)

    refs: set[str] = set()
    for table in parsed.find_all(exp.Table):
        catalog = table.args.get("catalog")
        db = table.args.get("db")
        proj = catalog.name if catalog else default_project
        ds = db.name if db else default_dataset
        if not proj or not ds:
            if table.name in cte_names:
                continue
            continue
        if table.name in cte_names and not catalog and not db:
            continue
        refs.add(f"{proj}.{ds}.{table.name}")
    return refs
