from __future__ import annotations

from typing import Literal

Dialect = Literal["bigquery", "databricks"]


_NUMERIC_TYPES_BQ = {"INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "FLOAT", "INTEGER"}
_NUMERIC_TYPES_SPARK = {"INT", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "LONG"}


def _is_numeric(col_type: str, dialect: Dialect) -> bool:
    base = col_type.upper().split("(")[0].strip()
    if dialect == "bigquery":
        return base in _NUMERIC_TYPES_BQ
    return any(base.startswith(t) for t in _NUMERIC_TYPES_SPARK)


def build_profile_sql(
    fqn: str,
    columns: list[tuple[str, str]],
    dialect: Dialect,
) -> str:
    """Build a single SELECT that returns one row with all profiling metrics.

    Each column produces several aliased aggregates (null_count, distinct_count,
    min, max, avg). We use a single query to minimize round-trips and DBU cost.
    """
    parts: list[str] = ["COUNT(*) AS _row_count"]

    for name, col_type in columns:
        safe = name.replace("`", "").replace('"', "")
        ref = f"`{safe}`" if dialect == "bigquery" else f"`{safe}`"
        parts.append(f"COUNTIF({ref} IS NULL) AS `{safe}__null_count`" if dialect == "bigquery"
                     else f"SUM(CASE WHEN {ref} IS NULL THEN 1 ELSE 0 END) AS `{safe}__null_count`")
        parts.append(f"APPROX_COUNT_DISTINCT({ref}) AS `{safe}__distinct_count`" if dialect == "bigquery"
                     else f"approx_count_distinct({ref}) AS `{safe}__distinct_count`")
        if _is_numeric(col_type, dialect):
            parts.append(f"CAST(MIN({ref}) AS FLOAT64) AS `{safe}__min`" if dialect == "bigquery"
                         else f"CAST(MIN({ref}) AS DOUBLE) AS `{safe}__min`")
            parts.append(f"CAST(MAX({ref}) AS FLOAT64) AS `{safe}__max`" if dialect == "bigquery"
                         else f"CAST(MAX({ref}) AS DOUBLE) AS `{safe}__max`")
            parts.append(f"AVG(CAST({ref} AS FLOAT64)) AS `{safe}__avg`" if dialect == "bigquery"
                         else f"AVG(CAST({ref} AS DOUBLE)) AS `{safe}__avg`")

    quoted = f"`{fqn}`" if dialect == "bigquery" else fqn
    return f"SELECT {', '.join(parts)} FROM {quoted}"


def parse_profile_row(row: dict, columns: list[tuple[str, str]], dialect: Dialect):
    """Convert a single profile row dict into ColumnMetric list and row_count."""
    from migrate.core.validate.report import ColumnMetric

    row_count = int(row.get("_row_count", 0))
    cols: list[ColumnMetric] = []
    for name, col_type in columns:
        safe = name.replace("`", "").replace('"', "")
        cols.append(ColumnMetric(
            name=name,
            type=col_type,
            null_count=int(row.get(f"{safe}__null_count", 0) or 0),
            distinct_count=int(row.get(f"{safe}__distinct_count", 0) or 0),
            min=row.get(f"{safe}__min"),
            max=row.get(f"{safe}__max"),
            avg=row.get(f"{safe}__avg"),
        ))
    return row_count, cols
