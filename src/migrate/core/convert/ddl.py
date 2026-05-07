from __future__ import annotations

from migrate.core.inventory.models import TableMetadata


_TYPE_MAP = {
    "INT64": "BIGINT", "INTEGER": "BIGINT",
    "FLOAT64": "DOUBLE", "FLOAT": "DOUBLE",
    "NUMERIC": "DECIMAL(38, 9)",
    "BIGNUMERIC": "DECIMAL(38, 18)",
    "STRING": "STRING",
    "BYTES": "BINARY",
    "BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP_NTZ",
    "TIMESTAMP": "TIMESTAMP",
    "TIME": "STRING",
    "GEOGRAPHY": "STRING",
    "JSON": "STRING",
    "INTERVAL": "STRING",
}


def _map_type(bq_type: str) -> str:
    base = bq_type.upper().split("(")[0].split("<")[0].strip()
    if base.startswith("ARRAY"):
        inner = bq_type[bq_type.index("<") + 1 : bq_type.rindex(">")]
        return f"ARRAY<{_map_type(inner)}>"
    if base.startswith("STRUCT"):
        return bq_type
    return _TYPE_MAP.get(base, "STRING")


def build_delta_ddl(table: TableMetadata, target_catalog: str, target_schema: str | None = None) -> str:
    schema = target_schema or table.dataset
    target_fqn = f"{target_catalog}.{schema}.{table.name}"

    lines = [f"CREATE TABLE IF NOT EXISTS {target_fqn} ("]
    col_lines = []
    for c in table.columns:
        suffix = "" if c.nullable else " NOT NULL"
        comment = f" COMMENT '{c.description.replace(chr(39), chr(39)*2)}'" if c.description else ""
        col_lines.append(f"  `{c.name}` {_map_type(c.type)}{suffix}{comment}")
    lines.append(",\n".join(col_lines))
    lines.append(") USING DELTA")
    if table.partitioning:
        lines.append(f"PARTITIONED BY (`{table.partitioning}`)")
    if table.clustering:
        cluster_cols = ", ".join(f"`{c}`" for c in table.clustering)
        lines.append(f"CLUSTER BY ({cluster_cols})")
    if table.description:
        lines.append(f"COMMENT '{table.description.replace(chr(39), chr(39)*2)}'")
    lines.append(";")
    return "\n".join(lines)
