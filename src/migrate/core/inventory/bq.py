from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from migrate.core.inventory.models import ColumnMetadata, Inventory, TableMetadata


def _client(project: str, sa_path: str | None):
    from google.cloud import bigquery
    if sa_path:
        return bigquery.Client.from_service_account_json(sa_path, project=project)
    return bigquery.Client(project=project)


def _list_datasets(client) -> list[str]:
    return [d.dataset_id for d in client.list_datasets()]


_TABLE_TYPE_MAP = {
    "BASE TABLE": "TABLE",
    "VIEW": "VIEW",
    "MATERIALIZED VIEW": "MATERIALIZED_VIEW",
    "EXTERNAL": "EXTERNAL",
    "EXTERNAL TABLE": "EXTERNAL",
}


def _scan_dataset(client, project: str, dataset: str) -> list[TableMetadata]:
    """Read INFORMATION_SCHEMA for all tables in one dataset."""
    cols_q = f"""
        SELECT table_name, column_name, data_type, is_nullable, description
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
        ORDER BY table_name, ordinal_position
    """
    tabs_q = f"""
        SELECT
          t.table_name,
          t.table_type,
          o.option_value AS description,
          ts.row_count,
          ts.size_bytes,
          v.view_definition
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` t
        LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS` o
          ON t.table_name = o.table_name AND o.option_name = 'description'
        LEFT JOIN `{project}.{dataset}.__TABLES__` ts
          ON t.table_name = ts.table_id
        LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS` v
          ON t.table_name = v.table_name
    """

    cols_by_table: dict[str, list[ColumnMetadata]] = {}
    for row in client.query(cols_q).result():
        cols_by_table.setdefault(row.table_name, []).append(
            ColumnMetadata(
                name=row.column_name,
                type=row.data_type,
                nullable=(row.is_nullable == "YES"),
                description=row.description or "",
            )
        )

    tables: list[TableMetadata] = []
    for row in client.query(tabs_q).result():
        ttype = _TABLE_TYPE_MAP.get(row.table_type, "TABLE")
        tables.append(
            TableMetadata(
                project=project,
                dataset=dataset,
                name=row.table_name,
                type=ttype,
                columns=cols_by_table.get(row.table_name, []),
                row_count=row.row_count,
                size_bytes=row.size_bytes,
                view_query=row.view_definition,
                description=row.description or "",
            )
        )
    return tables


def _query_heatmap(client, project: str) -> dict[str, int]:
    """Returns FQN → query count over last 30 days (best-effort)."""
    q = f"""
        SELECT
          referenced_table.project_id || '.' || referenced_table.dataset_id || '.' || referenced_table.table_id AS fqn,
          COUNT(*) AS hits
        FROM `{project}`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
             UNNEST(referenced_tables) AS referenced_table
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND state = 'DONE'
        GROUP BY fqn
    """
    try:
        return {row.fqn: row.hits for row in client.query(q).result()}
    except Exception:
        return {}


def scan_projects(project_ids: Iterable[str], sa_path: str | None = None) -> Inventory:
    project_list = list(project_ids)
    all_tables: list[TableMetadata] = []
    heatmap: dict[str, int] = {}

    for project in project_list:
        client = _client(project, sa_path)
        for dataset in _list_datasets(client):
            try:
                all_tables.extend(_scan_dataset(client, project, dataset))
            except Exception:
                continue
        heatmap.update(_query_heatmap(client, project))

    for t in all_tables:
        t.query_count_30d = heatmap.get(t.fqn, 0)

    return Inventory(
        scanned_at=datetime.now(timezone.utc),
        projects=project_list,
        tables=all_tables,
    )
