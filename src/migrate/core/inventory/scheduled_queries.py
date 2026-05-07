"""BigQuery scheduled queries via Data Transfer Service.

These show up in BQ as scheduled query configs (transferConfigs of type
'scheduled_query'). They define: a SQL query, a destination table, and a schedule.
"""
from __future__ import annotations

from migrate.core.inventory.models import ScheduledQuery


def scan_scheduled_queries(project: str, sa_path: str | None = None) -> list[ScheduledQuery]:
    try:
        from google.cloud import bigquery_datatransfer
    except ImportError:
        return []

    client = (
        bigquery_datatransfer.DataTransferServiceClient.from_service_account_json(sa_path)
        if sa_path else bigquery_datatransfer.DataTransferServiceClient()
    )
    parent = f"projects/{project}/locations/-"

    out: list[ScheduledQuery] = []
    try:
        for cfg in client.list_transfer_configs(parent=parent):
            if cfg.data_source_id != "scheduled_query":
                continue
            params = dict(cfg.params or {})
            query = params.get("query", "")
            destination = params.get("destination_table_name_template") or ""
            destination_dataset = cfg.destination_dataset_id
            dest_fqn = f"{project}.{destination_dataset}.{destination}" if destination else ""
            out.append(ScheduledQuery(
                name=cfg.display_name or cfg.name.split("/")[-1],
                config_id=cfg.name,
                schedule=cfg.schedule,
                query=query,
                destination_table=dest_fqn,
                project=project,
            ))
    except Exception:
        return out
    return out
