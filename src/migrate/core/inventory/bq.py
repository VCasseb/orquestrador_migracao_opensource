from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Iterable

from migrate.core.inventory.models import ColumnMetadata, Inventory, TableMetadata


def _client(project: str, sa_path: str | None):
    from google.cloud import bigquery
    if sa_path:
        return bigquery.Client.from_service_account_json(sa_path, project=project)
    return bigquery.Client(project=project)


def _list_datasets(client) -> list[tuple[str, str | None]]:
    """Returns [(dataset_id, location)] — location may be None if not retrievable."""
    out: list[tuple[str, str | None]] = []
    for d in client.list_datasets():
        try:
            full = client.get_dataset(d.reference)
            out.append((d.dataset_id, full.location))
        except Exception:
            out.append((d.dataset_id, None))
    return out


_TABLE_TYPE_MAP = {
    "BASE TABLE": "TABLE",
    "VIEW": "VIEW",
    "MATERIALIZED VIEW": "MATERIALIZED_VIEW",
    "EXTERNAL": "EXTERNAL",
    "EXTERNAL TABLE": "EXTERNAL",
}


def _scan_dataset(client, project: str, dataset: str) -> list[TableMetadata]:
    """Read INFORMATION_SCHEMA for all tables in one dataset.

    Resilient: uses 3 separate queries (TABLES + TABLE_OPTIONS + VIEWS) instead of
    a single JOIN-on-`__TABLES__` so missing `__TABLES__` access (common with VPC SC,
    org policies) doesn't kill the whole dataset. Row count / size come best-effort
    from `__TABLES__`; if unavailable, they're left as None.
    """
    cols_q = f"""
        SELECT
          c.table_name,
          c.column_name,
          c.data_type,
          c.is_nullable,
          cfp.description
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` c
        LEFT JOIN `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` cfp
          ON c.table_name = cfp.table_name
         AND c.column_name = cfp.column_name
         AND cfp.field_path = c.column_name
        ORDER BY c.table_name, c.ordinal_position
    """
    tabs_q = f"""
        SELECT t.table_name, t.table_type
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` t
    """
    opts_q = f"""
        SELECT table_name, option_value AS description
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`
        WHERE option_name = 'description'
    """
    views_q = f"""
        SELECT table_name, view_definition
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
    """
    legacy_q = f"SELECT table_id, row_count, size_bytes FROM `{project}.{dataset}.__TABLES__`"

    cols_by_table: dict[str, list[ColumnMetadata]] = {}
    try:
        for row in client.query(cols_q).result():
            cols_by_table.setdefault(row.table_name, []).append(
                ColumnMetadata(
                    name=row.column_name,
                    type=row.data_type,
                    nullable=(row.is_nullable == "YES"),
                    description=row.description or "",
                )
            )
    except Exception as e:
        print(f"  [WARN] {project}.{dataset}: columns query failed — {e}", file=sys.stderr)

    desc_by_table: dict[str, str] = {}
    try:
        for row in client.query(opts_q).result():
            desc_by_table[row.table_name] = (row.description or "").strip("\"'")
    except Exception:
        pass

    view_by_table: dict[str, str] = {}
    try:
        for row in client.query(views_q).result():
            view_by_table[row.table_name] = row.view_definition
    except Exception:
        pass

    legacy_by_table: dict[str, tuple[int | None, int | None]] = {}
    try:
        for row in client.query(legacy_q).result():
            legacy_by_table[row.table_id] = (row.row_count, row.size_bytes)
    except Exception:
        # __TABLES__ unavailable (VPC SC, org policy, missing legacy access) — skip
        pass

    tables: list[TableMetadata] = []
    try:
        for row in client.query(tabs_q).result():
            ttype = _TABLE_TYPE_MAP.get(row.table_type, "TABLE")
            row_count, size_bytes = legacy_by_table.get(row.table_name, (None, None))
            t = TableMetadata(
                project=project,
                dataset=dataset,
                name=row.table_name,
                type=ttype,
                columns=cols_by_table.get(row.table_name, []),
                row_count=row_count,
                size_bytes=size_bytes,
                view_query=view_by_table.get(row.table_name),
                description=desc_by_table.get(row.table_name, ""),
            )
            if ttype == "EXTERNAL":
                _attach_external_config(client, t)
            tables.append(t)
    except Exception as e:
        print(f"  [ERROR] {project}.{dataset}: TABLES query failed — {e}", file=sys.stderr)
    return tables


def _attach_external_config(client, t: TableMetadata) -> None:
    """For EXTERNAL tables, fetch the external_data_configuration via the BQ API
    (it's not exposed in INFORMATION_SCHEMA). Sets source_kind=external_gcs and
    populates external_source_uris/external_format/external_hive_partitioning."""
    try:
        full = client.get_table(f"{t.project}.{t.dataset}.{t.name}")
        cfg = getattr(full, "external_data_configuration", None)
        if not cfg:
            return
        uris = list(getattr(cfg, "source_uris", []) or [])
        fmt = getattr(cfg, "source_format", None)
        compression = getattr(cfg, "compression", None)
        hive_opts = getattr(cfg, "hive_partitioning_options", None)
        hive = bool(hive_opts and getattr(hive_opts, "mode", None))

        t.external_source_uris = uris
        t.external_format = str(fmt) if fmt else None
        t.external_compression = str(compression) if compression else None
        t.external_hive_partitioning = hive

        # Auto-link source_kind. If all URIs are gs://, mark as external_gcs.
        if uris and all(u.startswith("gs://") for u in uris):
            t.source_kind = "external_gcs"
    except Exception as e:
        print(f"  [WARN] {t.fqn}: external config fetch failed — {e}", file=sys.stderr)


def _bq_region(location: str | None) -> str:
    """Convert a BQ dataset location ('US', 'southamerica-east1', etc) into the
    INFORMATION_SCHEMA region literal ('region-us', 'region-southamerica-east1')."""
    if not location:
        return "region-us"
    loc = location.lower()
    return f"region-{loc}"


def _query_heatmap(client, project: str, region: str) -> dict[str, int]:
    """Returns FQN → query count over last 30 days (best-effort).

    Region must match the dataset region (multi-region 'US'/'EU' or location like
    'southamerica-east1'). Falls back to empty if unavailable.
    """
    q = f"""
        SELECT
          referenced_table.project_id || '.' || referenced_table.dataset_id || '.' || referenced_table.table_id AS fqn,
          COUNT(*) AS hits
        FROM `{project}`.`{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
             UNNEST(referenced_tables) AS referenced_table
        WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
          AND state = 'DONE'
        GROUP BY fqn
    """
    try:
        return {row.fqn: row.hits for row in client.query(q).result()}
    except Exception as e:
        print(f"  [WARN] heatmap query failed for {project} in {region}: {e}", file=sys.stderr)
        return {}


def scan_projects(project_ids: Iterable[str], sa_path: str | None = None) -> Inventory:
    project_list = list(project_ids)
    all_tables: list[TableMetadata] = []
    heatmap: dict[str, int] = {}

    region_override = os.environ.get("GCP_REGION", "").strip().lower() or None

    for project in project_list:
        print(f"[scan] {project}: connecting…", file=sys.stderr)
        client = _client(project, sa_path)
        try:
            datasets = _list_datasets(client)
        except Exception as e:
            print(f"  [ERROR] {project}: cannot list datasets — {e}", file=sys.stderr)
            continue
        print(f"[scan] {project}: {len(datasets)} dataset(s)", file=sys.stderr)

        regions_seen: set[str] = set()
        for dataset_id, location in datasets:
            print(f"  [scan] {project}.{dataset_id} (location: {location or 'unknown'})", file=sys.stderr)
            ds_tables = _scan_dataset(client, project, dataset_id)
            print(f"    → {len(ds_tables)} table(s)/view(s)", file=sys.stderr)
            all_tables.extend(ds_tables)
            if location:
                regions_seen.add(location.lower())

        # Heatmap once per region observed in this project (or env-overridden)
        regions_to_query = (
            {region_override} if region_override
            else regions_seen if regions_seen
            else {"us"}
        )
        for loc in regions_to_query:
            heatmap.update(_query_heatmap(client, project, _bq_region(loc)))

    for t in all_tables:
        t.query_count_30d = heatmap.get(t.fqn, 0)

    print(f"[scan] done: {len(all_tables)} object(s) total", file=sys.stderr)
    return Inventory(
        scanned_at=datetime.now(timezone.utc),
        projects=project_list,
        tables=all_tables,
    )
