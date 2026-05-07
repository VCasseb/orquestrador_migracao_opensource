from __future__ import annotations

from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.audit import timed
from migrate.core.validate.diff import compare
from migrate.core.validate.profile import build_profile_sql, parse_profile_row
from migrate.core.validate.report import TableMetrics, ValidationReport


def _bq_profile(fqn: str, columns: list[tuple[str, str]], sa_path: str | None) -> TableMetrics:
    from google.cloud import bigquery
    project = fqn.split(".")[0]
    client = bigquery.Client.from_service_account_json(sa_path, project=project) if sa_path \
        else bigquery.Client(project=project)
    sql = build_profile_sql(fqn, columns, "bigquery")
    row = dict(next(iter(client.query(sql).result())))
    row_count, cols = parse_profile_row(row, columns, "bigquery")
    return TableMetrics(fqn=fqn, row_count=row_count, columns=cols)


def _databricks_profile(fqn: str, columns: list[tuple[str, str]]) -> TableMetrics:
    from databricks.sdk import WorkspaceClient
    host = get_env("DATABRICKS_HOST")
    token = get_env("DATABRICKS_TOKEN")
    warehouse_id = get_env("DATABRICKS_SQL_WAREHOUSE_ID")
    if not warehouse_id:
        raise RuntimeError("DATABRICKS_SQL_WAREHOUSE_ID is required for validation")

    client = WorkspaceClient(host=host, token=token)
    sql = build_profile_sql(fqn, columns, "databricks")
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )

    schema = resp.manifest.schema
    data = resp.result.data_array[0] if resp.result and resp.result.data_array else []
    row = {col.name: data[i] for i, col in enumerate(schema.columns)}
    row_count, cols = parse_profile_row(row, columns, "databricks")
    return TableMetrics(fqn=fqn, row_count=row_count, columns=cols)


def run_validation(source_fqn: str, target_fqn: str | None = None) -> ValidationReport:
    """Profile both sides and produce a diff report. Requires real connections."""
    with timed("validate", fqn=source_fqn) as ctx:
        load_env()

        inv = load_inventory()
        if not inv:
            raise RuntimeError("No inventory found. Run a scan first.")
        source = inv.by_fqn.get(source_fqn)
        if not source:
            raise RuntimeError(f"Table not found in inventory: {source_fqn}")

        columns = [(c.name, c.type) for c in source.columns]
        if not columns:
            raise RuntimeError(f"No columns recorded for {source_fqn}")

        catalog = get_env("DATABRICKS_DEFAULT_CATALOG", "migrated_from_gcp")
        if not target_fqn:
            target_fqn = f"{catalog}.{source.dataset}.{source.name}"

        sa_path = get_env("GCP_SERVICE_ACCOUNT_JSON") or None
        src_metrics = _bq_profile(source_fqn, columns, sa_path)
        tgt_metrics = _databricks_profile(target_fqn, columns)

        report = compare(src_metrics, tgt_metrics, fqn_source=source_fqn, fqn_target=target_fqn)
        ctx.update({
            "verdict": report.verdict,
            "row_count_diff_pct": report.row_count_diff_pct,
            "target_fqn": target_fqn,
            "is_synthetic": False,
        })
        return report
