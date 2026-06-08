from __future__ import annotations

from datetime import datetime

from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.models import Inventory
from migrate.core.state.audit import timed

# Non-fatal, per-source errors from the most recent scan (e.g. a bad notebooks
# bucket while BQ succeeded). The web/CLI layer reads this to warn the user
# instead of silently showing zero results.
_LAST_ERRORS: list[str] = []


def last_scan_errors() -> list[str]:
    return list(_LAST_ERRORS)


def _folders() -> list[str]:
    return [f.strip() for f in get_env("GCP_NOTEBOOKS_FOLDERS").split(",") if f.strip()]


def run_scan(use_sample: bool = False) -> Inventory:
    global _LAST_ERRORS
    _LAST_ERRORS = []
    with timed("scan", fqn=None) as ctx:
        if use_sample:
            from migrate.core.inventory.sample import build_sample
            inv = build_sample()
            ctx.update({
                "mode": "sample",
                "objects": len(inv.tables),
                "projects": len(inv.projects),
                "dags": len(inv.dags),
                "notebooks": len(inv.notebooks),
                "scheduled_queries": len(inv.scheduled_queries),
            })
            return inv

        load_env()
        project_ids = [p.strip() for p in get_env("GCP_PROJECT_IDS").split(",") if p.strip()]
        dag_bucket = get_env("GCP_COMPOSER_DAG_BUCKET")
        nb_bucket = get_env("GCP_NOTEBOOKS_BUCKET")
        sa_path = get_env("GCP_SERVICE_ACCOUNT_JSON") or None

        if not (project_ids or dag_bucket or nb_bucket):
            raise RuntimeError(
                "Nenhuma origem GCP configurada. Defina o bucket do lake (GCS) e/ou "
                "GCP_PROJECT_IDS na aba Connections, ou use Sample / Upload."
            )

        # BigQuery tables are optional context — only scanned if project_ids given.
        if project_ids:
            from migrate.core.inventory.bq import scan_projects
            try:
                inv = scan_projects(project_ids, sa_path)
            except Exception as e:
                _LAST_ERRORS.append(f"BigQuery: {e}")
                inv = Inventory(scanned_at=datetime.now(), projects=[], tables=[])
        else:
            inv = Inventory(scanned_at=datetime.now(), projects=[], tables=[])

        # Composer DAG bucket (optional)
        if dag_bucket:
            try:
                from migrate.core.inventory.composer import scan_composer_bucket
                inv.dags = scan_composer_bucket(dag_bucket, sa_path)
            except Exception as e:
                _LAST_ERRORS.append(f"Composer DAG bucket ({dag_bucket}): {e}")

        # Notebooks/scripts from the GCS lake (bucket + folders) — independent of BQ.
        if nb_bucket:
            try:
                from migrate.core.inventory.notebooks import scan_gcs_notebooks
                inv.notebooks = scan_gcs_notebooks(nb_bucket, _folders(), sa_path)
                if not inv.notebooks:
                    _LAST_ERRORS.append(
                        f"Nenhum .py/.ipynb encontrado em {nb_bucket} "
                        f"(pastas: {', '.join(_folders()) or 'raiz'}). Verifique bucket/pastas."
                    )
            except Exception as e:
                _LAST_ERRORS.append(f"GCS notebooks ({nb_bucket}): {e}")

        # Scheduled queries (BQ Data Transfer Service) — only with BQ projects.
        if project_ids:
            try:
                from migrate.core.inventory.scheduled_queries import scan_scheduled_queries
                sqs = []
                for project in project_ids:
                    sqs.extend(scan_scheduled_queries(project, sa_path))
                inv.scheduled_queries = sqs
            except Exception as e:
                _LAST_ERRORS.append(f"Scheduled queries: {e}")

        # Cross-link: enrich TableMetadata.source_* from DAG/notebook/SQ writes
        _enrich_sources(inv)

        ctx.update({
            "mode": "real",
            "objects": len(inv.tables),
            "projects": len(inv.projects),
            "dags": len(inv.dags),
            "notebooks": len(inv.notebooks),
            "scheduled_queries": len(inv.scheduled_queries),
            "errors": len(_LAST_ERRORS),
        })
        return inv


def _enrich_sources(inv) -> None:
    """Attach producer info to TableMetadata.source_* based on what DAGs/notebooks/SQs write."""
    by_fqn = inv.by_fqn

    for sq in inv.scheduled_queries:
        t = by_fqn.get(sq.destination_table)
        if t and t.source_kind == "unknown":
            t.source_kind = "scheduled_query"
            t.source_sql = sq.query
            t.source_schedule = sq.schedule
            t.source_scheduled_query_id = sq.name

    for d in inv.dags:
        for task in d.tasks:
            if not task.destination_table:
                continue
            t = by_fqn.get(task.destination_table)
            if not t or t.source_kind not in ("unknown", "manual"):
                continue
            if task.notebook_uri:
                t.source_kind = "dag_notebook"
                t.source_notebook_id = task.notebook_uri.split("/")[-1].replace(".ipynb", "")
            elif task.sql:
                t.source_kind = "dag_sql"
                t.source_sql = task.sql
            elif task.operator.endswith("PythonOperator"):
                t.source_kind = "dag_python"
            t.source_dag_id = d.name
            t.source_dag_task = task.task_id
            t.source_schedule = d.schedule

    for nb in inv.notebooks:
        for write in nb.all_written_tables:
            t = by_fqn.get(write)
            if t and t.source_kind == "unknown":
                t.source_kind = "notebook"
                t.source_notebook_id = nb.name
