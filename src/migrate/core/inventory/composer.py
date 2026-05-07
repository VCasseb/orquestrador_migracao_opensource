"""Composer (Airflow) DAG scanner.

Reads .py files from a Composer DAG bucket and extracts the structure WITHOUT
executing them (pure AST). For each DAG we capture:
  - DAG name, schedule, owner, tags
  - Tasks with operator type
  - SQL embedded in BigQuery operators (extracted from `configuration.query.query`)
  - Destination tables (from `destinationTable` config)
  - Notebook URIs from notebook-execution operators
  - sqlglot-resolved table references
"""
from __future__ import annotations

import ast
import re
from typing import Any

from migrate.core.inventory.models import DAGMetadata, DAGTask
from migrate.core.lineage.parser import extract_refs

_BQ_SQL_OPERATORS = {
    "BigQueryInsertJobOperator",
    "BigQueryExecuteQueryOperator",
    "BigQueryOperator",
}
_NOTEBOOK_OPERATORS = {
    "VertexAINotebookExecuteOperator",
    "DataprocCreateBatchOperator",
    "DataprocSubmitPySparkJobOperator",
    "PapermillOperator",
    "DatabricksSubmitRunOperator",
}
_BQ_TABLE_OPERATORS = {
    "BigQueryToBigQueryOperator",
    "BigQueryCreateEmptyTableOperator",
}


def parse_dag_file(file_path: str, source: str) -> DAGMetadata | None:
    """Parse one DAG .py file (source string) into DAGMetadata. Returns None if no DAG found."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None

    dag_name = _find_dag_name(tree)
    if not dag_name:
        return None

    schedule = _find_dag_schedule(tree)
    owner = _find_dag_owner(tree)
    tags = _find_dag_tags(tree)

    tasks: list[DAGTask] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        op_name = _call_name(node)
        if op_name in _BQ_SQL_OPERATORS:
            tasks.append(_parse_bq_operator(node, op_name))
        elif op_name in _NOTEBOOK_OPERATORS:
            tasks.append(_parse_notebook_operator(node, op_name))
        elif op_name in _BQ_TABLE_OPERATORS:
            tasks.append(_parse_bq_table_operator(node, op_name))

    return DAGMetadata(
        name=dag_name,
        file_path=file_path,
        schedule=schedule,
        owner=owner,
        tags=tags,
        tasks=tasks,
        source_code=source,
    )


def _find_dag_name(tree: ast.Module) -> str | None:
    """Find first DAG('name', ...) or DAG(dag_id='name', ...) in the file."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) == "DAG":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    return arg.value
            for kw in node.keywords:
                if kw.arg in ("dag_id", "id") and isinstance(kw.value, ast.Constant):
                    return str(kw.value.value)
    return None


def _find_dag_schedule(tree: ast.Module) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) == "DAG":
            for kw in node.keywords:
                if kw.arg in ("schedule_interval", "schedule"):
                    if isinstance(kw.value, ast.Constant):
                        return str(kw.value.value) if kw.value.value is not None else None
    return None


def _find_dag_owner(tree: ast.Module) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id == "default_args" and isinstance(node.value, ast.Dict):
                    for k, v in zip(node.value.keys, node.value.values):
                        if isinstance(k, ast.Constant) and k.value == "owner":
                            if isinstance(v, ast.Constant):
                                return str(v.value)
    return None


def _find_dag_tags(tree: ast.Module) -> list[str]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) == "DAG":
            for kw in node.keywords:
                if kw.arg == "tags" and isinstance(kw.value, ast.List):
                    return [el.value for el in kw.value.elts if isinstance(el, ast.Constant)]
    return []


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _kw_value(node: ast.Call, name: str) -> ast.AST | None:
    for kw in node.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _kw_string(node: ast.Call, name: str) -> str | None:
    v = _kw_value(node, name)
    if isinstance(v, ast.Constant) and isinstance(v.value, str):
        return v.value
    return None


def _dict_to_python(node: ast.AST) -> Any:
    """Convert an ast.Dict to a Python dict (best-effort, only literals)."""
    if isinstance(node, ast.Dict):
        out = {}
        for k, v in zip(node.keys, node.values):
            key = _dict_to_python(k) if k else None
            out[key] = _dict_to_python(v)
        return out
    if isinstance(node, ast.List):
        return [_dict_to_python(e) for e in node.elts]
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.JoinedStr):  # f-string — return raw template
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            else:
                parts.append("{...}")
        return "".join(parts)
    return None


def _parse_bq_operator(node: ast.Call, op_name: str) -> DAGTask:
    task_id = _kw_string(node, "task_id") or "<anon>"
    sql: str | None = None
    destination: str | None = None

    config_node = _kw_value(node, "configuration")
    if config_node:
        config = _dict_to_python(config_node) or {}
        q = (config.get("query") or {}) if isinstance(config, dict) else {}
        if isinstance(q, dict):
            sql = q.get("query")
            dest = q.get("destinationTable")
            if isinstance(dest, dict):
                project = dest.get("projectId", "")
                dataset = dest.get("datasetId", "")
                table = dest.get("tableId", "")
                if project and dataset and table:
                    destination = f"{project}.{dataset}.{table}"

    sql = sql or _kw_string(node, "sql") or _kw_string(node, "query")
    destination = destination or _kw_string(node, "destination_dataset_table")

    refs = list(extract_refs(sql)) if sql else []
    return DAGTask(
        task_id=task_id, operator=op_name, sql=sql,
        destination_table=destination, referenced_tables=refs,
    )


def _parse_notebook_operator(node: ast.Call, op_name: str) -> DAGTask:
    task_id = _kw_string(node, "task_id") or "<anon>"
    notebook = (
        _kw_string(node, "notebook_uri")
        or _kw_string(node, "notebook_path")
        or _kw_string(node, "notebook")
        or _kw_string(node, "input_path")
    )
    return DAGTask(task_id=task_id, operator=op_name, notebook_uri=notebook)


def _parse_bq_table_operator(node: ast.Call, op_name: str) -> DAGTask:
    task_id = _kw_string(node, "task_id") or "<anon>"
    src = _kw_value(node, "source_project_dataset_tables")
    src_list = _dict_to_python(src) if src else []
    if isinstance(src_list, str):
        src_list = [src_list]
    dest = _kw_string(node, "destination_project_dataset_table")
    return DAGTask(
        task_id=task_id, operator=op_name,
        destination_table=dest, referenced_tables=list(src_list or []),
    )


def scan_composer_bucket(bucket_uri: str, sa_path: str | None = None) -> list[DAGMetadata]:
    """Scan a real Composer DAG bucket: gs://<bucket>/dags/*.py."""
    from google.cloud import storage

    if bucket_uri.startswith("gs://"):
        bucket_uri = bucket_uri[5:]
    parts = bucket_uri.split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else "dags/"

    client = (
        storage.Client.from_service_account_json(sa_path)
        if sa_path else storage.Client()
    )
    bucket = client.bucket(bucket_name)

    dags: list[DAGMetadata] = []
    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith(".py"):
            continue
        try:
            content = blob.download_as_text()
        except Exception:
            continue
        dag = parse_dag_file(f"gs://{bucket_name}/{blob.name}", content)
        if dag:
            dags.append(dag)
    return dags
