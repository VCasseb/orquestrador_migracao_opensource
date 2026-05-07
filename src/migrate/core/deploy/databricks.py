"""Thin wrappers over databricks-sdk for the operations the deploy phase needs."""
from __future__ import annotations

from migrate.core.credentials import get_env, load_env


def _client():
    from databricks.sdk import WorkspaceClient
    load_env()
    host = get_env("DATABRICKS_HOST")
    token = get_env("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("Databricks credentials not set")
    return WorkspaceClient(host=host, token=token)


def _warehouse_id() -> str:
    wid = get_env("DATABRICKS_SQL_WAREHOUSE_ID")
    if not wid:
        raise RuntimeError("DATABRICKS_SQL_WAREHOUSE_ID not set")
    return wid


def execute_sql(sql: str, *, wait_timeout: str = "60s") -> dict:
    client = _client()
    resp = client.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        wait_timeout=wait_timeout,
    )
    return {"statement_id": resp.statement_id, "state": str(resp.status.state) if resp.status else ""}


def count_rows(target_fqn: str) -> int:
    client = _client()
    resp = client.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=f"SELECT COUNT(*) AS c FROM {target_fqn}",
        wait_timeout="30s",
    )
    if resp.result and resp.result.data_array:
        return int(resp.result.data_array[0][0])
    return 0


def upload_notebook(workspace_path: str, content: str) -> str:
    """Upload a notebook (Python source format) to the workspace."""
    import base64
    from databricks.sdk.service.workspace import ImportFormat, Language
    client = _client()
    parent = "/".join(workspace_path.split("/")[:-1])
    if parent:
        try:
            client.workspace.mkdirs(parent)
        except Exception:
            pass
    encoded = base64.b64encode(content.encode()).decode()
    client.workspace.import_(
        path=workspace_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=encoded,
        overwrite=True,
    )
    return workspace_path


def remove_notebook(workspace_path: str) -> None:
    client = _client()
    client.workspace.delete(path=workspace_path, recursive=False)


def create_workflow(name: str, notebook_path: str) -> int:
    """Create a single-task Databricks Job pointing at the notebook."""
    from databricks.sdk.service.jobs import NotebookTask, Task
    client = _client()
    task = Task(
        task_key="run",
        notebook_task=NotebookTask(notebook_path=notebook_path),
    )
    resp = client.jobs.create(name=name, tasks=[task])
    return resp.job_id


def delete_workflow(job_id: int) -> None:
    client = _client()
    client.jobs.delete(job_id=job_id)


def drop_delta_table(fqn: str) -> None:
    execute_sql(f"DROP TABLE IF EXISTS {fqn}")
