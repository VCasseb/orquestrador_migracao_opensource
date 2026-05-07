"""LLM-driven code conversion for DAGs and notebooks.

Unlike the SQL/table conversion (sqlglot-first), DAGs and notebooks need
language-level rewriting that's only practical with an LLM:
  - Composer Airflow DAG → Databricks Workflow YAML / MWAA-compatible Python
  - Vertex Workbench notebook → Databricks notebook / PySpark on EMR

Target platform is configurable per-conversion (or via TARGET_PLATFORM env).
A custom_prompt slot lets the dev inject project-specific guidance.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.audit import hash_payload, timed

CONVERSIONS_DIR = Path(".migrate/conversions")

SourceType = Literal["airflow_dag", "vertex_notebook"]
TargetPlatform = Literal["databricks", "aws"]


_SYSTEM_PROMPTS = {
    ("airflow_dag", "databricks"): """You convert Apache Airflow DAGs (Cloud Composer flavor) into Databricks Asset Bundles (databricks.yml format) defining a Databricks Workflow.

Mapping rules:
- BigQueryInsertJobOperator → sql_task with Databricks SQL Warehouse, query rewritten to Spark SQL.
- VertexAINotebookExecuteOperator → notebook_task pointing at a Databricks notebook path.
- DataprocSubmitJobOperator → spark_jar_task or python_wheel_task on a job cluster.
- PythonOperator with pandas/google-cloud → notebook_task on an equivalent Databricks notebook (note this in a comment).
- SlackWebhookOperator / EmailOperator → use Databricks Workflow alerts or webhook_notifications.
- Sensors (GCSSensor, BigQueryTableExistenceSensor) → file_arrival or table_update triggers, or a Python notebook task that polls.
- Task dependencies (>>, set_downstream) → depends_on in the Workflow tasks.
- DAG schedule_interval → schedule.quartz_cron_expression (5-field quartz).

Output a single valid YAML document starting with `resources:` describing one workflow. Preserve the original DAG name. Add inline comments (# ...) where you made non-trivial substitutions.""",

    ("airflow_dag", "aws"): """You convert Apache Airflow DAGs (Cloud Composer flavor) into MWAA-compatible Airflow DAGs (Amazon Managed Workflows for Apache Airflow).

Replace Google Cloud providers with Amazon equivalents:
- BigQueryInsertJobOperator → AthenaOperator (s3_output) or RedshiftDataOperator.
- GCSToBigQueryOperator → S3ToRedshiftOperator or copy via S3CopyObjectOperator + AthenaOperator.
- VertexAINotebookExecuteOperator → SageMakerNotebookExecutionOperator (or KubernetesPodOperator running papermill).
- DataprocSubmitJobOperator → EmrAddStepsOperator + EmrStepSensor.
- GCSObjectExistenceSensor → S3KeySensor.
- BigQueryTableExistenceSensor → AthenaSensor.
- PubSubPublishMessageOperator → SnsPublishOperator or SqsPublishOperator.

Connections: replace `bigquery_default` / `google_cloud_default` with `aws_default`.
Imports: change `airflow.providers.google.cloud.*` to `airflow.providers.amazon.aws.*`.

Output valid Python code using `with DAG(...) as dag:` syntax. Keep the original DAG id. Add # MIGRATED-FROM-GCP comments where substitutions are non-obvious.""",

    ("vertex_notebook", "databricks"): """You convert a Vertex AI Workbench / GCS Jupyter notebook into a Databricks notebook (Python source format with `# COMMAND ----------` cell separators and `# MAGIC %md` for markdown).

Rewrites:
- `from google.cloud import bigquery` + `client.query(sql).to_dataframe()` → `spark.sql(sql).toPandas()` (only when small) or keep as Spark DataFrame `spark.sql(sql)`.
- `pandas_gbq.to_gbq(df, 'project.dataset.table')` → `spark.createDataFrame(df).write.format('delta').mode('overwrite').saveAsTable('catalog.schema.table')`.
- `from google.cloud import storage` + `bucket.blob(p).download_to_filename(...)` → `dbutils.fs.cp('s3://.../path', 'file:/tmp/...')` or read directly via Spark.
- `from google.cloud import aiplatform` (Vertex AI) → MLflow + Databricks MLR.
- `import pandas as pd` reads/writes from local paths → use Volumes (`/Volumes/<catalog>/<schema>/<vol>/...`) or DBFS.

Preserve cell structure and markdown. Add `dbutils.widgets` at the top for any parameters that were passed via Vertex notebook execution. Add # NOTE: comments where the rewrite is approximate.""",

    ("vertex_notebook", "aws"): """You convert a Vertex AI Workbench notebook into a PySpark notebook to run on AWS EMR (or Glue ETL job).

Rewrites:
- `from google.cloud import bigquery` → `boto3.client('athena')` or `awswrangler.athena.read_sql_query(...)`.
- `pandas_gbq.read_gbq` → `awswrangler.athena.read_sql_query` or `spark.read.format('jdbc')` from Redshift.
- `pandas_gbq.to_gbq` → `awswrangler.s3.to_parquet` + Glue Catalog table, or write to Redshift.
- `from google.cloud import storage` → `boto3.client('s3')` or `awswrangler.s3.read_*` / `to_*`.
- `from google.cloud import aiplatform` → SageMaker SDK (`boto3.client('sagemaker')` or `sagemaker` Python SDK).

Use `awswrangler` (`awswrangler.athena`, `awswrangler.s3`) for high-level read/write where it simplifies. Otherwise use boto3.
Output as `.py` notebook (Glue-compatible). Add # NOTE: where mapping is approximate.""",
}


@dataclass
class CodeConversionRequest:
    name: str
    source_type: SourceType
    target_platform: TargetPlatform
    source_code: str
    custom_prompt: str = ""
    additional_context: dict[str, Any] | None = None


class CodeConversionArtifact(BaseModel):
    name: str
    source_type: SourceType
    target_platform: TargetPlatform
    source_code: str
    converted_code: str
    output_path: str               # local file with the converted artifact
    target_suggested_path: str     # where it would land on the target platform
    custom_prompt_used: str = ""
    llm_model_used: str = ""
    notes: list[str] = Field(default_factory=list)
    error: str | None = None
    converted_at: datetime
    input_sha: str = ""
    output_sha: str = ""


def _llm_call(system: str, user: str) -> tuple[str, str]:
    """Dispatches to whichever LLM_PROVIDER is active (anthropic / openai / gemini / bedrock)."""
    from migrate.core.llm import complete
    text, model = complete(system, user, max_tokens=8000)
    if text.startswith("```"):
        lines = [ln for ln in text.splitlines() if not ln.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text, model


def _suggest_target_path(req: CodeConversionRequest) -> str:
    if req.target_platform == "databricks":
        prefix = get_env("TARGET_DATABRICKS_WORKSPACE_PREFIX", "/Workspace/migration")
        if req.source_type == "airflow_dag":
            return f"{prefix}/workflows/{req.name}.yaml"
        return f"{prefix}/notebooks/{req.name}"
    # aws
    if req.source_type == "airflow_dag":
        prefix = get_env("TARGET_MWAA_DAGS_PREFIX", "s3://acme-mwaa-bucket/dags")
        return f"{prefix}/{req.name}.py"
    prefix = get_env("TARGET_S3_NOTEBOOKS_PREFIX", "s3://acme-data-notebooks/migration")
    return f"{prefix}/{req.name}.py"


def _output_filename(req: CodeConversionRequest) -> tuple[str, str]:
    """Returns (artifact_filename, file_extension)."""
    base = f"{req.source_type}_{req.name}".replace(".", "_").replace("/", "_")
    if req.source_type == "airflow_dag" and req.target_platform == "databricks":
        return f"{base}.databricks-workflow.yaml", ".yaml"
    return f"{base}.py", ".py"


def convert_code(req: CodeConversionRequest) -> CodeConversionArtifact:
    """LLM converts the source code to the target platform with optional custom prompt."""
    with timed("convert_code", fqn=f"{req.source_type}/{req.name}") as ctx:
        ctx.update({
            "source_type": req.source_type,
            "target_platform": req.target_platform,
            "has_custom_prompt": bool(req.custom_prompt),
        })

        system = _SYSTEM_PROMPTS.get((req.source_type, req.target_platform))
        if not system:
            raise RuntimeError(
                f"No conversion prompt configured for ({req.source_type}, {req.target_platform})"
            )

        if req.custom_prompt.strip():
            system = system + "\n\n# Project-specific instructions from the user\n" + req.custom_prompt.strip()

        ctx_block = ""
        if req.additional_context:
            ctx_block = "\n\n# Additional context (from inventory)\n```yaml\n" + \
                yaml.safe_dump(req.additional_context, sort_keys=False) + "```"

        user_msg = (
            f"Convert this {req.source_type} (named `{req.name}`) to "
            f"{req.target_platform}. Reply with ONLY the converted code, no markdown fences, no prose.\n\n"
            f"# Source\n```\n{req.source_code}\n```{ctx_block}"
        )

        notes: list[str] = []
        error: str | None = None
        converted_code = ""
        model = ""
        try:
            converted_code, model = _llm_call(system, user_msg)
        except Exception as e:
            error = str(e)
            notes.append(f"LLM call failed: {e}")

        artifact_name, ext = _output_filename(req)
        target_path = _suggest_target_path(req)

        CONVERSIONS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = CONVERSIONS_DIR / artifact_name
        if converted_code:
            out_path.write_text(converted_code)

        artifact = CodeConversionArtifact(
            name=req.name,
            source_type=req.source_type,
            target_platform=req.target_platform,
            source_code=req.source_code,
            converted_code=converted_code,
            output_path=str(out_path),
            target_suggested_path=target_path,
            custom_prompt_used=req.custom_prompt,
            llm_model_used=model,
            notes=notes,
            error=error,
            converted_at=datetime.now(timezone.utc),
            input_sha=hash_payload(req.source_code),
            output_sha=hash_payload(converted_code),
        )

        meta_path = CONVERSIONS_DIR / f"{Path(artifact_name).stem}.meta.yaml"
        meta_path.write_text(yaml.safe_dump(artifact.model_dump(mode="json"), sort_keys=False))

        ctx.update({"output_path": str(out_path), "model": model})
        return artifact


def convert_dag(name: str, target: TargetPlatform | None = None, custom_prompt: str = "") -> CodeConversionArtifact:
    inv = load_inventory()
    if not inv:
        raise RuntimeError("No inventory loaded. Run scan first.")
    dag = inv.dags_by_id.get(name)
    if not dag:
        raise RuntimeError(f"DAG `{name}` not in inventory. Available: {list(inv.dags_by_id)[:10]}")
    if not dag.source_code:
        raise RuntimeError(f"DAG `{name}` has no captured source_code (real Composer scan would store it).")

    target = target or get_env("TARGET_PLATFORM", "databricks")
    additional = {
        "schedule": dag.schedule,
        "owner": dag.owner,
        "tags": dag.tags,
        "tasks_summary": [
            {"task_id": t.task_id, "operator": t.operator,
             "writes": t.destination_table, "reads": t.referenced_tables,
             "notebook_uri": t.notebook_uri}
            for t in dag.tasks
        ],
    }
    return convert_code(CodeConversionRequest(
        name=name, source_type="airflow_dag", target_platform=target,
        source_code=dag.source_code, custom_prompt=custom_prompt,
        additional_context=additional,
    ))


def convert_notebook(name: str, target: TargetPlatform | None = None, custom_prompt: str = "") -> CodeConversionArtifact:
    inv = load_inventory()
    if not inv:
        raise RuntimeError("No inventory loaded. Run scan first.")
    nb = inv.notebooks_by_id.get(name)
    if not nb:
        raise RuntimeError(f"Notebook `{name}` not in inventory. Available: {list(inv.notebooks_by_id)[:10]}")

    # Reconstruct .ipynb-like flat source (cell-by-cell with markers)
    parts = []
    for c in nb.cells:
        if c.cell_type == "markdown":
            parts.append(f"# === MARKDOWN ===\n{c.source}\n")
        else:
            parts.append(f"# === CODE ===\n{c.source}\n")
    source_code = "\n".join(parts)

    target = target or get_env("TARGET_PLATFORM", "databricks")
    additional = {
        "kind": nb.kind,
        "location": nb.location,
        "libraries_detected": nb.libraries,
        "tables_read": nb.all_referenced_tables,
        "tables_written": nb.all_written_tables,
    }
    return convert_code(CodeConversionRequest(
        name=name, source_type="vertex_notebook", target_platform=target,
        source_code=source_code, custom_prompt=custom_prompt,
        additional_context=additional,
    ))


def list_code_conversions() -> list[Path]:
    if not CONVERSIONS_DIR.exists():
        return []
    return sorted(p for p in CONVERSIONS_DIR.glob("*.meta.yaml")
                  if p.stem.startswith(("airflow_dag_", "vertex_notebook_")))


def load_code_conversion(meta_path: Path) -> CodeConversionArtifact:
    return CodeConversionArtifact.model_validate(yaml.safe_load(meta_path.read_text()))
