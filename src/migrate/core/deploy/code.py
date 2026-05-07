"""Code deploy: takes saved code conversions (DAGs / notebooks) and uploads
them to the target platform.

Sources:
  .migrate/conversions/airflow_dag_<name>.{py|yaml}
  .migrate/conversions/vertex_notebook_<name>.py

Targets:
  databricks: WorkspaceClient.workspace.upload  (notebooks → /Workspace/...)
              + jobs.create for DAG → Workflow YAML (asset bundle)
  aws:        boto3 s3.put_object  (notebooks → s3://...notebooks/...
              DAGs → s3://...mwaa-bucket/dags/...)

State persisted in .migrate/deployments/code_<source_type>_<name>.yaml so
re-deploys are idempotent (skip if input_hash unchanged).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

from migrate.core.convert.code import load_code_conversion
from migrate.core.credentials import get_env, load_env
from migrate.core.state.audit import timed

CODE_DEPLOYMENTS_DIR = Path(".migrate/deployments")

CodeDeployStatus = Literal["pending", "uploading", "completed", "failed", "skipped"]
CodeDeployMode = Literal["sample", "execute"]


class CodeDeploymentState(BaseModel):
    name: str
    source_type: str             # airflow_dag | vertex_notebook
    target_platform: str         # databricks | aws
    status: CodeDeployStatus = "pending"
    mode: CodeDeployMode = "sample"
    local_path: str = ""
    target_path: str = ""
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_step: str = ""
    input_hash: str = ""
    errors: list[str] = Field(default_factory=list)
    steps_log: list[dict] = Field(default_factory=list)

    @property
    def state_filename(self) -> str:
        safe = self.name.replace(".", "_").replace("/", "_")
        return f"code_{self.source_type}_{safe}.yaml"


def _state_path(state: CodeDeploymentState) -> Path:
    return CODE_DEPLOYMENTS_DIR / state.state_filename


def save_code_state(state: CodeDeploymentState) -> Path:
    CODE_DEPLOYMENTS_DIR.mkdir(parents=True, exist_ok=True)
    p = _state_path(state)
    p.write_text(yaml.safe_dump(state.model_dump(mode="json"), sort_keys=False))
    return p


def load_code_state(source_type: str, name: str) -> CodeDeploymentState | None:
    safe = name.replace(".", "_").replace("/", "_")
    p = CODE_DEPLOYMENTS_DIR / f"code_{source_type}_{safe}.yaml"
    if not p.exists():
        return None
    return CodeDeploymentState.model_validate(yaml.safe_load(p.read_text()))


def list_code_deployments() -> list[CodeDeploymentState]:
    if not CODE_DEPLOYMENTS_DIR.exists():
        return []
    out = []
    for p in sorted(CODE_DEPLOYMENTS_DIR.glob("code_*.yaml")):
        try:
            out.append(CodeDeploymentState.model_validate(yaml.safe_load(p.read_text())))
        except Exception:
            continue
    return out


def _step(state: CodeDeploymentState, name: str) -> None:
    state.last_step = name
    state.steps_log.append({"step": name, "ts": datetime.now(timezone.utc).isoformat()})
    save_code_state(state)


def deploy_code_artifact(meta_path: Path, mode: CodeDeployMode = "sample") -> CodeDeploymentState:
    """Deploy a single saved code conversion.

    `meta_path` is the .meta.yaml from .migrate/conversions/.
    """
    with timed("deploy_code", fqn=meta_path.stem) as ctx:
        ctx["mode"] = mode
        return _do_deploy_code(meta_path, mode, ctx)


def _do_deploy_code(meta_path: Path, mode: CodeDeployMode, ctx: dict) -> CodeDeploymentState:
    load_env()
    artifact = load_code_conversion(meta_path)

    state = load_code_state(artifact.source_type, artifact.name) or CodeDeploymentState(
        name=artifact.name,
        source_type=artifact.source_type,
        target_platform=artifact.target_platform,
        local_path=artifact.output_path,
        target_path=artifact.target_suggested_path,
    )
    state.target_platform = artifact.target_platform
    state.local_path = artifact.output_path
    state.target_path = artifact.target_suggested_path
    state.mode = mode
    state.errors = []
    state.steps_log = []

    if state.status == "completed" and state.input_hash == artifact.output_sha:
        state.last_step = "idempotent skip — already deployed at this hash"
        save_code_state(state)
        ctx["skipped"] = True
        return state
    state.input_hash = artifact.output_sha
    state.status = "uploading"
    state.started_at = datetime.now(timezone.utc)
    save_code_state(state)

    local = Path(artifact.output_path)
    if not local.exists():
        state.status = "failed"
        state.errors.append(f"Local artifact missing: {local}")
        save_code_state(state)
        raise RuntimeError(f"Local artifact missing: {local}")

    if mode == "sample":
        _step(state, f"would upload to {state.target_path}")
        time.sleep(0.5)
        _step(state, "simulated upload OK")
    else:
        try:
            if state.target_platform == "databricks":
                _upload_databricks(state, local, artifact)
            elif state.target_platform == "aws":
                _upload_aws(state, local)
            else:
                raise RuntimeError(f"Unknown target_platform: {state.target_platform}")
        except Exception as e:
            state.status = "failed"
            state.errors.append(str(e))
            save_code_state(state)
            raise

    state.status = "completed"
    state.completed_at = datetime.now(timezone.utc)
    save_code_state(state)
    ctx.update({"target": state.target_path, "platform": state.target_platform})
    return state


def _upload_databricks(state: CodeDeploymentState, local: Path, artifact) -> None:
    """For notebooks: workspace.upload as SOURCE python.
    For DAG/asset bundle YAML: upload as a workspace file (binary)."""
    import base64
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat, Language

    host = get_env("DATABRICKS_HOST")
    token = get_env("DATABRICKS_TOKEN")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST / DATABRICKS_TOKEN not configured")

    client = WorkspaceClient(host=host, token=token)
    target = state.target_path
    parent = "/".join(target.split("/")[:-1])
    if parent:
        try:
            client.workspace.mkdirs(parent)
        except Exception:
            pass

    content = local.read_bytes()
    encoded = base64.b64encode(content).decode()

    if artifact.source_type == "vertex_notebook":
        _step(state, f"upload notebook → {target} (SOURCE format, Python)")
        client.workspace.import_(
            path=target, format=ImportFormat.SOURCE,
            language=Language.PYTHON, content=encoded, overwrite=True,
        )
    else:
        # asset-bundle YAML or any non-notebook artifact — store as a workspace file
        _step(state, f"upload asset → {target} (workspace file)")
        try:
            from databricks.sdk.service.workspace import ImportFormat as F
            client.workspace.import_(
                path=target, format=F.AUTO,
                content=encoded, overwrite=True,
            )
        except Exception as e:
            raise RuntimeError(f"databricks workspace upload failed: {e}") from e


def _upload_aws(state: CodeDeploymentState, local: Path) -> None:
    """Upload to S3. target_path looks like s3://bucket/key."""
    import boto3
    target = state.target_path
    if not target.startswith("s3://"):
        raise RuntimeError(f"target_path is not s3://: {target}")
    rest = target[5:]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise RuntimeError(f"malformed s3 target: {target}")

    region = get_env("AWS_REGION", "us-east-1") or "us-east-1"
    s3 = boto3.client("s3", region_name=region)

    _step(state, f"s3.put_object → s3://{bucket}/{key}")
    with local.open("rb") as f:
        s3.put_object(Bucket=bucket, Key=key, Body=f.read())
