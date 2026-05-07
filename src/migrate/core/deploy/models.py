from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

DEPLOYMENTS_DIR = Path(".migrate/deployments")

DeployStatus = Literal[
    "pending", "provisioning", "loading", "uploading_notebook",
    "creating_workflow", "completed", "failed", "skipped",
]


class DeploymentState(BaseModel):
    fqn: str
    target_fqn: str
    status: DeployStatus = "pending"
    mode: Literal["sample", "dry-run", "execute"] = "sample"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_step: str = ""
    notebook_workspace_path: str | None = None
    workflow_id: int | None = None
    rows_loaded: int | None = None
    input_hash: str = ""
    output_hash: str = ""
    errors: list[str] = Field(default_factory=list)
    steps_log: list[dict] = Field(default_factory=list)


def state_path(fqn: str) -> Path:
    return DEPLOYMENTS_DIR / f"{fqn.replace('.', '_')}.yaml"


def save_state(state: DeploymentState) -> Path:
    DEPLOYMENTS_DIR.mkdir(parents=True, exist_ok=True)
    p = state_path(state.fqn)
    p.write_text(yaml.safe_dump(state.model_dump(mode="json"), sort_keys=False))
    return p


def load_state(fqn: str) -> DeploymentState | None:
    p = state_path(fqn)
    if not p.exists():
        return None
    return DeploymentState.model_validate(yaml.safe_load(p.read_text()))


def list_states() -> list[DeploymentState]:
    if not DEPLOYMENTS_DIR.exists():
        return []
    out = []
    for p in sorted(DEPLOYMENTS_DIR.glob("*.yaml")):
        if p.name.endswith(".rolled-back.yaml"):
            continue
        try:
            out.append(DeploymentState.model_validate(yaml.safe_load(p.read_text())))
        except Exception:
            continue
    return out
