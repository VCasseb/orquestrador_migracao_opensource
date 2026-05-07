from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ValidationThresholds(BaseModel):
    row_count_tolerance_pct: float = 0.0
    null_pct_drift_alert: float = 5.0
    distribution_drift_alert_pct: float = 10.0


class MigrationConfig(BaseModel):
    """Non-sensitive project config. Lives in .migrate/config.yaml (git-tracked)."""

    version: int = 1
    gcp_project_ids: list[str] = Field(default_factory=list)
    databricks_default_catalog: str = "migrated_from_gcp"
    default_concurrency: int = 4
    validation: ValidationThresholds = Field(default_factory=ValidationThresholds)


CONFIG_DIR = Path(".migrate")
CONFIG_FILE = CONFIG_DIR / "config.yaml"


def load_config(path: Path = CONFIG_FILE) -> MigrationConfig:
    if not path.exists():
        return MigrationConfig()
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    return MigrationConfig.model_validate(raw)


def save_config(config: MigrationConfig, path: Path = CONFIG_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config.model_dump(), sort_keys=False))
