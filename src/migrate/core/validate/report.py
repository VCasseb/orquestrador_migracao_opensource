from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

REPORTS_DIR = Path(".migrate/reports")

Verdict = Literal["pass", "warn", "fail"]


class ColumnMetric(BaseModel):
    name: str
    type: str
    null_count: int = 0
    distinct_count: int = 0
    min: Any = None
    max: Any = None
    avg: float | None = None
    top_values: list[tuple[Any, int]] = Field(default_factory=list)


class TableMetrics(BaseModel):
    fqn: str
    row_count: int
    columns: list[ColumnMetric] = Field(default_factory=list)


class ColumnDiff(BaseModel):
    name: str
    null_count_diff_pct: float = 0.0
    distinct_count_diff_pct: float = 0.0
    avg_diff_pct: float | None = None
    verdict: Verdict = "pass"
    notes: list[str] = Field(default_factory=list)


class ValidationReport(BaseModel):
    fqn_source: str
    fqn_target: str
    created_at: datetime
    source: TableMetrics
    target: TableMetrics
    row_count_diff_pct: float
    columns_diff: list[ColumnDiff]
    verdict: Verdict
    diagnosis: str = ""
    is_synthetic: bool = False


def save_report(report: ValidationReport, dir: Path = REPORTS_DIR) -> Path:
    dir.mkdir(parents=True, exist_ok=True)
    safe_name = report.fqn_source.replace(".", "_")
    path = dir / f"{safe_name}.yaml"
    path.write_text(yaml.safe_dump(report.model_dump(mode="json"), sort_keys=False))
    return path


def load_report(path: Path) -> ValidationReport:
    return ValidationReport.model_validate(yaml.safe_load(path.read_text()))


def list_reports(dir: Path = REPORTS_DIR) -> list[Path]:
    if not dir.exists():
        return []
    return sorted(dir.glob("*.yaml"))
