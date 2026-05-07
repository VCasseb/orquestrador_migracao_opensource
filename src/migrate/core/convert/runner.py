from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from migrate.core.convert.ddl import build_delta_ddl
from migrate.core.convert.notebook import render_notebook
from migrate.core.convert.sql import TranspileResult, transpile_bq_to_databricks
from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.catalog import load_inventory
from migrate.core.inventory.models import TableMetadata
from migrate.core.state.audit import hash_payload, timed

CONVERSIONS_DIR = Path(".migrate/conversions")


class ConversionArtifact(BaseModel):
    fqn: str
    converted_sql: str | None = None
    ddl: str
    notebook_path: str | None = None
    confidence: str
    method: str
    notes: list[str]
    llm_model_used: str | None = None
    error: str | None = None
    converted_at: datetime


def _save_artifacts(art: ConversionArtifact, table: TableMetadata, ddl: str, notebook: str | None) -> ConversionArtifact:
    CONVERSIONS_DIR.mkdir(parents=True, exist_ok=True)
    base = table.fqn.replace(".", "_")

    if art.converted_sql:
        (CONVERSIONS_DIR / f"{base}.sql").write_text(art.converted_sql)
    (CONVERSIONS_DIR / f"{base}.ddl.sql").write_text(ddl)
    if notebook:
        nb_path = CONVERSIONS_DIR / f"{base}.notebook.py"
        nb_path.write_text(notebook)
        art.notebook_path = str(nb_path)

    (CONVERSIONS_DIR / f"{base}.meta.yaml").write_text(
        yaml.safe_dump(art.model_dump(mode="json"), sort_keys=False)
    )
    return art


def convert_table(
    fqn: str,
    use_llm_fallback: bool = True,
    target_catalog: str | None = None,
) -> ConversionArtifact:
    with timed("convert", fqn=fqn) as ctx:
        return _do_convert(fqn, use_llm_fallback, target_catalog, ctx)


def _do_convert(fqn: str, use_llm_fallback: bool, target_catalog: str | None, ctx: dict) -> ConversionArtifact:
    load_env()
    inv = load_inventory()
    if not inv:
        raise RuntimeError("No inventory loaded. Run a scan first.")
    table = inv.by_fqn.get(fqn)
    if not table:
        raise RuntimeError(f"Not found: {fqn}")

    target_catalog = target_catalog or get_env("DATABRICKS_DEFAULT_CATALOG", "migrated_from_gcp")
    ddl = build_delta_ddl(table, target_catalog)

    converted_sql: str | None = None
    confidence = "n/a"
    method = "none"
    notes: list[str] = []
    error: str | None = None
    llm_model: str | None = None

    bq_sql = table.view_query or table.source_sql
    if bq_sql:
        if not table.view_query and table.source_sql:
            notes.append(f"Source extracted from {table.source_kind} ({table.source_dag_id or table.source_scheduled_query_id or table.source_notebook_id or '—'}).")
        result: TranspileResult = transpile_bq_to_databricks(bq_sql)
        notes = notes + list(result.notes)
        if result.method == "sqlglot" and not result.error:
            converted_sql = result.converted_sql
            confidence = "high"
            method = "sqlglot"
        elif use_llm_fallback:
            try:
                from migrate.core.convert.llm import llm_repair
                converted_sql, llm_model = llm_repair(bq_sql, result.error)
                confidence = "medium"
                method = "llm"
                notes.append(f"sqlglot failed; repaired with {llm_model} — needs human review.")
            except Exception as e:
                error = f"sqlglot failed and LLM fallback unavailable: {e}"
                confidence = "0%"
                method = "failed"
        else:
            error = result.error
            confidence = "0%"
            method = "failed"
    else:
        notes.append(
            f"Source is a TABLE without recorded SQL (source_kind={table.source_kind}) — only DDL generated."
        )
        if table.source_kind in ("dag_python", "dag_notebook", "manual"):
            notes.append("Producer is non-SQL (Python/notebook/manual). Notebook generation skipped — port producer separately.")
        method = "ddl-only"
        confidence = "high"

    notebook_text = None
    if converted_sql:
        try:
            notebook_text = render_notebook(table, converted_sql, target_catalog)
        except Exception as e:
            notes.append(f"Notebook render failed: {e}")

    if table.source_schedule:
        notes.append(f"Source schedule `{table.source_schedule}` — recommended Workflow cron.")

    art = ConversionArtifact(
        fqn=fqn,
        converted_sql=converted_sql,
        ddl=ddl,
        confidence=confidence,
        method=method,
        notes=notes,
        llm_model_used=llm_model,
        error=error,
        converted_at=datetime.now(timezone.utc),
    )
    saved = _save_artifacts(art, table, ddl, notebook_text)
    ctx.update({
        "method": method,
        "confidence": confidence,
        "input_sha": hash_payload(table.view_query),
        "output_sha": hash_payload(converted_sql),
        "ddl_sha": hash_payload(ddl),
        "artifacts_count": 3 if notebook_text else 2,
    })
    return saved


def list_conversions(dir: Path = CONVERSIONS_DIR) -> list[Path]:
    if not dir.exists():
        return []
    return sorted(dir.glob("*.meta.yaml"))


def load_conversion(fqn: str) -> ConversionArtifact | None:
    base = fqn.replace(".", "_")
    path = CONVERSIONS_DIR / f"{base}.meta.yaml"
    if not path.exists():
        return None
    return ConversionArtifact.model_validate(yaml.safe_load(path.read_text()))
