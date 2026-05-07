"""Deploy orchestrator: provision Delta target, load data, upload notebook, create workflow.

Modes:
  - sample : simulate everything with delays — useful for demo / dev without creds
  - dry-run: print actions, no side effects
  - execute: real Databricks calls

State is persisted per-table; the runner resumes from where it stopped.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal

from migrate.core.convert.runner import load_conversion
from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.catalog import load_inventory
from migrate.core.plan.waves import MigrationPlan, load_plan
from migrate.core.state.approval import is_approved
from migrate.core.state.audit import hash_payload, log_action, timed
from migrate.core.deploy.models import DeploymentState, load_state, save_state, list_states

Mode = Literal["sample", "dry-run", "execute"]


def load_deployment(fqn: str) -> DeploymentState | None:
    return load_state(fqn)


def list_deployments() -> list[DeploymentState]:
    return list_states()


def _input_hash_for(fqn: str) -> str:
    art = load_conversion(fqn)
    if not art:
        return ""
    payload = (art.converted_sql or "") + "||" + art.ddl
    return hash_payload(payload)


def _step(state: DeploymentState, name: str, fn: Callable[[], dict | None]) -> dict | None:
    state.last_step = name
    state.steps_log.append({"step": name, "started": datetime.now(timezone.utc).isoformat()})
    save_state(state)
    try:
        return fn()
    except Exception as e:
        state.errors.append(f"{name}: {e}")
        state.steps_log[-1]["error"] = str(e)
        raise


def deploy_one(
    fqn: str,
    mode: Mode = "sample",
    require_approvals: bool = True,
    create_workflow: bool = False,
    auto_validate: bool = False,
) -> DeploymentState:
    """Deploy a single table. Returns final state. If `auto_validate` is True
    and the deploy succeeds, runs validation immediately and saves the report."""
    with timed("deploy_one", fqn=fqn) as ctx:
        ctx["mode"] = mode
        state = _do_deploy(fqn, mode, require_approvals, create_workflow, ctx)
        if auto_validate and state.status == "completed":
            try:
                _run_post_deploy_validation(fqn, mode)
                ctx["validated"] = True
            except Exception as e:
                state.errors.append(f"auto-validate: {e}")
                state.steps_log.append({"step": "auto-validate failed", "error": str(e)})
                save_state(state)
        return state


def _run_post_deploy_validation(fqn: str, mode: Mode) -> None:
    """Trigger validation after a successful deploy, saving the report."""
    from migrate.core.validate.report import save_report
    if mode == "sample":
        from migrate.core.validate.sample import build_synthetic_report
        report = build_synthetic_report(fqn)
    else:
        from migrate.core.validate.runner import run_validation
        report = run_validation(fqn)
    save_report(report)


def _do_deploy(fqn: str, mode: Mode, require_approvals: bool, create_workflow: bool, ctx: dict) -> DeploymentState:
    load_env()

    inv = load_inventory()
    if not inv:
        raise RuntimeError("No inventory loaded.")
    table = inv.by_fqn.get(fqn)
    if not table:
        raise RuntimeError(f"Not found: {fqn}")

    artifact = load_conversion(fqn)
    if not artifact:
        raise RuntimeError(f"No conversion artifact for {fqn} — run convert first.")

    if require_approvals and mode == "execute":
        if not is_approved(fqn, "conversion"):
            raise RuntimeError(f"{fqn}: conversion not approved — block deploy")

    catalog = get_env("DATABRICKS_DEFAULT_CATALOG", "migrated_from_gcp")
    target_fqn = f"{catalog}.{table.dataset}.{table.name}"
    notebook_path = f"/Workspace/migration/{table.dataset}/{table.name}"

    state = load_state(fqn) or DeploymentState(fqn=fqn, target_fqn=target_fqn, mode=mode)
    state.target_fqn = target_fqn
    state.mode = mode
    state.errors = []
    state.steps_log = []

    new_hash = _input_hash_for(fqn)
    if state.status == "completed" and state.input_hash == new_hash:
        ctx["skipped"] = True
        state.last_step = "idempotent skip"
        save_state(state)
        return state
    state.input_hash = new_hash

    state.status = "provisioning"
    state.started_at = datetime.now(timezone.utc)
    save_state(state)

    if mode == "sample":
        return _deploy_sample(state, table, artifact, notebook_path, create_workflow)
    if mode == "dry-run":
        return _deploy_dry_run(state, artifact, notebook_path, create_workflow)
    return _deploy_execute(state, artifact, notebook_path, create_workflow)


def _deploy_sample(state: DeploymentState, table, artifact, notebook_path: str, create_wf: bool) -> DeploymentState:
    _step(state, "provision: CREATE TABLE Delta (simulated)", lambda: time.sleep(0.6) or None)
    state.status = "loading"
    save_state(state)

    rows = table.row_count or 100_000
    _step(state, f"load: federation CTAS ({rows:,} rows, simulated)",
          lambda: time.sleep(0.8 + min(2.0, rows / 5_000_000_000)) or None)
    state.rows_loaded = rows

    if artifact.notebook_path:
        state.status = "uploading_notebook"
        save_state(state)
        _step(state, f"upload notebook → {notebook_path} (simulated)",
              lambda: time.sleep(0.4) or None)
        state.notebook_workspace_path = notebook_path

    if create_wf and artifact.notebook_path:
        state.status = "creating_workflow"
        save_state(state)
        _step(state, "create Databricks workflow (simulated)",
              lambda: time.sleep(0.4) or None)
        state.workflow_id = 99000 + (hash(state.fqn) % 1000)

    state.status = "completed"
    state.completed_at = datetime.now(timezone.utc)
    state.last_step = "completed (sample mode)"
    save_state(state)
    return state


def _deploy_dry_run(state: DeploymentState, artifact, notebook_path: str, create_wf: bool) -> DeploymentState:
    plan = []
    plan.append(f"[DDL] {state.target_fqn}")
    if artifact.converted_sql:
        plan.append(f"[LOAD] CTAS into {state.target_fqn} from converted SQL ({len(artifact.converted_sql)} chars)")
    else:
        plan.append(f"[LOAD] (skipped — base table; load via federation or Auto Loader manually)")
    if artifact.notebook_path:
        plan.append(f"[NOTEBOOK] upload to {notebook_path}")
        if create_wf:
            plan.append(f"[WORKFLOW] create Databricks Job pointing at {notebook_path}")

    state.steps_log.append({"step": "dry-run plan", "items": plan})
    state.status = "completed"
    state.last_step = "dry-run"
    state.completed_at = datetime.now(timezone.utc)
    save_state(state)
    return state


def _deploy_execute(state: DeploymentState, artifact, notebook_path: str, create_wf: bool) -> DeploymentState:
    from migrate.core.deploy.databricks import (
        execute_sql, upload_notebook, create_workflow as create_wf_fn, count_rows,
    )

    _step(state, f"DDL: {state.target_fqn}", lambda: execute_sql(artifact.ddl))

    if artifact.converted_sql:
        state.status = "loading"
        save_state(state)
        ctas = (
            f"CREATE OR REPLACE TABLE {state.target_fqn} AS\n"
            f"{artifact.converted_sql}"
        )
        _step(state, "load: CTAS from converted SQL", lambda: execute_sql(ctas, wait_timeout="600s"))
        state.rows_loaded = count_rows(state.target_fqn)
    else:
        state.steps_log.append({
            "step": "load skipped",
            "note": "base table — load externally (federation/Auto Loader)",
        })

    if artifact.notebook_path:
        state.status = "uploading_notebook"
        save_state(state)
        nb_text = Path(artifact.notebook_path).read_text()
        _step(state, f"upload notebook → {notebook_path}",
              lambda: upload_notebook(notebook_path, nb_text))
        state.notebook_workspace_path = notebook_path

    if create_wf and artifact.notebook_path:
        state.status = "creating_workflow"
        save_state(state)
        wf_name = f"migrate-{state.target_fqn.replace('.', '-')}"
        state.workflow_id = _step(state, "create workflow", lambda: create_wf_fn(wf_name, notebook_path))

    state.status = "completed"
    state.completed_at = datetime.now(timezone.utc)
    state.last_step = "completed (execute mode)"
    save_state(state)
    return state


def deploy_plan(
    plan_name_or_path: str,
    mode: Mode = "sample",
    concurrency: int = 4,
    require_approvals: bool = True,
    stop_on_failure: bool = False,
    create_workflow: bool = False,
    auto_validate: bool = False,
) -> dict:
    """Run waves serially; tables within a wave run in parallel up to `concurrency`."""
    p = Path(plan_name_or_path)
    if not p.exists() and not plan_name_or_path.endswith(".yaml"):
        p = Path(".migrate/plans") / f"{plan_name_or_path}.yaml"
    if not p.exists():
        raise RuntimeError(f"Plan not found: {plan_name_or_path}")
    plan: MigrationPlan = load_plan(p)

    summary = {"plan": plan.name, "mode": mode, "waves": []}
    for wave in plan.waves:
        wave_summary = {"index": wave.index, "tables": [], "ok": 0, "failed": 0, "skipped": 0}
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = {
                ex.submit(deploy_one, it.fqn, mode, require_approvals, create_workflow, auto_validate): it.fqn
                for it in wave.items
            }
            for fut in as_completed(futures):
                fqn = futures[fut]
                try:
                    state = fut.result()
                    wave_summary["tables"].append({"fqn": fqn, "status": state.status, "rows": state.rows_loaded})
                    if state.status == "completed":
                        wave_summary["ok"] += 1
                    elif state.status == "skipped":
                        wave_summary["skipped"] += 1
                    else:
                        wave_summary["failed"] += 1
                except Exception as e:
                    wave_summary["failed"] += 1
                    wave_summary["tables"].append({"fqn": fqn, "status": "failed", "error": str(e)})

        summary["waves"].append(wave_summary)
        log_action(
            "deploy_wave",
            payload={
                "plan": plan.name, "wave": wave.index,
                "ok": wave_summary["ok"], "failed": wave_summary["failed"],
                "skipped": wave_summary["skipped"],
            },
        )
        if stop_on_failure and wave_summary["failed"] > 0:
            summary["aborted_after_wave"] = wave.index
            break

    return summary
