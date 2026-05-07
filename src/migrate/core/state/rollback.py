"""Rollback / undo for migration actions.

Each rollback is itself an action recorded in the audit log. Rollbacks are
idempotent: rolling back something already rolled back is a no-op.
"""
from __future__ import annotations

from pathlib import Path

from migrate.core.state.approval import revoke as revoke_approval
from migrate.core.state.audit import log_action

CONVERSIONS_DIR = Path(".migrate/conversions")
REPORTS_DIR = Path(".migrate/reports")
DEPLOYMENTS_DIR = Path(".migrate/deployments")


def rollback_conversion(fqn: str, reason: str = "") -> dict:
    """Delete conversion artifacts (.sql, .ddl.sql, .notebook.py, .meta.yaml)."""
    base = fqn.replace(".", "_")
    deleted: list[str] = []
    for ext in (".sql", ".ddl.sql", ".notebook.py", ".meta.yaml"):
        p = CONVERSIONS_DIR / f"{base}{ext}"
        if p.exists():
            p.unlink()
            deleted.append(p.name)
    revoke_approval(fqn, "conversion", reason or "rollback")
    return log_action(
        action="rollback_conversion",
        fqn=fqn,
        payload={"deleted": deleted, "reason": reason},
    )


def rollback_validation(fqn: str, reason: str = "") -> dict:
    """Move validation report to .revoked.yaml (preserves history) and revoke approval."""
    base = fqn.replace(".", "_")
    p = REPORTS_DIR / f"{base}.yaml"
    moved: str | None = None
    if p.exists():
        target = REPORTS_DIR / f"{base}.revoked.yaml"
        if target.exists():
            target.unlink()
        p.rename(target)
        moved = target.name
    revoke_approval(fqn, "validation", reason or "rollback")
    return log_action(
        action="rollback_validation",
        fqn=fqn,
        payload={"moved_to": moved, "reason": reason},
    )


def rollback_approval(fqn: str, stage: str, reason: str = "") -> dict:
    """Just revoke (no artifact deletion) — re-runs allowed."""
    revoke_approval(fqn, stage, reason or "rollback")
    return log_action(
        action="rollback_approval",
        fqn=fqn,
        payload={"stage": stage, "reason": reason},
    )


def rollback_deployment(fqn: str, reason: str = "", execute: bool = False) -> dict:
    """Drop Delta target + remove notebook + delete workflow.

    Without --execute, returns the plan of what *would* be deleted (dry-run).
    With --execute, performs DROP TABLE and removes notebook/workflow via Databricks SDK.
    """
    from migrate.core.deploy.runner import load_deployment

    state = load_deployment(fqn)
    if not state:
        return log_action(
            "rollback_deployment", fqn=fqn,
            payload={"reason": reason, "noop": True}, result="ok",
        )

    plan = {
        "drop_table": state.target_fqn,
        "remove_notebook": state.notebook_workspace_path,
        "delete_workflow_id": state.workflow_id,
    }

    if not execute:
        return log_action(
            "rollback_deployment_dry_run", fqn=fqn,
            payload={"would_do": plan, "reason": reason},
        )

    from migrate.core.deploy.databricks import drop_delta_table, remove_notebook, delete_workflow
    errors = []
    try:
        if state.target_fqn:
            drop_delta_table(state.target_fqn)
    except Exception as e:
        errors.append(f"drop: {e}")
    try:
        if state.notebook_workspace_path:
            remove_notebook(state.notebook_workspace_path)
    except Exception as e:
        errors.append(f"notebook: {e}")
    try:
        if state.workflow_id:
            delete_workflow(state.workflow_id)
    except Exception as e:
        errors.append(f"workflow: {e}")

    state_path = DEPLOYMENTS_DIR / f"{fqn.replace('.', '_')}.yaml"
    if state_path.exists():
        target = DEPLOYMENTS_DIR / f"{fqn.replace('.', '_')}.rolled-back.yaml"
        if target.exists():
            target.unlink()
        state_path.rename(target)

    revoke_approval(fqn, "deployment", reason or "rollback")
    return log_action(
        "rollback_deployment", fqn=fqn,
        payload={"plan": plan, "errors": errors, "reason": reason},
        result="error" if errors else "ok",
    )
