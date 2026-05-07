from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.convert.code import list_code_conversions, load_code_conversion
from migrate.core.credentials import get_env, load_env
from migrate.core.deploy.code import (
    deploy_code_artifact, list_code_deployments, load_code_state,
)
from migrate.core.deploy.models import list_states
from migrate.core.deploy.runner import deploy_one, deploy_plan
from migrate.core.inventory.catalog import load_inventory
from migrate.core.plan.waves import list_plans


def _saved_artifacts() -> list[dict]:
    """All saved code conversions on disk + their current deployment status."""
    out = []
    for meta in list_code_conversions():
        try:
            art = load_code_conversion(meta)
        except Exception:
            continue
        st = load_code_state(art.source_type, art.name)
        out.append({
            "meta_path": str(meta),
            "artifact": art,
            "state": st,
        })
    return out


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/deploy", response_class=HTMLResponse)
    def deploy_page(request: Request):
        load_env()
        target_platform = get_env("TARGET_PLATFORM", "databricks")
        return templates.TemplateResponse(
            request,
            "deploy.html",
            {
                "active": "deploy",
                "target_platform": target_platform,
                "saved_artifacts": _saved_artifacts(),
                "code_deployments": list_code_deployments(),
                "plans": list_plans(),
                "data_states": list_states(),
            },
        )

    @app.post("/deploy/code/run")
    def deploy_code_run(request: Request, meta_path: str = Form(...), mode: str = Form("sample")):
        try:
            state = deploy_code_artifact(Path(meta_path), mode=mode)
            return templates.TemplateResponse(
                request, "_code_deploy_state.html", {"state": state},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>"
            )

    @app.post("/deploy/code/run-all")
    def deploy_code_run_all(request: Request, mode: str = Form("sample")):
        results = []
        errors = []
        for meta in list_code_conversions():
            try:
                state = deploy_code_artifact(meta, mode=mode)
                results.append({"name": state.name, "source_type": state.source_type, "status": state.status})
            except Exception as e:
                errors.append(f"{meta.stem}: {e}")
        return templates.TemplateResponse(
            request, "_code_deploy_summary.html",
            {"results": results, "errors": errors, "mode": mode},
        )

    # legacy data deploy (kept for backward compat)
    @app.post("/deploy/plan")
    def deploy_plan_action(
        request: Request,
        plan_name: str = Form(...),
        mode: str = Form("sample"),
        concurrency: int = Form(4),
        require_approvals: str = Form(""),
        create_workflow: str = Form(""),
        stop_on_failure: str = Form(""),
        auto_validate: str = Form(""),
    ):
        try:
            summary = deploy_plan(
                plan_name, mode=mode, concurrency=concurrency,
                require_approvals=bool(require_approvals),
                stop_on_failure=bool(stop_on_failure),
                create_workflow=bool(create_workflow),
                auto_validate=bool(auto_validate),
            )
            return templates.TemplateResponse(
                request, "_deploy_summary.html",
                {"summary": summary, "states": list_states()},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>"
            )
