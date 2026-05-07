from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.deploy.models import list_states
from migrate.core.deploy.runner import deploy_one, deploy_plan
from migrate.core.inventory.catalog import load_inventory
from migrate.core.plan.waves import list_plans


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/deploy", response_class=HTMLResponse)
    def deploy_page(request: Request):
        plans = list_plans()
        states = list_states()
        return templates.TemplateResponse(
            request,
            "deploy.html",
            {"active": "deploy", "plans": plans, "states": states},
        )

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
                plan_name,
                mode=mode,
                concurrency=concurrency,
                require_approvals=bool(require_approvals),
                stop_on_failure=bool(stop_on_failure),
                create_workflow=bool(create_workflow),
                auto_validate=bool(auto_validate),
            )
            return templates.TemplateResponse(
                request,
                "_deploy_summary.html",
                {"summary": summary, "states": list_states()},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>"
            )

    @app.post("/deploy/one")
    def deploy_one_action(
        request: Request,
        fqn: str = Form(...),
        mode: str = Form("sample"),
        require_approvals: str = Form(""),
    ):
        try:
            state = deploy_one(fqn, mode=mode, require_approvals=bool(require_approvals))
            return templates.TemplateResponse(
                request,
                "_deploy_state_card.html",
                {"state": state},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>"
            )

    @app.get("/deploy/states")
    def deploy_states_partial(request: Request):
        return templates.TemplateResponse(
            request,
            "_deploy_states.html",
            {"states": list_states()},
        )
