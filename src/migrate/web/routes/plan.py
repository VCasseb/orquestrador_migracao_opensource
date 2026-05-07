from __future__ import annotations

import re
from datetime import datetime

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.inventory.catalog import load_inventory
from migrate.core.lineage.graph import build_graph
from migrate.core.plan.waves import build_plan, list_plans, load_plan, save_plan
from migrate.core.state.selection import load_selection


def _slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_-]+", "-", name.strip()).strip("-")
    return s or f"plan-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/plan", response_class=HTMLResponse)
    def plan_page(request: Request, include_upstream: int = 1):
        inv = load_inventory()
        selected = load_selection()
        plans = list_plans()

        preview = None
        if inv and selected:
            graph = build_graph(inv)
            preview = build_plan(
                "preview", selected, inv, graph,
                include_upstream=bool(include_upstream),
            )

        return templates.TemplateResponse(
            request,
            "plan.html",
            {
                "active": "plan",
                "inv": inv,
                "selected": selected,
                "plans": plans,
                "preview": preview,
                "include_upstream": bool(include_upstream),
            },
        )

    @app.post("/plan/save")
    def plan_save(request: Request, plan_name: str = Form(...), include_upstream: str = Form("1")):
        inv = load_inventory()
        selected = load_selection()
        if not inv or not selected:
            return HTMLResponse(
                "<div class='text-rose-400'>Need inventory + selected tables before saving a plan.</div>",
                status_code=400,
            )
        graph = build_graph(inv)
        plan = build_plan(
            _slug(plan_name), selected, inv, graph,
            include_upstream=bool(include_upstream),
        )
        path = save_plan(plan)
        return templates.TemplateResponse(
            request,
            "_plan_saved.html",
            {"path": str(path), "plan": plan},
        )

    @app.get("/plan/view/{name}")
    def plan_view(request: Request, name: str):
        from pathlib import Path
        path = Path(".migrate/plans") / f"{name}.yaml"
        if not path.exists():
            return HTMLResponse("<div class='text-rose-400'>Plan not found.</div>", status_code=404)
        plan = load_plan(path)
        return templates.TemplateResponse(
            request,
            "_plan_detail.html",
            {"plan": plan},
        )
