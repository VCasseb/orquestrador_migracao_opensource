from __future__ import annotations

import re
from datetime import datetime

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.inventory.catalog import load_inventory
from migrate.core.lineage.graph import build_graph
from migrate.core.plan.waves import (
    build_dag_plan, build_notebook_plan, build_plan,
    list_plans, load_plan, save_plan,
)
from migrate.core.state.selection import load_selection


def _slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_-]+", "-", name.strip()).strip("-")
    return s or f"plan-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


def _split_selection(selected: set[str]) -> dict[str, set[str]]:
    """Split flat selection into (notebooks, dags, tables) by prefix."""
    notebooks, dags, tables = set(), set(), set()
    for item in selected:
        if item.startswith("notebook:"):
            notebooks.add(item.split(":", 1)[1])
        elif item.startswith("dag:"):
            dags.add(item.split(":", 1)[1])
        else:
            tables.add(item)
    return {"notebook": notebooks, "dag": dags, "table": tables}


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/plan", response_class=HTMLResponse)
    def plan_page(request: Request, kind: str = "notebook", include_upstream: int = 1):
        inv = load_inventory()
        selected = load_selection()
        plans = list_plans()

        per_kind = _split_selection(selected) if selected else {"notebook": set(), "dag": set(), "table": set()}

        preview = None
        if inv:
            if kind == "notebook" and per_kind["notebook"]:
                preview = build_notebook_plan(
                    "preview", per_kind["notebook"], inv,
                    include_upstream=bool(include_upstream),
                )
            elif kind == "dag" and per_kind["dag"]:
                preview = build_dag_plan(
                    "preview", per_kind["dag"], inv,
                    include_upstream=bool(include_upstream),
                )
            elif kind == "table" and per_kind["table"]:
                graph = build_graph(inv)
                preview = build_plan(
                    "preview", per_kind["table"], inv, graph,
                    include_upstream=bool(include_upstream),
                )

        return templates.TemplateResponse(
            request,
            "plan.html",
            {
                "active": "plan",
                "inv": inv,
                "kind": kind,
                "selected": selected,
                "per_kind": per_kind,
                "plans": plans,
                "preview": preview,
                "include_upstream": bool(include_upstream),
            },
        )

    @app.post("/plan/save")
    def plan_save(
        request: Request,
        plan_name: str = Form(...),
        kind: str = Form("notebook"),
        include_upstream: str = Form("1"),
    ):
        inv = load_inventory()
        selected = load_selection()
        if not inv or not selected:
            return HTMLResponse(
                "<div class='text-rose-400'>Need inventory + selected items before saving.</div>",
                status_code=400,
            )
        per_kind = _split_selection(selected)
        upstream = bool(include_upstream)
        if kind == "notebook":
            plan = build_notebook_plan(_slug(plan_name), per_kind["notebook"], inv, include_upstream=upstream)
        elif kind == "dag":
            plan = build_dag_plan(_slug(plan_name), per_kind["dag"], inv, include_upstream=upstream)
        else:
            graph = build_graph(inv)
            plan = build_plan(_slug(plan_name), per_kind["table"], inv, graph, include_upstream=upstream)
        path = save_plan(plan)
        return templates.TemplateResponse(
            request, "_plan_saved.html",
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
            request, "_plan_detail.html",
            {"plan": plan},
        )
