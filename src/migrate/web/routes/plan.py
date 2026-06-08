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
    def plan_page(request: Request):
        """Unified batch view: all selected codes (notebooks + DAGs), dependency-ordered
        into a single sequence to be migrated one after another. Upstream chain is always
        auto-included so the batch is self-contained."""
        inv = load_inventory()
        selected = load_selection()
        plans = list_plans()

        per_kind = _split_selection(selected) if selected else {"notebook": set(), "dag": set(), "table": set()}

        nb_preview = (
            build_notebook_plan("preview", per_kind["notebook"], inv, include_upstream=True)
            if inv and per_kind["notebook"] else None
        )
        dag_preview = (
            build_dag_plan("preview", per_kind["dag"], inv, include_upstream=True)
            if inv and per_kind["dag"] else None
        )

        # Flatten both plans' waves into one global, dependency-ordered sequence:
        # notebooks first (they produce the data), then the DAGs that orchestrate them.
        sequence: list[dict] = []
        for prev, code_type in ((nb_preview, "notebook"), (dag_preview, "dag")):
            if not prev:
                continue
            for wave in prev.waves:
                for it in wave.items:
                    sequence.append({"step": len(sequence) + 1, "code_type": code_type, "item": it})

        # Local token + cost estimate for the whole batch (no API key needed).
        batch_estimate = None
        if sequence:
            from migrate.core.convert.estimate import estimate_item
            from migrate.core.credentials import get_env
            tp = get_env("TARGET_PLATFORM", "databricks")
            agg = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0,
                   "cost_usd": 0.0, "model": "", "price_known": True}
            for s in sequence:
                e = estimate_item(inv, s["code_type"], s["item"].fqn, tp)
                if not e:
                    continue
                agg["input_tokens"] += e["input_tokens"]
                agg["output_tokens"] += e["output_tokens"]
                agg["total_tokens"] += e["total_tokens"]
                agg["cost_usd"] += e["cost_usd"]
                agg["model"] = e["model"]
                agg["price_known"] = agg["price_known"] and e["price_known"]
            agg["cost_usd"] = round(agg["cost_usd"], 4)
            batch_estimate = agg

        missing = sorted(set(
            (nb_preview.missing_upstream if nb_preview else [])
            + (dag_preview.missing_upstream if dag_preview else [])
        ))
        counts = {
            "total": len(sequence),
            "notebooks": sum(1 for s in sequence if s["code_type"] == "notebook"),
            "dags": sum(1 for s in sequence if s["code_type"] == "dag"),
            "auto": sum(1 for s in sequence if s["item"].origin == "auto-included"),
            "selected": len(per_kind["notebook"]) + len(per_kind["dag"]),
        }

        return templates.TemplateResponse(
            request,
            "plan.html",
            {
                "active": "plan",
                "inv": inv,
                "selected": selected,
                "sequence": sequence,
                "missing": missing,
                "counts": counts,
                "batch_estimate": batch_estimate,
                "plans": plans,
            },
        )

    @app.post("/plan/save")
    def plan_save(request: Request, plan_name: str = Form(...)):
        """Persist the batch. Saves a plan per code type present in the selection
        (suffixed -notebooks / -dags when both are present). Upstream always included."""
        inv = load_inventory()
        selected = load_selection()
        if not inv or not selected:
            return HTMLResponse(
                "<div class='text-rose-400 p-3'>Selecione códigos no inventário antes de salvar.</div>",
                status_code=400,
            )
        per_kind = _split_selection(selected)
        base = _slug(plan_name)
        both = bool(per_kind["notebook"]) and bool(per_kind["dag"])

        saved: list[tuple[str, object]] = []
        if per_kind["notebook"]:
            name = f"{base}-notebooks" if both else base
            plan = build_notebook_plan(name, per_kind["notebook"], inv, include_upstream=True)
            saved.append((str(save_plan(plan)), plan))
        if per_kind["dag"]:
            name = f"{base}-dags" if both else base
            plan = build_dag_plan(name, per_kind["dag"], inv, include_upstream=True)
            saved.append((str(save_plan(plan)), plan))

        if not saved:
            return HTMLResponse(
                "<div class='text-rose-400 p-3'>Nada selecionado para salvar.</div>",
                status_code=400,
            )
        return templates.TemplateResponse(request, "_plan_saved.html", {"saved": saved})

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
