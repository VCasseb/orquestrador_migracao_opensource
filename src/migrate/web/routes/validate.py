from __future__ import annotations

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.selection import load_selection
from migrate.core.validate.report import list_reports, load_report, save_report
from migrate.core.validate.runner import run_validation
from migrate.core.validate.sample import build_synthetic_report


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/validate", response_class=HTMLResponse)
    def validate_page(request: Request):
        from migrate.core.deploy.models import list_states
        inv = load_inventory()
        selected = load_selection()
        reports = list_reports()
        deployments = list_states()
        deployed_fqns = {s.fqn for s in deployments if s.status == "completed"}
        return templates.TemplateResponse(
            request,
            "validate.html",
            {
                "active": "validate",
                "inv": inv,
                "selected": selected,
                "reports": reports,
                "deployed_fqns": deployed_fqns,
                "deployments_count": len(deployed_fqns),
                "total_count": len(inv.tables) if inv else 0,
            },
        )

    @app.post("/validate/run")
    def validate_run(request: Request, fqn: str = Form(...), mode: str = Form("real")):
        try:
            if mode == "sample":
                report = build_synthetic_report(fqn)
            else:
                report = run_validation(fqn)
            path = save_report(report)
            return templates.TemplateResponse(
                request,
                "_validation_report.html",
                {"report": report, "saved_path": str(path)},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>",
            )

    @app.get("/validate/report/{name}")
    def validate_report_view(request: Request, name: str):
        from pathlib import Path
        path = Path(".migrate/reports") / f"{name}.yaml"
        if not path.exists():
            return HTMLResponse("<div class='text-rose-400'>Report not found.</div>", status_code=404)
        report = load_report(path)
        return templates.TemplateResponse(
            request,
            "_validation_report.html",
            {"report": report, "saved_path": str(path)},
        )
