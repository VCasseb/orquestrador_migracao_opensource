from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.convert.code import list_code_conversions, load_code_conversion
from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.approval import approve as approve_action
from migrate.core.state.approval import get_state, reject as reject_action
from migrate.core.state.selection import load_selection
from migrate.core.validate.report import list_reports, load_report


def _kind_for(source_type: str) -> str:
    return "dag" if source_type == "airflow_dag" else "notebook"


def _conversions_with_state() -> list[dict[str, Any]]:
    """Code conversions (DAGs / notebooks) awaiting review. Robust to unreadable
    metas — a single bad file never takes down the page."""
    out = []
    for path in list_code_conversions():
        try:
            art = load_code_conversion(path)
        except Exception:
            continue
        fqn = art.name
        state = get_state(fqn, "conversion")
        out.append({
            "fqn": fqn,
            "artifact": art,
            "state": state,
            "kind": _kind_for(art.source_type),
            "meta": str(path),
        })
    return out


def _load_code_by_name(name: str):
    for path in list_code_conversions():
        try:
            art = load_code_conversion(path)
        except Exception:
            continue
        if art.name == name:
            return art, str(path)
    return None, None


def _reports_with_state() -> list[dict[str, Any]]:
    out = []
    for path in list_reports():
        fqn = path.stem.replace("_", ".")
        report = load_report(path)
        state = get_state(report.fqn_source, "validation")
        out.append({"fqn": report.fqn_source, "report": report, "state": state})
    return out


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/review", response_class=HTMLResponse)
    def review_page(request: Request):
        conversions = _conversions_with_state()
        return templates.TemplateResponse(
            request,
            "review.html",
            {
                "active": "review",
                "conversions": conversions,
            },
        )

    @app.post("/review/approve")
    def review_approve(request: Request, fqn: str = Form(...), stage: str = Form(...), comment: str = Form("")):
        approve_action(fqn, stage, comment)
        return _refresh_card(request, fqn, stage)

    @app.post("/review/reject")
    def review_reject(request: Request, fqn: str = Form(...), stage: str = Form(...), comment: str = Form("")):
        reject_action(fqn, stage, comment)
        return _refresh_card(request, fqn, stage)

    @app.post("/review/bulk-approve")
    def review_bulk_approve(request: Request, stage: str = Form(...), filter: str = Form("auto")):
        """Bulk approve. filter='auto' = only auto-mergeable (sqlglot/ddl-only with high confidence).
        filter='all-pending' = everything pending in this stage."""
        approved = []
        if stage == "conversion":
            for entry in _conversions_with_state():
                if entry["state"].status != "pending":
                    continue
                a = entry["artifact"]
                eligible = filter == "all-pending" or (
                    filter == "auto" and not a.notes and not a.error
                )
                if eligible:
                    approve_action(entry["fqn"], "conversion", f"bulk-approve ({filter})")
                    approved.append(entry["fqn"])
        elif stage == "validation":
            for entry in _reports_with_state():
                if entry["state"].status != "pending":
                    continue
                eligible = filter == "all-pending" or (
                    filter == "auto" and entry["report"].verdict == "pass"
                )
                if eligible:
                    approve_action(entry["fqn"], "validation", f"bulk-approve ({filter})")
                    approved.append(entry["fqn"])

        return HTMLResponse(
            f"<div class='p-3 bg-emerald-950/30 border border-emerald-800/40 text-emerald-300 text-sm rounded'>"
            f"Approved {len(approved)} item(s) in {stage}. <a href='/review' class='underline'>refresh</a>"
            f"</div>"
        )

    def _refresh_card(request: Request, fqn: str, stage: str) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "_review_card.html",
            {"entry": _entry_for(fqn, stage), "stage": stage},
        )

    def _entry_for(fqn: str, stage: str) -> dict[str, Any]:
        state = get_state(fqn, stage)
        if stage == "conversion":
            art, meta = _load_code_by_name(fqn)
            return {
                "fqn": fqn, "artifact": art, "state": state, "meta": meta,
                "kind": _kind_for(art.source_type) if art else "notebook",
            }
        if stage == "validation":
            from migrate.core.validate.report import load_report
            from pathlib import Path
            p = Path(".migrate/reports") / f"{fqn.replace('.', '_')}.yaml"
            return {"fqn": fqn, "report": load_report(p) if p.exists() else None, "state": state}
        return {"fqn": fqn, "state": state}

    @app.post("/inventory/bulk-select")
    def inventory_bulk_select(
        request: Request,
        fqns: str = Form(""),
        action: str = Form("add"),
    ):
        from migrate.core.state.selection import load_selection, save_selection
        sel = load_selection()
        target = {f.strip() for f in fqns.split(",") if f.strip()}
        if action == "add":
            sel |= target
        elif action == "remove":
            sel -= target
        elif action == "set":
            sel = target
        elif action == "clear":
            sel = set()
        save_selection(sel)
        return HTMLResponse(
            f"<div class='text-xs text-emerald-400'>Selection updated: {len(sel)} table(s)</div>"
        )
