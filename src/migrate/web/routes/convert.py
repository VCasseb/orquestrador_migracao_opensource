from __future__ import annotations

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.convert.runner import (
    convert_table,
    list_conversions,
    load_conversion,
)
from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.selection import load_selection


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/convert", response_class=HTMLResponse)
    def convert_page(request: Request):
        inv = load_inventory()
        selected = load_selection()
        existing = list_conversions()
        return templates.TemplateResponse(
            request,
            "convert.html",
            {
                "active": "convert",
                "inv": inv,
                "selected": selected,
                "existing": existing,
            },
        )

    def _render_result(request: Request, artifact, fqn: str):
        from pathlib import Path
        inv = load_inventory()
        table = inv.by_fqn.get(fqn) if inv else None
        notebook_text = ""
        if artifact.notebook_path:
            p = Path(artifact.notebook_path)
            if p.exists():
                notebook_text = p.read_text()
        return templates.TemplateResponse(
            request,
            "_conversion_result.html",
            {"artifact": artifact, "table": table, "notebook_text": notebook_text},
        )

    @app.post("/convert/run")
    def convert_run(
        request: Request,
        fqn: str = Form(...),
        use_llm: str = Form(""),
    ):
        try:
            artifact = convert_table(fqn=fqn, use_llm_fallback=bool(use_llm))
            return _render_result(request, artifact, fqn)
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>",
            )

    @app.get("/convert/view/{fqn}")
    def convert_view(request: Request, fqn: str):
        artifact = load_conversion(fqn)
        if not artifact:
            return HTMLResponse("<div class='text-rose-400'>Conversion not found.</div>", status_code=404)
        return _render_result(request, artifact, fqn)

    @app.get("/convert/raw-notebook")
    def convert_raw_notebook(fqn: str):
        from pathlib import Path
        artifact = load_conversion(fqn)
        if not artifact or not artifact.notebook_path:
            return HTMLResponse("(no notebook)", status_code=404)
        p = Path(artifact.notebook_path)
        if not p.exists():
            return HTMLResponse("(missing on disk)", status_code=404)
        return HTMLResponse(p.read_text(), media_type="text/plain")

    @app.get("/convert/review/{fqn}")
    def convert_review(request: Request, fqn: str):
        from migrate.core.state.approval import get_state
        artifact = load_conversion(fqn)
        if not artifact:
            return HTMLResponse("<div class='text-rose-400'>Conversion not found.</div>", status_code=404)
        inv = load_inventory()
        table = inv.by_fqn.get(fqn) if inv else None
        state = get_state(fqn, "conversion")
        return templates.TemplateResponse(
            request,
            "convert_review.html",
            {"active": "convert", "artifact": artifact, "table": table, "state": state},
        )
