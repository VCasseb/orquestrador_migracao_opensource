from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from migrate.core.docs.wiki import DOCS_DIR, list_docs, render_table_doc, save_doc
from migrate.core.inventory.catalog import load_inventory


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/docs", response_class=HTMLResponse)
    def docs_page(request: Request):
        from migrate.core.deploy.models import list_states
        inv = load_inventory()
        existing = list_docs()
        deployed_fqns = {s.fqn for s in list_states() if s.status == "completed"}
        return templates.TemplateResponse(
            request,
            "docs.html",
            {
                "active": "docs",
                "inv": inv,
                "existing": existing,
                "deployed_fqns": deployed_fqns,
            },
        )

    @app.post("/docs/generate")
    def docs_generate(request: Request, fqn: str = Form(...)):
        try:
            content = render_table_doc(fqn)
            path = save_doc(fqn, content)
            return templates.TemplateResponse(
                request,
                "_doc_preview.html",
                {"fqn": fqn, "content": content, "path": str(path)},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>"
            )

    @app.get("/docs/view/{name}")
    def docs_view(request: Request, name: str):
        path = DOCS_DIR / f"{name}.md"
        if not path.exists():
            return HTMLResponse("<div class='text-rose-400'>Doc not found.</div>", status_code=404)
        content = path.read_text()
        fqn = name.replace("_", ".")
        return templates.TemplateResponse(
            request,
            "_doc_preview.html",
            {"fqn": fqn, "content": content, "path": str(path)},
        )

    @app.get("/docs/raw/{name}")
    def docs_raw(name: str):
        path = DOCS_DIR / f"{name}.md"
        if not path.exists():
            return PlainTextResponse("(not found)", status_code=404)
        return PlainTextResponse(path.read_text())
