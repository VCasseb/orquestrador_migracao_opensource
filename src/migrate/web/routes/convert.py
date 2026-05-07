from __future__ import annotations

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from migrate.core.convert.runner import (
    convert_table,
    list_conversions,
    load_conversion,
)
from migrate.core.convert.code import (
    convert_dag, convert_notebook, list_code_conversions, load_code_conversion,
)
from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.selection import load_selection


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/convert", response_class=HTMLResponse)
    def convert_page(request: Request, kind: str = "dag"):
        load_env()
        inv = load_inventory()
        selected = load_selection()
        target_platform = get_env("TARGET_PLATFORM", "databricks")

        code_paths = list_code_conversions()
        dag_conversions = [p for p in code_paths if p.stem.startswith("airflow_dag_")]
        notebook_conversions = [p for p in code_paths if p.stem.startswith("vertex_notebook_")]

        return templates.TemplateResponse(
            request,
            "convert.html",
            {
                "active": "convert",
                "inv": inv,
                "selected": selected,
                "kind": kind,
                "target_platform": target_platform,
                "table_conversions": list_conversions(),
                "dag_conversions": dag_conversions,
                "notebook_conversions": notebook_conversions,
            },
        )

    @app.post("/convert/code")
    def convert_code_run(
        request: Request,
        kind: str = Form(...),
        name: str = Form(...),
        target: str = Form(""),
        prompt: str = Form(""),
    ):
        try:
            target_p = target or None
            if kind == "dag":
                art = convert_dag(name=name, target=target_p, custom_prompt=prompt)
            elif kind == "notebook":
                art = convert_notebook(name=name, target=target_p, custom_prompt=prompt)
            else:
                return HTMLResponse(f"<div class='text-rose-400 p-4'>Unknown kind: {kind}</div>")
            return templates.TemplateResponse(
                request, "_code_conversion_result.html",
                {"artifact": art},
            )
        except Exception as e:
            return HTMLResponse(
                f"<div class='p-4 bg-rose-950/30 border border-rose-800/40 text-rose-300 rounded text-sm'>{e}</div>"
            )

    @app.get("/convert/source.json")
    def convert_source_json(kind: str, name: str):
        inv = load_inventory()
        if not inv:
            return JSONResponse({"error": "no inventory"}, status_code=404)
        if kind == "dag":
            d = inv.dags_by_id.get(name)
            if not d:
                return JSONResponse({"error": f"DAG not found: {name}"}, status_code=404)
            return JSONResponse({
                "code": d.source_code or "(no source captured)",
                "lang": "python",
                "label": d.file_path or d.name,
                "extra": {
                    "schedule": d.schedule, "owner": d.owner,
                    "tasks": len(d.tasks), "tags": d.tags,
                },
            })
        if kind == "notebook":
            nb = inv.notebooks_by_id.get(name)
            if not nb:
                return JSONResponse({"error": f"Notebook not found: {name}"}, status_code=404)
            parts = []
            for c in nb.cells:
                if c.cell_type == "markdown":
                    parts.append(f"# === MARKDOWN ===\n{c.source}\n")
                else:
                    parts.append(f"# === CODE ===\n{c.source}\n")
            return JSONResponse({
                "code": "\n".join(parts) if parts else "(no cells)",
                "lang": "python",
                "label": nb.location,
                "extra": {"kind": nb.kind, "cells": len(nb.cells), "libraries": nb.libraries},
            })
        if kind == "table":
            t = inv.by_fqn.get(name)
            if not t:
                return JSONResponse({"error": f"Table not found: {name}"}, status_code=404)
            return JSONResponse({
                "code": t.view_query or t.source_sql or "-- (no SQL — base table)",
                "lang": "sql",
                "label": t.fqn,
                "extra": {"type": t.type, "source_kind": t.source_kind},
            })
        return JSONResponse({"error": f"Unknown kind: {kind}"}, status_code=400)

    @app.get("/convert/history-load")
    def convert_history_load(meta: str):
        from pathlib import Path
        p = Path(meta)
        if not p.exists() or not p.name.endswith(".meta.yaml"):
            return JSONResponse({"error": "meta not found"}, status_code=404)
        art = load_code_conversion(p)
        # source_lang
        s_lang = "python"
        c_lang = "yaml" if (art.source_type == "airflow_dag" and art.target_platform == "databricks") else "python"
        return JSONResponse({
            "source": art.source_code,
            "source_lang": s_lang,
            "source_label": art.name,
            "converted": art.converted_code,
            "converted_lang": c_lang,
        })

    @app.post("/convert/code.json")
    def convert_code_json(
        kind: str = Form(...), name: str = Form(...),
        target: str = Form(""), prompt: str = Form(""),
    ):
        target_p = target or None
        try:
            if kind == "dag":
                art = convert_dag(name=name, target=target_p, custom_prompt=prompt)
                out_lang = "yaml" if art.target_platform == "databricks" else "python"
            elif kind == "notebook":
                art = convert_notebook(name=name, target=target_p, custom_prompt=prompt)
                out_lang = "python"
            elif kind == "table":
                from migrate.core.convert.runner import convert_table
                art_t = convert_table(fqn=name)
                return JSONResponse({
                    "converted": art_t.converted_sql or "-- (no SQL — DDL only)\n" + art_t.ddl,
                    "lang": "sql",
                    "model": "sqlglot" if art_t.method == "sqlglot" else (art_t.llm_model_used or ""),
                    "method": art_t.method,
                    "confidence": art_t.confidence,
                    "target_path": "(local artifact in .migrate/conversions/)",
                    "notes": art_t.notes,
                    "error": art_t.error,
                })
            else:
                return JSONResponse({"error": f"Unknown kind: {kind}"}, status_code=400)
            return JSONResponse({
                "converted": art.converted_code,
                "lang": out_lang,
                "model": art.llm_model_used,
                "target_path": art.target_suggested_path,
                "output_path": art.output_path,
                "notes": art.notes,
                "custom_prompt": art.custom_prompt_used,
                "error": art.error,
            })
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.get("/convert/code/view")
    def convert_code_view(request: Request, file: str):
        from pathlib import Path
        p = Path(file)
        if not p.exists() or not p.name.endswith(".meta.yaml"):
            return HTMLResponse("<div class='text-rose-400'>Not found.</div>", status_code=404)
        art = load_code_conversion(p)
        return templates.TemplateResponse(
            request, "_code_conversion_result.html",
            {"artifact": art},
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
