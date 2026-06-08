from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.inventory.catalog import load_inventory, save_inventory
from migrate.core.inventory.composer import parse_dag_file
from migrate.core.inventory.models import Inventory
from migrate.core.inventory.notebooks import parse_notebook_file
from migrate.core.inventory.scanner import run_scan
from migrate.core.state.selection import load_selection, toggle


def _humanize_bytes(b: int | None) -> str:
    if not b:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.0f}{unit}" if unit == "B" else f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}PB"


def _humanize_count(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _ingest_materials(kind: str, materials: list[tuple[str, str]]) -> tuple[list[dict], list[dict], list[str]]:
    """Classify, write and parse a list of (filename, content) into the inventory.
    Shared by file upload and paste. Returns (added, replaced, errors) where
    added/replaced are dicts {name, type, ext}. Saves the inventory at the end."""
    inv = load_inventory() or Inventory(
        scanned_at=datetime.now(timezone.utc), projects=[], tables=[],
    )
    nb_dir = Path(".migrate/uploads/notebooks")
    dag_dir = Path(".migrate/uploads/dags")
    nb_dir.mkdir(parents=True, exist_ok=True)
    dag_dir.mkdir(parents=True, exist_ok=True)

    added: list[dict] = []
    replaced: list[dict] = []
    errors: list[str] = []

    for filename, content in materials:
        low = filename.lower()
        ext = low.rsplit(".", 1)[-1] if "." in low else ""

        # Decide the type: explicit override, else auto-detect from content.
        as_dag = kind == "dag"
        if kind == "auto" and low.endswith(".py"):
            try:
                as_dag = parse_dag_file(filename, content) is not None
            except Exception:
                as_dag = False

        target_dir = dag_dir if as_dag else nb_dir
        local_path = target_dir / filename
        try:
            local_path.write_text(content)
        except Exception as e:
            errors.append(f"{filename}: falha ao gravar — {e}")
            continue

        try:
            if as_dag:
                dag = parse_dag_file(str(local_path), content)
                if not dag:
                    errors.append(f"{filename}: nenhum DAG() encontrado no código")
                    continue
                existing = next((i for i, x in enumerate(inv.dags) if x.name == dag.name), None)
                entry = {"name": dag.name, "type": "dag", "ext": ext}
                if existing is not None:
                    inv.dags[existing] = dag
                    replaced.append(entry)
                else:
                    inv.dags.append(dag)
                    added.append(entry)
            else:
                nb = parse_notebook_file(filename, content, location=str(local_path))
                existing = next((i for i, x in enumerate(inv.notebooks) if x.name == nb.name), None)
                entry = {"name": nb.name, "type": "notebook", "ext": ext}
                if existing is not None:
                    inv.notebooks[existing] = nb
                    replaced.append(entry)
                else:
                    inv.notebooks.append(nb)
                    added.append(entry)
        except Exception as e:
            errors.append(f"{filename}: parse falhou — {e}")

    save_inventory(inv)
    return added, replaced, errors


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    def _upload_resp(request: Request, added: list, replaced: list, errors: list):
        resp = templates.TemplateResponse(
            request, "_upload_result.html",
            {"added": added, "replaced": replaced, "errors": errors},
        )
        # Pages with a self-refreshing list listen for this.
        if added or replaced:
            resp.headers["HX-Trigger"] = "artifactsUploaded"
        # Forms only auto-reload when this is "0" — otherwise the error banner
        # would be wiped by the reload before the user can read it.
        resp.headers["X-Upload-Errors"] = str(len(errors))
        return resp


    @app.get("/inventory", response_class=HTMLResponse)
    def inventory_page(request: Request, kind: str = "notebook", layer: str = ""):
        inv = load_inventory()
        selected = load_selection()
        return templates.TemplateResponse(
            request,
            "inventory.html",
            {
                "active": "inventory",
                "kind": kind,
                "layer": layer,
                "inv": inv,
                "selected": selected,
                "humanize_bytes": _humanize_bytes,
                "humanize_count": _humanize_count,
            },
        )

    @app.post("/inventory/toggle")
    def inventory_toggle_item(request: Request, item: str = Form(...)):
        from migrate.core.state.selection import toggle
        chosen = toggle(item)
        return templates.TemplateResponse(
            request, "_select_item_button.html",
            {"item": item, "chosen": chosen},
        )

    @app.post("/inventory/clear-cache")
    def inventory_clear_cache(request: Request):
        """Delete the local .migrate/inventory.yaml ONLY. Does not touch BigQuery,
        Databricks, or anything else. After clearing, click 'Scan now' to refetch."""
        from pathlib import Path
        from migrate.core.state.audit import log_action
        deleted: list[str] = []
        for p in [
            Path(".migrate/inventory.yaml"),
            Path(".migrate/catalog/"),
        ]:
            if p.is_file():
                p.unlink()
                deleted.append(str(p))
            elif p.is_dir():
                import shutil
                shutil.rmtree(p)
                deleted.append(str(p))
        log_action("clear_inventory_cache", payload={"deleted": deleted})
        return templates.TemplateResponse(
            request,
            "_inventory_kind.html",
            {
                "inv": None,
                "kind": "notebook",
                "selected": load_selection(),
                "humanize_bytes": _humanize_bytes,
                "humanize_count": _humanize_count,
                "filter": {},
                "error": None,
                "cache_cleared": True,
            },
        )

    @app.post("/inventory/scan")
    def inventory_scan(request: Request, source: str = Form("auto"), kind: str = Form("notebook")):
        from migrate.core.credentials import get_env, load_env
        from migrate.core.inventory.scanner import last_scan_errors
        use_sample = source == "sample"
        if source == "auto":
            load_env()
            has_source = bool(
                get_env("GCP_NOTEBOOKS_BUCKET")
                or get_env("GCP_COMPOSER_DAG_BUCKET")
                or get_env("GCP_PROJECT_IDS")
            )
            use_sample = not has_source
        warnings: list[str] = []
        try:
            inv = run_scan(use_sample=use_sample)
            save_inventory(inv)
            error: str | None = None
            if not use_sample:
                warnings = last_scan_errors()
        except Exception as e:
            inv = load_inventory() or Inventory(scanned_at=__import__("datetime").datetime.now(), projects=[], tables=[])
            error = str(e)

        return templates.TemplateResponse(
            request,
            "_inventory_kind.html",
            {
                "inv": inv,
                "kind": kind,
                "selected": load_selection(),
                "error": error,
                "warnings": warnings,
                "humanize_bytes": _humanize_bytes,
                "humanize_count": _humanize_count,
                "filter": {},
            },
        )

    @app.post("/inventory/filter")
    def inventory_filter(
        request: Request,
        project: str = Form(""),
        type: str = Form(""),
        complexity: str = Form(""),
        heat: str = Form(""),
        source_kind: str = Form(""),
        search: str = Form(""),
    ):
        inv = load_inventory()
        return templates.TemplateResponse(
            request,
            "_inventory_table.html",
            {
                "inv": inv,
                "selected": load_selection(),
                "humanize_bytes": _humanize_bytes,
                "humanize_count": _humanize_count,
                "filter": {
                    "project": project or None,
                    "type": type or None,
                    "complexity": complexity or None,
                    "heat": heat or None,
                    "source_kind": source_kind or None,
                    "search": search or None,
                },
            },
        )

    @app.post("/inventory/select/{fqn}")
    def inventory_select(request: Request, fqn: str):
        chosen = toggle(fqn)
        return templates.TemplateResponse(
            request,
            "_select_button.html",
            {"fqn": fqn, "chosen": chosen},
        )

    @app.post("/inventory/upload")
    async def inventory_upload(
        request: Request,
        kind: str = Form("auto"),
        files: list[UploadFile] = File(...),
    ):
        """Manual upload — accepts arbitrary code/query files and auto-detects whether
        each is an Airflow DAG or a notebook/code artifact. The file extension is just
        a label: a DAG, notebook or query can arrive as .py / .ipynb / .sql / .json.
        kind='auto' (default) detects per file; 'dag'/'notebook' force the type.
        .zip bundles are extracted in-memory. Idempotent: same name replaces."""
        import io
        import zipfile

        if kind not in ("notebook", "dag", "auto"):
            return HTMLResponse(f"<div class='text-rose-400 p-3'>Unsupported kind: {kind}</div>", status_code=400)

        errors: list[str] = []
        # Materialize uploads as a flat list of (filename, content) — handles zips inline
        materials: list[tuple[str, str]] = []
        for f in files:
            if not f.filename:
                continue
            try:
                raw = await f.read()
            except Exception as e:
                errors.append(f"{f.filename}: leitura falhou — {e}")
                continue

            if f.filename.lower().endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                        for entry in zf.namelist():
                            if entry.endswith("/"):
                                continue
                            base = entry.split("/")[-1]
                            if not base or base.startswith("."):
                                continue
                            try:
                                materials.append((base, zf.read(entry).decode("utf-8", errors="replace")))
                            except Exception as e:
                                errors.append(f"{entry} (em {f.filename}): {e}")
                except Exception as e:
                    errors.append(f"{f.filename}: zip inválido — {e}")
            else:
                try:
                    materials.append((f.filename, raw.decode("utf-8", errors="replace")))
                except Exception as e:
                    errors.append(f"{f.filename}: decode falhou — {e}")

        added, replaced, ingest_errors = _ingest_materials(kind, materials)
        return _upload_resp(request, added, replaced, errors + ingest_errors)

    @app.post("/inventory/upload-paste")
    def inventory_upload_paste(
        request: Request,
        name: str = Form(...),
        code: str = Form(...),
        kind: str = Form("auto"),
    ):
        """Add a notebook/DAG by pasting code + a name (no file needed). The name's
        extension drives parsing (defaults to .py). Type is auto-detected."""
        if kind not in ("notebook", "dag", "auto"):
            return HTMLResponse(f"<div class='text-rose-400 p-3'>Unsupported kind: {kind}</div>", status_code=400)
        name = (name or "").strip().replace("/", "_").replace("\\", "_")
        if not name:
            return _upload_resp(request, [], [], ["Informe um nome para o código."])
        if not code.strip():
            return _upload_resp(request, [], [], [f"{name}: nenhum código colado."])
        # Default to .py when no extension was given.
        if "." not in name:
            name = name + ".py"
        added, replaced, errors = _ingest_materials(kind, [(name, code)])
        return _upload_resp(request, added, replaced, errors)

    @app.get("/upload", response_class=HTMLResponse)
    def upload_page(request: Request):
        """Offline-first entry point: upload notebooks/DAGs and convert them without
        any cloud connection. Shown in the nav (instead of Inventory/Lineage) when
        no source cloud is configured."""
        inv = load_inventory()
        return templates.TemplateResponse(
            request, "upload.html", {"active": "upload", "inv": inv},
        )

    @app.get("/upload/list", response_class=HTMLResponse)
    def upload_list(request: Request):
        inv = load_inventory()
        return templates.TemplateResponse(
            request, "_upload_list.html", {"inv": inv},
        )

    @app.get("/inventory/preview/{kind}/{name:path}")
    def inventory_preview(request: Request, kind: str, name: str):
        """Code popup for an uploaded/scanned notebook or DAG — shows the raw source
        with a shortcut to the Convert flow. Used by the Inventory click-to-preview."""
        inv = load_inventory()
        if not inv:
            return HTMLResponse("<div class='p-4 text-rose-400'>No inventory loaded.</div>")

        if kind == "notebook":
            nb = inv.notebooks_by_id.get(name)
            if not nb:
                return HTMLResponse(f"<div class='p-4 text-rose-400'>Notebook not found: {name}</div>")
            parts: list[str] = []
            for c in nb.cells:
                marker = "# === MARKDOWN ===" if c.cell_type == "markdown" else "# === CODE ==="
                parts.append(f"{marker}\n{c.source}\n")
            code = "\n".join(parts) if parts else "(no cells)"
            return templates.TemplateResponse(
                request, "_code_preview.html",
                {"title": nb.name, "subtitle": nb.location, "code": code, "kind": "notebook", "name": nb.name},
            )

        if kind == "dag":
            d = inv.dags_by_id.get(name)
            if not d:
                return HTMLResponse(f"<div class='p-4 text-rose-400'>DAG not found: {name}</div>")
            return templates.TemplateResponse(
                request, "_code_preview.html",
                {"title": d.name, "subtitle": d.file_path, "code": d.source_code or "(no source captured)",
                 "kind": "dag", "name": d.name},
            )

        return HTMLResponse(f"<div class='p-4 text-rose-400'>Unknown kind: {kind}</div>")

    @app.get("/inventory/detail/{fqn}")
    def inventory_detail(request: Request, fqn: str):
        inv = load_inventory()
        if not inv:
            return HTMLResponse("<div class='p-4 text-rose-400'>No inventory loaded.</div>")
        table = inv.by_fqn.get(fqn)
        if not table:
            return HTMLResponse(f"<div class='p-4 text-rose-400'>Not found: {fqn}</div>")
        return templates.TemplateResponse(
            request,
            "_table_detail.html",
            {
                "table": table,
                "humanize_bytes": _humanize_bytes,
                "humanize_count": _humanize_count,
            },
        )

    @app.get("/inventory/source/{kind}/{name:path}")
    def inventory_source_detail(request: Request, kind: str, name: str):
        inv = load_inventory()
        if not inv:
            return HTMLResponse("<div class='p-4 text-rose-400'>No inventory loaded.</div>")

        if kind == "dag":
            dag = inv.dags_by_id.get(name)
            if not dag:
                return HTMLResponse(
                    f"<div class='p-4 text-rose-400'>DAG <code>{name}</code> not in scanned set. "
                    f"Configure <code>GCP_COMPOSER_DAG_BUCKET</code> and rescan.</div>"
                )
            consumers = [t for t in inv.tables
                         if t.source_dag_id == name or name in (t.source_dag_id or "")]
            return templates.TemplateResponse(
                request, "_source_dag.html",
                {"dag": dag, "consumers": consumers},
            )

        if kind == "notebook":
            nb = inv.notebooks_by_id.get(name)
            if not nb:
                return HTMLResponse(
                    f"<div class='p-4 text-rose-400'>Notebook <code>{name}</code> not scanned. "
                    f"Configure <code>GCP_NOTEBOOKS_BUCKET</code>.</div>"
                )
            consumers = [t for t in inv.tables if t.source_notebook_id == name]
            return templates.TemplateResponse(
                request, "_source_notebook.html",
                {"notebook": nb, "consumers": consumers},
            )

        if kind == "sq":
            sq = next((s for s in inv.scheduled_queries if s.name == name), None)
            if not sq:
                return HTMLResponse(
                    f"<div class='p-4 text-rose-400'>Scheduled query <code>{name}</code> not found.</div>"
                )
            consumers = [t for t in inv.tables if t.source_scheduled_query_id == name]
            return templates.TemplateResponse(
                request, "_source_sq.html",
                {"sq": sq, "consumers": consumers},
            )

        return HTMLResponse(f"<div class='p-4 text-rose-400'>Unknown source kind: {kind}</div>")
