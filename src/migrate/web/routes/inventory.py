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


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

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
        use_sample = source == "sample"
        if source == "auto":
            from migrate.core.credentials import get_env, load_env
            load_env()
            use_sample = not get_env("GCP_PROJECT_IDS")
        try:
            inv = run_scan(use_sample=use_sample)
            save_inventory(inv)
            error: str | None = None
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
        kind: str = Form(...),
        files: list[UploadFile] = File(...),
    ):
        """Manual upload — parses uploaded files as if they came from a real scan
        and merges into inventory.yaml. Idempotent: same name replaces existing entry.
        Accepts .py / .ipynb files OR .zip bundles (extracted in-memory)."""
        import io
        import zipfile

        if kind not in ("notebook", "dag"):
            return HTMLResponse(f"<div class='text-rose-400 p-3'>Unsupported kind: {kind}</div>", status_code=400)

        inv = load_inventory() or Inventory(
            scanned_at=datetime.now(timezone.utc), projects=[], tables=[],
        )

        target_dir = Path(".migrate/uploads") / ("notebooks" if kind == "notebook" else "dags")
        target_dir.mkdir(parents=True, exist_ok=True)

        added: list[str] = []
        replaced: list[str] = []
        errors: list[str] = []

        # Materialize uploads as a flat list of (filename, content) — handles zips inline
        materials: list[tuple[str, str]] = []
        for f in files:
            if not f.filename:
                continue
            try:
                raw = await f.read()
            except Exception as e:
                errors.append(f"{f.filename}: read failed — {e}")
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
                                errors.append(f"{entry} (in {f.filename}): {e}")
                except Exception as e:
                    errors.append(f"{f.filename}: invalid zip — {e}")
            else:
                try:
                    materials.append((f.filename, raw.decode("utf-8", errors="replace")))
                except Exception as e:
                    errors.append(f"{f.filename}: decode failed — {e}")

        for filename, content in materials:
            local_path = target_dir / filename
            try:
                local_path.write_text(content)
            except Exception as e:
                errors.append(f"{filename}: write failed — {e}")
                continue

            try:
                if kind == "notebook":
                    if not filename.lower().endswith((".py", ".ipynb")):
                        errors.append(f"{filename}: only .py and .ipynb supported (skipped)")
                        continue
                    nb = parse_notebook_file(filename, content, location=str(local_path))
                    existing = next((i for i, x in enumerate(inv.notebooks) if x.name == nb.name), None)
                    if existing is not None:
                        inv.notebooks[existing] = nb
                        replaced.append(nb.name)
                    else:
                        inv.notebooks.append(nb)
                        added.append(nb.name)
                else:  # dag
                    if not filename.lower().endswith(".py"):
                        errors.append(f"{filename}: DAG must be .py (skipped)")
                        continue
                    dag = parse_dag_file(str(local_path), content)
                    if not dag:
                        errors.append(f"{filename}: no DAG() found in source")
                        continue
                    existing = next((i for i, x in enumerate(inv.dags) if x.name == dag.name), None)
                    if existing is not None:
                        inv.dags[existing] = dag
                        replaced.append(dag.name)
                    else:
                        inv.dags.append(dag)
                        added.append(dag.name)
            except Exception as e:
                errors.append(f"{filename}: parse failed — {e}")

        save_inventory(inv)
        return templates.TemplateResponse(
            request, "_upload_result.html",
            {"kind": kind, "added": added, "replaced": replaced, "errors": errors},
        )

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
