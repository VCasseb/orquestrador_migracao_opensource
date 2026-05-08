from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from migrate.core.inventory.catalog import load_inventory
from migrate.core.lineage.graph import build_graph
from migrate.core.state.selection import load_selection


_HEAT_COLOR = {"hot": "#f97316", "warm": "#eab308", "cold": "#475569"}
_TYPE_COLOR = {"TABLE": "#06b6d4", "VIEW": "#a855f7", "MATERIALIZED_VIEW": "#ec4899", "EXTERNAL": "#64748b"}
_DAG_COLOR = "#10b981"        # emerald
_NOTEBOOK_COLOR = "#f59e0b"   # amber (default)
_SQ_COLOR = "#8b5cf6"          # violet

# medallion-layer fill colors for notebooks
_LAYER_COLOR = {
    "raw":    "#64748b",   # slate
    "bronze": "#d97706",   # amber-700
    "silver": "#06b6d4",   # cyan
    "gold":   "#eab308",   # yellow
}


def _extract_entity(fqn: str) -> str | None:
    """Best-effort entity extraction from a node id.
    Examples:
      'prj-data-prod.gold.cartoes'      → 'cartoes'
      'notebook.gold.cartoes'           → 'cartoes'
      'composer.cartoes_pipeline'       → 'cartoes_pipeline'
    """
    if not fqn:
        return None
    if fqn.startswith("composer."):
        return fqn.split(".", 1)[1]
    if fqn.startswith("notebook."):
        rest = fqn.split(".", 1)[1]
        # 'gold.cartoes' → 'cartoes'
        return rest.split(".")[-1] if "." in rest else rest
    if fqn.startswith("sq."):
        return fqn.split(".", 1)[1]
    parts = fqn.split(".")
    return parts[-1] if parts else None


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/lineage", response_class=HTMLResponse)
    def lineage_page(request: Request):
        inv = load_inventory()
        return templates.TemplateResponse(
            request,
            "lineage.html",
            {"active": "lineage", "inv": inv},
        )

    @app.get("/lineage/graph.json")
    def lineage_graph_json():
        inv = load_inventory()
        if not inv:
            return JSONResponse({"nodes": [], "edges": []})
        graph = build_graph(inv)
        selected = load_selection()
        by_fqn = inv.by_fqn
        dags_by_fqn = {d.fqn: d for d in inv.dags}
        nbs_by_fqn = {n.fqn: n for n in inv.notebooks}

        elements = []
        for fqn in sorted(graph.nodes):
            t = by_fqn.get(fqn)
            d = dags_by_fqn.get(fqn)
            nb = nbs_by_fqn.get(fqn)

            kind = "table" if t else ("dag" if d else ("notebook" if nb else ("sq" if fqn.startswith("sq.") else "external")))
            shape = {
                "table": "round-rectangle",
                "dag": "hexagon",
                "notebook": "diamond",
                "sq": "ellipse",
                "external": "round-rectangle",
            }[kind]

            layer_attr = None
            if t:
                heat_color = _HEAT_COLOR.get(t.heat, "#475569")
                type_color = _TYPE_COLOR.get(t.type, "#64748b")
                label = f"{t.dataset}.{t.name}"
                layer_attr = t.dataset.lower() if t.dataset else None
            elif d:
                heat_color = _DAG_COLOR
                type_color = _DAG_COLOR
                label = f"▣ {d.name}"
                layer_attr = d.layer
            elif nb:
                # color notebooks by medallion layer when known
                ly = nb.layer
                layer_attr = ly
                fill = _LAYER_COLOR.get(ly, _NOTEBOOK_COLOR)
                heat_color = fill
                type_color = fill
                label = f"📓 {nb.name}"
            elif kind == "sq":
                heat_color = _SQ_COLOR
                type_color = _SQ_COLOR
                label = f"⏱ {fqn.split('.', 1)[1]}"
            else:
                heat_color = "#1f2a3d"
                type_color = "#1f2a3d"
                label = fqn.split(".", 2)[-1]

            elements.append({
                "data": {
                    "id": fqn,
                    "label": label,
                    "fqn": fqn,
                    "kind": kind,
                    "type": t.type if t else kind.upper(),
                    "heat": t.heat if t else "cold",
                    "complexity": t.complexity if t else "low",
                    "in_inventory": bool(t or d or nb or kind == "sq"),
                    "selected": fqn in selected,
                    "border_color": heat_color,
                    "background_color": type_color,
                    "shape": shape,
                    "layer": layer_attr or "",
                    "entity": _extract_entity(fqn) or "",
                }
            })
        for edge in graph.edges:
            elements.append({
                "data": {
                    "id": f"{edge.upstream}->{edge.downstream}",
                    "source": edge.upstream,
                    "target": edge.downstream,
                }
            })
        return JSONResponse({"elements": elements})
