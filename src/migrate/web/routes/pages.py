from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates


PHASES: dict[str, tuple[str, str, str]] = {}


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.get("/connections")
    def connections_page(request: Request):
        from migrate.web.routes.connections import current_state
        return templates.TemplateResponse(
            request,
            "connections.html",
            {"active": "connections", "state": current_state()},
        )

    for slug, (title, phase, blurb) in PHASES.items():
        def _make(s: str, t: str, p: str, b: str):
            def view(request: Request):
                return templates.TemplateResponse(
                    request,
                    "placeholder.html",
                    {"active": s, "title": t, "phase": p, "blurb": b},
                )
            return view

        app.add_api_route(f"/{slug}", _make(slug, title, phase, blurb), methods=["GET"])
