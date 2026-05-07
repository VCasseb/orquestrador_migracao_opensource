from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from migrate.core.credentials import load_env
from migrate.web.routes import connections, convert, deploy, docs, inventory, lineage, pages, plan, review, validate

WEB_DIR = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(WEB_DIR / "templates"))
STATIC = WEB_DIR / "static"

load_env()

app = FastAPI(title="migrate", docs_url=None, redoc_url=None)

if STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/connections", status_code=302)


pages.attach(app, TEMPLATES)
connections.attach(app, TEMPLATES)
inventory.attach(app, TEMPLATES)
lineage.attach(app, TEMPLATES)
plan.attach(app, TEMPLATES)
convert.attach(app, TEMPLATES)
validate.attach(app, TEMPLATES)
review.attach(app, TEMPLATES)
deploy.attach(app, TEMPLATES)
docs.attach(app, TEMPLATES)
