from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from migrate.core.credentials import get_env, load_env
from migrate.web.routes import connections, convert, deploy, docs, inventory, lineage, pages, plan, review, validate

WEB_DIR = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(WEB_DIR / "templates"))
STATIC = WEB_DIR / "static"

load_env()


def _source_connected() -> bool:
    """True when a source cloud (GCP) is configured — the GCS lake bucket, a
    Composer DAG bucket, or BigQuery projects. Drives nav: when False, the app
    shows the Upload tab instead of Inventory so it works offline."""
    load_env()
    return bool(
        get_env("GCP_NOTEBOOKS_BUCKET")
        or get_env("GCP_COMPOSER_DAG_BUCKET")
        or get_env("GCP_PROJECT_IDS")
    )


TEMPLATES.env.globals["source_connected"] = _source_connected

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
