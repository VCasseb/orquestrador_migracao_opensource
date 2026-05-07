from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.templating import Jinja2Templates

from migrate.core.credentials import (
    get_env,
    load_env,
    test_anthropic,
    test_databricks,
    test_gcp,
    update_env,
)


def mask(value: str, keep: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= keep:
        return "•" * len(value)
    return "•" * (len(value) - keep) + value[-keep:]


def current_state() -> dict[str, Any]:
    load_env()
    return {
        "gcp": {
            "service_account_json": get_env("GCP_SERVICE_ACCOUNT_JSON"),
            "project_ids": get_env("GCP_PROJECT_IDS"),
        },
        "databricks": {
            "host": get_env("DATABRICKS_HOST"),
            "token_masked": mask(get_env("DATABRICKS_TOKEN")),
            "has_token": bool(get_env("DATABRICKS_TOKEN")),
            "default_catalog": get_env("DATABRICKS_DEFAULT_CATALOG"),
            "warehouse_id": get_env("DATABRICKS_SQL_WAREHOUSE_ID"),
        },
        "anthropic": {
            "api_key_masked": mask(get_env("ANTHROPIC_API_KEY")),
            "has_key": bool(get_env("ANTHROPIC_API_KEY")),
            "model": get_env("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
        },
    }


def attach(app: FastAPI, templates: Jinja2Templates) -> None:

    @app.post("/connections/gcp/save")
    def save_gcp(
        request: Request,
        gcp_service_account_json: str = Form(""),
        gcp_project_ids: str = Form(""),
    ):
        update_env({
            "GCP_SERVICE_ACCOUNT_JSON": gcp_service_account_json.strip(),
            "GCP_PROJECT_IDS": gcp_project_ids.strip(),
        })
        return templates.TemplateResponse(
            request,
            "_card_gcp.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/gcp/test")
    def run_test_gcp(request: Request):
        load_env()
        result = test_gcp()
        return templates.TemplateResponse(
            request,
            "_test_result.html",
            {"result": result, "service": "gcp"},
        )

    @app.post("/connections/databricks/save")
    def save_databricks(
        request: Request,
        databricks_host: str = Form(""),
        databricks_token: str = Form(""),
        databricks_default_catalog: str = Form(""),
        databricks_sql_warehouse_id: str = Form(""),
    ):
        updates = {
            "DATABRICKS_HOST": databricks_host.strip(),
            "DATABRICKS_DEFAULT_CATALOG": databricks_default_catalog.strip(),
            "DATABRICKS_SQL_WAREHOUSE_ID": databricks_sql_warehouse_id.strip(),
        }
        if databricks_token.strip():
            updates["DATABRICKS_TOKEN"] = databricks_token.strip()
        update_env(updates)
        return templates.TemplateResponse(
            request,
            "_card_databricks.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/databricks/test")
    def run_test_databricks(request: Request):
        load_env()
        result = test_databricks()
        return templates.TemplateResponse(
            request,
            "_test_result.html",
            {"result": result, "service": "databricks"},
        )

    @app.post("/connections/anthropic/save")
    def save_anthropic(
        request: Request,
        anthropic_api_key: str = Form(""),
        anthropic_model: str = Form("claude-sonnet-4-6"),
    ):
        updates = {"ANTHROPIC_MODEL": anthropic_model.strip()}
        if anthropic_api_key.strip():
            updates["ANTHROPIC_API_KEY"] = anthropic_api_key.strip()
        update_env(updates)
        return templates.TemplateResponse(
            request,
            "_card_anthropic.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/anthropic/test")
    def run_test_anthropic(request: Request):
        load_env()
        result = test_anthropic()
        return templates.TemplateResponse(
            request,
            "_test_result.html",
            {"result": result, "service": "anthropic"},
        )
