from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from migrate.core.credentials import (
    TestResult,
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
    active_llm = (get_env("LLM_PROVIDER", "anthropic") or "anthropic").lower()
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
        "active_llm": active_llm,
        "anthropic": {
            "api_key_masked": mask(get_env("ANTHROPIC_API_KEY")),
            "has_key": bool(get_env("ANTHROPIC_API_KEY")),
            "model": get_env("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
            "active": active_llm == "anthropic",
        },
        "openai": {
            "api_key_masked": mask(get_env("OPENAI_API_KEY")),
            "has_key": bool(get_env("OPENAI_API_KEY")),
            "model": get_env("OPENAI_MODEL", "gpt-4o-mini"),
            "active": active_llm == "openai",
        },
        "gemini": {
            "api_key_masked": mask(get_env("GEMINI_API_KEY")),
            "has_key": bool(get_env("GEMINI_API_KEY")),
            "model": get_env("GEMINI_MODEL", "gemini-2.0-flash"),
            "active": active_llm == "gemini",
        },
        "bedrock": {
            "region": get_env("AWS_REGION", "us-east-1"),
            "model_id": get_env("BEDROCK_MODEL_ID", "anthropic.claude-sonnet-4-20250514-v1:0"),
            "has_aws_key": bool(get_env("AWS_ACCESS_KEY_ID")),
            "aws_key_masked": mask(get_env("AWS_ACCESS_KEY_ID")),
            "active": active_llm == "bedrock",
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

    @app.post("/connections/openai/save")
    def save_openai(
        request: Request,
        openai_api_key: str = Form(""),
        openai_model: str = Form("gpt-4o-mini"),
    ):
        updates = {"OPENAI_MODEL": openai_model.strip()}
        if openai_api_key.strip():
            updates["OPENAI_API_KEY"] = openai_api_key.strip()
        update_env(updates)
        return templates.TemplateResponse(
            request, "_card_openai.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/openai/test")
    def run_test_openai(request: Request):
        load_env()
        from migrate.core.llm import test_connection as llm_test
        ok, message, detail = llm_test("openai")
        return templates.TemplateResponse(
            request, "_test_result.html",
            {"result": TestResult(ok, message, detail), "service": "openai"},
        )

    @app.post("/connections/gemini/save")
    def save_gemini(
        request: Request,
        gemini_api_key: str = Form(""),
        gemini_model: str = Form("gemini-2.0-flash"),
    ):
        updates = {"GEMINI_MODEL": gemini_model.strip()}
        if gemini_api_key.strip():
            updates["GEMINI_API_KEY"] = gemini_api_key.strip()
        update_env(updates)
        return templates.TemplateResponse(
            request, "_card_gemini.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/gemini/test")
    def run_test_gemini(request: Request):
        load_env()
        from migrate.core.llm import test_connection as llm_test
        ok, message, detail = llm_test("gemini")
        return templates.TemplateResponse(
            request, "_test_result.html",
            {"result": TestResult(ok, message, detail), "service": "gemini"},
        )

    @app.post("/connections/bedrock/save")
    def save_bedrock(
        request: Request,
        aws_region: str = Form(""),
        bedrock_model_id: str = Form(""),
        aws_access_key_id: str = Form(""),
        aws_secret_access_key: str = Form(""),
    ):
        updates = {}
        if aws_region.strip():
            updates["AWS_REGION"] = aws_region.strip()
        if bedrock_model_id.strip():
            updates["BEDROCK_MODEL_ID"] = bedrock_model_id.strip()
        if aws_access_key_id.strip():
            updates["AWS_ACCESS_KEY_ID"] = aws_access_key_id.strip()
        if aws_secret_access_key.strip():
            updates["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key.strip()
        update_env(updates)
        return templates.TemplateResponse(
            request, "_card_bedrock.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/bedrock/test")
    def run_test_bedrock(request: Request):
        load_env()
        from migrate.core.llm import test_connection as llm_test
        ok, message, detail = llm_test("bedrock")
        return templates.TemplateResponse(
            request, "_test_result.html",
            {"result": TestResult(ok, message, detail), "service": "bedrock"},
        )

    @app.post("/connections/llm/active")
    def set_active_llm(request: Request, provider: str = Form(...)):
        if provider not in ("anthropic", "openai", "gemini", "bedrock"):
            return HTMLResponse(f"<div class='text-rose-400'>Unknown provider: {provider}</div>", status_code=400)
        update_env({"LLM_PROVIDER": provider})
        return templates.TemplateResponse(
            request, "_llm_active_pill.html",
            {"state": current_state()},
        )

    @app.post("/connections/llm/select")
    def select_llm(request: Request, provider: str = Form(...)):
        """Pick a single active LLM provider — sets LLM_PROVIDER in .env and
        returns the selected provider's config card to swap into the slot."""
        if provider not in ("anthropic", "openai", "gemini", "bedrock"):
            return HTMLResponse(f"<div class='text-rose-400'>Unknown provider: {provider}</div>", status_code=400)
        update_env({"LLM_PROVIDER": provider})
        return templates.TemplateResponse(
            request, f"_card_{provider}.html",
            {"state": current_state()},
        )
