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
    test_gcp,
    test_gcs,
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
    target_platform = (get_env("TARGET_PLATFORM", "databricks") or "databricks").lower()
    return {
        "gcp": {
            "service_account_json": get_env("GCP_SERVICE_ACCOUNT_JSON"),
            "project_ids": get_env("GCP_PROJECT_IDS"),
            "notebooks_bucket": get_env("GCP_NOTEBOOKS_BUCKET"),
            "notebooks_folders": get_env("GCP_NOTEBOOKS_FOLDERS"),
            "composer_bucket": get_env("GCP_COMPOSER_DAG_BUCKET"),
            "connected": bool(
                get_env("GCP_PROJECT_IDS")
                or get_env("GCP_NOTEBOOKS_BUCKET")
                or get_env("GCP_COMPOSER_DAG_BUCKET")
            ),
        },
        "target_platform": target_platform,
        "target": {
            "databricks_workspace_prefix": get_env("TARGET_DATABRICKS_WORKSPACE_PREFIX", "/Workspace/migration"),
            "s3_notebooks_prefix": get_env("TARGET_S3_NOTEBOOKS_PREFIX", "s3://acme-data-notebooks/migration"),
            "mwaa_dags_prefix": get_env("TARGET_MWAA_DAGS_PREFIX", "s3://acme-mwaa-bucket/dags"),
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
        gcp_notebooks_bucket: str = Form(""),
        gcp_notebooks_folders: str = Form(""),
        gcp_composer_dag_bucket: str = Form(""),
    ):
        update_env({
            "GCP_SERVICE_ACCOUNT_JSON": gcp_service_account_json.strip(),
            "GCP_PROJECT_IDS": gcp_project_ids.strip(),
            "GCP_NOTEBOOKS_BUCKET": gcp_notebooks_bucket.strip(),
            "GCP_NOTEBOOKS_FOLDERS": gcp_notebooks_folders.strip(),
            "GCP_COMPOSER_DAG_BUCKET": gcp_composer_dag_bucket.strip(),
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

    @app.post("/connections/gcp/test-gcs")
    def run_test_gcs(request: Request):
        load_env()
        result = test_gcs()
        return templates.TemplateResponse(
            request,
            "_test_result.html",
            {"result": result, "service": "gcs"},
        )

    @app.post("/connections/target/select")
    def select_target(request: Request, platform: str = Form(...)):
        """Pick the destination platform — sets TARGET_PLATFORM in .env and returns
        the refreshed destination card. Drives the output format of all conversions."""
        if platform not in ("databricks", "aws"):
            return HTMLResponse(f"<div class='text-rose-400'>Unknown platform: {platform}</div>", status_code=400)
        update_env({"TARGET_PLATFORM": platform})
        return templates.TemplateResponse(
            request, "_card_target.html",
            {"state": current_state(), "result": {"saved": True}},
        )

    @app.post("/connections/target/save-paths")
    def save_target_paths(
        request: Request,
        target_databricks_workspace_prefix: str = Form(""),
        target_s3_notebooks_prefix: str = Form(""),
        target_mwaa_dags_prefix: str = Form(""),
    ):
        updates: dict[str, str] = {}
        if target_databricks_workspace_prefix.strip():
            updates["TARGET_DATABRICKS_WORKSPACE_PREFIX"] = target_databricks_workspace_prefix.strip()
        if target_s3_notebooks_prefix.strip():
            updates["TARGET_S3_NOTEBOOKS_PREFIX"] = target_s3_notebooks_prefix.strip()
        if target_mwaa_dags_prefix.strip():
            updates["TARGET_MWAA_DAGS_PREFIX"] = target_mwaa_dags_prefix.strip()
        if updates:
            update_env(updates)
        return templates.TemplateResponse(
            request, "_card_target.html",
            {"state": current_state(), "result": {"saved": True}},
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
