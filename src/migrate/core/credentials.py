from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv, set_key

ENV_FILE = Path(".env")


@dataclass
class TestResult:
    ok: bool
    message: str
    detail: str = ""


def load_env(path: Path = ENV_FILE) -> None:
    if path.exists():
        load_dotenv(dotenv_path=path, override=True)


def update_env(updates: dict[str, str], path: Path = ENV_FILE) -> None:
    """Write/update keys in .env. Creates file if missing."""
    if not path.exists():
        path.touch()
    for key, value in updates.items():
        set_key(str(path), key, value, quote_mode="never")


def get_env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def test_gcp() -> TestResult:
    project_ids = [p.strip() for p in get_env("GCP_PROJECT_IDS").split(",") if p.strip()]
    if not project_ids:
        return TestResult(False, "Missing GCP_PROJECT_IDS")

    try:
        from google.cloud import bigquery
    except ImportError:
        return TestResult(False, "google-cloud-bigquery not installed")

    sa_path = get_env("GCP_SERVICE_ACCOUNT_JSON")
    try:
        if sa_path:
            client = bigquery.Client.from_service_account_json(sa_path, project=project_ids[0])
        else:
            client = bigquery.Client(project=project_ids[0])
        datasets = list(client.list_datasets(max_results=5))
    except Exception as e:
        return TestResult(False, "Connection failed", str(e))

    return TestResult(
        True,
        f"Connected to {project_ids[0]}",
        f"{len(datasets)} dataset(s) visible (showing up to 5)",
    )


def test_databricks() -> TestResult:
    host = get_env("DATABRICKS_HOST")
    token = get_env("DATABRICKS_TOKEN")
    if not host or not token:
        return TestResult(False, "Missing DATABRICKS_HOST or DATABRICKS_TOKEN")

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        return TestResult(False, "databricks-sdk not installed")

    try:
        client = WorkspaceClient(host=host, token=token)
        catalogs = list(client.catalogs.list())
    except Exception as e:
        return TestResult(False, "Connection failed", str(e))

    return TestResult(
        True,
        f"Connected to {host}",
        f"{len(catalogs)} catalog(s) visible",
    )


def test_anthropic() -> TestResult:
    api_key = get_env("ANTHROPIC_API_KEY")
    if not api_key:
        return TestResult(False, "Missing ANTHROPIC_API_KEY")

    try:
        from anthropic import Anthropic
    except ImportError:
        return TestResult(False, "anthropic not installed")

    model = get_env("ANTHROPIC_MODEL", "claude-sonnet-4-6")
    try:
        client = Anthropic(api_key=api_key)
        client.messages.create(
            model=model,
            max_tokens=1,
            messages=[{"role": "user", "content": "hi"}],
        )
    except Exception as e:
        return TestResult(False, "Request failed", str(e))

    return TestResult(True, f"API responded with model {model}")
