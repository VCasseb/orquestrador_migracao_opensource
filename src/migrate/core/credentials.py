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


def test_gcs() -> TestResult:
    """Test the GCS lake: list a few notebook/script files in the configured
    bucket + folders. Independent of BigQuery."""
    bucket_uri = get_env("GCP_NOTEBOOKS_BUCKET")
    if not bucket_uri:
        return TestResult(False, "Defina o bucket do lake (GCS)")

    try:
        from google.cloud import storage
    except ImportError:
        return TestResult(False, "google-cloud-storage não instalado")

    from migrate.core.inventory.notebooks import _split_bucket, _NOTEBOOK_EXTS

    sa_path = get_env("GCP_SERVICE_ACCOUNT_JSON") or None
    folders = [f.strip().strip("/") for f in get_env("GCP_NOTEBOOKS_FOLDERS").split(",") if f.strip()]
    bucket_name, base_prefix = _split_bucket(bucket_uri)

    try:
        client = (
            storage.Client.from_service_account_json(sa_path)
            if sa_path else storage.Client()
        )
        bucket = client.bucket(bucket_name)
        prefixes = []
        for f in (folders or [""]):
            prefixes.append("/".join(p for p in (base_prefix, f) if p))
        found = 0
        examples: list[str] = []
        for prefix in prefixes:
            for blob in bucket.list_blobs(prefix=(prefix + "/" if prefix else ""), max_results=200):
                if blob.name.lower().endswith(_NOTEBOOK_EXTS):
                    found += 1
                    if len(examples) < 3:
                        examples.append(blob.name)
    except Exception as e:
        return TestResult(False, "Falha ao acessar o GCS", str(e))

    where = f"gs://{bucket_name}" + (f" · pastas: {', '.join(folders)}" if folders else "")
    if found == 0:
        return TestResult(False, f"Conectou, mas 0 .py/.ipynb em {where}", "Verifique as pastas.")
    return TestResult(True, f"{found} arquivo(s) encontrados em {where}", " · ".join(examples))


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
