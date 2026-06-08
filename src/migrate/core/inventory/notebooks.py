"""Notebook scanner — pulls .ipynb from GCS / Vertex Workbench bucket.

Parses cells, identifies SQL strings inside common BigQuery client calls,
flags imports that need rewriting on the Databricks side.
"""
from __future__ import annotations

import json
import re

from migrate.core.inventory.models import NotebookCell, NotebookMetadata
from migrate.core.lineage.parser import extract_refs

_SQL_INSIDE_QUERY_RE = re.compile(
    r"(?:client|bq|bigquery_client)\.query\(\s*[fr]?[\"']{1,3}(.+?)[\"']{1,3}", re.S
)
_PANDAS_GBQ_READ_RE = re.compile(
    r"pandas_gbq\.read_gbq\(\s*[fr]?[\"']{1,3}(.+?)[\"']{1,3}", re.S
)
_PANDAS_GBQ_WRITE_RE = re.compile(
    r"to_gbq\([^,]+,\s*[\"']([^\"']+)[\"']", re.S
)

_LIB_INTEREST = (
    "google.cloud.bigquery", "pandas_gbq", "google.cloud.storage",
    "google.cloud.aiplatform", "vertexai", "tensorflow", "torch",
)


def _detect_libs(source: str) -> list[str]:
    out: set[str] = set()
    for line in source.splitlines():
        s = line.strip()
        for lib in _LIB_INTEREST:
            if s.startswith(f"import {lib}") or s.startswith(f"from {lib}"):
                out.add(lib)
    return sorted(out)


def parse_notebook_py(content: str, name: str, location: str) -> NotebookMetadata:
    """Parse a Python-format notebook (.py).

    Handles the Databricks notebook source format that uses `# COMMAND ----------`
    as cell separators and `# MAGIC %md` for markdown cells. Falls back to a single
    code cell if no separators are found.
    """
    libraries = set(_detect_libs(content))

    # Split by Databricks COMMAND markers if present
    raw_cells: list[str] = []
    if "# COMMAND ----------" in content:
        raw_cells = [c.strip() for c in content.split("# COMMAND ----------") if c.strip()]
    else:
        raw_cells = [content]

    cells: list[NotebookCell] = []
    for chunk in raw_cells:
        # detect markdown cell (Databricks: lines start with `# MAGIC %md` or `# MAGIC #...`)
        is_md = chunk.lstrip().startswith("# MAGIC %md") or chunk.lstrip().startswith("# MAGIC ")
        if is_md:
            cells.append(NotebookCell(cell_type="markdown", source=chunk))
            continue
        c = NotebookCell(cell_type="code", source=chunk)
        sqls = _SQL_INSIDE_QUERY_RE.findall(chunk) + _PANDAS_GBQ_READ_RE.findall(chunk)
        if sqls:
            c.has_sql = True
            c.sql_extracted = "\n".join(s.strip() for s in sqls)
            refs: set[str] = set()
            for sql in sqls:
                refs.update(extract_refs(sql))
            c.referenced_tables = sorted(refs)
        writes = _PANDAS_GBQ_WRITE_RE.findall(chunk)
        if writes:
            c.writes_tables = sorted(set(writes))
        cells.append(c)

    return NotebookMetadata(
        name=name,
        location=location,
        kind="gcs_ipynb",
        cells=cells,
        libraries=sorted(libraries),
    )


def parse_notebook_file(filename: str, content: str, location: str) -> NotebookMetadata:
    """Dispatcher: parse .ipynb or .py based on filename suffix."""
    base = filename.rsplit(".", 1)[0]
    if filename.lower().endswith(".ipynb"):
        return parse_notebook_json(content, name=base, location=location)
    return parse_notebook_py(content, name=base, location=location)


def parse_notebook_json(content: str, name: str, location: str) -> NotebookMetadata:
    data = json.loads(content)
    cells_raw = data.get("cells", [])
    libraries: set[str] = set()

    cells: list[NotebookCell] = []
    for cell in cells_raw:
        ctype = cell.get("cell_type", "code")
        source_arr = cell.get("source", [])
        if isinstance(source_arr, list):
            source = "".join(source_arr)
        else:
            source = str(source_arr)

        c = NotebookCell(cell_type=ctype, source=source)
        if ctype == "code":
            libraries.update(_detect_libs(source))
            sqls = _SQL_INSIDE_QUERY_RE.findall(source) + _PANDAS_GBQ_READ_RE.findall(source)
            if sqls:
                c.has_sql = True
                c.sql_extracted = "\n".join(s.strip() for s in sqls)
                refs: set[str] = set()
                for sql in sqls:
                    refs.update(extract_refs(sql))
                c.referenced_tables = sorted(refs)
            writes = _PANDAS_GBQ_WRITE_RE.findall(source)
            if writes:
                c.writes_tables = sorted(set(writes))
        cells.append(c)

    return NotebookMetadata(
        name=name,
        location=location,
        kind="gcs_ipynb",
        cells=cells,
        libraries=sorted(libraries),
    )


_NOTEBOOK_EXTS = (".py", ".ipynb")


def _split_bucket(bucket_uri: str) -> tuple[str, str]:
    """`gs://bucket/some/prefix` → (`bucket`, `some/prefix`). Bare bucket is fine too."""
    if bucket_uri.startswith("gs://"):
        bucket_uri = bucket_uri[5:]
    bucket_uri = bucket_uri.strip("/")
    parts = bucket_uri.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


def scan_gcs_notebooks(
    bucket_uri: str,
    folders: list[str] | None = None,
    sa_path: str | None = None,
) -> list[NotebookMetadata]:
    """Scan a GCS lake bucket for notebooks/scripts (.py AND .ipynb).

    `folders` is a list of prefixes within the bucket (e.g. ["scripts", "dags/prod"]).
    When empty, the whole bucket (or the prefix embedded in bucket_uri) is scanned.
    Raises on auth / bucket errors so the caller can surface a clear message.
    """
    from google.cloud import storage

    bucket_name, base_prefix = _split_bucket(bucket_uri)
    client = (
        storage.Client.from_service_account_json(sa_path)
        if sa_path else storage.Client()
    )
    bucket = client.bucket(bucket_name)

    # Build the list of prefixes to walk (base prefix + each requested folder).
    folders = [f.strip().strip("/") for f in (folders or []) if f.strip()]
    if folders:
        prefixes = []
        for f in folders:
            joined = "/".join(p for p in (base_prefix, f) if p)
            prefixes.append(joined + "/" if joined else "")
    else:
        prefixes = [base_prefix + "/" if base_prefix else ""]

    out: list[NotebookMetadata] = []
    seen: set[str] = set()
    for prefix in prefixes:
        for blob in bucket.list_blobs(prefix=prefix):
            full = blob.name
            if full.endswith("/") or not full.lower().endswith(_NOTEBOOK_EXTS):
                continue
            if full in seen:
                continue
            seen.add(full)
            filename = full.split("/")[-1]
            try:
                content = blob.download_as_text()
                nb = parse_notebook_file(filename, content, location=f"gs://{bucket_name}/{full}")
                out.append(nb)
            except Exception:
                # Skip a single unreadable/unparseable file; keep scanning the rest.
                continue
    return out


def scan_notebooks_bucket(bucket_uri: str, sa_path: str | None = None) -> list[NotebookMetadata]:
    """Back-compat shim: scan the whole bucket/prefix for .py + .ipynb notebooks."""
    return scan_gcs_notebooks(bucket_uri, folders=None, sa_path=sa_path)
