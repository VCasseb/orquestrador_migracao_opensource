from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from migrate.core.inventory.models import TableMetadata

TEMPLATES_DIR = Path(__file__).parent / "_templates"


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(disabled_extensions=("py", "j2")),
        keep_trailing_newline=True,
    )


def render_notebook(
    table: TableMetadata,
    converted_sql: str,
    target_catalog: str,
    target_schema: str | None = None,
    merge_keys: list[str] | None = None,
) -> str:
    schema = target_schema or table.dataset
    target_fqn = f"{target_catalog}.{schema}.{table.name}"
    pk_guess = merge_keys or _guess_keys(table)

    env = _env()
    tmpl = env.get_template("notebook.py.j2")
    return tmpl.render(
        table=table,
        converted_sql=converted_sql.strip(),
        target_fqn=target_fqn,
        target_catalog=target_catalog,
        target_schema=schema,
        merge_keys=pk_guess,
        is_view=table.type == "VIEW",
    )


def _guess_keys(table: TableMetadata) -> list[str]:
    candidates = []
    for c in table.columns:
        n = c.name.lower()
        if n.endswith("_id") or n == "id":
            candidates.append(c.name)
    return candidates[:2]
