"""Azure DevOps Wiki Markdown generator — one document per table.

Combines inventory + conversion + validation + approval log into a single
human-readable doc you can paste straight into the Azure DevOps Wiki.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from migrate.core.convert.ddl import _map_type
from migrate.core.convert.runner import load_conversion
from migrate.core.credentials import get_env, load_env
from migrate.core.inventory.catalog import load_inventory
from migrate.core.lineage.graph import build_graph
from migrate.core.state.approval import get_all_states
from migrate.core.state.audit import read_log

DOCS_DIR = Path(".migrate/docs")


def _human_size(b: int | None) -> str:
    if not b:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.0f}{unit}"
        b /= 1024
    return f"{b:.1f}PB"


def _human_count(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:,}"


def _lineage_mermaid(fqn: str) -> str:
    inv = load_inventory()
    if not inv:
        return ""
    graph = build_graph(inv)
    upstream = graph.upstream_closure(fqn)
    downstream = graph.downstream_closure(fqn)
    nodes = upstream | downstream | {fqn}
    if not nodes or len(nodes) == 1:
        return ""

    def safe(n: str) -> str:
        return n.replace(".", "_").replace("-", "_")

    lines = ["```mermaid", "graph LR"]
    for n in sorted(nodes):
        label = ".".join(n.split(".")[-2:])
        style = ":::focus" if n == fqn else ""
        lines.append(f'    {safe(n)}["{label}"]{style}')
    edges_seen = set()
    for d, ups in graph.upstream.items():
        if d not in nodes:
            continue
        for u in ups:
            if u in nodes:
                key = (u, d)
                if key not in edges_seen:
                    edges_seen.add(key)
                    lines.append(f"    {safe(u)} --> {safe(d)}")
    lines.append("    classDef focus fill:#06b6d4,stroke:#22d3ee,color:#fff;")
    lines.append("```")
    return "\n".join(lines)


def _action_history_table(fqn: str) -> str:
    log = read_log(fqn)
    if not log:
        return "_(no recorded actions)_"
    rows = ["| When | Who | Action | Result | Detail |", "|---|---|---|---|---|"]
    for e in log[-20:]:
        ts = e.get("ts", "—")[:19].replace("T", " ")
        user = e.get("user", "—")
        action = e.get("action", "—")
        result = e.get("result", "ok")
        payload = e.get("payload", {})
        detail_parts = []
        for k in ("method", "verdict", "confidence", "comment", "error"):
            if k in payload and payload[k]:
                detail_parts.append(f"{k}=`{payload[k]}`")
        detail = " · ".join(detail_parts) or "—"
        rows.append(f"| {ts} | {user} | `{action}` | {result} | {detail} |")
    return "\n".join(rows)


def _validation_section(fqn: str) -> str:
    from migrate.core.validate.report import load_report
    p = DOCS_DIR.parent / "reports" / f"{fqn.replace('.', '_')}.yaml"
    if not p.exists():
        return "_(no validation report yet)_"
    rep = load_report(p)
    vc = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(rep.verdict, "•")
    rows = [
        f"**Overall verdict:** {vc} **{rep.verdict.upper()}**",
        f"**Run at:** {rep.created_at.strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Source ↔ Target:** `{rep.fqn_source}` ↔ `{rep.fqn_target}`",
        "",
        "| Metric | Source | Target | Δ |",
        "|---|---|---|---|",
        f"| row_count | {rep.source.row_count:,} | {rep.target.row_count:,} | {rep.row_count_diff_pct:+.2f}% |",
    ]
    if rep.diagnosis:
        rows += ["", f":::warning", "**Diagnosis:** " + rep.diagnosis, ":::"]
    rows += [
        "",
        "### Per-column drift",
        "",
        "| Column | NULL drift | Distinct drift | Avg drift | Verdict |",
        "|---|---|---|---|---|",
    ]
    for d in rep.columns_diff:
        avg = f"{d.avg_diff_pct:+.2f}%" if d.avg_diff_pct is not None else "—"
        v = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(d.verdict, "•")
        rows.append(
            f"| `{d.name}` | {d.null_count_diff_pct:+.2f}% | {d.distinct_count_diff_pct:+.2f}% | {avg} | {v} |"
        )
    return "\n".join(rows)


def _conversion_section(fqn: str) -> str:
    art = load_conversion(fqn)
    if not art:
        return "_(no conversion artifact yet)_"
    parts = [
        f"**Method:** `{art.method}` · **Confidence:** `{art.confidence}`",
    ]
    if art.llm_model_used:
        parts.append(f"**LLM model:** `{art.llm_model_used}`")
    if art.notes:
        parts.append("\n### What changed\n" + "\n".join(f"- {n}" for n in art.notes))
    if art.error:
        parts.append(f"\n:::warning\n**Error:** {art.error}\n:::")
    if art.converted_sql:
        parts += [
            "\n### Converted SQL",
            "```sql",
            art.converted_sql.strip(),
            "```",
        ]
    parts += [
        "\n### Delta DDL",
        "```sql",
        art.ddl.strip(),
        "```",
    ]
    if art.notebook_path:
        parts.append(f"\n**Notebook artifact:** `{art.notebook_path}`")
    return "\n".join(parts)


def _approval_section(fqn: str) -> str:
    states = get_all_states(fqn)
    rows = ["| Stage | Status | By | When | Comment |", "|---|---|---|---|---|"]
    icon = {"pending": "⏳", "approved": "✅", "rejected": "❌", "revoked": "↩️"}
    for stage, st in states.items():
        when = st.at.strftime("%Y-%m-%d %H:%M UTC") if st.at else "—"
        by = st.by or "—"
        rows.append(f"| {stage} | {icon.get(st.status, '•')} {st.status} | {by} | {when} | {st.comment or '—'} |")
    return "\n".join(rows)


def render_table_doc(fqn: str) -> str:
    load_env()
    inv = load_inventory()
    if not inv:
        raise RuntimeError("No inventory loaded.")
    table = inv.by_fqn.get(fqn)
    if not table:
        raise RuntimeError(f"Not found: {fqn}")

    target_catalog = get_env("DATABRICKS_DEFAULT_CATALOG", "migrated_from_gcp")
    target_fqn = f"{target_catalog}.{table.dataset}.{table.name}"

    schema_rows = ["| Column | BQ type | Delta type | Nullable | Notes |", "|---|---|---|---|---|"]
    for c in table.columns:
        delta_type = _map_type(c.type)
        nullable = "yes" if c.nullable else "no"
        notes = ""
        if c.type.upper() in ("JSON", "GEOGRAPHY", "INTERVAL"):
            notes = "⚠️ no direct Delta equivalent — coerced to STRING"
        schema_rows.append(f"| `{c.name}` | `{c.type}` | `{delta_type}` | {nullable} | {notes} |")

    parts: list[str] = []
    parts.append("[[_TOC_]]")
    parts.append("")
    parts.append(f"# `{target_fqn}`")
    parts.append("")
    parts.append(f"> Migrated from `{table.fqn}` · generated by `migrate` on {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    parts.append("")
    parts.append("## Source")
    parts.append("")
    parts.append("| | |")
    parts.append("|---|---|")
    parts.append(f"| FQN | `{table.fqn}` |")
    parts.append(f"| Type | {table.type} |")
    parts.append(f"| Rows | {_human_count(table.row_count)} |")
    parts.append(f"| Size | {_human_size(table.size_bytes)} |")
    if table.partitioning:
        parts.append(f"| Partitioned by | `{table.partitioning}` |")
    if table.clustering:
        parts.append(f"| Clustered by | {', '.join(f'`{c}`' for c in table.clustering)} |")
    parts.append(f"| Heat (30d queries) | {table.heat} ({_human_count(table.query_count_30d)}) |")
    parts.append(f"| Complexity | {table.complexity} |")
    parts.append(f"| Source kind | `{table.source_kind}` |")
    if table.source_dag_id:
        parts.append(f"| Source DAG | `{table.source_dag_id}` (task: `{table.source_dag_task or '—'}`) |")
    if table.source_notebook_id:
        parts.append(f"| Source notebook | `{table.source_notebook_id}` |")
    if table.source_scheduled_query_id:
        parts.append(f"| Source scheduled query | `{table.source_scheduled_query_id}` |")
    if table.source_schedule:
        parts.append(f"| Source schedule | `{table.source_schedule}` *(use as Databricks Workflow cron)* |")
    if table.description:
        parts.append(f"| Description | {table.description} |")

    parts.append("")
    parts.append("## Target")
    parts.append("")
    parts.append(f"`{target_fqn}` (Delta table in Unity Catalog)")

    mermaid = _lineage_mermaid(fqn)
    if mermaid:
        parts.append("")
        parts.append("## Lineage")
        parts.append("")
        parts.append(mermaid)

    parts.append("")
    parts.append("## Schema")
    parts.append("")
    parts.append("\n".join(schema_rows))

    parts.append("")
    parts.append("## Conversion")
    parts.append("")
    parts.append(_conversion_section(fqn))

    parts.append("")
    parts.append("## Validation")
    parts.append("")
    parts.append(_validation_section(fqn))

    parts.append("")
    parts.append("## Approvals")
    parts.append("")
    parts.append(_approval_section(fqn))

    parts.append("")
    parts.append("## Action history")
    parts.append("")
    parts.append(_action_history_table(fqn))

    return "\n".join(parts) + "\n"


def save_doc(fqn: str, content: str | None = None) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    body = content or render_table_doc(fqn)
    path = DOCS_DIR / f"{fqn.replace('.', '_')}.md"
    path.write_text(body)
    return path


def render_plan_index(plan_name: str) -> str:
    from migrate.core.plan.waves import load_plan
    p = Path(".migrate/plans") / f"{plan_name}.yaml"
    if not p.exists():
        raise RuntimeError(f"Plan not found: {plan_name}")
    plan = load_plan(p)

    lines = ["[[_TOC_]]", "", f"# Migration plan: `{plan.name}`", "",
             f"> Generated {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} · {len(plan.selected)} table(s) across {len(plan.waves)} wave(s)", ""]
    for w in plan.waves:
        lines.append(f"## Wave {w.index} ({len(w.items)} tables)")
        lines.append("")
        lines.append("| Table | Type | Complexity | Heat |")
        lines.append("|---|---|---|---|")
        for it in w.items:
            anchor = it.fqn.replace(".", "_")
            lines.append(f"| [{it.fqn}](./{anchor}.md) | {it.type} | {it.complexity} | {it.heat} |")
        lines.append("")
    return "\n".join(lines) + "\n"


def list_docs() -> list[Path]:
    if not DOCS_DIR.exists():
        return []
    return sorted(DOCS_DIR.glob("*.md"))
