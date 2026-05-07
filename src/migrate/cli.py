from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import typer
from rich.console import Console

from migrate.core.config import CONFIG_DIR, CONFIG_FILE, MigrationConfig, save_config
from migrate.core.credentials import ENV_FILE, load_env

app = typer.Typer(help="GCP → Databricks/AWS migration framework", no_args_is_help=True)
console = Console()

ROOT = Path.cwd()
ENV_EXAMPLE = Path(".env.example")


@app.command()
def init() -> None:
    """Initialize a project: creates .migrate/, .env, ensures .gitignore."""
    CONFIG_DIR.mkdir(exist_ok=True)

    if not CONFIG_FILE.exists():
        save_config(MigrationConfig())
        console.print(f"[green]✓[/green] Created [bold]{CONFIG_FILE}[/bold]")
    else:
        console.print(f"[yellow]·[/yellow] {CONFIG_FILE} already exists")

    if not ENV_FILE.exists():
        if ENV_EXAMPLE.exists():
            shutil.copy(ENV_EXAMPLE, ENV_FILE)
            console.print(f"[green]✓[/green] Created [bold]{ENV_FILE}[/bold] from .env.example")
        else:
            ENV_FILE.touch()
            console.print(f"[green]✓[/green] Created empty [bold]{ENV_FILE}[/bold]")
    else:
        console.print(f"[yellow]·[/yellow] {ENV_FILE} already exists")

    gitignore = Path(".gitignore")
    needed = [".env", ".migrate/credentials/", ".migrate/runs/", ".migrate/reports/"]
    if gitignore.exists():
        existing = gitignore.read_text()
        missing = [line for line in needed if line not in existing]
        if missing:
            with gitignore.open("a") as f:
                f.write("\n# added by migrate init\n" + "\n".join(missing) + "\n")
            console.print(f"[green]✓[/green] Updated .gitignore ({len(missing)} entries)")

    console.print("\n[bold]Next:[/bold]")
    console.print("  1. Edit [cyan].env[/cyan] with your credentials, or")
    console.print("  2. Run [cyan]migrate web[/cyan] and use the Connections page")


@app.command()
def web(
    host: str = typer.Option(None, help="Bind host (default 127.0.0.1)"),
    port: int = typer.Option(None, help="Port (default 8000)"),
    reload: bool = typer.Option(False, "--reload", help="Auto-reload on code changes"),
) -> None:
    """Launch the local web UI."""
    load_env()
    import os

    bind_host = host or os.environ.get("MIGRATE_WEB_HOST", "127.0.0.1")
    bind_port = port or int(os.environ.get("MIGRATE_WEB_PORT", "8000"))

    console.print(f"[bold green]migrate[/bold green] web → http://{bind_host}:{bind_port}\n")

    cmd = [
        sys.executable, "-m", "uvicorn",
        "migrate.web.app:app",
        "--host", bind_host,
        "--port", str(bind_port),
    ]
    if reload:
        cmd.append("--reload")
    subprocess.run(cmd, check=False)


@app.command()
def inventory(
    sample: bool = typer.Option(False, "--sample", help="Use synthetic sample data instead of GCP."),
) -> None:
    """Scan GCP projects and write .migrate/inventory.yaml."""
    load_env()
    from migrate.core.inventory.catalog import save_inventory
    from migrate.core.inventory.scanner import run_scan
    from rich.table import Table

    inv = run_scan(use_sample=sample)
    save_inventory(inv)
    console.print(f"[green]✓[/green] Saved inventory: {len(inv.tables)} object(s) across {len(inv.projects)} project(s)")

    summary = Table(show_header=True, header_style="bold")
    summary.add_column("Project")
    summary.add_column("Tables", justify="right")
    summary.add_column("Views", justify="right")
    summary.add_column("Other", justify="right")
    by_proj: dict[str, dict[str, int]] = {}
    for t in inv.tables:
        bucket = by_proj.setdefault(t.project, {"TABLE": 0, "VIEW": 0, "OTHER": 0})
        bucket[t.type if t.type in ("TABLE", "VIEW") else "OTHER"] = bucket.get(
            t.type if t.type in ("TABLE", "VIEW") else "OTHER", 0
        ) + 1
    for proj, b in sorted(by_proj.items()):
        summary.add_row(proj, str(b["TABLE"]), str(b["VIEW"]), str(b["OTHER"]))
    console.print(summary)


@app.command()
def plan(
    name: str = typer.Argument(..., help="Plan name (e.g. sprint-1)"),
) -> None:
    """Build a wave plan from currently-selected tables."""
    load_env()
    from migrate.core.inventory.catalog import load_inventory
    from migrate.core.lineage.graph import build_graph
    from migrate.core.plan.waves import build_plan, save_plan
    from migrate.core.state.selection import load_selection

    inv = load_inventory()
    if not inv:
        console.print("[red]No inventory.[/red] Run `migrate inventory` first.")
        raise typer.Exit(1)
    selected = load_selection()
    if not selected:
        console.print("[red]No tables selected.[/red] Use the Inventory page or set them in .migrate/selection.yaml.")
        raise typer.Exit(1)

    graph = build_graph(inv)
    plan_obj = build_plan(name, selected, inv, graph)
    path = save_plan(plan_obj)
    console.print(f"[green]✓[/green] Saved plan to {path} — {len(plan_obj.waves)} wave(s), {len(plan_obj.selected)} table(s)")
    for w in plan_obj.waves:
        console.print(f"  Wave {w.index}: {len(w.items)} table(s)")


@app.command()
def convert(
    fqn: str = typer.Argument(..., help="Fully-qualified BigQuery name: project.dataset.table"),
    no_llm: bool = typer.Option(False, "--no-llm", help="Disable Claude fallback even if sqlglot fails."),
) -> None:
    """Transpile SQL, generate Delta DDL and a notebook for one object."""
    load_env()
    from migrate.core.convert.runner import convert_table
    art = convert_table(fqn=fqn, use_llm_fallback=not no_llm)
    color = {"high": "green", "medium": "yellow", "0%": "red"}.get(art.confidence, "white")
    console.print(f"[{color}]✓[/{color}] {fqn} — method={art.method}, confidence={art.confidence}")
    for n in art.notes:
        console.print(f"  · {n}")
    if art.error:
        console.print(f"[red]error:[/red] {art.error}")


@app.command()
def validate(
    fqn: str = typer.Argument(..., help="Source BQ FQN to validate."),
    sample: bool = typer.Option(False, "--sample", help="Generate synthetic report instead of hitting real systems."),
) -> None:
    """Profile source vs target and emit a validation report."""
    load_env()
    from migrate.core.validate.report import save_report
    if sample:
        from migrate.core.validate.sample import build_synthetic_report
        report = build_synthetic_report(fqn)
    else:
        from migrate.core.validate.runner import run_validation
        report = run_validation(fqn)
    path = save_report(report)
    color = {"pass": "green", "warn": "yellow", "fail": "red"}[report.verdict]
    console.print(f"[{color}]{report.verdict.upper()}[/{color}] · {fqn} · saved {path}")
    console.print(f"  row count Δ: {report.row_count_diff_pct:+.2f}%")
    fail_cols = [c for c in report.columns_diff if c.verdict == "fail"]
    if fail_cols:
        console.print(f"  [red]{len(fail_cols)} column(s) failing[/red]: {', '.join(c.name for c in fail_cols[:5])}")


@app.command()
def status() -> None:
    """Quick summary of project state."""
    from migrate.core.inventory.catalog import load_inventory
    from migrate.core.plan.waves import list_plans
    from migrate.core.state.selection import load_selection
    from migrate.core.validate.report import list_reports
    from migrate.core.convert.runner import list_conversions
    from migrate.core.deploy.models import list_states

    inv = load_inventory()
    sel = load_selection()
    plans = list_plans()
    reports = list_reports()
    conversions = list_conversions()
    deployments = list_states()

    console.print(f"  Inventory:    {len(inv.tables) if inv else 0} object(s)")
    console.print(f"  Selected:     {len(sel)} table(s)")
    console.print(f"  Plans:        {len(plans)}")
    console.print(f"  Conversions:  {len(conversions)}")
    console.print(f"  Reports:      {len(reports)}")
    console.print(f"  Deployments:  {len(deployments)}")


@app.command()
def approve(
    fqn: str = typer.Argument(...),
    stage: str = typer.Argument(..., help="conversion | validation | deployment"),
    comment: str = typer.Option("", "--comment", "-m"),
) -> None:
    """Mark an artifact as approved by current user."""
    from migrate.core.state.approval import approve as approve_a
    approve_a(fqn, stage, comment)
    console.print(f"[green]✓[/green] {stage} approved: {fqn}")


@app.command()
def reject(
    fqn: str = typer.Argument(...),
    stage: str = typer.Argument(...),
    comment: str = typer.Option("", "--comment", "-m"),
) -> None:
    """Mark an artifact as rejected."""
    from migrate.core.state.approval import reject as reject_a
    reject_a(fqn, stage, comment)
    console.print(f"[red]✗[/red] {stage} rejected: {fqn}")


@app.command()
def history(
    fqn: str = typer.Argument(None, help="Filter to one FQN (or omit for global log)"),
    limit: int = typer.Option(20, "--limit", "-n"),
) -> None:
    """Show audit log entries (most recent last)."""
    from migrate.core.state.audit import read_log
    entries = read_log(fqn, limit=limit)
    if not entries:
        console.print("[dim](no entries)[/dim]")
        return
    for e in entries:
        ts = e.get("ts", "")[:19].replace("T", " ")
        user = e.get("user", "—")
        action = e.get("action", "—")
        result = e.get("result", "ok")
        target = e.get("fqn", "—") or "—"
        color = "green" if result == "ok" else "red"
        console.print(f"  [{color}]{ts}[/{color}] {user:24s} [bold]{action:28s}[/bold] {target}")


@app.command()
def rollback(
    kind: str = typer.Argument(..., help="conversion | validation | approval | deployment"),
    fqn: str = typer.Argument(...),
    stage: str = typer.Option(None, "--stage", help="for approval rollback only"),
    reason: str = typer.Option("", "--reason"),
    execute: bool = typer.Option(False, "--execute", help="for deployment: actually run DROP TABLE etc."),
) -> None:
    """Roll back a previous action."""
    from migrate.core.state.rollback import (
        rollback_approval, rollback_conversion, rollback_deployment, rollback_validation,
    )
    if kind == "conversion":
        rollback_conversion(fqn, reason)
    elif kind == "validation":
        rollback_validation(fqn, reason)
    elif kind == "approval":
        if not stage:
            console.print("[red]--stage required for approval rollback[/red]")
            raise typer.Exit(1)
        rollback_approval(fqn, stage, reason)
    elif kind == "deployment":
        rollback_deployment(fqn, reason, execute=execute)
    else:
        console.print(f"[red]Unknown rollback kind: {kind}[/red]")
        raise typer.Exit(1)
    console.print(f"[yellow]↩[/yellow] rolled back {kind} for {fqn}")


@app.command()
def deploy(
    plan_name: str = typer.Argument(..., help="Plan name (e.g. sprint-1) or path to YAML"),
    mode: str = typer.Option("sample", "--mode", "-m", help="sample | dry-run | execute"),
    concurrency: int = typer.Option(4, "--concurrency", "-c"),
    no_approvals: bool = typer.Option(False, "--no-approvals", help="Skip approval gating (dangerous in execute mode)"),
    create_workflow: bool = typer.Option(False, "--create-workflow"),
    stop_on_failure: bool = typer.Option(False, "--stop-on-failure"),
    auto_validate: bool = typer.Option(False, "--auto-validate", help="Run validation on each table immediately after deploy completes"),
) -> None:
    """Deploy a wave plan to Databricks."""
    from migrate.core.deploy.runner import deploy_plan
    summary = deploy_plan(
        plan_name, mode=mode, concurrency=concurrency,
        require_approvals=not no_approvals,
        create_workflow=create_workflow,
        stop_on_failure=stop_on_failure,
        auto_validate=auto_validate,
    )
    console.print(f"\nDeploy [bold]{summary['plan']}[/bold] · mode={summary['mode']}")
    for w in summary["waves"]:
        console.print(f"  Wave {w['index']}: [green]{w['ok']} ok[/green] · "
                      f"[yellow]{w['skipped']} skipped[/yellow] · [red]{w['failed']} failed[/red]")
        for t in w["tables"]:
            color = {"completed": "green", "failed": "red", "skipped": "yellow"}.get(t["status"], "white")
            console.print(f"    [{color}]{t['status']:10s}[/{color}] {t['fqn']}")


@app.command()
def docs(
    fqn: str = typer.Argument(None, help="Single table FQN (omit to use --plan)"),
    plan: str = typer.Option(None, "--plan", help="Generate docs for every table in this plan"),
) -> None:
    """Generate Azure DevOps Wiki Markdown docs."""
    from migrate.core.docs.wiki import save_doc, render_plan_index, DOCS_DIR
    if plan:
        from migrate.core.plan.waves import load_plan
        from pathlib import Path
        p = Path(".migrate/plans") / f"{plan}.yaml"
        plan_obj = load_plan(p)
        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        (DOCS_DIR / f"{plan}_index.md").write_text(render_plan_index(plan))
        for fqn in plan_obj.selected:
            try:
                save_doc(fqn)
                console.print(f"[green]✓[/green] {fqn}.md")
            except Exception as e:
                console.print(f"[red]✗[/red] {fqn}: {e}")
        console.print(f"\nGenerated docs in {DOCS_DIR}/")
    elif fqn:
        path = save_doc(fqn)
        console.print(f"[green]✓[/green] saved {path}")
    else:
        console.print("[red]Provide either FQN or --plan[/red]")
        raise typer.Exit(1)


@app.command()
def version() -> None:
    """Show version."""
    from migrate import __version__
    console.print(f"migrate {__version__}")


if __name__ == "__main__":
    app()
