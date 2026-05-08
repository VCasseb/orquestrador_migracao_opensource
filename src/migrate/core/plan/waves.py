from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import yaml
from pydantic import BaseModel, Field

from migrate.core.inventory.models import Inventory
from migrate.core.lineage.graph import DependencyGraph

PLANS_DIR = Path(".migrate/plans")

PlanKind = "table"  # legacy default; new kinds: notebook, dag


class WaveItem(BaseModel):
    fqn: str
    type: str
    complexity: str = "low"
    heat: str = "cold"
    size_bytes: int | None = None
    origin: str = "selected"   # "selected" | "auto-included"
    layer: str | None = None       # raw / bronze / silver / gold (notebooks only)
    reads: list[str] = Field(default_factory=list)    # tables it reads (notebooks)
    writes: list[str] = Field(default_factory=list)   # tables it writes (notebooks)


class Wave(BaseModel):
    index: int
    items: list[WaveItem]


class MigrationPlan(BaseModel):
    name: str
    created_at: datetime
    kind: str = "table"            # "table" | "notebook" | "dag"
    selected: list[str]            # the user's original selection
    auto_included: list[str] = Field(default_factory=list)
    missing_upstream: list[str] = Field(default_factory=list)
    waves: list[Wave] = Field(default_factory=list)


def build_plan(
    name: str, selected: Iterable[str], inv: Inventory, graph: DependencyGraph,
    include_upstream: bool = True,
) -> MigrationPlan:
    """Build a wave plan.

    When ``include_upstream`` is True (default), all upstream tables that exist in
    the inventory are auto-added to the plan so the chain is complete. External
    references (not in inventory) are recorded as ``missing_upstream``.
    """
    user_selected = set(selected)
    by_fqn = inv.by_fqn

    candidates = set(user_selected)
    auto_included: set[str] = set()
    missing_upstream: set[str] = set()

    if include_upstream:
        for fqn in user_selected:
            for upstream in graph.upstream_closure(fqn):
                if upstream in by_fqn:
                    if upstream not in user_selected:
                        auto_included.add(upstream)
                    candidates.add(upstream)
                elif "." in upstream and not upstream.startswith(("composer.", "notebook.", "sq.")):
                    missing_upstream.add(upstream)

    raw_waves = graph.topological_waves(candidates=candidates)

    waves: list[Wave] = []
    for i, names in enumerate(raw_waves, start=1):
        items: list[WaveItem] = []
        for fqn in names:
            t = by_fqn.get(fqn)
            if not t:
                continue
            items.append(WaveItem(
                fqn=fqn, type=t.type, complexity=t.complexity, heat=t.heat,
                size_bytes=t.size_bytes,
                origin="selected" if fqn in user_selected else "auto-included",
            ))
        if items:
            waves.append(Wave(index=i, items=items))

    return MigrationPlan(
        name=name,
        created_at=datetime.now(timezone.utc),
        selected=sorted(user_selected),
        auto_included=sorted(auto_included),
        missing_upstream=sorted(missing_upstream),
        waves=waves,
    )


def build_notebook_plan(
    name: str, selected_notebook_names: Iterable[str], inv: Inventory,
    include_upstream: bool = True,
) -> MigrationPlan:
    """Build dependency-ordered waves of NOTEBOOKS.

    Notebook A depends on notebook B if A reads a table that B writes.
    Auto-includes the upstream chain by default (raw → bronze → silver → gold).
    """
    user_selected = set(selected_notebook_names)
    by_name = inv.notebooks_by_id
    table_to_writer: dict[str, str] = {}
    for nb in inv.notebooks:
        for w in nb.all_written_tables:
            table_to_writer[w] = nb.name

    candidates: set[str] = set(user_selected)
    auto_included: set[str] = set()
    missing_upstream: set[str] = set()

    if include_upstream:
        stack = list(user_selected)
        seen: set[str] = set()
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            nb = by_name.get(n)
            if not nb:
                continue
            for ref in nb.all_referenced_tables:
                writer = table_to_writer.get(ref)
                if writer and writer in by_name:
                    if writer not in user_selected:
                        auto_included.add(writer)
                    if writer not in candidates:
                        candidates.add(writer)
                        stack.append(writer)
                else:
                    missing_upstream.add(ref)

    # Build upstream map among candidates
    upstream: dict[str, set[str]] = {n: set() for n in candidates}
    for n in candidates:
        nb = by_name.get(n)
        if not nb:
            continue
        for ref in nb.all_referenced_tables:
            w = table_to_writer.get(ref)
            if w and w in candidates and w != n:
                upstream[n].add(w)

    # Topological waves (Kahn-style)
    remaining = set(candidates)
    waves_raw: list[list[str]] = []
    while remaining:
        ready = sorted(n for n in remaining if not (upstream[n] & remaining))
        if not ready:
            waves_raw.append(sorted(remaining))
            break
        waves_raw.append(ready)
        remaining.difference_update(ready)

    waves: list[Wave] = []
    for i, names in enumerate(waves_raw, start=1):
        items: list[WaveItem] = []
        for n in names:
            nb = by_name.get(n)
            if not nb:
                continue
            items.append(WaveItem(
                fqn=n, type="NOTEBOOK",
                origin="selected" if n in user_selected else "auto-included",
                layer=nb.layer,
                reads=nb.all_referenced_tables,
                writes=nb.all_written_tables,
            ))
        if items:
            waves.append(Wave(index=i, items=items))

    return MigrationPlan(
        name=name, kind="notebook",
        created_at=datetime.now(timezone.utc),
        selected=sorted(user_selected),
        auto_included=sorted(auto_included),
        missing_upstream=sorted(missing_upstream),
        waves=waves,
    )


def build_dag_plan(
    name: str, selected_dag_names: Iterable[str], inv: Inventory,
    include_upstream: bool = True,
) -> MigrationPlan:
    """Build wave plan of DAGs. DAG A depends on DAG B if A reads a table B writes."""
    user_selected = set(selected_dag_names)
    by_name = inv.dags_by_id
    table_to_writer: dict[str, str] = {}
    for d in inv.dags:
        for w in d.writes_tables:
            table_to_writer[w] = d.name

    candidates: set[str] = set(user_selected)
    auto_included: set[str] = set()
    missing_upstream: set[str] = set()

    if include_upstream:
        stack = list(user_selected)
        seen: set[str] = set()
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            d = by_name.get(n)
            if not d:
                continue
            for ref in d.reads_tables:
                writer = table_to_writer.get(ref)
                if writer and writer in by_name:
                    if writer not in user_selected:
                        auto_included.add(writer)
                    if writer not in candidates:
                        candidates.add(writer)
                        stack.append(writer)
                else:
                    missing_upstream.add(ref)

    upstream: dict[str, set[str]] = {n: set() for n in candidates}
    for n in candidates:
        d = by_name.get(n)
        if not d:
            continue
        for ref in d.reads_tables:
            w = table_to_writer.get(ref)
            if w and w in candidates and w != n:
                upstream[n].add(w)

    remaining = set(candidates)
    waves_raw: list[list[str]] = []
    while remaining:
        ready = sorted(n for n in remaining if not (upstream[n] & remaining))
        if not ready:
            waves_raw.append(sorted(remaining))
            break
        waves_raw.append(ready)
        remaining.difference_update(ready)

    waves: list[Wave] = []
    for i, names in enumerate(waves_raw, start=1):
        items: list[WaveItem] = []
        for n in names:
            d = by_name.get(n)
            if not d:
                continue
            items.append(WaveItem(
                fqn=n, type="DAG",
                origin="selected" if n in user_selected else "auto-included",
                layer=d.layer,
                reads=d.reads_tables,
                writes=d.writes_tables,
            ))
        if items:
            waves.append(Wave(index=i, items=items))

    return MigrationPlan(
        name=name, kind="dag",
        created_at=datetime.now(timezone.utc),
        selected=sorted(user_selected),
        auto_included=sorted(auto_included),
        missing_upstream=sorted(missing_upstream),
        waves=waves,
    )


def save_plan(plan: MigrationPlan, dir: Path = PLANS_DIR) -> Path:
    dir.mkdir(parents=True, exist_ok=True)
    path = dir / f"{plan.name}.yaml"
    path.write_text(yaml.safe_dump(plan.model_dump(mode="json"), sort_keys=False))
    return path


def load_plan(path: Path) -> MigrationPlan:
    return MigrationPlan.model_validate(yaml.safe_load(path.read_text()))


def list_plans(dir: Path = PLANS_DIR) -> list[Path]:
    if not dir.exists():
        return []
    return sorted(dir.glob("*.yaml"))
