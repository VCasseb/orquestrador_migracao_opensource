from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import yaml
from pydantic import BaseModel, Field

from migrate.core.inventory.models import Inventory
from migrate.core.lineage.graph import DependencyGraph

PLANS_DIR = Path(".migrate/plans")


class WaveItem(BaseModel):
    fqn: str
    type: str
    complexity: str
    heat: str
    size_bytes: int | None = None
    origin: str = "selected"   # "selected" | "auto-included"


class Wave(BaseModel):
    index: int
    items: list[WaveItem]


class MigrationPlan(BaseModel):
    name: str
    created_at: datetime
    selected: list[str]                          # the user's original selection
    auto_included: list[str] = Field(default_factory=list)
    missing_upstream: list[str] = Field(default_factory=list)  # external refs not in inventory
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
