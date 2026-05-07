from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from migrate.core.inventory.models import Inventory
from migrate.core.lineage.parser import extract_refs


@dataclass
class LineageEdge:
    upstream: str
    downstream: str


@dataclass
class DependencyGraph:
    nodes: set[str] = field(default_factory=set)
    upstream: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    downstream: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))

    def add_node(self, fqn: str) -> None:
        self.nodes.add(fqn)
        self.upstream.setdefault(fqn, set())
        self.downstream.setdefault(fqn, set())

    def add_edge(self, upstream: str, downstream: str) -> None:
        if upstream == downstream:
            return
        self.add_node(upstream)
        self.add_node(downstream)
        self.upstream[downstream].add(upstream)
        self.downstream[upstream].add(downstream)

    @property
    def edges(self) -> list[LineageEdge]:
        return [
            LineageEdge(u, d)
            for d, ups in self.upstream.items()
            for u in ups
        ]

    def topological_waves(self, candidates: set[str] | None = None) -> list[list[str]]:
        """Group nodes into dependency-ordered waves (Kahn-style).

        Only nodes in `candidates` are placed in waves; others are treated as "already
        present" (their existence satisfies upstream deps but they're not migrated).
        Cycles are broken by emitting remaining nodes in a final wave.
        """
        target = set(candidates) if candidates is not None else set(self.nodes)
        remaining = set(target)
        waves: list[list[str]] = []

        while remaining:
            ready = sorted(
                n for n in remaining
                if not (self.upstream.get(n, set()) & remaining)
            )
            if not ready:
                waves.append(sorted(remaining))
                break
            waves.append(ready)
            remaining.difference_update(ready)

        return waves

    def upstream_closure(self, fqn: str) -> set[str]:
        seen: set[str] = set()
        stack = [fqn]
        while stack:
            n = stack.pop()
            for u in self.upstream.get(n, set()):
                if u not in seen:
                    seen.add(u)
                    stack.append(u)
        return seen

    def downstream_closure(self, fqn: str) -> set[str]:
        seen: set[str] = set()
        stack = [fqn]
        while stack:
            n = stack.pop()
            for d in self.downstream.get(n, set()):
                if d not in seen:
                    seen.add(d)
                    stack.append(d)
        return seen


def build_graph(inv: Inventory) -> DependencyGraph:
    g = DependencyGraph()
    for t in inv.tables:
        g.add_node(t.fqn)

    # Edges from views and DAG/scheduled SQL refs
    for t in inv.tables:
        sql = t.view_query or t.source_sql
        if sql:
            for ref in extract_refs(sql, default_project=t.project, default_dataset=t.dataset):
                g.add_edge(ref, t.fqn)

    # DAG nodes — connect them to tables they read/write
    for d in inv.dags:
        node_id = d.fqn  # "composer.<name>"
        g.add_node(node_id)
        for read in d.reads_tables:
            g.add_edge(read, node_id)
        for write in d.writes_tables:
            g.add_edge(node_id, write)

    # Notebook nodes
    for nb in inv.notebooks:
        node_id = nb.fqn  # "notebook.<name>"
        g.add_node(node_id)
        for read in nb.all_referenced_tables:
            g.add_edge(read, node_id)
        for write in nb.all_written_tables:
            g.add_edge(node_id, write)

    # Scheduled queries
    for sq in inv.scheduled_queries:
        node_id = sq.fqn  # "sq.<name>"
        g.add_node(node_id)
        if sq.query:
            for ref in extract_refs(sq.query):
                g.add_edge(ref, node_id)
        if sq.destination_table:
            g.add_edge(node_id, sq.destination_table)

    return g
