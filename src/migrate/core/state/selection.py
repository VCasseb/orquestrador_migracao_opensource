from __future__ import annotations

from pathlib import Path

import yaml

SELECTION_FILE = Path(".migrate/selection.yaml")


def load_selection(path: Path = SELECTION_FILE) -> set[str]:
    if not path.exists():
        return set()
    raw = yaml.safe_load(path.read_text()) or {}
    return set(raw.get("selected", []))


def save_selection(selected: set[str], path: Path = SELECTION_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"selected": sorted(selected)}, sort_keys=False))


def toggle(fqn: str, path: Path = SELECTION_FILE) -> bool:
    sel = load_selection(path)
    if fqn in sel:
        sel.discard(fqn)
        chosen = False
    else:
        sel.add(fqn)
        chosen = True
    save_selection(sel, path)
    return chosen
