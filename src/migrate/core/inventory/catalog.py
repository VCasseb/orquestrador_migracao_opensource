from __future__ import annotations

from pathlib import Path

import yaml

from migrate.core.inventory.models import Inventory

INVENTORY_FILE = Path(".migrate/inventory.yaml")


def save_inventory(inv: Inventory, path: Path = INVENTORY_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(inv.model_dump(mode="json"), sort_keys=False))


def load_inventory(path: Path = INVENTORY_FILE) -> Inventory | None:
    if not path.exists():
        return None
    return Inventory.model_validate(yaml.safe_load(path.read_text()))
