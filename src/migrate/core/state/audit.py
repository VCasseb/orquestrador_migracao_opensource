"""Action log — append-only JSONL, indexed by FQN.

Every state-changing operation (scan, convert, validate, approve, deploy, rollback)
writes one line. The log is the source of truth for *what happened*; artifacts on disk
are the source of truth for *current state*.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

RUNS_DIR = Path(".migrate/runs")
GLOBAL_LOG = RUNS_DIR / "_global.jsonl"


def _identify_user() -> str:
    if u := os.environ.get("MIGRATE_USER"):
        return u
    try:
        out = subprocess.run(
            ["git", "config", "user.email"],
            capture_output=True, text=True, timeout=2, check=False,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return os.environ.get("USER", "anonymous")


def _per_fqn_log(fqn: str) -> Path:
    safe = fqn.replace(".", "_").replace("/", "_")
    return RUNS_DIR / f"{safe}.jsonl"


def _hash_str(s: str | None) -> str:
    if s is None:
        return ""
    return "sha256:" + hashlib.sha256(s.encode()).hexdigest()[:16]


def log_action(
    action: str,
    fqn: str | None = None,
    payload: dict[str, Any] | None = None,
    result: str = "ok",
    duration_ms: int | None = None,
) -> dict[str, Any]:
    """Append one structured action to the audit log."""
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "ts": datetime.now(timezone.utc).isoformat(),
        "user": _identify_user(),
        "action": action,
        "fqn": fqn,
        "result": result,
        "duration_ms": duration_ms,
        "payload": payload or {},
    }
    line = json.dumps(entry, default=str) + "\n"
    with GLOBAL_LOG.open("a") as f:
        f.write(line)
    if fqn:
        with _per_fqn_log(fqn).open("a") as f:
            f.write(line)
    return entry


@contextmanager
def timed(action: str, fqn: str | None = None, payload: dict | None = None) -> Iterator[dict]:
    """Context manager: logs once on exit with duration. Yields a mutable dict you can
    update with output info before exit."""
    start = time.monotonic()
    out: dict[str, Any] = dict(payload or {})
    error: Exception | None = None
    try:
        yield out
    except Exception as e:
        error = e
        raise
    finally:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        log_action(
            action=action,
            fqn=fqn,
            payload=out,
            result="error" if error else "ok",
            duration_ms=elapsed_ms,
        )


def read_log(fqn: str | None = None, limit: int | None = None) -> list[dict]:
    path = _per_fqn_log(fqn) if fqn else GLOBAL_LOG
    if not path.exists():
        return []
    lines = path.read_text().splitlines()
    if limit:
        lines = lines[-limit:]
    return [json.loads(line) for line in lines if line.strip()]


def hash_payload(s: str | None) -> str:
    return _hash_str(s)


def current_user() -> str:
    return _identify_user()
