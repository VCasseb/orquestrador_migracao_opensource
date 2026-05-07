"""Approval lifecycle for migration artifacts.

Stages: conversion, validation, deployment.
States: pending → approved | rejected → (revoked).
State is computed from the audit log (latest event wins) so rollback is just
appending a revoke event.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from migrate.core.state.audit import log_action, read_log

Stage = Literal["conversion", "validation", "deployment"]
Status = Literal["pending", "approved", "rejected", "revoked"]


class ApprovalState(BaseModel):
    stage: Stage
    status: Status
    by: str | None = None
    at: datetime | None = None
    comment: str = ""


_APPROVE_ACTIONS = {
    "conversion": "approve_conversion",
    "validation": "approve_validation",
    "deployment": "approve_deployment",
}
_REJECT_ACTIONS = {
    "conversion": "reject_conversion",
    "validation": "reject_validation",
    "deployment": "reject_deployment",
}
_REVOKE_ACTIONS = {
    "conversion": "revoke_conversion",
    "validation": "revoke_validation",
    "deployment": "revoke_deployment",
}


def approve(fqn: str, stage: Stage, comment: str = "") -> dict:
    return log_action(_APPROVE_ACTIONS[stage], fqn=fqn, payload={"comment": comment})


def reject(fqn: str, stage: Stage, comment: str = "") -> dict:
    return log_action(_REJECT_ACTIONS[stage], fqn=fqn, payload={"comment": comment})


def revoke(fqn: str, stage: Stage, comment: str = "") -> dict:
    return log_action(_REVOKE_ACTIONS[stage], fqn=fqn, payload={"comment": comment})


def get_state(fqn: str, stage: Stage) -> ApprovalState:
    actions = {_APPROVE_ACTIONS[stage], _REJECT_ACTIONS[stage], _REVOKE_ACTIONS[stage]}
    log = [e for e in read_log(fqn) if e.get("action") in actions]
    if not log:
        return ApprovalState(stage=stage, status="pending")
    latest = log[-1]
    a = latest["action"]
    if a == _APPROVE_ACTIONS[stage]:
        st: Status = "approved"
    elif a == _REJECT_ACTIONS[stage]:
        st = "rejected"
    else:
        st = "revoked"
    return ApprovalState(
        stage=stage,
        status=st,
        by=latest.get("user"),
        at=datetime.fromisoformat(latest["ts"].replace("Z", "+00:00")) if latest.get("ts") else None,
        comment=latest.get("payload", {}).get("comment", ""),
    )


def get_all_states(fqn: str) -> dict[Stage, ApprovalState]:
    return {
        "conversion": get_state(fqn, "conversion"),
        "validation": get_state(fqn, "validation"),
        "deployment": get_state(fqn, "deployment"),
    }


def is_approved(fqn: str, stage: Stage) -> bool:
    return get_state(fqn, stage).status == "approved"
