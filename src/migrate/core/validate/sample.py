"""Synthetic validation report for demo when real warehouses aren't configured."""
from __future__ import annotations

from datetime import datetime, timezone

from migrate.core.inventory.catalog import load_inventory
from migrate.core.state.audit import timed
from migrate.core.validate.report import (
    ColumnDiff,
    ColumnMetric,
    TableMetrics,
    ValidationReport,
)


def _mk_metric(name: str, col_type: str, null_count: int, distinct: int, avg: float | None = None) -> ColumnMetric:
    return ColumnMetric(name=name, type=col_type, null_count=null_count, distinct_count=distinct, avg=avg)


def build_synthetic_report(source_fqn: str, target_fqn: str | None = None) -> ValidationReport:
    with timed("validate_synthetic", fqn=source_fqn) as ctx:
        report = _synth_inner(source_fqn, target_fqn)
        ctx.update({"verdict": report.verdict, "is_synthetic": True})
        return report


def _synth_inner(source_fqn: str, target_fqn: str | None) -> ValidationReport:
    inv = load_inventory()
    if not inv:
        raise RuntimeError("Load sample inventory first.")
    table = inv.by_fqn.get(source_fqn)
    if not table:
        raise RuntimeError(f"Not found: {source_fqn}")

    target = target_fqn or f"migrated_from_gcp.{table.dataset}.{table.name}"
    base_rows = table.row_count or 1_000_000

    src_cols: list[ColumnMetric] = []
    tgt_cols: list[ColumnMetric] = []
    col_diffs: list[ColumnDiff] = []

    drift_targets = {"customer_id", "user_id", "amount"}

    for i, c in enumerate(table.columns):
        is_numeric = c.type.upper() in ("INT64", "INTEGER", "NUMERIC", "FLOAT64", "FLOAT")
        nulls_s = int(base_rows * 0.001 * (i % 7 + 1))
        distinct_s = max(1, base_rows // (10 ** (i % 4 + 1)))
        avg_s = 1234.5 if is_numeric else None

        if c.name in drift_targets:
            nulls_t = int(nulls_s * 1.18)
            distinct_t = int(distinct_s * 0.99)
            avg_t = (avg_s * 1.012) if avg_s is not None else None
        else:
            nulls_t = nulls_s
            distinct_t = distinct_s
            avg_t = avg_s

        src_cols.append(_mk_metric(c.name, c.type, nulls_s, distinct_s, avg_s))
        tgt_cols.append(_mk_metric(c.name, c.type, nulls_t, distinct_t, avg_t))

        null_diff = ((nulls_t - nulls_s) / max(nulls_s, 1)) * 100.0
        distinct_diff = ((distinct_t - distinct_s) / max(distinct_s, 1)) * 100.0
        avg_diff = (((avg_t - avg_s) / abs(avg_s)) * 100.0) if avg_s and avg_t else None
        worst = max(abs(null_diff), abs(distinct_diff), abs(avg_diff or 0))
        verdict = "fail" if worst > 10 else ("warn" if worst > 5 else "pass")
        col_diffs.append(ColumnDiff(
            name=c.name,
            null_count_diff_pct=null_diff,
            distinct_count_diff_pct=distinct_diff,
            avg_diff_pct=avg_diff,
            verdict=verdict,
        ))

    src_metrics = TableMetrics(fqn=source_fqn, row_count=base_rows, columns=src_cols)
    tgt_metrics = TableMetrics(fqn=target, row_count=base_rows, columns=tgt_cols)

    overall_verdict = "fail" if any(d.verdict == "fail" for d in col_diffs) else \
                      ("warn" if any(d.verdict == "warn" for d in col_diffs) else "pass")

    return ValidationReport(
        fqn_source=source_fqn,
        fqn_target=target,
        created_at=datetime.now(timezone.utc),
        source=src_metrics,
        target=tgt_metrics,
        row_count_diff_pct=0.0,
        columns_diff=col_diffs,
        verdict=overall_verdict,
        is_synthetic=True,
        diagnosis=(
            "Synthetic example. NULL drift detected in `customer_id` and `user_id` (likely "
            "join-based enrichment dropping rows in target), and avg drift in `amount` (likely "
            "TIMESTAMP→TIMESTAMP_NTZ timezone shift affecting filter window)."
        ) if any(d.verdict == "fail" for d in col_diffs) else "All metrics within tolerance."
    )
