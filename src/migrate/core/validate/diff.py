from __future__ import annotations

from datetime import datetime, timezone

from migrate.core.validate.report import (
    ColumnDiff,
    ColumnMetric,
    TableMetrics,
    ValidationReport,
    Verdict,
)


def _pct_change(old: float | int | None, new: float | int | None) -> float:
    if old is None or new is None:
        return 0.0
    if old == 0:
        return 0.0 if new == 0 else 100.0
    return ((new - old) / abs(old)) * 100.0


def _verdict_from(diffs: list[float], thresholds: dict[str, float]) -> Verdict:
    fail = thresholds.get("fail_pct", 10.0)
    warn = thresholds.get("warn_pct", 5.0)
    worst = max((abs(d) for d in diffs), default=0.0)
    if worst >= fail:
        return "fail"
    if worst >= warn:
        return "warn"
    return "pass"


def compare(
    source: TableMetrics,
    target: TableMetrics,
    fqn_source: str | None = None,
    fqn_target: str | None = None,
    thresholds: dict[str, float] | None = None,
) -> ValidationReport:
    thresh = thresholds or {"warn_pct": 5.0, "fail_pct": 10.0}

    row_diff_pct = _pct_change(source.row_count, target.row_count)

    src_cols: dict[str, ColumnMetric] = {c.name: c for c in source.columns}
    tgt_cols: dict[str, ColumnMetric] = {c.name: c for c in target.columns}
    all_names = sorted(set(src_cols) | set(tgt_cols))

    col_diffs: list[ColumnDiff] = []
    for name in all_names:
        sc = src_cols.get(name)
        tc = tgt_cols.get(name)
        notes: list[str] = []
        if not sc:
            notes.append("Missing in source")
            col_diffs.append(ColumnDiff(name=name, verdict="fail", notes=notes))
            continue
        if not tc:
            notes.append("Missing in target")
            col_diffs.append(ColumnDiff(name=name, verdict="fail", notes=notes))
            continue

        null_diff = _pct_change(
            sc.null_count / max(source.row_count, 1) * 100.0,
            tc.null_count / max(target.row_count, 1) * 100.0,
        )
        distinct_diff = _pct_change(sc.distinct_count, tc.distinct_count)
        avg_diff = _pct_change(sc.avg, tc.avg) if sc.avg is not None and tc.avg is not None else None

        diffs = [null_diff, distinct_diff]
        if avg_diff is not None:
            diffs.append(avg_diff)

        col_diffs.append(ColumnDiff(
            name=name,
            null_count_diff_pct=null_diff,
            distinct_count_diff_pct=distinct_diff,
            avg_diff_pct=avg_diff,
            verdict=_verdict_from(diffs, thresh),
            notes=notes,
        ))

    overall_diffs = [row_diff_pct] + [d.null_count_diff_pct for d in col_diffs] + \
                    [d.distinct_count_diff_pct for d in col_diffs]
    verdict = _verdict_from(overall_diffs, thresh)
    if any(d.verdict == "fail" for d in col_diffs):
        verdict = "fail"

    return ValidationReport(
        fqn_source=fqn_source or source.fqn,
        fqn_target=fqn_target or target.fqn,
        created_at=datetime.now(timezone.utc),
        source=source,
        target=target,
        row_count_diff_pct=row_diff_pct,
        columns_diff=col_diffs,
        verdict=verdict,
    )
